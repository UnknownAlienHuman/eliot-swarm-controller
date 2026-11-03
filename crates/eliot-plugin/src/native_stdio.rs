//! Shared stdio NDJSON JSON-RPC client for native plugin hosts.
//!
//! Moved verbatim from the Codex plugin's native client (the Muse plugin carried
//! a line-for-line copy with different nouns); the two hosts differ only in the
//! [`NativeStdioConfig`] labels and in how they map a native response error to
//! an outcome. Behaviour is unchanged for both callers.

use std::{
    collections::HashMap,
    ffi::OsString,
    marker::PhantomData,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::{
    process::{Child, ChildStdin, Command},
    sync::{Semaphore, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot, StderrSource};

use crate::{StderrCapture, StderrView, spawn_stderr, sync::LockExt};

const MAX_NATIVE_FRAME_BYTES: usize = 1024 * 1024;
const MAX_PENDING_CALLS: usize = 16;
const NATIVE_EVENT_QUEUE: usize = 64;

type PendingCalls = Arc<StdMutex<HashMap<String, oneshot::Sender<Result<Value>>>>>;
type NativeWriter = Arc<tokio::sync::Mutex<FramedWrite<ChildStdin, LinesCodec>>>;

/// Per-host parameterization: display nouns plus the response-error mapping.
///
/// Both native hosts implement this; the transport below is shared.
pub trait NativeStdioConfig: Clone + Send + Sync + 'static {
    /// Full server noun in diagnostics, e.g. `"Codex app-server"`.
    const SERVER: &'static str;
    /// Short host noun in diagnostics, e.g. `"Codex"`.
    const SHORT: &'static str;
    /// Request id prefix, e.g. `"codex"`.
    const ID_PREFIX: &'static str;
    /// Map a native JSON-RPC response error to a call outcome.
    fn map_response_error(error: NativeJsonRpcErrorBody) -> Result<Value>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamState {
    Open,
    Overflow,
    Closed,
    Malformed,
}

pub enum NativeFrame {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
}

pub struct NativeLaunch {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
}

/// Minimal JSON-RPC error body decoded from native responses.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
pub struct NativeJsonRpcErrorBody {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Clone)]
pub struct StdioClient<C> {
    writer: NativeWriter,
    pending: PendingCalls,
    pending_slots: Arc<Semaphore>,
    next_id: Arc<AtomicU64>,
    stderr: StderrView,
    policy: SecretPolicy,
    config: PhantomData<C>,
}

pub struct StdioProcess<C> {
    child: Child,
    client: StdioClient<C>,
    frames: Option<mpsc::Receiver<NativeFrame>>,
    stream: watch::Receiver<StreamState>,
    output_reader: Option<tokio::task::JoinHandle<Result<()>>>,
    output_reader_result: Option<Result<()>>,
    /// Present until [`StdioProcess::await_output_drained`] finishes stderr.
    pub stderr: Option<StderrCapture>,
}

#[derive(Serialize)]
struct OutboundRequest<'a> {
    jsonrpc: &'static str,
    id: String,
    method: &'a str,
    params: Value,
}

#[derive(Serialize)]
struct OutboundNotification<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Serialize)]
struct OutboundResponse {
    jsonrpc: &'static str,
    id: Value,
    error: NativeRpcError,
}

#[derive(Serialize)]
struct OutboundResultResponse {
    jsonrpc: &'static str,
    id: Value,
    result: Value,
}

#[derive(Serialize)]
struct NativeRpcError {
    code: i64,
    message: &'static str,
}

#[derive(serde::Deserialize)]
struct NativeServerRequest {
    id: Value,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(serde::Deserialize)]
struct NativeServerResponse {
    id: Value,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<NativeJsonRpcErrorBody>,
}

#[derive(serde::Deserialize)]
struct NativeServerNotification {
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

struct PendingGuard {
    pending: PendingCalls,
    id: String,
}

impl<C: NativeStdioConfig> StdioProcess<C> {
    pub async fn spawn(launch: NativeLaunch, policy: SecretPolicy) -> Result<Self> {
        let mut command = Command::new(&launch.program);
        command
            .args(&launch.args)
            .current_dir(&launch.cwd)
            .envs(launch.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|error| {
            Error::new(
                "native_spawn",
                policy.redact_text(&format!(
                    "could not start the configured {}: {error}",
                    C::SERVER
                )),
            )
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            Error::new("native_spawn", format!("{} stdin was not piped", C::SERVER))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::new(
                "native_spawn",
                format!("{} stdout was not piped", C::SERVER),
            )
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            Error::new(
                "native_spawn",
                format!("{} stderr was not piped", C::SERVER),
            )
        })?;
        let stderr = spawn_stderr(stderr, Arc::new(policy.clone()));

        let writer = Arc::new(tokio::sync::Mutex::new(FramedWrite::new(
            stdin,
            LinesCodec::new(),
        )));
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let client = StdioClient {
            writer,
            pending: Arc::clone(&pending),
            pending_slots: Arc::new(Semaphore::new(MAX_PENDING_CALLS)),
            next_id: Arc::new(AtomicU64::new(0)),
            stderr: stderr.view(),
            policy,
            config: PhantomData,
        };
        let (frame_tx, frame_rx) = mpsc::channel(NATIVE_EVENT_QUEUE);
        let (stream_tx, stream_rx) = watch::channel(StreamState::Open);
        let reader_client = client.clone();
        let output_reader = tokio::spawn(async move {
            read_native_stream(stdout, frame_tx, stream_tx, reader_client).await
        });

        Ok(Self {
            child,
            client,
            frames: Some(frame_rx),
            stream: stream_rx,
            output_reader: Some(output_reader),
            output_reader_result: None,
            stderr: Some(stderr),
        })
    }

    /// The actual app-server child PID; birth and custody are verified by the host.
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn client(&self) -> StdioClient<C> {
        self.client.clone()
    }

    pub fn take_frames(&mut self) -> Result<mpsc::Receiver<NativeFrame>> {
        self.frames.take().ok_or_else(|| {
            Error::new(
                "bad_state",
                format!("{} native event reader already has an owner", C::SHORT),
            )
        })
    }

    pub fn stream_state(&self) -> watch::Receiver<StreamState> {
        self.stream.clone()
    }

    /// Wait for the single stdout reader to finish; a timeout leaves that same
    /// reader running so a later wait can observe its completion without reuse.
    pub async fn await_output_drained(&mut self, deadline: Instant) -> Result<()> {
        let result = self.await_stdout_drained(deadline).await;
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.finish(deadline).await;
        }
        result.map_err(|error| self.client.diagnostic_error(error))
    }

    async fn await_stdout_drained(&mut self, deadline: Instant) -> Result<()> {
        if let Some(result) = &self.output_reader_result {
            return result.clone();
        }
        let reader_result = {
            let reader = self.output_reader.as_mut().ok_or_else(|| {
                Error::new(
                    "unknown",
                    format!("{} stdout reader is unavailable", C::SHORT),
                )
            })?;
            if reader.is_finished() {
                (&mut *reader).await
            } else {
                match timeout_at(deadline, reader).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(Error::new(
                            "unknown",
                            format!(
                                "{} stdout reader did not drain before its deadline",
                                C::SHORT
                            ),
                        ));
                    }
                }
            }
        };
        let result = match reader_result {
            Ok(result) => result,
            Err(error) => Err(Error::new(
                "unknown",
                format!("{} stdout reader task failed: {error}", C::SHORT),
            )),
        };
        drop(self.output_reader.take());
        self.output_reader_result = Some(result.clone());
        result
    }

    pub async fn terminate(&mut self, force: bool, deadline: Instant) -> Result<ExitStatus> {
        if let Some(status) = self.child.try_wait().map_err(|error| {
            Error::new(
                "native_process",
                format!("{} process readback failed: {error}", C::SHORT),
            )
        })? {
            self.await_output_drained(deadline).await?;
            return Ok(status);
        }

        if !force {
            let close_deadline = (Instant::now() + Duration::from_millis(500)).min(deadline);
            let _ = self.client.close_input(close_deadline).await;
            let grace_deadline = (Instant::now() + Duration::from_secs(2)).min(deadline);
            if let Ok(Ok(status)) = timeout_at(grace_deadline, self.child.wait()).await {
                self.await_output_drained(deadline).await?;
                return Ok(status);
            }
        }

        match timeout_at(deadline, self.child.kill()).await {
            Err(_) => {
                return Err(Error::new(
                    "unknown",
                    format!("{} process termination outcome is unknown", C::SHORT),
                ));
            }
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if let Some(status) = self.child.try_wait().map_err(|readback| {
                    Error::new(
                        "unknown",
                        format!(
                            "{} process exit readback failed after termination error: {readback}",
                            C::SHORT
                        ),
                    )
                })? {
                    self.await_output_drained(deadline).await?;
                    return Ok(status);
                }
                return Err(Error::new(
                    "native_process",
                    format!("{} process termination failed: {error}", C::SHORT),
                ));
            }
        }
        let status = timeout_at(deadline, self.child.wait())
            .await
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("{} process exit could not be confirmed", C::SHORT),
                )
            })?
            .map_err(|error| {
                Error::new(
                    "native_process",
                    format!("{} process wait failed: {error}", C::SHORT),
                )
            })?;
        self.await_output_drained(deadline).await?;
        Ok(status)
    }
}

impl<C: NativeStdioConfig> StdioClient<C> {
    pub async fn call(&self, method: &str, params: Value, deadline: Instant) -> Result<Value> {
        self.call_inner(method, params, deadline)
            .await
            .map_err(|error| self.diagnostic_error(error))
    }

    pub fn stderr_snapshot(&self) -> StderrSnapshot {
        self.stderr.snapshot()
    }

    pub fn diagnostic_error(&self, mut error: Error) -> Error {
        error.message = self.policy.redact_text(&error.message);
        error.with_stderr(StderrSource::Harness, self.stderr_snapshot())
    }

    async fn call_inner(&self, method: &str, params: Value, deadline: Instant) -> Result<Value> {
        if method.trim().is_empty() || !params.is_object() {
            return Err(Error::new(
                "invalid",
                format!("{} call requires a method and object params", C::SERVER),
            ));
        }

        let sequence = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| {
                Error::new(
                    "internal",
                    format!("{} request id space exhausted", C::SHORT),
                )
            })?;
        let id = format!("{}-{sequence}", C::ID_PREFIX);
        let encoded = encode_native_frame::<C>(&OutboundRequest {
            jsonrpc: "2.0",
            id: id.clone(),
            method,
            params,
        })?;
        let _permit = timeout_at(deadline, Arc::clone(&self.pending_slots).acquire_owned())
            .await
            .map_err(|_| {
                Error::new(
                    "native_not_dispatched",
                    format!(
                        "{} request capacity deadline elapsed before dispatch",
                        C::SHORT
                    ),
                )
            })?
            .map_err(|_| {
                Error::new(
                    "native_not_dispatched",
                    format!("{} response router closed before dispatch", C::SHORT),
                )
            })?;

        let (response_tx, response_rx) = oneshot::channel();
        {
            self.pending.lock_recover().insert(id.clone(), response_tx);
        }
        let _guard = PendingGuard {
            pending: Arc::clone(&self.pending),
            id,
        };

        if let Err(error) = self.write_encoded(encoded, deadline).await {
            if error.code == "native_not_dispatched" {
                return Err(error);
            }
            return Err(Error::new(
                "unknown",
                format!("{} {method} dispatch outcome is unknown", C::SHORT),
            ));
        }
        timeout_at(deadline, response_rx)
            .await
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("{} {method} response is unknown", C::SHORT),
                )
            })?
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("{} {method} response channel closed", C::SHORT),
                )
            })?
    }

    pub async fn notify(
        &self,
        method: &str,
        params: Option<Value>,
        deadline: Instant,
    ) -> Result<()> {
        if method.trim().is_empty() || params.as_ref().is_some_and(|value| !value.is_object()) {
            return Err(Error::new(
                "invalid",
                format!("{} notification has invalid method or params", C::SERVER),
            ));
        }
        let encoded = encode_native_frame::<C>(&OutboundNotification {
            jsonrpc: "2.0",
            method,
            params,
        })?;
        self.write_encoded(encoded, deadline).await
    }

    pub async fn close_input(&self, deadline: Instant) -> Result<()> {
        let mut writer = match timeout_at(deadline, self.writer.lock()).await {
            Ok(writer) => writer,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    format!("{} input close deadline elapsed before dispatch", C::SHORT),
                ));
            }
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                format!("{} input close deadline elapsed before dispatch", C::SHORT),
            ));
        }
        match timeout_at(deadline, SinkExt::<String>::close(&mut *writer)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                format!("{} input close outcome is unknown", C::SERVER),
            )),
        }
    }

    pub async fn reply_server_request(
        &self,
        id: Value,
        result: Value,
        deadline: Instant,
    ) -> Result<()> {
        let encoded = encode_native_frame::<C>(&OutboundResultResponse {
            jsonrpc: "2.0",
            id,
            result,
        })?;
        self.write_encoded(encoded, deadline).await
    }

    pub async fn reject_unsupported_request(
        &self,
        id: Value,
        method: &str,
        params: &Value,
        deadline: Instant,
    ) -> Result<()> {
        let (code, message) = if method.trim().is_empty() || !params.is_object() {
            (-32600, "unsupported native request envelope")
        } else {
            (-32601, "unsupported")
        };
        let encoded = encode_native_frame::<C>(&OutboundResponse {
            jsonrpc: "2.0",
            id,
            error: NativeRpcError { code, message },
        })?;
        self.write_encoded(encoded, deadline).await
    }

    async fn write_encoded(&self, encoded: String, deadline: Instant) -> Result<()> {
        self.write_encoded_inner(encoded, deadline)
            .await
            .map_err(|error| self.diagnostic_error(error))
    }

    async fn write_encoded_inner(&self, encoded: String, deadline: Instant) -> Result<()> {
        let mut writer = match timeout_at(deadline, self.writer.lock()).await {
            Ok(writer) => writer,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    format!("{} write deadline elapsed before dispatch", C::SHORT),
                ));
            }
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                format!("{} write deadline elapsed before dispatch", C::SHORT),
            ));
        }
        match timeout_at(deadline, writer.send(encoded)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                format!("{} write outcome is unknown", C::SERVER),
            )),
        }
    }

    fn route_response(&self, id: &Value, result: Result<Value>) {
        let key = id
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| serde_json::to_string(id).ok());
        let Some(key) = key else { return };
        let sender = self.pending.lock_recover().remove(&key);
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
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

async fn read_native_stream<C: NativeStdioConfig>(
    stdout: tokio::process::ChildStdout,
    frames: mpsc::Sender<NativeFrame>,
    stream: watch::Sender<StreamState>,
    client: StdioClient<C>,
) -> Result<()> {
    let mut reader = FramedRead::new(
        stdout,
        LinesCodec::new_with_max_length(MAX_NATIVE_FRAME_BYTES.saturating_sub(1)),
    );
    let mut overflowed = false;
    while let Some(line) = reader.next().await {
        let line = match line {
            Ok(line) if line.len().saturating_add(1) <= MAX_NATIVE_FRAME_BYTES => line,
            _ => {
                return Err(mark_stream_malformed(
                    &stream,
                    &client,
                    &format!(
                        "{} stream ended on an invalid or oversized frame",
                        C::SERVER
                    ),
                ));
            }
        };
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                return Err(mark_stream_malformed(
                    &stream,
                    &client,
                    &format!("{} stream contained invalid JSON", C::SERVER),
                ));
            }
        };
        if !valid_jsonrpc_version(&value) {
            return Err(mark_stream_malformed(
                &stream,
                &client,
                &format!("{} stream contained an invalid JSON-RPC version", C::SERVER),
            ));
        }

        if let Some(id) = value.get("id").cloned() {
            if value.get("method").is_some() {
                let request: NativeServerRequest = match serde_json::from_value(value) {
                    Ok(request) => request,
                    Err(_) => {
                        return Err(mark_stream_malformed(
                            &stream,
                            &client,
                            &format!(
                                "{} server request did not match its JSON-RPC envelope",
                                C::SHORT
                            ),
                        ));
                    }
                };
                if request.id != id {
                    return Err(mark_stream_malformed(
                        &stream,
                        &client,
                        &format!("{} server request id changed while decoding", C::SHORT),
                    ));
                }
                send_native_frame(
                    &frames,
                    NativeFrame::Request {
                        id: request.id,
                        method: request.method,
                        params: request.params.unwrap_or_else(empty_object),
                    },
                    &stream,
                    &mut overflowed,
                );
            } else if value.get("result").is_some() || value.get("error").is_some() {
                let response: NativeServerResponse = match serde_json::from_value(value) {
                    Ok(response) => response,
                    Err(_) => {
                        return Err(mark_stream_malformed(
                            &stream,
                            &client,
                            &format!(
                                "{} server response did not match its JSON-RPC envelope",
                                C::SHORT
                            ),
                        ));
                    }
                };
                if response.id != id {
                    return Err(mark_stream_malformed(
                        &stream,
                        &client,
                        &format!("{} server response id changed while decoding", C::SHORT),
                    ));
                }
                let result = match (response.result, response.error) {
                    (Some(result), None) => Ok(result),
                    (None, Some(error)) => C::map_response_error(error),
                    _ => Err(Error::new(
                        "native_protocol",
                        format!(
                            "{} response must contain exactly one result or error",
                            C::SHORT
                        ),
                    )),
                };
                client.route_response(&response.id, result);
            } else {
                return Err(mark_stream_malformed(
                    &stream,
                    &client,
                    &format!("{} response omitted both result and error", C::SERVER),
                ));
            }
        } else {
            if value.get("method").is_none() {
                return Err(mark_stream_malformed(
                    &stream,
                    &client,
                    &format!("{} frame was not a notification or response", C::SERVER),
                ));
            }
            let notification: NativeServerNotification = match serde_json::from_value(value) {
                Ok(notification) => notification,
                Err(_) => {
                    return Err(mark_stream_malformed(
                        &stream,
                        &client,
                        &format!(
                            "{} notification did not match its JSON-RPC envelope",
                            C::SHORT
                        ),
                    ));
                }
            };
            send_native_frame(
                &frames,
                NativeFrame::Notification {
                    method: notification.method,
                    params: notification.params.unwrap_or_else(empty_object),
                },
                &stream,
                &mut overflowed,
            );
        }
    }

    if overflowed {
        stream.send_replace(StreamState::Overflow);
        let error = Error::new(
            "unknown",
            format!(
                "{} event queue overflowed before stdout reached EOF",
                C::SERVER
            ),
        );
        client.fail_pending(error.clone());
        return Err(error);
    }

    stream.send_replace(StreamState::Closed);
    client.fail_pending(Error::new(
        "unknown",
        format!(
            "{} stream closed while an outcome may be pending",
            C::SERVER
        ),
    ));
    Ok(())
}

fn mark_stream_malformed<C: NativeStdioConfig>(
    stream: &watch::Sender<StreamState>,
    client: &StdioClient<C>,
    message: &str,
) -> Error {
    stream.send_replace(StreamState::Malformed);
    let error = Error::new("unknown", message);
    client.fail_pending(error.clone());
    error
}

fn send_native_frame(
    frames: &mpsc::Sender<NativeFrame>,
    frame: NativeFrame,
    stream: &watch::Sender<StreamState>,
    overflowed: &mut bool,
) {
    if *overflowed {
        return;
    }
    if frames.try_send(frame).is_err() {
        *overflowed = true;
        stream.send_replace(StreamState::Overflow);
    }
}

fn valid_jsonrpc_version(value: &Value) -> bool {
    value
        .get("jsonrpc")
        .is_none_or(|version| version.as_str() == Some("2.0"))
}

fn encode_native_frame<C: NativeStdioConfig>(message: &impl Serialize) -> Result<String> {
    let encoded = serde_json::to_string(message).map_err(|_| {
        Error::new(
            "native_encode",
            format!("{} frame could not be encoded", C::SERVER),
        )
    })?;
    if encoded.len().saturating_add(1) > MAX_NATIVE_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            format!("{} frame exceeds 1 MiB", C::SERVER),
        ));
    }
    Ok(encoded)
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock_recover().remove(&self.id);
    }
}

impl<C> Drop for StdioProcess<C> {
    fn drop(&mut self) {
        if let Some(reader) = &self.output_reader {
            reader.abort();
        }
        // StderrCapture owns and cancels its reader; client views cannot keep it alive.
    }
}
