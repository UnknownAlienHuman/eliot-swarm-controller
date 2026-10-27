use eliot_core::{Error, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::HashMap,
    ffi::OsString,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io,
    process::{Child, ChildStdin, Command},
    sync::{Semaphore, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use crate::native::types::NativeJsonRpcErrorBody;

const MAX_NATIVE_FRAME_BYTES: usize = 1024 * 1024;
const MAX_PENDING_CALLS: usize = 16;
const NATIVE_EVENT_QUEUE: usize = 64;

type PendingCalls = Arc<StdMutex<HashMap<String, oneshot::Sender<Result<Value>>>>>;
type NativeWriter = Arc<tokio::sync::Mutex<FramedWrite<ChildStdin, LinesCodec>>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamState {
    Open,
    Overflow,
    Closed,
    Malformed,
}

pub(crate) enum NativeFrame {
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

pub(crate) struct NativeLaunch {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
}

#[derive(Clone)]
pub(crate) struct NativeClient {
    writer: NativeWriter,
    pending: PendingCalls,
    pending_slots: Arc<Semaphore>,
    next_id: Arc<AtomicU64>,
}

pub(crate) struct NativeProcess {
    child: Child,
    client: NativeClient,
    frames: Option<mpsc::Receiver<NativeFrame>>,
    stream: watch::Receiver<StreamState>,
    output_reader: Option<tokio::task::JoinHandle<Result<()>>>,
    output_reader_result: Option<Result<()>>,
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

impl NativeProcess {
    pub async fn spawn(launch: NativeLaunch) -> Result<Self> {
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
                format!("could not start the configured Codex app-server: {error}"),
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::new("native_spawn", "Codex app-server stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::new("native_spawn", "Codex app-server stdout was not piped"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::new("native_spawn", "Codex app-server stderr was not piped"))?;
        tokio::spawn(async move {
            let _ = io::copy(&mut stderr, &mut io::sink()).await;
        });

        let writer = Arc::new(tokio::sync::Mutex::new(FramedWrite::new(
            stdin,
            LinesCodec::new(),
        )));
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let client = NativeClient {
            writer,
            pending: Arc::clone(&pending),
            pending_slots: Arc::new(Semaphore::new(MAX_PENDING_CALLS)),
            next_id: Arc::new(AtomicU64::new(0)),
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
        })
    }

    pub fn client(&self) -> NativeClient {
        self.client.clone()
    }

    pub fn take_frames(&mut self) -> Result<mpsc::Receiver<NativeFrame>> {
        self.frames.take().ok_or_else(|| {
            Error::new(
                "bad_state",
                "Codex native event reader already has an owner",
            )
        })
    }

    pub fn stream_state(&self) -> watch::Receiver<StreamState> {
        self.stream.clone()
    }

    /// Wait for the single stdout reader to finish; a timeout leaves that same
    /// reader running so a later wait can observe its completion without reuse.
    pub async fn await_output_drained(&mut self, deadline: Instant) -> Result<()> {
        if let Some(result) = &self.output_reader_result {
            return result.clone();
        }
        let reader_result = {
            let reader = self
                .output_reader
                .as_mut()
                .ok_or_else(|| Error::new("unknown", "Codex stdout reader is unavailable"))?;
            if reader.is_finished() {
                (&mut *reader).await
            } else {
                match timeout_at(deadline, reader).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(Error::new(
                            "unknown",
                            "Codex stdout reader did not drain before its deadline",
                        ));
                    }
                }
            }
        };
        let result = match reader_result {
            Ok(result) => result,
            Err(error) => Err(Error::new(
                "unknown",
                format!("Codex stdout reader task failed: {error}"),
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
                format!("Codex process readback failed: {error}"),
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
                    "Codex process termination outcome is unknown",
                ));
            }
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if let Some(status) = self.child.try_wait().map_err(|readback| {
                    Error::new(
                        "unknown",
                        format!(
                            "Codex process exit readback failed after termination error: {readback}"
                        ),
                    )
                })? {
                    self.await_output_drained(deadline).await?;
                    return Ok(status);
                }
                return Err(Error::new(
                    "native_process",
                    format!("Codex process termination failed: {error}"),
                ));
            }
        }
        let status = timeout_at(deadline, self.child.wait())
            .await
            .map_err(|_| Error::new("unknown", "Codex process exit could not be confirmed"))?
            .map_err(|error| {
                Error::new(
                    "native_process",
                    format!("Codex process wait failed: {error}"),
                )
            })?;
        self.await_output_drained(deadline).await?;
        Ok(status)
    }
}

impl NativeClient {
    pub async fn call(&self, method: &str, params: Value, deadline: Instant) -> Result<Value> {
        if method.trim().is_empty() || !params.is_object() {
            return Err(Error::new(
                "invalid",
                "Codex app-server call requires a method and object params",
            ));
        }

        let sequence = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| Error::new("internal", "Codex request id space exhausted"))?;
        let id = format!("codex-{sequence}");
        let encoded = encode_native_frame(&OutboundRequest {
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
                    "Codex request capacity deadline elapsed before dispatch",
                )
            })?
            .map_err(|_| {
                Error::new(
                    "native_not_dispatched",
                    "Codex response router closed before dispatch",
                )
            })?;

        let (response_tx, response_rx) = oneshot::channel();
        {
            self.pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id.clone(), response_tx);
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
                format!("Codex {method} dispatch outcome is unknown"),
            ));
        }
        timeout_at(deadline, response_rx)
            .await
            .map_err(|_| Error::new("unknown", format!("Codex {method} response is unknown")))?
            .map_err(|_| Error::new("unknown", format!("Codex {method} response channel closed")))?
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
                "Codex app-server notification has invalid method or params",
            ));
        }
        let encoded = encode_native_frame(&OutboundNotification {
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
                    "Codex input close deadline elapsed before dispatch",
                ));
            }
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                "Codex input close deadline elapsed before dispatch",
            ));
        }
        match timeout_at(deadline, SinkExt::<String>::close(&mut *writer)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                "Codex app-server input close outcome is unknown",
            )),
        }
    }

    pub async fn reply_server_request(
        &self,
        id: Value,
        result: Value,
        deadline: Instant,
    ) -> Result<()> {
        let encoded = encode_native_frame(&OutboundResultResponse {
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
        let encoded = encode_native_frame(&OutboundResponse {
            jsonrpc: "2.0",
            id,
            error: NativeRpcError { code, message },
        })?;
        self.write_encoded(encoded, deadline).await
    }

    async fn write_encoded(&self, encoded: String, deadline: Instant) -> Result<()> {
        let mut writer = match timeout_at(deadline, self.writer.lock()).await {
            Ok(writer) => writer,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    "Codex write deadline elapsed before dispatch",
                ));
            }
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                "Codex write deadline elapsed before dispatch",
            ));
        }
        match timeout_at(deadline, writer.send(encoded)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                "Codex app-server write outcome is unknown",
            )),
        }
    }

    fn route_response(&self, id: &Value, result: Result<Value>) {
        let key = id
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| serde_json::to_string(id).ok());
        let Some(key) = key else { return };
        let sender = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
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

async fn read_native_stream(
    stdout: tokio::process::ChildStdout,
    frames: mpsc::Sender<NativeFrame>,
    stream: watch::Sender<StreamState>,
    client: NativeClient,
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
                    "Codex app-server stream ended on an invalid or oversized frame",
                ));
            }
        };
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                return Err(mark_stream_malformed(
                    &stream,
                    &client,
                    "Codex app-server stream contained invalid JSON",
                ));
            }
        };
        if !valid_jsonrpc_version(&value) {
            return Err(mark_stream_malformed(
                &stream,
                &client,
                "Codex app-server stream contained an invalid JSON-RPC version",
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
                            "Codex server request did not match its JSON-RPC envelope",
                        ));
                    }
                };
                if request.id != id {
                    return Err(mark_stream_malformed(
                        &stream,
                        &client,
                        "Codex server request id changed while decoding",
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
                            "Codex server response did not match its JSON-RPC envelope",
                        ));
                    }
                };
                if response.id != id {
                    return Err(mark_stream_malformed(
                        &stream,
                        &client,
                        "Codex server response id changed while decoding",
                    ));
                }
                let result = match (response.result, response.error) {
                    (Some(result), None) => Ok(result),
                    (None, Some(error)) => Err(Error::new("native_rejected", error.message)),
                    _ => Err(Error::new(
                        "native_protocol",
                        "Codex response must contain exactly one result or error",
                    )),
                };
                client.route_response(&response.id, result);
            } else {
                return Err(mark_stream_malformed(
                    &stream,
                    &client,
                    "Codex app-server response omitted both result and error",
                ));
            }
        } else {
            if value.get("method").is_none() {
                return Err(mark_stream_malformed(
                    &stream,
                    &client,
                    "Codex app-server frame was not a notification or response",
                ));
            }
            let notification: NativeServerNotification = match serde_json::from_value(value) {
                Ok(notification) => notification,
                Err(_) => {
                    return Err(mark_stream_malformed(
                        &stream,
                        &client,
                        "Codex notification did not match its JSON-RPC envelope",
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
            "Codex app-server event queue overflowed before stdout reached EOF",
        );
        client.fail_pending(error.clone());
        return Err(error);
    }

    stream.send_replace(StreamState::Closed);
    client.fail_pending(Error::new(
        "unknown",
        "Codex app-server stream closed while an outcome may be pending",
    ));
    Ok(())
}

fn mark_stream_malformed(
    stream: &watch::Sender<StreamState>,
    client: &NativeClient,
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

fn encode_native_frame(message: &impl Serialize) -> Result<String> {
    let encoded = serde_json::to_string(message).map_err(|_| {
        Error::new(
            "native_encode",
            "Codex app-server frame could not be encoded",
        )
    })?;
    if encoded.len().saturating_add(1) > MAX_NATIVE_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "Codex app-server frame exceeds 1 MiB",
        ));
    }
    Ok(encoded)
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}
