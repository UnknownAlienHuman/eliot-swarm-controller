//! Claude native transport: NDJSON stream-json over stdio.
//!
//! Unlike the JSON-RPC hosts, `claude -p` speaks plain typed NDJSON records.
//! `control_request` writes correlate with `control_response` records by
//! `request_id`; every other record flows to the event task. Process,
//! stderr and drain semantics mirror the shared
//! [`eliot_plugin::native_stdio`] client: continuous stderr drainage with a
//! redacted 16 KiB tail, a 1 MiB frame bound, and output drained before any
//! loss conclusion.

use std::{
    collections::HashMap,
    ffi::OsString,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    process::{Child, ChildStdin, Command},
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot, StderrSource};
use eliot_plugin::native_stdio::StreamState;
use eliot_plugin::{StderrCapture, StderrView, spawn_stderr, sync::LockExt};

use crate::native::types::{ControlResponse, OutboundControlRequest, OutboundUserInput};

const MAX_NATIVE_FRAME_BYTES: usize = 1024 * 1024;
const NATIVE_EVENT_QUEUE: usize = 64;

type PendingControls = Arc<StdMutex<HashMap<String, oneshot::Sender<Result<Value>>>>>;
type NativeWriter = Arc<tokio::sync::Mutex<FramedWrite<ChildStdin, LinesCodec>>>;

/// One non-response stream record for the event task.
pub(crate) enum ClaudeFrame {
    Record(Value),
}

pub(crate) struct ClaudeLaunch {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
}

#[derive(Clone)]
pub(crate) struct ClaudeClient {
    writer: NativeWriter,
    pending: PendingControls,
    next_id: Arc<AtomicU64>,
    stderr: StderrView,
    policy: SecretPolicy,
}

pub(crate) struct ClaudeProcess {
    child: Child,
    client: ClaudeClient,
    frames: Option<mpsc::Receiver<ClaudeFrame>>,
    stream: watch::Receiver<StreamState>,
    output_reader: Option<tokio::task::JoinHandle<Result<()>>>,
    output_reader_result: Option<Result<()>>,
    /// Present until [`ClaudeProcess::await_output_drained`] finishes stderr.
    pub stderr: Option<StderrCapture>,
}

struct PendingGuard {
    pending: PendingControls,
    id: String,
}

impl ClaudeProcess {
    pub async fn spawn(launch: ClaudeLaunch, policy: SecretPolicy) -> Result<Self> {
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
                policy.redact_text(&format!("could not start the configured Claude: {error}")),
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::new("native_spawn", "Claude stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::new("native_spawn", "Claude stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::new("native_spawn", "Claude stderr was not piped"))?;
        let stderr = spawn_stderr(stderr, Arc::new(policy.clone()));

        let writer = Arc::new(tokio::sync::Mutex::new(FramedWrite::new(
            stdin,
            LinesCodec::new(),
        )));
        let client = ClaudeClient {
            writer,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(0)),
            stderr: stderr.view(),
            policy,
        };
        let (frame_tx, frame_rx) = mpsc::channel(NATIVE_EVENT_QUEUE);
        let (stream_tx, stream_rx) = watch::channel(StreamState::Open);
        let reader_client = client.clone();
        let output_reader =
            tokio::spawn(
                async move { read_stream(stdout, frame_tx, stream_tx, reader_client).await },
            );

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

    /// The actual CLI child PID; birth and custody are verified by the host.
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn client(&self) -> ClaudeClient {
        self.client.clone()
    }

    pub fn take_frames(&mut self) -> Result<mpsc::Receiver<ClaudeFrame>> {
        self.frames.take().ok_or_else(|| {
            Error::new(
                "bad_state",
                "Claude native event reader already has an owner",
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
            let reader = self
                .output_reader
                .as_mut()
                .ok_or_else(|| Error::new("unknown", "Claude stdout reader is unavailable"))?;
            if reader.is_finished() {
                (&mut *reader).await
            } else {
                match timeout_at(deadline, reader).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(Error::new(
                            "unknown",
                            "Claude stdout reader did not drain before its deadline",
                        ));
                    }
                }
            }
        };
        let result = match reader_result {
            Ok(result) => result,
            Err(error) => Err(Error::new(
                "unknown",
                format!("Claude stdout reader task failed: {error}"),
            )),
        };
        drop(self.output_reader.take());
        self.output_reader_result = Some(result.clone());
        result
    }

    /// Close input, then end the owned tree. `claude -p` exits on stdin EOF;
    /// the kill path only fires when the graceful close cannot be confirmed.
    pub async fn terminate(
        &mut self,
        force: bool,
        deadline: Instant,
    ) -> Result<std::process::ExitStatus> {
        if let Some(status) = self.child.try_wait().map_err(|error| {
            Error::new(
                "native_process",
                format!("Claude process readback failed: {error}"),
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
                    "Claude process termination outcome is unknown",
                ));
            }
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if let Some(status) = self.child.try_wait().map_err(|readback| {
                    Error::new(
                        "unknown",
                        format!(
                            "Claude process exit readback failed after termination error: {readback}"
                        ),
                    )
                })? {
                    self.await_output_drained(deadline).await?;
                    return Ok(status);
                }
                return Err(Error::new(
                    "native_process",
                    format!("Claude process termination failed: {error}"),
                ));
            }
        }
        let status = timeout_at(deadline, self.child.wait())
            .await
            .map_err(|_| Error::new("unknown", "Claude process exit could not be confirmed"))?
            .map_err(|error| {
                Error::new(
                    "native_process",
                    format!("Claude process wait failed: {error}"),
                )
            })?;
        self.await_output_drained(deadline).await?;
        Ok(status)
    }
}

impl ClaudeClient {
    /// One stdin `control_request` round trip. Only a `success` subtype proves
    /// the request ran; an `error` subtype, a lost reply and a closed stream
    /// all leave the outcome unknown, never rejected.
    pub async fn control(&self, subtype: &str, request: Value, deadline: Instant) -> Result<Value> {
        self.control_inner(subtype, request, deadline)
            .await
            .map_err(|error| self.diagnostic_error(error))
    }

    /// One stdin `user` record. There is no transport ack: a queued write is
    /// the dispatch, and the exact turn association arrives separately as
    /// `command_lifecycle` on the stream.
    pub async fn send_user(&self, input: OutboundUserInput, deadline: Instant) -> Result<()> {
        if input.uuid.trim().is_empty() || input.session_id.trim().is_empty() {
            return Err(Error::new(
                "invalid",
                "Claude user input requires the exact session and turn uuids",
            ));
        }
        let encoded = encode_native_frame(&input)?;
        self.write_encoded(encoded, deadline).await
    }

    pub fn stderr_snapshot(&self) -> StderrSnapshot {
        self.stderr.snapshot()
    }

    pub fn diagnostic_error(&self, mut error: Error) -> Error {
        error.message = self.policy.redact_text(&error.message);
        error.with_stderr(StderrSource::Harness, self.stderr_snapshot())
    }

    async fn control_inner(
        &self,
        subtype: &str,
        mut request: Value,
        deadline: Instant,
    ) -> Result<Value> {
        if subtype.trim().is_empty() || !request.is_object() {
            return Err(Error::new(
                "invalid",
                "Claude control requires a subtype and object request",
            ));
        }
        if let Some(object) = request.as_object_mut() {
            object.insert("subtype".to_owned(), Value::String(subtype.to_owned()));
        }
        let sequence = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| Error::new("internal", "Claude request id space exhausted"))?;
        let id = format!("claude-{sequence}");
        let encoded = encode_native_frame(&OutboundControlRequest {
            kind: "control_request",
            request_id: id.clone(),
            request,
        })?;

        let (response_tx, response_rx) = oneshot::channel();
        {
            self.pending.lock_recover().insert(id.clone(), response_tx);
        }
        let _guard = PendingGuard {
            pending: Arc::clone(&self.pending),
            id,
        };

        if let Err(error) = self.write_encoded_inner(encoded, deadline).await {
            if error.code == "native_not_dispatched" {
                return Err(error);
            }
            return Err(Error::new(
                "unknown",
                format!("Claude {subtype} dispatch outcome is unknown"),
            ));
        }
        timeout_at(deadline, response_rx)
            .await
            .map_err(|_| Error::new("unknown", format!("Claude {subtype} response is unknown")))?
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("Claude {subtype} response channel closed"),
                )
            })?
    }

    async fn close_input(&self, deadline: Instant) -> Result<()> {
        let mut writer = match timeout_at(deadline, self.writer.lock()).await {
            Ok(writer) => writer,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    "Claude input close deadline elapsed before dispatch",
                ));
            }
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                "Claude input close deadline elapsed before dispatch",
            ));
        }
        match timeout_at(deadline, SinkExt::<String>::close(&mut *writer)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                "Claude input close outcome is unknown",
            )),
        }
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
                    "Claude write deadline elapsed before dispatch",
                ));
            }
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                "Claude write deadline elapsed before dispatch",
            ));
        }
        match timeout_at(deadline, writer.send(encoded)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new("unknown", "Claude write outcome is unknown")),
        }
    }

    fn route_response(&self, request_id: &str, result: Result<Value>) {
        let sender = self.pending.lock_recover().remove(request_id);
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

async fn read_stream(
    stdout: tokio::process::ChildStdout,
    frames: mpsc::Sender<ClaudeFrame>,
    stream: watch::Sender<StreamState>,
    client: ClaudeClient,
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
                    "Claude stream ended on an invalid or oversized frame",
                ));
            }
        };
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                return Err(mark_stream_malformed(
                    &stream,
                    &client,
                    "Claude stream contained invalid JSON",
                ));
            }
        };
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            return Err(mark_stream_malformed(
                &stream,
                &client,
                "Claude stream record omitted its type",
            ));
        };
        if kind == "control_response" {
            let response: ControlResponse = match serde_json::from_value(value) {
                Ok(response) => response,
                Err(_) => {
                    return Err(mark_stream_malformed(
                        &stream,
                        &client,
                        "Claude control_response did not match its envelope",
                    ));
                }
            };
            let result = match response.response.subtype.as_str() {
                "success" => Ok(response.response.response.unwrap_or(Value::Null)),
                _ => Err(Error::new(
                    "unknown",
                    format!(
                        "Claude control {} left its outcome uncertain",
                        response.response.subtype
                    ),
                )),
            };
            client.route_response(&response.response.request_id, result);
            continue;
        }
        if overflowed {
            continue;
        }
        if frames.try_send(ClaudeFrame::Record(value)).is_err() {
            overflowed = true;
            stream.send_replace(StreamState::Overflow);
        }
    }

    if overflowed {
        stream.send_replace(StreamState::Overflow);
        let error = Error::new(
            "unknown",
            "Claude event queue overflowed before stdout reached EOF",
        );
        client.fail_pending(error.clone());
        return Err(error);
    }

    stream.send_replace(StreamState::Closed);
    client.fail_pending(Error::new(
        "unknown",
        "Claude stream closed while an outcome may be pending",
    ));
    Ok(())
}

fn mark_stream_malformed(
    stream: &watch::Sender<StreamState>,
    client: &ClaudeClient,
    message: &str,
) -> Error {
    stream.send_replace(StreamState::Malformed);
    let error = Error::new("unknown", message);
    client.fail_pending(error.clone());
    error
}

fn encode_native_frame(message: &impl Serialize) -> Result<String> {
    let encoded = serde_json::to_string(message)
        .map_err(|_| Error::new("native_encode", "Claude frame could not be encoded"))?;
    if encoded.len().saturating_add(1) > MAX_NATIVE_FRAME_BYTES {
        return Err(Error::new("frame_too_large", "Claude frame exceeds 1 MiB"));
    }
    Ok(encoded)
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock_recover().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::{ffi::OsString, time::Duration};

    #[test]
    fn stderr_fixture_child() {
        if std::env::var_os("ELIOT_CLAUDE_NATIVE_STDERR_FIXTURE").as_deref()
            != Some(std::ffi::OsStr::new("1"))
        {
            return;
        }
        let mut stderr = std::io::stderr().lock();
        stderr
            .write_all(&vec![b'x'; 32 * 1024])
            .expect("fixture stderr");
        stderr
            .write_all(b"\nnative-child failure vendor-secret xy\n")
            .expect("fixture marker");
        stderr.flush().expect("flush fixture stderr");
    }

    #[tokio::test]
    async fn native_stderr_is_owned_redacted_and_keeps_unknown() {
        let launch = ClaudeLaunch {
            program: std::env::current_exe()
                .expect("test executable")
                .into_os_string(),
            args: [
                "--exact",
                "native::client::tests::stderr_fixture_child",
                "--nocapture",
            ]
            .into_iter()
            .map(OsString::from)
            .collect(),
            cwd: std::env::current_dir().expect("test cwd"),
            environment: vec![(
                OsString::from("ELIOT_CLAUDE_NATIVE_STDERR_FIXTURE"),
                OsString::from("1"),
            )],
        };
        let mut native = ClaudeProcess::spawn(launch, SecretPolicy::new(["vendor-secret", "xy"]))
            .await
            .expect("start synthetic child");
        let client = native.client();
        // The test harness's stdout is deliberately not a stream-json record.
        let error = client
            .control(
                "initialize",
                serde_json::json!({}),
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .expect_err("malformed/closed transport has unknown outcome");
        let _ = native
            .terminate(false, Instant::now() + Duration::from_secs(5))
            .await;
        assert_eq!(error.code, "unknown");
        assert!(native.stderr.is_none());
        let snapshot = client.stderr_snapshot();
        assert!(snapshot.eof);
        assert!(!snapshot.read_error);
        assert!(snapshot.truncated);
        assert!(snapshot.text.len() <= eliot_core::MAX_STDERR_BYTES);
        assert!(snapshot.text.contains("native-child failure"));
        assert!(!snapshot.text.contains("vendor-secret"));
        assert!(!snapshot.text.contains("xy"));
        assert_eq!(error.diagnostics.len(), 1);
        let encoded = serde_json::to_string(&error).expect("bounded error wire");
        assert!(!encoded.contains("vendor-secret"));
        assert!(!encoded.contains("xy"));
        let restored: Error = serde_json::from_str(&encoded).expect("decode error diagnostics");
        assert_eq!(restored, error);
    }
}
