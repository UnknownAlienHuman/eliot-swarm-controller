//! Protocol-independent native child transport for harness plugins.
//!
//! Process spawn and custody, the stdin writer queue, the bounded framed-line
//! pump, output drain and termination are identical for every stdio harness;
//! only line framing and request/response correlation differ. The JSON-RPC
//! layer ([`crate::native_stdio`]) and the stream-json layer (Claude) build
//! on this module so those pieces exist exactly once.

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

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot, StderrSource};

use crate::{StderrCapture, StderrView, spawn_stderr, sync::LockExt};

const MAX_NATIVE_FRAME_BYTES: usize = 1024 * 1024;
const NATIVE_EVENT_QUEUE: usize = 64;

/// Display nouns for transport diagnostics, e.g. `("Muse", "Muse session
/// host")`. The protocol layers pass their exact historical labels so
/// shared errors read exactly as before.
#[derive(Clone, Copy)]
pub struct NativeLabels {
    pub short: &'static str,
    pub server: &'static str,
}

pub struct NativeLaunch {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamState {
    Open,
    Overflow,
    Closed,
    Malformed,
}

/// One owned native child: process handle, stdin writer queue, stdout-reader
/// task ownership, stderr capture. The protocol layer spawns its own reader
/// task over the returned stdout (framing is its business) and hands the
/// task back for drain ownership.
pub struct NativeChild {
    child: Child,
    writer: NativeWriter,
    output_reader: Option<tokio::task::JoinHandle<Result<()>>>,
    output_reader_result: Option<Result<()>>,
    /// Present until [`NativeChild::await_output_drained`] finishes stderr.
    pub stderr: Option<StderrCapture>,
    stderr_view: StderrView,
    policy: SecretPolicy,
    labels: NativeLabels,
}

/// One stdin writer queue. Cloned into the protocol client; every write and
/// close serializes on the queue with the caller's absolute deadline. The
/// queue empties only through [`NativeWriter::shutdown_input`], which drops
/// the stdin handle so the child really sees EOF.
#[derive(Clone)]
pub struct NativeWriter {
    writer: Arc<tokio::sync::Mutex<Option<FramedWrite<ChildStdin, LinesCodec>>>>,
    labels: NativeLabels,
}

/// Correlated request/response calls: id minting plus the pending map. The
/// protocol layer decides the wire envelope and the response mapping.
#[derive(Clone)]
pub struct PendingCalls {
    calls: Arc<StdMutex<HashMap<String, oneshot::Sender<Result<Value>>>>>,
    next_id: Arc<AtomicU64>,
    id_prefix: &'static str,
    labels: NativeLabels,
}

pub struct PendingGuard {
    pending: PendingCalls,
    id: String,
}

/// What one decoded stream record means to the pump.
pub enum PumpAction<Frame> {
    /// A protocol frame for the event task (subject to the queue bound).
    Emit(Frame),
    /// A correlated reply for one pending call.
    Route { key: String, result: Result<Value> },
    /// The stream is unusable from here on; the pump gaps and fails pending.
    Malformed(String),
    /// A record the protocol ignores (e.g. an echo of our own input).
    Skip,
}

impl NativeChild {
    pub async fn spawn(
        launch: NativeLaunch,
        policy: SecretPolicy,
        labels: NativeLabels,
    ) -> Result<(Self, ChildStdout)> {
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
                    labels.server
                )),
            )
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            Error::new(
                "native_spawn",
                format!("{} stdin was not piped", labels.server),
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::new(
                "native_spawn",
                format!("{} stdout was not piped", labels.server),
            )
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            Error::new(
                "native_spawn",
                format!("{} stderr was not piped", labels.server),
            )
        })?;
        let stderr = spawn_stderr(stderr, Arc::new(policy.clone()));
        let child = Self {
            child,
            writer: NativeWriter::new(stdin, labels),
            output_reader: None,
            output_reader_result: None,
            stderr_view: stderr.view(),
            stderr: Some(stderr),
            policy,
            labels,
        };
        Ok((child, stdout))
    }

    pub fn set_output_reader(&mut self, reader: tokio::task::JoinHandle<Result<()>>) {
        self.output_reader = Some(reader);
    }

    pub fn writer(&self) -> NativeWriter {
        self.writer.clone()
    }

    /// The actual native child PID; birth and custody are verified by the host.
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn stderr_snapshot(&self) -> StderrSnapshot {
        self.stderr_view.snapshot()
    }

    pub fn stderr_view(&self) -> StderrView {
        self.stderr_view.clone()
    }

    pub fn diagnostic_error(&self, mut error: Error) -> Error {
        error.message = self.policy.redact_text(&error.message);
        error.with_stderr(StderrSource::Harness, self.stderr_snapshot())
    }

    /// Wait for the single stdout reader to finish; a timeout leaves that same
    /// reader running so a later wait can observe its completion without reuse.
    pub async fn await_output_drained(&mut self, deadline: Instant) -> Result<()> {
        let result = self.await_stdout_drained(deadline).await;
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.finish(deadline).await;
        }
        result.map_err(|error| self.diagnostic_error(error))
    }

    async fn await_stdout_drained(&mut self, deadline: Instant) -> Result<()> {
        if let Some(result) = &self.output_reader_result {
            return result.clone();
        }
        let reader_result = {
            let reader = self.output_reader.as_mut().ok_or_else(|| {
                Error::new(
                    "unknown",
                    format!("{} stdout reader is unavailable", self.labels.short),
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
                                self.labels.short
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
                format!("{} stdout reader task failed: {error}", self.labels.short),
            )),
        };
        drop(self.output_reader.take());
        self.output_reader_result = Some(result.clone());
        result
    }

    /// Flush input, then end the owned process. The graceful close only fires
    /// when the process is still running; the kill path only fires when the
    /// graceful close cannot be confirmed.
    pub async fn terminate(&mut self, force: bool, deadline: Instant) -> Result<ExitStatus> {
        self.terminate_inner(force, deadline, false).await
    }

    /// Drop stdin so the child really sees EOF, then end the owned process.
    /// Same sequence as [`NativeChild::terminate`], but the graceful close
    /// delivers EOF instead of only flushing.
    pub async fn terminate_dropping_input(
        &mut self,
        force: bool,
        deadline: Instant,
    ) -> Result<ExitStatus> {
        self.terminate_inner(force, deadline, true).await
    }

    async fn terminate_inner(
        &mut self,
        force: bool,
        deadline: Instant,
        drop_input: bool,
    ) -> Result<ExitStatus> {
        if let Some(status) = self.child.try_wait().map_err(|error| {
            Error::new(
                "native_process",
                format!("{} process readback failed: {error}", self.labels.short),
            )
        })? {
            self.await_output_drained(deadline).await?;
            return Ok(status);
        }

        if !force {
            let close_deadline = (Instant::now() + Duration::from_millis(500)).min(deadline);
            if drop_input {
                let _ = self.writer.shutdown_input(close_deadline).await;
            } else {
                let _ = self.writer.close_input(close_deadline).await;
            }
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
                    format!(
                        "{} process termination outcome is unknown",
                        self.labels.short
                    ),
                ));
            }
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if let Some(status) = self.child.try_wait().map_err(|readback| {
                    Error::new(
                        "unknown",
                        format!(
                            "{} process exit readback failed after termination error: {readback}",
                            self.labels.short
                        ),
                    )
                })? {
                    self.await_output_drained(deadline).await?;
                    return Ok(status);
                }
                return Err(Error::new(
                    "native_process",
                    format!("{} process termination failed: {error}", self.labels.short),
                ));
            }
        }
        let status = timeout_at(deadline, self.child.wait())
            .await
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("{} process exit could not be confirmed", self.labels.short),
                )
            })?
            .map_err(|error| {
                Error::new(
                    "native_process",
                    format!("{} process wait failed: {error}", self.labels.short),
                )
            })?;
        self.await_output_drained(deadline).await?;
        Ok(status)
    }
}

impl Drop for NativeChild {
    fn drop(&mut self) {
        if let Some(reader) = &self.output_reader {
            reader.abort();
        }
        // StderrCapture owns and cancels its reader; client views cannot keep it alive.
    }
}

impl NativeWriter {
    pub fn new(stdin: ChildStdin, labels: NativeLabels) -> Self {
        Self {
            writer: Arc::new(tokio::sync::Mutex::new(Some(FramedWrite::new(
                stdin,
                LinesCodec::new(),
            )))),
            labels,
        }
    }

    /// One already-encoded frame. Raw errors: the protocol layer maps them
    /// through its own diagnostics. Only a queued write proves dispatch; a
    /// lost write is unknown, never silently dropped.
    pub async fn send(&self, encoded: String, deadline: Instant) -> Result<()> {
        let mut writer = match timeout_at(deadline, self.writer.lock()).await {
            Ok(writer) => writer,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    format!(
                        "{} write deadline elapsed before dispatch",
                        self.labels.short
                    ),
                ));
            }
        };
        let Some(writer) = writer.as_mut() else {
            return Err(Error::new(
                "native_not_dispatched",
                format!(
                    "{} stdin is shut down; write was not dispatched",
                    self.labels.short
                ),
            ));
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                format!(
                    "{} write deadline elapsed before dispatch",
                    self.labels.short
                ),
            ));
        }
        match timeout_at(deadline, writer.send(encoded)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                format!("{} write outcome is unknown", self.labels.server),
            )),
        }
    }

    /// Flush input. This does not close the pipe: the stdin handle stays
    /// queued until [`NativeWriter::shutdown_input`] drops it.
    pub async fn close_input(&self, deadline: Instant) -> Result<()> {
        let mut writer = match timeout_at(deadline, self.writer.lock()).await {
            Ok(writer) => writer,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    format!(
                        "{} input close deadline elapsed before dispatch",
                        self.labels.short
                    ),
                ));
            }
        };
        let Some(writer) = writer.as_mut() else {
            return Ok(());
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                format!(
                    "{} input close deadline elapsed before dispatch",
                    self.labels.short
                ),
            ));
        }
        match timeout_at(deadline, SinkExt::<String>::close(writer)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                format!("{} input close outcome is unknown", self.labels.server),
            )),
        }
    }

    /// Flush input and drop the stdin handle so the child really sees EOF.
    /// Idempotent: input stays shut. The flush result is irrelevant — after
    /// the drop the pipe is closed either way.
    pub async fn shutdown_input(&self, deadline: Instant) -> Result<()> {
        let mut guard = match timeout_at(deadline, self.writer.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    format!(
                        "{} input shutdown deadline elapsed before dispatch",
                        self.labels.short
                    ),
                ));
            }
        };
        let Some(writer) = guard.as_mut() else {
            return Ok(());
        };
        let _ = timeout_at(deadline, SinkExt::<String>::close(writer)).await;
        *guard = None;
        Ok(())
    }
}

impl PendingCalls {
    pub fn new(id_prefix: &'static str, labels: NativeLabels) -> Self {
        Self {
            calls: Arc::new(StdMutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(0)),
            id_prefix,
            labels,
        }
    }

    pub fn mint_id(&self) -> Result<String> {
        let sequence = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| {
                Error::new(
                    "internal",
                    format!("{} request id space exhausted", self.labels.short),
                )
            })?;
        Ok(format!("{}-{sequence}", self.id_prefix))
    }

    /// Register one pending reply; the guard unregisters it unless the reply
    /// arrives first.
    pub fn insert(&self, id: String) -> (PendingGuard, oneshot::Receiver<Result<Value>>) {
        let (response_tx, response_rx) = oneshot::channel();
        self.calls.lock_recover().insert(id.clone(), response_tx);
        (
            PendingGuard {
                pending: self.clone(),
                id,
            },
            response_rx,
        )
    }

    pub fn route(&self, key: &str, result: Result<Value>) {
        let sender = self.calls.lock_recover().remove(key);
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }

    pub fn fail_all(&self, error: Error) {
        let pending = {
            let mut pending = self.calls.lock_recover();
            std::mem::take(&mut *pending)
        };
        for (_, sender) in pending {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.calls.lock_recover().remove(&self.id);
    }
}

/// Bounded native event queue between the stdout pump and the event task.
pub fn native_frame_channel<Frame>() -> (mpsc::Sender<Frame>, mpsc::Receiver<Frame>) {
    mpsc::channel(NATIVE_EVENT_QUEUE)
}

pub fn encode_frame(server: &'static str, message: &impl Serialize) -> Result<String> {
    let encoded = serde_json::to_string(message).map_err(|_| {
        Error::new(
            "native_encode",
            format!("{server} frame could not be encoded"),
        )
    })?;
    if encoded.len().saturating_add(1) > MAX_NATIVE_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            format!("{server} frame exceeds 1 MiB"),
        ));
    }
    Ok(encoded)
}

/// Spawn a native child with its stdout pump armed in one step: process
/// birth, the correlated-call map, the bounded frame queue, the stream-state
/// watch and the reader task handed back for drain ownership. Both protocol
/// layers (JSON-RPC and stream-json) use this so the wiring exists exactly
/// once; each builds its own client over the returned writer/pending pair.
pub async fn spawn_pumped<Frame, Classify>(
    launch: NativeLaunch,
    policy: SecretPolicy,
    labels: NativeLabels,
    id_prefix: &'static str,
    classify: Classify,
) -> Result<(
    NativeChild,
    PendingCalls,
    mpsc::Receiver<Frame>,
    watch::Receiver<StreamState>,
)>
where
    Frame: Send + 'static,
    Classify: FnMut(Value) -> PumpAction<Frame> + Send + 'static,
{
    let (mut transport, stdout) = NativeChild::spawn(launch, policy, labels).await?;
    let pending = PendingCalls::new(id_prefix, labels);
    let (frame_tx, frame_rx) = native_frame_channel();
    let (stream_tx, stream_rx) = watch::channel(StreamState::Open);
    let output_reader = tokio::spawn(pump_framed_lines(
        stdout,
        frame_tx,
        stream_tx,
        pending.clone(),
        labels.server,
        classify,
    ));
    transport.set_output_reader(output_reader);
    Ok((transport, pending, frame_rx, stream_rx))
}

/// Bounded stdout line pump: length-checked NDJSON lines, protocol
/// classification of each parsed record, one overflow gap, output drained
/// before any loss conclusion. The classifier owns every protocol decision;
/// the pump owns every transport decision.
pub async fn pump_framed_lines<Frame, Classify>(
    stdout: ChildStdout,
    frames: mpsc::Sender<Frame>,
    stream: watch::Sender<StreamState>,
    pending: PendingCalls,
    server: &'static str,
    mut classify: Classify,
) -> Result<()>
where
    Frame: Send + 'static,
    Classify: FnMut(Value) -> PumpAction<Frame>,
{
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
                    stream,
                    pending,
                    &format!("{server} stream ended on an invalid or oversized frame"),
                ));
            }
        };
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                return Err(mark_stream_malformed(
                    stream,
                    pending,
                    &format!("{server} stream contained invalid JSON"),
                ));
            }
        };
        match classify(value) {
            PumpAction::Emit(frame) => {
                if overflowed {
                    continue;
                }
                if frames.try_send(frame).is_err() {
                    overflowed = true;
                    stream.send_replace(StreamState::Overflow);
                }
            }
            PumpAction::Route { key, result } => {
                pending.route(&key, result);
            }
            PumpAction::Malformed(message) => {
                return Err(mark_stream_malformed(stream, pending, &message));
            }
            PumpAction::Skip => {}
        }
    }

    if overflowed {
        stream.send_replace(StreamState::Overflow);
        let error = Error::new(
            "unknown",
            format!("{server} event queue overflowed before stdout reached EOF"),
        );
        pending.fail_all(error.clone());
        return Err(error);
    }

    stream.send_replace(StreamState::Closed);
    pending.fail_all(Error::new(
        "unknown",
        format!("{server} stream closed while an outcome may be pending"),
    ));
    Ok(())
}

fn mark_stream_malformed(
    stream: watch::Sender<StreamState>,
    pending: PendingCalls,
    message: &str,
) -> Error {
    stream.send_replace(StreamState::Malformed);
    let error = Error::new("unknown", message);
    pending.fail_all(error.clone());
    error
}
