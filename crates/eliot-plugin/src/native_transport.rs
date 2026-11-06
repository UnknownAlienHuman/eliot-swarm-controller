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
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    process::{ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot, StderrSource};

use crate::{StderrCapture, StderrView, spawn_stderr, sync::LockExt};

const MAX_NATIVE_FRAME_BYTES: usize = 1024 * 1024;
const NATIVE_EVENT_QUEUE: usize = 64;
const NATIVE_EXIT_DRAIN: Duration = Duration::from_secs(1);

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
    Malformed(NativeStreamFault),
}

/// Bounded, nonsecret cause of a StreamState::Malformed transition.
/// Fixed classes only: no frame bytes, bodies, or credentials cross here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeStreamFault {
    /// Framed-line codec failure or oversize past the transport bound.
    Frame,
    /// A framed line was not valid JSON.
    Json,
    /// A parsed record did not match its protocol envelope.
    Envelope,
    /// Stdout stayed open past the post-exit drain deadline.
    Drain,
    /// A malformed record without a finer proven kind.
    Unclassified,
}

impl NativeStreamFault {
    pub fn label(self) -> &'static str {
        match self {
            Self::Frame => "invalid_or_oversized",
            Self::Json => "invalid_json",
            Self::Envelope => "invalid_envelope",
            Self::Drain => "exit_drain",
            Self::Unclassified => "unclassified",
        }
    }
}

/// One owned native child: process waiter, stdin writer queue, stdout-reader
/// task ownership, stderr capture. The protocol layer spawns its own reader
/// task over the returned stdout (framing is its business) and hands the
/// task back for drain ownership.
pub struct NativeChild {
    pid: Option<u32>,
    kill: Option<oneshot::Sender<()>>,
    process_exit: watch::Receiver<Option<ProcessExit>>,
    process_task: Option<tokio::task::JoinHandle<()>>,
    writer: NativeWriter,
    output_reader: Option<tokio::task::JoinHandle<Result<()>>>,
    output_reader_result: Option<Result<()>>,
    /// Present until [`NativeChild::await_output_drained`] finishes stderr.
    pub stderr: Option<StderrCapture>,
    stderr_view: StderrView,
    policy: SecretPolicy,
    labels: NativeLabels,
}

#[derive(Clone)]
struct ProcessExit {
    status: ExitStatus,
    observed_at: Instant,
}

/// One stdin writer queue. Cloned into the protocol client; every write and
/// close serializes on the queue with the caller's absolute deadline. The
/// queue empties on input shutdown or child exit.
#[derive(Clone)]
pub struct NativeWriter {
    writer: Arc<tokio::sync::Mutex<Option<FramedWrite<ChildStdin, LinesCodec>>>>,
    process_exited: Arc<AtomicBool>,
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

/// How the pump delivers emitted frames when the bounded queue is full.
/// JSON-RPC callers select [`PumpMode::LossyOverflow`] so reply routing stays
/// nonblocking; only the stream-json caller with no RPC replies selects
/// [`PumpMode::LosslessBackpressure`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PumpMode {
    /// `try_send`; on a full queue latch overflow and drop later emits.
    LossyOverflow,
    /// Wait for queue space holding at most one frame, respecting receiver
    /// closure and the existing exit-drain deadline.
    LosslessBackpressure,
}

/// Read one retained process exit. The waiter publishes `Some(status)`
/// then drops its sender; Tokio reports closure even when that final
/// value is retained, so a closed channel is reread and only a still-
/// `None` value concludes unknown.
fn read_process_exit(
    process_exit: &watch::Receiver<Option<ProcessExit>>,
    short: &'static str,
) -> Result<Option<ExitStatus>> {
    let status = process_exit.borrow().as_ref().map(|exit| exit.status);
    if status.is_some() {
        return Ok(status);
    }
    if process_exit.has_changed().is_err() {
        let status = process_exit.borrow().as_ref().map(|exit| exit.status);
        if status.is_some() {
            return Ok(status);
        }
        return Err(Error::new(
            "unknown",
            format!("{short} process exit could not be confirmed"),
        ));
    }
    Ok(None)
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
        let pid = child.id();
        let (exit_tx, process_exit) = watch::channel(None);
        let (kill, mut kill_rx) = oneshot::channel();
        let writer = NativeWriter::new(stdin, labels);
        let exit_writer = writer.clone();
        let process_task = tokio::spawn(async move {
            let result = tokio::select! {
                status = child.wait() => status,
                _ = &mut kill_rx => {
                    match child.start_kill() {
                        Ok(()) => child.wait().await,
                        Err(kill_error) => match child.try_wait() {
                            Ok(Some(status)) => Ok(status),
                            Ok(None) => Err(kill_error),
                            Err(wait_error) => Err(wait_error),
                        },
                    }
                }
            };
            exit_writer.mark_process_exited();
            if let Ok(status) = result {
                exit_tx.send_replace(Some(ProcessExit {
                    status,
                    observed_at: Instant::now(),
                }));
            }
        });
        let child = Self {
            pid,
            kill: Some(kill),
            process_exit,
            process_task: Some(process_task),
            writer,
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
        self.pid
    }

    fn process_exit_signal(&self) -> watch::Receiver<Option<ProcessExit>> {
        self.process_exit.clone()
    }

    /// True only after the owned child waiter observed its OS exit status.
    pub fn process_exit_observed(&self) -> bool {
        self.process_exit.borrow().is_some()
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

    fn process_exit_status(&self) -> Result<Option<ExitStatus>> {
        read_process_exit(&self.process_exit, self.labels.short)
    }

    async fn wait_process_exit(&mut self) -> Result<ExitStatus> {
        loop {
            if let Some(status) = self.process_exit_status()? {
                return Ok(status);
            }
            if self.process_exit.changed().await.is_err() {
                // Closed without a prior observed change. Reread the
                // retained final value before concluding unknown.
                if let Some(status) = self.process_exit.borrow().as_ref().map(|exit| exit.status) {
                    return Ok(status);
                }
                return Err(Error::new(
                    "unknown",
                    format!("{} process exit could not be confirmed", self.labels.short),
                ));
            }
        }
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
        if let Some(status) = self.process_exit_status()? {
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
            if let Ok(Ok(status)) = timeout_at(grace_deadline, self.wait_process_exit()).await {
                self.await_output_drained(deadline).await?;
                return Ok(status);
            }
        }

        if let Some(status) = self.process_exit_status()? {
            self.await_output_drained(deadline).await?;
            return Ok(status);
        }

        if Instant::now() >= deadline {
            return Err(Error::new(
                "unknown",
                format!(
                    "{} process termination outcome is unknown",
                    self.labels.short
                ),
            ));
        }
        if let Some(kill) = self.kill.take() {
            let _ = kill.send(());
        }
        let status = timeout_at(deadline, self.wait_process_exit())
            .await
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("{} process exit could not be confirmed", self.labels.short),
                )
            })??;
        self.await_output_drained(deadline).await?;
        Ok(status)
    }
}

impl Drop for NativeChild {
    fn drop(&mut self) {
        if let Some(reader) = &self.output_reader {
            reader.abort();
        }
        if let Some(process) = self.process_task.take() {
            process.abort();
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
            process_exited: Arc::new(AtomicBool::new(false)),
            labels,
        }
    }

    fn mark_process_exited(&self) {
        self.process_exited.store(true, Ordering::Release);
        if let Ok(mut writer) = self.writer.try_lock() {
            *writer = None;
        }
    }

    /// True once the owned child waiter finished (exit or wait error)
    /// and fenced input. Conservative input unavailability only; it does
    /// NOT prove an OS exit. Protocol layers use it to distinguish an
    /// unavailable destination (unknown) from a live pre-dispatch
    /// refusal (rejected).
    pub fn process_unavailable(&self) -> bool {
        self.process_exited.load(Ordering::Acquire)
    }

    /// One already-encoded frame. Raw errors: the protocol layer maps them
    /// through its own diagnostics. Only a queued write proves dispatch; a
    /// lost write is unknown, never silently dropped.
    pub async fn send(&self, encoded: String, deadline: Instant) -> Result<()> {
        if self.process_exited.load(Ordering::Acquire) {
            return Err(self.process_exited_error());
        }
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
        if self.process_exited.load(Ordering::Acquire) {
            *writer = None;
            return Err(self.process_exited_error());
        }
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

    fn process_exited_error(&self) -> Error {
        Error::new(
            "native_not_dispatched",
            format!("{} process exited before input dispatch", self.labels.short),
        )
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
/// The caller selects the pump mode explicitly: JSON-RPC keeps
/// [`PumpMode::LossyOverflow`], only the reply-less stream-json caller
/// selects [`PumpMode::LosslessBackpressure`].
pub async fn spawn_pumped<Frame, Classify>(
    launch: NativeLaunch,
    policy: SecretPolicy,
    labels: NativeLabels,
    id_prefix: &'static str,
    mode: PumpMode,
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
    let process_exit = transport.process_exit_signal();
    let pending = PendingCalls::new(id_prefix, labels);
    let (frame_tx, frame_rx) = native_frame_channel();
    let (stream_tx, stream_rx) = watch::channel(StreamState::Open);
    let output_reader = tokio::spawn(pump_framed_lines(
        stdout,
        frame_tx,
        stream_tx,
        pending.clone(),
        labels.server,
        process_exit,
        mode,
        classify,
    ));
    transport.set_output_reader(output_reader);
    Ok((transport, pending, frame_rx, stream_rx))
}

fn finish_exit_drain_deadline(
    overflowed: bool,
    stream: &watch::Sender<StreamState>,
    pending: &PendingCalls,
    server: &str,
) -> Result<()> {
    if overflowed {
        stream.send_replace(StreamState::Overflow);
        let error = Error::new(
            "unknown",
            format!("{server} event queue overflowed before exit drain ended"),
        );
        pending.fail_all(error.clone());
        return Err(error);
    }
    stream.send_replace(StreamState::Malformed(NativeStreamFault::Drain));
    pending.fail_all(Error::new(
        "unknown",
        format!("{server} stdout remained open after its process exit drain deadline"),
    ));
    // The owner sees `Malformed` and records the uncovered-tail gap.
    Ok(())
}

enum HeldSend {
    Sent,
    Closed,
    DeadlineOverflow,
}

/// Wait for queue space for one held frame. Respects receiver closure and
/// the existing exit-drain deadline; never touches the line reader.
async fn send_held_frame<Frame>(
    frames: &mpsc::Sender<Frame>,
    frame: Frame,
    exit_deadline: &mut Option<Instant>,
    process_exit: &mut watch::Receiver<Option<ProcessExit>>,
) -> HeldSend {
    let mut send = Box::pin(frames.send(frame));
    loop {
        match *exit_deadline {
            Some(deadline) => {
                if Instant::now() >= deadline {
                    return HeldSend::DeadlineOverflow;
                }
                tokio::select! {
                    result = &mut send => {
                        return if result.is_err() { HeldSend::Closed } else { HeldSend::Sent };
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        return HeldSend::DeadlineOverflow;
                    }
                }
            }
            None => {
                tokio::select! {
                    result = &mut send => {
                        return if result.is_err() { HeldSend::Closed } else { HeldSend::Sent };
                    }
                    _ = process_exit.changed() => {
                        *exit_deadline = Some(process_exit.borrow().as_ref().map(|exit| {
                            exit.observed_at + NATIVE_EXIT_DRAIN
                        }).unwrap_or_else(|| Instant::now() + NATIVE_EXIT_DRAIN));
                    }
                }
            }
        }
    }
}

/// Bounded stdout line pump: length-checked NDJSON lines, protocol
/// classification of each parsed record, one overflow gap, and a bounded
/// post-exit drain before any loss conclusion. The classifier owns every
/// protocol decision; the pump owns every transport decision.
#[allow(clippy::too_many_arguments)]
async fn pump_framed_lines<Frame, Classify>(
    stdout: ChildStdout,
    frames: mpsc::Sender<Frame>,
    stream: watch::Sender<StreamState>,
    pending: PendingCalls,
    server: &'static str,
    mut process_exit: watch::Receiver<Option<ProcessExit>>,
    mode: PumpMode,
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
    let mut exit_deadline = process_exit
        .borrow()
        .as_ref()
        .map(|exit| exit.observed_at + NATIVE_EXIT_DRAIN);
    if exit_deadline.is_none() && process_exit.has_changed().is_err() {
        exit_deadline = Some(Instant::now() + NATIVE_EXIT_DRAIN);
    }
    let mut post_deadline_ready_lines = 0;
    loop {
        let line = match exit_deadline {
            Some(deadline) if Instant::now() >= deadline => {
                if post_deadline_ready_lines >= NATIVE_EVENT_QUEUE {
                    return finish_exit_drain_deadline(overflowed, &stream, &pending, server);
                }
                match reader.next().now_or_never() {
                    Some(line) => {
                        post_deadline_ready_lines += 1;
                        line
                    }
                    None => {
                        return finish_exit_drain_deadline(overflowed, &stream, &pending, server);
                    }
                }
            }
            Some(deadline) => {
                tokio::select! {
                    // Prefer ready output while time remains. The loop checks
                    // the deadline before every poll, so continuous output
                    // cannot starve it.
                    biased;
                    line = reader.next() => line,
                    _ = tokio::time::sleep_until(deadline) => continue,
                }
            }
            None => {
                tokio::select! {
                    line = reader.next() => line,
                    _ = process_exit.changed() => {
                        exit_deadline = Some(
                            process_exit
                                .borrow()
                                .as_ref()
                                .map(|exit| exit.observed_at + NATIVE_EXIT_DRAIN)
                                .unwrap_or_else(|| Instant::now() + NATIVE_EXIT_DRAIN),
                        );
                        continue;
                    }
                }
            }
        };
        let Some(line) = line else {
            break;
        };
        let line = match line {
            Ok(line) if line.len().saturating_add(1) <= MAX_NATIVE_FRAME_BYTES => line,
            _ => {
                return Err(mark_stream_malformed(
                    stream,
                    pending,
                    NativeStreamFault::Frame,
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
                    NativeStreamFault::Json,
                    &format!("{server} stream contained invalid JSON"),
                ));
            }
        };
        let mut receiver_closed = false;
        match classify(value) {
            PumpAction::Emit(frame) => {
                if overflowed {
                    continue;
                }
                match mode {
                    PumpMode::LossyOverflow => {
                        if frames.try_send(frame).is_err() {
                            overflowed = true;
                            stream.send_replace(StreamState::Overflow);
                        }
                    }
                    PumpMode::LosslessBackpressure => {
                        match send_held_frame(&frames, frame, &mut exit_deadline, &mut process_exit)
                            .await
                        {
                            HeldSend::Sent => {}
                            HeldSend::Closed => receiver_closed = true,
                            HeldSend::DeadlineOverflow => overflowed = true,
                        }
                    }
                }
            }
            PumpAction::Route { key, result } => {
                pending.route(&key, result);
            }
            PumpAction::Malformed(message) => {
                return Err(mark_stream_malformed(
                    stream,
                    pending,
                    NativeStreamFault::Envelope,
                    &message,
                ));
            }
            PumpAction::Skip => {}
        }
        if receiver_closed {
            // The frame consumer is gone; stop like EOF so pending calls
            // fail honestly below instead of waiting forever.
            break;
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
    fault: NativeStreamFault,
    message: &str,
) -> Error {
    stream.send_replace(StreamState::Malformed(fault));
    let error = Error::new("unknown", message);
    pending.fail_all(error.clone());
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn exit_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(0)
    }

    #[cfg(windows)]
    fn exit_status() -> ExitStatus {
        use std::os::windows::process::ExitStatusExt;
        ExitStatus::from_raw(0)
    }

    #[test]
    fn publish_then_close_retains_positive_exit() {
        // The waiter publishes `Some(status)` then drops its sender.
        // Tokio reports closure even with the final value retained; the
        // read must still prove the exit, not conclude unknown.
        let (exit_tx, exit_rx) = watch::channel(None);
        exit_tx.send_replace(Some(ProcessExit {
            status: exit_status(),
            observed_at: Instant::now(),
        }));
        drop(exit_tx);
        assert!(exit_rx.has_changed().is_err());
        let status = read_process_exit(&exit_rx, "Test").expect("retained exit");
        assert!(status.is_some_and(|status| status.success()));
    }

    #[test]
    fn close_without_publish_stays_unknown() {
        // No publish before the drop: there is no exit to prove.
        let (exit_tx, exit_rx) = watch::channel(None);
        drop(exit_tx);
        let error = read_process_exit(&exit_rx, "Test").expect_err("unknown exit");
        assert_eq!(error.code, "unknown");
    }

    #[test]
    fn open_without_publish_stays_pending() {
        // The waiter has not published and still holds its sender.
        let (_exit_tx, exit_rx) = watch::channel(None);
        let status = read_process_exit(&exit_rx, "Test").expect("pending");
        assert!(status.is_none());
    }
}
