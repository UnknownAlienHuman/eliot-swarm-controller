//! A bounded diagnostic excerpt for an owned child pipe.
//!
//! Uses the existing Tokio child-pipe primitive from the donor map. Unlike the
//! finite Git capture, reaching this tail's cap never stops pipe drainage.

use crate::sync::LockExt;
pub use eliot_core::StderrSnapshot;
use eliot_core::{Error, MAX_STDERR_BYTES, SecretPolicy, StderrSource};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

const TAIL_BYTES: usize = MAX_STDERR_BYTES;
const PREFIX_BYTES: usize = 4 * 1024;
const READ_BYTES: usize = 8192;

pub fn harness_diagnostic(
    error: Error,
    policy: &SecretPolicy,
    stderr: Option<StderrSnapshot>,
) -> Error {
    let error = match stderr {
        Some(stderr) => error.with_stderr(StderrSource::Harness, stderr),
        None => error,
    };
    error.sanitized(policy)
}

#[derive(Default)]
struct CaptureState {
    // Only bytes already emitted by the shared policy are retained here.
    bytes: VecDeque<u8>,
    keep_prefix: bool,
    truncated: bool,
    eof: bool,
    read_error: bool,
    pending_bytes: usize,
}

impl CaptureState {
    fn append(&mut self, sanitized: &[u8]) {
        if self.keep_prefix {
            let available = PREFIX_BYTES.saturating_sub(self.bytes.len());
            self.bytes.extend(sanitized.iter().take(available));
            self.truncated |= sanitized.len() > available;
            return;
        }
        let excess = self
            .bytes
            .len()
            .saturating_add(sanitized.len())
            .saturating_sub(TAIL_BYTES);
        if excess > 0 {
            self.truncated = true;
        }
        if sanitized.len() >= TAIL_BYTES {
            self.bytes.clear();
            self.bytes
                .extend(&sanitized[sanitized.len() - TAIL_BYTES..]);
        } else {
            self.bytes.drain(..excess.min(self.bytes.len()));
            self.bytes.extend(sanitized);
        }
    }

    fn end(&mut self, eof: bool, error: bool) {
        self.eof |= eof;
        self.read_error |= error;
        // A withheld possible secret prefix is deliberately never flushed as
        // diagnostic text, including on EOF, cancellation or read failure.
        self.truncated |= self.pending_bytes != 0;
        self.pending_bytes = 0;
    }
}

/// Cloneable read-only access. Dropping a view never affects pipe drainage.
#[derive(Clone)]
pub struct StderrView {
    state: Arc<Mutex<CaptureState>>,
    crash_line_prefix: Option<Arc<str>>,
}

impl StderrView {
    /// The spawn-bound line prefix that authenticates a crash report on this
    /// exact owned pipe, when the owner bound one at spawn.
    pub fn crash_line_prefix(&self) -> Option<&str> {
        self.crash_line_prefix.as_deref()
    }

    pub fn snapshot(&self) -> StderrSnapshot {
        let (bytes, keep_prefix, mut truncated, eof, read_error) = {
            let state = self.state.lock_recover();
            (
                state.bytes.iter().copied().collect::<Vec<_>>(),
                state.keep_prefix,
                state.truncated,
                state.eof,
                state.read_error,
            )
        };
        // Redaction happened on bytes before any lossy conversion. Lossy UTF-8
        // may expand invalid bytes, so enforce the rendered byte cap as well.
        let rendered = String::from_utf8_lossy(&bytes);
        let text = if keep_prefix {
            let mut end = rendered.len().min(PREFIX_BYTES);
            while !rendered.is_char_boundary(end) {
                end -= 1;
            }
            truncated |= end < rendered.len();
            rendered[..end].to_owned()
        } else {
            let mut start = rendered.len().saturating_sub(TAIL_BYTES);
            while !rendered.is_char_boundary(start) {
                start += 1;
            }
            truncated |= start != 0;
            rendered[start..].to_owned()
        };
        StderrSnapshot {
            text,
            truncated,
            eof,
            read_error,
        }
    }
}

/// Sole owner of the drain task. Views cannot clone, finish or abort the task.
pub struct StderrCapture {
    view: StderrView,
    task: Option<JoinHandle<()>>,
}

impl StderrCapture {
    pub fn view(&self) -> StderrView {
        self.view.clone()
    }

    pub fn snapshot(&self) -> StderrSnapshot {
        self.view.snapshot()
    }

    /// Call after child shutdown. On deadline, abort and join the same task;
    /// never detach a still-running drain or expose its private raw carry.
    pub async fn finish(mut self, deadline: Instant) -> StderrSnapshot {
        if let Some(mut task) = self.task.take() {
            match timeout_at(deadline, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => self.mark_failed(),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    self.mark_failed();
                }
            }
        }
        self.snapshot()
    }

    fn mark_failed(&self) {
        self.view.state.lock_recover().end(false, true);
    }
}

impl Drop for StderrCapture {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            // This is the sole task owner's cancellation path, never a view's
            // Drop. Explicit finish is required to await cancellation completion.
            task.abort();
            let mut state = self.view.state.lock_recover();
            if !state.eof {
                state.end(false, true);
            }
        }
    }
}

pub fn spawn_stderr<R>(reader: R, policy: Arc<SecretPolicy>) -> StderrCapture
where
    R: AsyncRead + Unpin + Send + 'static,
{
    spawn_stderr_inner(reader, policy, None, false, None)
}

/// Retain the beginning of sanitized stderr while draining the entire pipe.
pub fn spawn_stderr_prefix<R>(reader: R, policy: Arc<SecretPolicy>) -> StderrCapture
where
    R: AsyncRead + Unpin + Send + 'static,
{
    spawn_stderr_inner(reader, policy, None, true, None)
}

/// Plugin-owned telemetry channel. Filter before the diagnostic tail, including
/// malformed and oversized telemetry lines. Native harness stderr uses the
/// ordinary capture and cannot impersonate plugin telemetry. Crash reports on
/// this pipe count only when they carry `crash_line_prefix`, the per-spawn
/// nonce and owned PID chosen by the process owner.
pub fn spawn_plugin_stderr<R>(
    reader: R,
    policy: Arc<SecretPolicy>,
    telemetry: impl Fn(&str) + Send + Sync + 'static,
    crash_line_prefix: String,
) -> StderrCapture
where
    R: AsyncRead + Unpin + Send + 'static,
{
    spawn_stderr_inner(
        reader,
        policy,
        Some(Box::new(telemetry)),
        false,
        Some(crash_line_prefix.into()),
    )
}

type Telemetry = Box<dyn Fn(&str) + Send + Sync>;

fn spawn_stderr_inner<R>(
    reader: R,
    policy: Arc<SecretPolicy>,
    telemetry: Option<Telemetry>,
    keep_prefix: bool,
    crash_line_prefix: Option<Arc<str>>,
) -> StderrCapture
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let view = StderrView {
        state: Arc::new(Mutex::new(CaptureState {
            keep_prefix,
            ..CaptureState::default()
        })),
        crash_line_prefix,
    };
    let state = Arc::clone(&view.state);
    let task = tokio::spawn(drain(reader, policy, state, telemetry));
    StderrCapture {
        view,
        task: Some(task),
    }
}

async fn drain<R: AsyncRead + Unpin>(
    mut reader: R,
    policy: Arc<SecretPolicy>,
    state: Arc<Mutex<CaptureState>>,
    telemetry: Option<Telemetry>,
) {
    let mut input = [0_u8; READ_BYTES];
    // The raw suffix is local to this task. Its maximum carry is determined by
    // the longest value in the fixed, already-bounded source secret snapshot.
    let mut pending = Vec::new();
    let mut filter = TelemetryFilter::default();
    loop {
        let count = match reader.read(&mut input).await {
            Ok(0) => {
                state.lock_recover().end(true, false);
                return;
            }
            Ok(count) => count,
            Err(_) => {
                state.lock_recover().end(false, true);
                return;
            }
        };
        let filtered;
        let bytes = if let Some(telemetry) = telemetry.as_ref() {
            filtered = filter.push(&input[..count], telemetry.as_ref());
            filtered.as_slice()
        } else {
            &input[..count]
        };
        pending.extend_from_slice(bytes);
        let (sanitized, consumed) = policy.redact_prefix(&pending, false);
        if consumed > pending.len() {
            state.lock_recover().end(false, true);
            return;
        }
        pending.drain(..consumed);
        let mut captured = state.lock_recover();
        captured.append(&sanitized);
        captured.pending_bytes = pending.len();
    }
}

#[derive(Default)]
struct TelemetryFilter {
    line: Vec<u8>,
    ordinary: bool,
    oversized: bool,
}

impl TelemetryFilter {
    fn push(&mut self, bytes: &[u8], telemetry: &dyn Fn(&str)) -> Vec<u8> {
        const PREFIX: &[u8] = b"ELIOT_DEBUG ";
        let mut output = Vec::new();
        for &byte in bytes {
            if self.ordinary {
                output.push(byte);
            } else if !self.oversized {
                self.line.push(byte);
                if self.line.len() <= PREFIX.len() && !PREFIX.starts_with(&self.line) {
                    output.append(&mut self.line);
                    self.ordinary = true;
                } else if self.line.len() > 4110 {
                    self.line.clear();
                    self.oversized = true;
                }
            }
            if byte == b'\n' {
                if !self.ordinary
                    && !self.oversized
                    && let Ok(line) = std::str::from_utf8(&self.line)
                {
                    telemetry(line);
                }
                self.line.clear();
                self.ordinary = false;
                self.oversized = false;
            }
        }
        output
    }
}
