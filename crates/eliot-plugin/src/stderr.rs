//! One bounded diagnostic tail for an owned child pipe.
//!
//! Uses the existing Tokio child-pipe primitive from the donor map. Unlike the
//! finite Git capture, reaching this tail's cap never stops pipe drainage.

use crate::sync::LockExt;
pub use eliot_core::StderrSnapshot;
use eliot_core::{MAX_STDERR_BYTES, SecretPolicy};
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
const READ_BYTES: usize = 8192;

#[derive(Default)]
struct TailState {
    // Only bytes already emitted by the shared policy are retained here.
    bytes: VecDeque<u8>,
    truncated: bool,
    eof: bool,
    read_error: bool,
    pending_bytes: usize,
}

impl TailState {
    fn append(&mut self, sanitized: &[u8]) {
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
    state: Arc<Mutex<TailState>>,
}

impl StderrView {
    pub fn snapshot(&self) -> StderrSnapshot {
        let (bytes, mut truncated, eof, read_error) = {
            let state = self.state.lock_recover();
            (
                state.bytes.iter().copied().collect::<Vec<_>>(),
                state.truncated,
                state.eof,
                state.read_error,
            )
        };
        // Redaction happened on bytes before any lossy conversion. Lossy UTF-8
        // may expand invalid bytes, so enforce the rendered byte cap as well.
        let rendered = String::from_utf8_lossy(&bytes);
        let mut start = rendered.len().saturating_sub(TAIL_BYTES);
        while !rendered.is_char_boundary(start) {
            start += 1;
        }
        truncated |= start != 0;
        StderrSnapshot {
            text: rendered[start..].to_owned(),
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
    spawn_stderr_inner(reader, policy, None)
}

/// Plugin-owned telemetry channel. Filter before the diagnostic tail, including
/// malformed and oversized telemetry lines. Native harness stderr uses the
/// ordinary capture and cannot impersonate plugin telemetry.
pub fn spawn_plugin_stderr<R>(
    reader: R,
    policy: Arc<SecretPolicy>,
    telemetry: impl Fn(&str) + Send + Sync + 'static,
) -> StderrCapture
where
    R: AsyncRead + Unpin + Send + 'static,
{
    spawn_stderr_inner(reader, policy, Some(Box::new(telemetry)))
}

type Telemetry = Box<dyn Fn(&str) + Send + Sync>;

fn spawn_stderr_inner<R>(
    reader: R,
    policy: Arc<SecretPolicy>,
    telemetry: Option<Telemetry>,
) -> StderrCapture
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let view = StderrView {
        state: Arc::new(Mutex::new(TailState::default())),
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
    state: Arc<Mutex<TailState>>,
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
        let mut tail = state.lock_recover();
        tail.append(&sanitized);
        tail.pending_bytes = pending.len();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };
    use tokio::io::{AsyncWriteExt, ReadBuf, duplex};

    #[tokio::test]
    async fn plugin_telemetry_is_routed_and_never_retained() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::clone(&records);
        let input = format!(
            "before\nELIOT_DEBUG {{\"code\":\"plugin.frame\",\"fields\":{{\"bytes\":7}}}}\nELIOT_DEBUG malformed\nELIOT_DEBUG {}\nafter\n",
            "x".repeat(6000)
        );
        let capture = spawn_plugin_stderr(
            io::Cursor::new(input.into_bytes()),
            policy(&[]),
            move |line| {
                if let Some(record) = crate::parse_debug_line(line) {
                    received.lock().unwrap().push(record);
                }
            },
        );
        let snapshot = capture
            .finish(Instant::now() + Duration::from_secs(5))
            .await;
        assert_eq!(snapshot.text, "before\nafter\n");
        assert_eq!(records.lock().unwrap().len(), 1);
        let mut filter = TelemetryFilter::default();
        assert!(filter.push(b"ELIOT_", &|_| {}).is_empty());
        assert!(filter.push(b"DEBUG broken\n", &|_| {}).is_empty());
    }

    fn policy(values: &[&str]) -> Arc<SecretPolicy> {
        Arc::new(SecretPolicy::new(
            values
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>(),
        ))
    }

    #[tokio::test]
    async fn overflow_keeps_draining_to_eof() {
        let input: Vec<u8> = (0..TAIL_BYTES * 3 + 17)
            .map(|i| b'a' + (i % 26) as u8)
            .collect();
        let expected = input[input.len() - TAIL_BYTES..].to_vec();
        let (mut writer, reader) = duplex(128);
        let capture = spawn_stderr(reader, policy(&[]));
        let view = capture.view();
        drop(view.clone());
        let written = tokio::spawn(async move {
            writer.write_all(&input).await.expect("all bytes drained");
            writer.shutdown().await.expect("writer EOF");
        });
        let snapshot = capture
            .finish(Instant::now() + Duration::from_secs(5))
            .await;
        written.await.expect("writer task");
        assert_eq!(snapshot.text.as_bytes(), expected);
        assert!(snapshot.truncated && snapshot.eof);
        assert!(!snapshot.read_error);
        assert_eq!(view.snapshot(), snapshot);
    }

    #[tokio::test]
    async fn secret_crosses_read_and_retention_boundaries() {
        let secret = "credential-value";
        let prefix = "x".repeat(READ_BYTES - 3);
        let suffix = "z".repeat(TAIL_BYTES - 5);
        let input = format!("{prefix}{secret}{suffix}");
        let sanitized = format!("{prefix}[REDACTED]{suffix}");
        let expected = &sanitized[sanitized.len() - TAIL_BYTES..];
        let capture = spawn_stderr(io::Cursor::new(input.into_bytes()), policy(&[secret]));
        let snapshot = capture
            .finish(Instant::now() + Duration::from_secs(5))
            .await;
        assert_eq!(snapshot.text, expected);
        assert!(!snapshot.text.contains("value"));
        assert!(snapshot.truncated && snapshot.eof && !snapshot.read_error);
    }

    #[tokio::test]
    async fn unicode_secret_is_masked_before_lossy_conversion() {
        let secret = "s🔐ecret";
        let prefix = "x".repeat(READ_BYTES - 2);
        let input = format!("{prefix}{secret} done");
        let capture = spawn_stderr(io::Cursor::new(input.into_bytes()), policy(&[secret]));
        let snapshot = capture
            .finish(Instant::now() + Duration::from_secs(5))
            .await;
        assert_eq!(snapshot.text, format!("{prefix}[REDACTED] done"));
        assert!(snapshot.eof && !snapshot.read_error);
    }

    #[tokio::test]
    async fn pending_secret_prefix_is_never_visible_even_at_eof() {
        let (mut writer, reader) = duplex(64);
        let capture = spawn_stderr(reader, policy(&["token123"]));
        let view = capture.view();
        writer
            .write_all(b"before tok")
            .await
            .expect("write partial secret");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let pending = { view.state.lock().unwrap().pending_bytes };
                if pending != 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("prefix processed");
        let partial = view.snapshot();
        assert_eq!(partial.text, "before ");
        assert!(!partial.eof && !partial.read_error);
        writer.shutdown().await.expect("EOF");
        let snapshot = capture
            .finish(Instant::now() + Duration::from_secs(2))
            .await;
        assert_eq!(snapshot.text, "before ");
        assert!(snapshot.truncated && snapshot.eof && !snapshot.read_error);
    }

    #[tokio::test]
    async fn finish_deadline_aborts_and_joins_without_affecting_views() {
        let (mut writer, reader) = duplex(64);
        let capture = spawn_stderr(reader, policy(&[]));
        let view = capture.view();
        let snapshot = capture.finish(Instant::now()).await;
        assert!(!snapshot.eof && snapshot.read_error);
        assert_eq!(view.snapshot(), snapshot);
        assert!(writer.write_all(b"reader closed").await.is_err());
    }

    #[tokio::test]
    async fn lossy_utf8_expansion_still_respects_the_rendered_cap() {
        let capture = spawn_stderr(io::Cursor::new(vec![0xff; TAIL_BYTES * 2]), policy(&[]));
        let snapshot = capture
            .finish(Instant::now() + Duration::from_secs(2))
            .await;
        assert!(snapshot.text.len() <= TAIL_BYTES);
        assert!(snapshot.truncated && snapshot.eof && !snapshot.read_error);
    }

    #[tokio::test]
    async fn short_secret_expansion_is_bounded_after_redaction() {
        let input = vec![b'a'; READ_BYTES * 3];
        let capture = spawn_stderr(io::Cursor::new(input), policy(&["a"]));
        let snapshot = capture
            .finish(Instant::now() + Duration::from_secs(2))
            .await;
        assert_eq!(snapshot.text.len(), TAIL_BYTES);
        assert!(!snapshot.text.contains('a'));
        assert!(snapshot.truncated && snapshot.eof && !snapshot.read_error);
    }

    struct BrokenReader;

    impl AsyncRead for BrokenReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("read failed")))
        }
    }

    #[tokio::test]
    async fn read_failure_is_explicit_without_claiming_eof() {
        let capture = spawn_stderr(BrokenReader, policy(&[]));
        let snapshot = capture
            .finish(Instant::now() + Duration::from_secs(2))
            .await;
        assert!(!snapshot.eof && snapshot.read_error);
        assert!(snapshot.text.is_empty());
    }
}
