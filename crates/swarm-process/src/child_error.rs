//! Bounded stderr forwarding and safe structured error projection for known
//! child processes.

use serde_json::{Map, Value};
use std::{
    io::{self, Read, Write},
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
    time::Instant,
};

const MAX_ERROR_LINE_BYTES: usize = 8 * 1024;
const MAX_WRAPPER_ENVELOPE_BYTES: usize = 8 * 1024;
const STDERR_BUFFER_BYTES: usize = 4 * 1024;
const STDERR_QUEUE_CHUNKS: usize = 16;
const MAX_CODE_BYTES: usize = 96;
const MAX_PHASE_BYTES: usize = 64;
const MAX_SECONDARY_CODE_BYTES: usize = 64;

/// A small, non-authoritative projection of a sibling process failure.
///
/// `message` is present only for the fixed, safe startup/wrapper messages this
/// crate recognizes. The child's original stderr is forwarded unchanged when
/// the bounded output queue and inherited sink keep up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildError {
    pub code: String,
    pub phase: Option<String>,
    pub message: Option<String>,
    pub secondary_codes: Vec<String>,
}

/// Result of reading a sibling's stderr pipe. `child_error` remains available
/// even when the inherited stderr sink cannot keep up with raw forwarding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StderrForwardResult {
    /// True only after the pipe reaches EOF. A descendant retaining the pipe or
    /// a read error leaves this false.
    pub pipe_drained: bool,
    /// True when every read chunk was accepted by the bounded sink queue. A
    /// queue overflow is reported here while the reader continues draining.
    pub forwarding_complete: bool,
    pub child_error: Option<ChildError>,
}

enum SinkCommand {
    Bytes(Vec<u8>),
    Flush(SyncSender<bool>),
    Envelope(Vec<u8>, SyncSender<bool>),
}

/// A bounded, detached stderr sink. Only its worker thread performs potentially
/// blocking writes; readers and wrappers communicate with it through a fixed
/// size queue and deadline-bounded acknowledgements.
#[derive(Clone)]
pub struct BoundedStderrSink {
    sender: SyncSender<SinkCommand>,
}

impl BoundedStderrSink {
    /// Start the single stderr writer used for both raw child output and wrapper
    /// envelopes. Dropping the handle never joins the potentially blocked
    /// writer thread.
    pub fn spawn() -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(STDERR_QUEUE_CHUNKS);
        thread::Builder::new()
            .name("swarm-wrapper-stderr".to_owned())
            .spawn(move || sink_loop(receiver))?;
        Ok(Self { sender })
    }

    /// Enqueue one already-bounded pipe chunk without waiting for the sink.
    fn try_forward_chunk(&self, bytes: &[u8]) -> bool {
        if bytes.is_empty() || bytes.len() > STDERR_BUFFER_BYTES {
            return false;
        }
        matches!(
            self.sender.try_send(SinkCommand::Bytes(bytes.to_vec())),
            Ok(())
        )
    }

    /// Flush preceding child bytes only until the caller's absolute deadline.
    pub fn flush_until(&self, deadline: Instant) -> bool {
        let (ack_sender, ack_receiver) = mpsc::sync_channel(1);
        if !matches!(self.sender.try_send(SinkCommand::Flush(ack_sender)), Ok(())) {
            return false;
        }
        receive_ack(ack_receiver, deadline)
    }

    /// Send one bounded wrapper record through the same writer as raw stderr.
    /// This prevents a direct wrapper write from blocking after child exit.
    pub fn write_envelope_until(&self, envelope: &str, deadline: Instant) -> bool {
        let envelope_bytes = envelope.as_bytes();
        if envelope_bytes.is_empty() || envelope_bytes.len() + 1 > MAX_WRAPPER_ENVELOPE_BYTES {
            return false;
        }
        let mut bytes = Vec::with_capacity(envelope_bytes.len() + 1);
        bytes.extend_from_slice(envelope_bytes);
        bytes.push(b'\n');
        let (ack_sender, ack_receiver) = mpsc::sync_channel(1);
        if !matches!(
            self.sender
                .try_send(SinkCommand::Envelope(bytes, ack_sender)),
            Ok(())
        ) {
            return false;
        }
        receive_ack(ack_receiver, deadline)
    }
}

fn receive_ack(receiver: Receiver<bool>, deadline: Instant) -> bool {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return false;
    }
    matches!(receiver.recv_timeout(remaining), Ok(true))
}

fn sink_loop(receiver: Receiver<SinkCommand>) {
    // This is the only thread that locks or writes inherited stderr. If its
    // sink blocks, the pipe reader can still drain and parse independently.
    let mut available = true;

    while let Ok(command) = receiver.recv() {
        match command {
            SinkCommand::Bytes(bytes) => {
                if available && !write_stderr(&bytes, false) {
                    available = false;
                }
            }
            SinkCommand::Flush(ack) => {
                if available && !write_stderr(&[], true) {
                    available = false;
                }
                let _ = ack.try_send(available);
            }
            SinkCommand::Envelope(bytes, ack) => {
                if available && !write_stderr(&bytes, true) {
                    available = false;
                }
                let _ = ack.try_send(available);
            }
        }
    }
}

fn write_stderr(bytes: &[u8], flush: bool) -> bool {
    let stderr = io::stderr();
    let mut output = stderr.lock();
    output.write_all(bytes).is_ok() && (!flush || output.flush().is_ok())
}

/// Drain a child stderr pipe without allowing inherited stderr backpressure to
/// block the child. A full or disconnected sink queue stops raw forwarding but
/// does not stop parsing or draining the remaining pipe bytes.
pub fn forward_stderr<R: Read>(
    mut reader: R,
    sink: Option<BoundedStderrSink>,
) -> StderrForwardResult {
    let mut forwarding_complete = sink.is_some();
    let mut pipe_drained = false;
    let mut read_buffer = [0_u8; STDERR_BUFFER_BYTES];
    let mut line = Vec::with_capacity(MAX_ERROR_LINE_BYTES);
    let mut line_overflowed = false;
    let mut child_error = None;

    loop {
        let count = match reader.read(&mut read_buffer) {
            Ok(0) => {
                pipe_drained = true;
                break;
            }
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };

        for byte in &read_buffer[..count] {
            if *byte == b'\n' {
                if !line_overflowed {
                    if let Some(valid_error) = parse_child_error_line(&line) {
                        // Keep the last valid terminal-shaped sibling record;
                        // an earlier diagnostic line is only provisional.
                        child_error = Some(valid_error);
                    }
                }
                line.clear();
                line_overflowed = false;
            } else if !line_overflowed {
                if line.len() < MAX_ERROR_LINE_BYTES {
                    line.push(*byte);
                } else {
                    line.clear();
                    line_overflowed = true;
                }
            }
        }

        if forwarding_complete {
            let accepted = match &sink {
                Some(sink) => sink.try_forward_chunk(&read_buffer[..count]),
                None => false,
            };
            if !accepted {
                // Do not wait for a slow sink and do not buffer/spool further
                // chunks. Continue bounded parsing and draining to EOF.
                forwarding_complete = false;
            }
        }
    }

    if pipe_drained && !line_overflowed && !line.is_empty() {
        if let Some(valid_error) = parse_child_error_line(&line) {
            child_error = Some(valid_error);
        }
    }

    StderrForwardResult {
        pipe_drained,
        forwarding_complete,
        child_error,
    }
}

/// Serialize the wrapper's stable error while optionally including a validated
/// sibling projection and the child's actual process exit code.
pub fn wrapper_error_json(
    code: &str,
    message: &str,
    include_exit_code: bool,
    exit_code: Option<i32>,
    child_error: Option<&ChildError>,
) -> String {
    let mut error = Map::new();
    error.insert("code".to_owned(), Value::String(code.to_owned()));
    error.insert("message".to_owned(), Value::String(message.to_owned()));
    if include_exit_code {
        error.insert(
            "exit_code".to_owned(),
            exit_code.map_or(Value::Null, |code| Value::from(code)),
        );
    }
    if let Some(child_error) = child_error {
        error.insert("child_error".to_owned(), child_error_value(child_error));
    }
    let mut envelope = Map::new();
    envelope.insert("error".to_owned(), Value::Object(error));
    Value::Object(envelope).to_string()
}

fn child_error_value(child_error: &ChildError) -> Value {
    let mut error = Map::new();
    error.insert("code".to_owned(), Value::String(child_error.code.clone()));
    if let Some(phase) = &child_error.phase {
        error.insert("phase".to_owned(), Value::String(phase.clone()));
    }
    if let Some(message) = &child_error.message {
        error.insert("message".to_owned(), Value::String(message.clone()));
    }
    if !child_error.secondary_codes.is_empty() {
        error.insert(
            "secondary_codes".to_owned(),
            Value::Array(
                child_error
                    .secondary_codes
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    Value::Object(error)
}

/// Project one bounded structured diagnostic without retaining raw messages.
/// Callers draining async child pipes can reuse the wrapper's safe code parser.
pub fn project_child_error_line(line: &[u8]) -> Option<ChildError> {
    if line.is_empty() || line.len() > MAX_ERROR_LINE_BYTES {
        return None;
    }
    parse_child_error_line(line)
}

fn parse_child_error_line(line: &[u8]) -> Option<ChildError> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let envelope: Value = serde_json::from_slice(line).ok()?;
    let envelope = envelope.as_object()?;
    if envelope.len() != 1 {
        return None;
    }
    let error = envelope.get("error")?.as_object()?;

    if let Some(child_error) = error.get("child_error") {
        if let Some(child_error) = parse_projected_child_error(child_error) {
            return Some(child_error);
        }
    }
    parse_sibling_error(error)
}

fn parse_projected_child_error(value: &Value) -> Option<ChildError> {
    let value = value.as_object()?;
    if value.keys().any(|key| {
        !matches!(
            key.as_str(),
            "code" | "phase" | "message" | "secondary_codes"
        )
    }) {
        return None;
    }
    parse_error_fields(value, true)
}

fn parse_sibling_error(error: &Map<String, Value>) -> Option<ChildError> {
    if error.keys().any(|key| {
        !matches!(
            key.as_str(),
            "code"
                | "message"
                | "phase"
                | "exit_code"
                | "rejection_class"
                | "native_http_failure"
                | "secondary_codes"
                | "child_error"
        )
    }) {
        return None;
    }
    parse_error_fields(error, false)
}

fn parse_error_fields(error: &Map<String, Value>, projected: bool) -> Option<ChildError> {
    let code = error.get("code")?.as_str()?;
    if !valid_code(code) {
        return None;
    }

    let safe_message = error
        .get("message")
        .and_then(Value::as_str)
        .and_then(known_safe_message);
    let (mut phase, mut message) =
        safe_message.map_or((None, None), |(phase, message)| (phase, Some(message)));

    let phase_matches = match error.get("phase") {
        Some(Value::String(explicit_phase)) => {
            valid_phase(explicit_phase) && phase.as_deref() == Some(explicit_phase.as_str())
        }
        Some(_) => false,
        None => !projected || phase.is_none(),
    };
    if !phase_matches {
        // Phase and message are optional evidence; malformed or inconsistent
        // evidence must not discard an otherwise valid machine code.
        phase = None;
        message = None;
    }

    let secondary_codes = parse_secondary_codes(error).unwrap_or_default();

    Some(ChildError {
        code: code.to_owned(),
        phase,
        message,
        secondary_codes,
    })
}

fn parse_secondary_codes(error: &Map<String, Value>) -> Option<Vec<String>> {
    let Some(value) = error.get("secondary_codes") else {
        return Some(Vec::new());
    };
    let codes = value.as_array()?;
    if codes.len() > 2 {
        return None;
    }
    codes
        .iter()
        .map(|code| {
            let code = code.as_str()?;
            valid_secondary_code(code).then(|| code.to_owned())
        })
        .collect()
}

fn valid_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= MAX_CODE_BYTES
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        && code.bytes().any(|byte| byte.is_ascii_uppercase())
}

fn valid_phase(phase: &str) -> bool {
    !phase.is_empty()
        && phase.len() <= MAX_PHASE_BYTES
        && matches!(phase, "data_root" | "credential_bootstrap" | "store_start")
}

fn valid_secondary_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= MAX_SECONDARY_CODE_BYTES
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn known_safe_message(message: &str) -> Option<(Option<String>, String)> {
    const STARTUP_PREFIX: &str = "host startup failed at ";
    const STARTUP_SUFFIX: &str = "; inspect that stage before starting the host again";

    if let Some(phase) = message
        .strip_prefix(STARTUP_PREFIX)
        .and_then(|message| message.strip_suffix(STARTUP_SUFFIX))
        .filter(|phase| valid_phase(phase))
    {
        return Some((Some(phase.to_owned()), message.to_owned()));
    }

    matches!(
        message,
        "the relocated kernel host exited unsuccessfully"
            | "the explicit host command exited unsuccessfully"
            | "could not locate the public host executable"
            | "install the swarm-kernel-host sibling beside swarm-host and retry"
            | "could not locate the public CLI executable"
            | "install the swarm-host sibling beside the public swarm executable and retry"
            | "check the host executable, runtime dependencies and launch permissions, then retry"
    )
    .then(|| (None, message.to_owned()))
}
