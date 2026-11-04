//! Claude native transport: stream-json framing over the shared child transport.
//!
//! Unlike the JSON-RPC hosts, `claude -p` speaks plain typed NDJSON records.
//! `control_request` writes correlate with `control_response` records by
//! `request_id`; every other record flows to the event task. Process,
//! stderr and drain semantics are the shared
//! [`eliot_plugin::native_transport`] ones: continuous stderr drainage with a
//! redacted 16 KiB tail, a 1 MiB frame bound, and output drained before any
//! loss conclusion.

use std::process::ExitStatus;

use serde_json::Value;
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, timeout_at},
};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot, StderrSource};
use eliot_plugin::{
    StderrView,
    native_transport::{
        NativeChild, NativeLabels, NativeLaunch, NativeWriter, PendingCalls, PumpAction,
        StreamState, encode_frame, native_frame_channel, pump_framed_lines,
    },
};

use crate::native::types::{ControlResponse, OutboundControlRequest, OutboundUserInput};

const LABELS: NativeLabels = NativeLabels {
    short: "Claude",
    server: "Claude",
};
const ID_PREFIX: &str = "claude";

/// One non-response stream record for the event task.
pub(crate) enum ClaudeFrame {
    Record(Value),
}

#[derive(Clone)]
pub(crate) struct ClaudeClient {
    writer: NativeWriter,
    pending: PendingCalls,
    stderr: StderrView,
    policy: SecretPolicy,
}

pub(crate) struct ClaudeProcess {
    pub transport: NativeChild,
    client: ClaudeClient,
    frames: Option<mpsc::Receiver<ClaudeFrame>>,
    stream: watch::Receiver<StreamState>,
}

impl ClaudeProcess {
    pub async fn spawn(launch: NativeLaunch, policy: SecretPolicy) -> Result<Self> {
        let (mut transport, stdout) = NativeChild::spawn(launch, policy.clone(), LABELS).await?;
        let pending = PendingCalls::new(ID_PREFIX, LABELS);
        let client = ClaudeClient {
            writer: transport.writer(),
            pending: pending.clone(),
            stderr: transport.stderr_view(),
            policy,
        };
        let (frame_tx, frame_rx) = native_frame_channel();
        let (stream_tx, stream_rx) = watch::channel(StreamState::Open);
        let output_reader = tokio::spawn(pump_framed_lines(
            stdout,
            frame_tx,
            stream_tx,
            pending,
            LABELS.server,
            classify_claude_frame,
        ));
        transport.set_output_reader(output_reader);

        Ok(Self {
            transport,
            client,
            frames: Some(frame_rx),
            stream: stream_rx,
        })
    }

    /// The actual CLI child PID; birth and custody are verified by the host.
    pub fn pid(&self) -> Option<u32> {
        self.transport.pid()
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
        self.transport.await_output_drained(deadline).await
    }

    /// Drop stdin so `claude -p` sees EOF and exits, then end the owned
    /// tree. The kill path only fires when the graceful close cannot be
    /// confirmed.
    pub async fn terminate(&mut self, force: bool, deadline: Instant) -> Result<ExitStatus> {
        self.transport
            .terminate_dropping_input(force, deadline)
            .await
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
        let encoded = encode_frame(LABELS.server, &input)?;
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
        let id = self.pending.mint_id()?;
        let encoded = encode_frame(
            LABELS.server,
            &OutboundControlRequest {
                kind: "control_request",
                request_id: id.clone(),
                request,
            },
        )?;

        let (_guard, response_rx) = self.pending.insert(id);
        if let Err(error) = self.write_encoded(encoded, deadline).await {
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

    async fn write_encoded(&self, encoded: String, deadline: Instant) -> Result<()> {
        self.writer
            .send(encoded, deadline)
            .await
            .map_err(|error| self.diagnostic_error(error))
    }
}

fn classify_claude_frame(value: Value) -> PumpAction<ClaudeFrame> {
    let Some(kind) = value.get("type").and_then(Value::as_str) else {
        return PumpAction::Malformed("Claude stream record omitted its type".to_owned());
    };
    if kind == "control_response" {
        let response: ControlResponse = match serde_json::from_value(value) {
            Ok(response) => response,
            Err(_) => {
                return PumpAction::Malformed(
                    "Claude control_response did not match its envelope".to_owned(),
                );
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
        return PumpAction::Route {
            key: response.response.request_id,
            result,
        };
    }
    PumpAction::Emit(ClaudeFrame::Record(value))
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
        let launch = NativeLaunch {
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
        assert!(native.transport.stderr.is_none());
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
