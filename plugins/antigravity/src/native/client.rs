//! Antigravity native transport: AGY ACP over the shared child transport.
//!
//! The process, pump and correlation machinery live in
//! [`eliot_plugin::native_transport`]; this module is the ACP framing on
//! top, following the Claude plugin's custom-client precedent instead of
//! the shared JSON-RPC client. The one protocol-specific piece is the
//! **fenced prompt call**: a `session/prompt` response is the turn's
//! terminal fact, so it must be processed behind every `session/update`
//! that preceded it on the wire. A normal correlated reply would route
//! straight to the awaiting task and could settle the turn before queued
//! updates are consumed (dropping pending text and misattributing late
//! chunks). Fenced replies therefore re-enter the ordered frame channel
//! as [`AgyFrame::PromptResponse`] and are consumed by the same event
//! task, in wire order. Ordinary calls keep the nonblocking
//! lossy-overflow reply contract: routing never waits behind the emit
//! queue, and an overflow latches the existing stream-fault gap.

use std::{
    collections::HashMap,
    process::ExitStatus,
    sync::{Arc, Mutex as StdMutex},
};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot, StderrSource};
use eliot_plugin::{
    StderrView,
    native_transport::{
        DEFAULT_MAX_NATIVE_FRAME_BYTES, NativeChild, NativeLabels, NativeLaunch, NativeStreamFault,
        NativeWriter, PendingCalls, PendingGuard, PumpAction, PumpMode, encode_frame, spawn_pumped,
    },
};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, timeout_at},
};

const LABELS: NativeLabels = NativeLabels {
    short: "Antigravity",
    server: "Antigravity ACP server",
};
pub(crate) use eliot_plugin::native_transport::StreamState;
const ID_PREFIX: &str = "agy";

/// Which ordered fence a fenced reply belongs to. The classifier emits
/// the frame variant matching the pending call's kind, so a load reply
/// can never be mistaken for a prompt terminal (or vice versa).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FencedKind {
    Prompt,
    Load,
}

/// One ACP stream record for the event task, in wire order.
#[derive(Debug)]
pub(crate) enum AgyFrame {
    /// Ordered record-local fault, behind every retained pre-fault frame.
    StreamFault(NativeStreamFault),
    /// A `session/update` notification body.
    Update(Value),
    /// The terminal reply of a fenced `session/prompt` call.
    PromptResponse(Result<Value>),
    /// The reply of a fenced `session/load` call: everything before it
    /// on the wire is validated load history, everything after is live.
    LoadResponse {
        request_id: String,
        result: Result<Value>,
    },
    /// An agent-to-client request (permission, terminal, fs, elicitation).
    ServerRequest {
        id: Value,
        method: String,
        params: Value,
    },
}

/// RAII registration for one fenced prompt call. Exactly one owner (the
/// event state) holds it from dispatch until the terminal frame,
/// expiry, stream loss, or drop retires it; dropping removes both the
/// pending-map entry (via [`PendingGuard`]) and the fenced id. Never
/// `mem::forget`-ed: the reply channel receiver is dropped at dispatch
/// because the reply travels as an ordered frame instead.
pub(crate) struct FencedGuard {
    /// Held for its `Drop` only: removes the pending-map entry.
    _guard: PendingGuard,
    fenced: Arc<StdMutex<HashMap<String, FencedKind>>>,
    id: String,
}

/// A locally registered fenced call whose native write has not started.
/// The caller can arm the corresponding event state with `request_id`
/// before dispatching it.
pub(crate) struct PreparedFencedCall {
    id: String,
    method: String,
    encoded: String,
    registration: FencedGuard,
}

impl PreparedFencedCall {
    pub fn request_id(&self) -> &str {
        &self.id
    }
}

impl Drop for FencedGuard {
    fn drop(&mut self) {
        self.fenced
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

#[derive(Clone)]
pub(crate) struct AgyClient {
    writer: NativeWriter,
    pending: PendingCalls,
    fenced: Arc<StdMutex<HashMap<String, FencedKind>>>,
    stderr: StderrView,
    policy: SecretPolicy,
}

pub(crate) struct AgyProcess {
    pub transport: NativeChild,
    client: AgyClient,
    frames: Option<mpsc::Receiver<AgyFrame>>,
    stream: watch::Receiver<StreamState>,
}

/// Inbound ACP methods the server can legitimately send: the
/// `session/update` notification Eliot consumes (agent-to-client requests
/// are rejected without answering, so their methods are never recorded).
/// Oversized-frame diagnosis records only these exact names.
const OVERSIZED_KNOWN_METHODS: &[&str] = &[
    "initialize",
    "session/new",
    "session/load",
    "session/prompt",
    "session/update",
    "session/cancel",
    "session/close",
];

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
struct NativeRpcError {
    code: i64,
    message: &'static str,
}

impl AgyProcess {
    pub async fn spawn(launch: NativeLaunch, policy: SecretPolicy) -> Result<Self> {
        let fenced = Arc::new(StdMutex::new(HashMap::new()));
        let fenced_classify = Arc::clone(&fenced);
        // JSON-RPC: keep nonblocking routing; a pending reply must never
        // wait behind a saturated emit queue.
        let (transport, pending, frame_rx, stream_rx) = spawn_pumped(
            launch,
            policy.clone(),
            LABELS,
            ID_PREFIX,
            PumpMode::LossyOverflow,
            DEFAULT_MAX_NATIVE_FRAME_BYTES,
            OVERSIZED_KNOWN_METHODS,
            move |value| classify_agy_frame(value, &fenced_classify),
            Some(AgyFrame::StreamFault),
        )
        .await?;
        let client = AgyClient {
            writer: transport.writer(),
            pending,
            fenced,
            stderr: transport.stderr_view(),
            policy,
        };
        Ok(Self {
            transport,
            client,
            frames: Some(frame_rx),
            stream: stream_rx,
        })
    }

    /// The actual ACP child PID; birth and custody are verified by the host.
    pub fn pid(&self) -> Option<u32> {
        self.transport.pid()
    }

    pub fn client(&self) -> AgyClient {
        self.client.clone()
    }

    pub fn take_frames(&mut self) -> Result<mpsc::Receiver<AgyFrame>> {
        self.frames.take().ok_or_else(|| {
            Error::new(
                "bad_state",
                "Antigravity native event reader already has an owner",
            )
        })
    }

    pub fn stream_state(&self) -> watch::Receiver<StreamState> {
        self.stream.clone()
    }

    pub async fn terminate(&mut self, force: bool, deadline: Instant) -> Result<ExitStatus> {
        self.transport.terminate(force, deadline).await
    }
}

impl AgyClient {
    /// One correlated ACP request/response round trip.
    pub async fn call(&self, method: &str, params: Value, deadline: Instant) -> Result<Value> {
        self.call_inner(method, params, deadline)
            .await
            .map_err(|error| self.diagnostic_error(error))
    }

    /// One ACP notification (no reply is expected or awaited).
    pub async fn notify(
        &self,
        method: &str,
        params: Option<Value>,
        deadline: Instant,
    ) -> Result<()> {
        if method.trim().is_empty() || params.as_ref().is_some_and(|value| !value.is_object()) {
            return Err(Error::new(
                "invalid",
                "Antigravity notification has invalid method or params",
            ));
        }
        let encoded = encode_frame(
            LABELS.server,
            &OutboundNotification {
                jsonrpc: "2.0",
                method,
                params,
            },
        )?;
        self.write_encoded(encoded, deadline).await
    }

    /// Dispatch one request whose reply must be consumed in wire order
    /// behind preceding stream records. Returns the request id plus the
    /// RAII registration guard; the reply arrives as the matching
    /// [`AgyFrame`] variant on the event channel.
    pub async fn call_fenced(
        &self,
        method: &str,
        params: Value,
        deadline: Instant,
        kind: FencedKind,
    ) -> Result<(String, FencedGuard)> {
        let prepared = self.prepare_fenced(method, params, kind)?;
        self.dispatch_fenced(prepared, deadline).await
    }

    /// Reserve the exact request id and its ordered response route without
    /// writing to the native process. Load callers use this to arm the
    /// event-state filter before any `session/load` I/O.
    pub fn prepare_fenced(
        &self,
        method: &str,
        params: Value,
        kind: FencedKind,
    ) -> Result<PreparedFencedCall> {
        if method.trim().is_empty() || !params.is_object() {
            return Err(Error::new(
                "invalid",
                "Antigravity fenced call requires a method and object params",
            ));
        }
        let id = self.pending.mint_id()?;
        let encoded = encode_frame(
            LABELS.server,
            &OutboundRequest {
                jsonrpc: "2.0",
                id: id.clone(),
                method,
                params,
            },
        )?;
        self.fenced
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.clone(), kind);
        let (guard, _receiver) = self.pending.insert(id.clone());
        let registration = FencedGuard {
            _guard: guard,
            fenced: Arc::clone(&self.fenced),
            id: id.clone(),
        };
        Ok(PreparedFencedCall {
            id,
            method: method.to_owned(),
            encoded,
            registration,
        })
    }

    /// Start a previously registered fenced call's native write.
    pub async fn dispatch_fenced(
        &self,
        prepared: PreparedFencedCall,
        deadline: Instant,
    ) -> Result<(String, FencedGuard)> {
        let PreparedFencedCall {
            id,
            method,
            encoded,
            registration,
        } = prepared;
        match self.write_encoded(encoded, deadline).await {
            Ok(()) => Ok((id, registration)),
            Err(error) => {
                // `registration` drops here and retires both maps.
                if error.code == "native_not_dispatched" {
                    return Err(error);
                }
                Err(Error::new(
                    "unknown",
                    format!("Antigravity {method} dispatch outcome is unknown"),
                ))
            }
        }
    }

    /// Reject one agent-to-client request as unsupported. There is no
    /// approve/answer channel on this route, so every such request is an
    /// explicit rejection, never an auto-approval.
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
        let encoded = encode_frame(
            LABELS.server,
            &OutboundResponse {
                jsonrpc: "2.0",
                id,
                error: NativeRpcError { code, message },
            },
        )?;
        self.write_encoded(encoded, deadline).await
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
                "Antigravity call requires a method and object params",
            ));
        }
        let id = self.pending.mint_id()?;
        let encoded = encode_frame(
            LABELS.server,
            &OutboundRequest {
                jsonrpc: "2.0",
                id: id.clone(),
                method,
                params,
            },
        )?;
        let (_guard, response_rx) = self.pending.insert(id);
        if let Err(error) = self.write_encoded(encoded, deadline).await {
            if error.code == "native_not_dispatched" {
                return Err(error);
            }
            return Err(Error::new(
                "unknown",
                format!("Antigravity {method} dispatch outcome is unknown"),
            ));
        }
        timeout_at(deadline, response_rx)
            .await
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("Antigravity {method} response is unknown"),
                )
            })?
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("Antigravity {method} response channel closed"),
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

/// Map a native JSON-RPC error body to a call outcome: standard validation
/// errors prove non-admission; anything else leaves an admitted write
/// unruled-out and stays unknown. The message is never surfaced.
fn map_response_error(code: i64, message: String) -> Result<Value> {
    if matches!(code, -32602..=-32600) {
        Err(Error::new("native_rejected", message))
    } else {
        Err(Error::new(
            "unknown",
            format!("Antigravity ACP call failed with native code {code}"),
        ))
    }
}

fn classify_agy_frame(
    value: Value,
    fenced: &Arc<StdMutex<HashMap<String, FencedKind>>>,
) -> PumpAction<AgyFrame> {
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return PumpAction::Malformed(
            "Antigravity stream contained an invalid JSON-RPC version".to_owned(),
        );
    }
    if let Some(id) = value.get("id").cloned() {
        if value.get("method").is_some() {
            let method = value
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            return PumpAction::Emit(AgyFrame::ServerRequest {
                id,
                method,
                params: value.get("params").cloned().unwrap_or(Value::Null),
            });
        }
        let result = match (value.get("result").cloned(), value.get("error").cloned()) {
            (Some(result), None) => Ok(result),
            (None, Some(error)) => match (
                error.get("code").and_then(Value::as_i64),
                error.get("message").and_then(Value::as_str),
            ) {
                (Some(code), Some(message)) => map_response_error(code, message.to_owned()),
                _ => Err(Error::new(
                    "unknown",
                    "Antigravity ACP error response did not match its envelope",
                )),
            },
            _ => {
                return PumpAction::Malformed(
                    "Antigravity ACP response did not contain exactly one result or error"
                        .to_owned(),
                );
            }
        };
        let key = id
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| serde_json::to_string(&id).ok());
        let Some(key) = key else {
            return PumpAction::Skip;
        };
        let kind = fenced
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key);
        if let Some(kind) = kind {
            return PumpAction::Emit(match kind {
                FencedKind::Prompt => AgyFrame::PromptResponse(result),
                FencedKind::Load => AgyFrame::LoadResponse {
                    request_id: key,
                    result,
                },
            });
        }
        return PumpAction::Route { key, result };
    }
    match value.get("method").and_then(Value::as_str) {
        Some("session/update") => PumpAction::Emit(AgyFrame::Update(
            value.get("params").cloned().unwrap_or(Value::Null),
        )),
        Some(_) => PumpAction::Skip,
        None => {
            PumpAction::Malformed("Antigravity frame was not a notification or response".to_owned())
        }
    }
}
