//! Shared stdio NDJSON JSON-RPC client for native plugin hosts.
//!
//! Moved verbatim from the Codex plugin's native client (the Muse plugin carried
//! a line-for-line copy with different nouns); the two hosts differ only in the
//! [`NativeStdioConfig`] labels and in how they map a native response error to
//! an outcome. Behaviour is unchanged for both callers.
//!
//! Process ownership, the stdin writer queue, the bounded line pump, output
//! drain and termination live in [`crate::native_transport`] and are shared
//! with the stream-json layer; this module is only the JSON-RPC framing.

use std::{marker::PhantomData, process::ExitStatus};

use serde::Serialize;
use serde_json::{Map, Value};
use tokio::{
    sync::{Semaphore, mpsc, watch},
    time::{Instant, timeout_at},
};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot, StderrSource};

use crate::StderrView;
use crate::native_transport::{
    NativeChild, NativeLabels, NativeWriter, PendingCalls, PumpAction, PumpMode, encode_frame,
    spawn_pumped,
};
use std::sync::Arc;

pub use crate::native_transport::{NativeLaunch, NativeStreamFault, StreamState};

const MAX_PENDING_CALLS: usize = 16;

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

    /// Whether the host semantically consumes a native notification method.
    /// A well-formed notification with any other method is dropped at the pump
    /// (Skip) before the bounded queue, exactly as if the event task ignored it.
    /// Server requests and responses are never filtered. The default keeps every
    /// notification (previous behavior).
    fn consumes_notification(method: &str) -> bool {
        let _ = method;
        true
    }
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

/// Internal startup signal: the owned native endpoint exited after a
/// dispatched start/resume effect, so the operation remains unknown while the
/// daemon settles plugin custody and Agent loss.
pub const NATIVE_ENDPOINT_EXITED: &str = "native_endpoint_exited";

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
    stderr: StderrView,
    policy: SecretPolicy,
    config: PhantomData<C>,
}

pub struct StdioProcess<C> {
    pub transport: NativeChild,
    client: StdioClient<C>,
    frames: Option<mpsc::Receiver<NativeFrame>>,
    stream: watch::Receiver<StreamState>,
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

impl<C: NativeStdioConfig> StdioProcess<C> {
    pub async fn spawn(launch: NativeLaunch, policy: SecretPolicy) -> Result<Self> {
        let labels = NativeLabels {
            short: C::SHORT,
            server: C::SERVER,
        };
        // JSON-RPC: keep nonblocking routing; a pending reply must never
        // wait behind a saturated emit queue.
        let (transport, pending, frame_rx, stream_rx) = spawn_pumped(
            launch,
            policy.clone(),
            labels,
            C::ID_PREFIX,
            PumpMode::LossyOverflow,
            classify_jsonrpc_frame::<C>,
        )
        .await?;
        let client = StdioClient {
            writer: transport.writer(),
            pending,
            pending_slots: Arc::new(Semaphore::new(MAX_PENDING_CALLS)),
            stderr: transport.stderr_view(),
            policy,
            config: PhantomData,
        };

        Ok(Self {
            transport,
            client,
            frames: Some(frame_rx),
            stream: stream_rx,
        })
    }

    /// The actual app-server child PID; birth and custody are verified by the host.
    pub fn pid(&self) -> Option<u32> {
        self.transport.pid()
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
        self.transport.await_output_drained(deadline).await
    }

    pub async fn terminate(&mut self, force: bool, deadline: Instant) -> Result<ExitStatus> {
        self.transport.terminate(force, deadline).await
    }
}

impl<C> Drop for StdioProcess<C> {
    fn drop(&mut self) {
        // Owner drop/cancel: fence the writer+router before the transport's
        // own Drop aborts reader/waiter. In-flight calls resolve Unknown now;
        // fresh writes are refused `native_not_dispatched` by the shared
        // closed flag. No await here; fail_all only drains the pending map.
        self.client.pending.fail_all(Error::new(
            "unknown",
            "native process dropped with an outcome pending",
        ));
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

        let id = self.pending.mint_id()?;
        let encoded = encode_frame(
            C::SERVER,
            &OutboundRequest {
                jsonrpc: "2.0",
                id: id.clone(),
                method,
                params,
            },
        )?;
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

        if self.pending.is_closed() {
            return Err(Error::new(
                "native_not_dispatched",
                format!("{} response router closed before dispatch", C::SHORT),
            ));
        }
        let (_guard, response_rx) = self.pending.insert(id);
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
        let encoded = encode_frame(
            C::SERVER,
            &OutboundNotification {
                jsonrpc: "2.0",
                method,
                params,
            },
        )?;
        self.write_encoded(encoded, deadline).await
    }

    pub async fn close_input(&self, deadline: Instant) -> Result<()> {
        self.writer.close_input(deadline).await
    }

    pub async fn reply_server_request(
        &self,
        id: Value,
        result: Value,
        deadline: Instant,
    ) -> Result<()> {
        let encoded = encode_frame(
            C::SERVER,
            &OutboundResultResponse {
                jsonrpc: "2.0",
                id,
                result,
            },
        )?;
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
        let encoded = encode_frame(
            C::SERVER,
            &OutboundResponse {
                jsonrpc: "2.0",
                id,
                error: NativeRpcError { code, message },
            },
        )?;
        self.write_encoded(encoded, deadline).await
    }

    async fn write_encoded(&self, encoded: String, deadline: Instant) -> Result<()> {
        self.writer
            .send(encoded, deadline)
            .await
            .map_err(|error| self.diagnostic_error(error))
    }
}

fn classify_jsonrpc_frame<C: NativeStdioConfig>(value: Value) -> PumpAction<NativeFrame> {
    if !valid_jsonrpc_version(&value) {
        return PumpAction::Malformed(format!(
            "{} stream contained an invalid JSON-RPC version",
            C::SERVER
        ));
    }

    if let Some(id) = value.get("id").cloned() {
        if value.get("method").is_some() {
            let request: NativeServerRequest = match serde_json::from_value(value) {
                Ok(request) => request,
                Err(_) => {
                    return PumpAction::Malformed(format!(
                        "{} server request did not match its JSON-RPC envelope",
                        C::SHORT
                    ));
                }
            };
            if request.id != id {
                return PumpAction::Malformed(format!(
                    "{} server request id changed while decoding",
                    C::SHORT
                ));
            }
            PumpAction::Emit(NativeFrame::Request {
                id: request.id,
                method: request.method,
                params: request.params.unwrap_or_else(empty_object),
            })
        } else if value.get("result").is_some() || value.get("error").is_some() {
            let response: NativeServerResponse = match serde_json::from_value(value) {
                Ok(response) => response,
                Err(_) => {
                    return PumpAction::Malformed(format!(
                        "{} server response did not match its JSON-RPC envelope",
                        C::SHORT
                    ));
                }
            };
            if response.id != id {
                return PumpAction::Malformed(format!(
                    "{} server response id changed while decoding",
                    C::SHORT
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
            let key = response
                .id
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| serde_json::to_string(&response.id).ok());
            let Some(key) = key else {
                return PumpAction::Skip;
            };
            PumpAction::Route { key, result }
        } else {
            PumpAction::Malformed(format!(
                "{} response omitted both result and error",
                C::SERVER
            ))
        }
    } else {
        if value.get("method").is_none() {
            return PumpAction::Malformed(format!(
                "{} frame was not a notification or response",
                C::SERVER
            ));
        }
        let notification: NativeServerNotification = match serde_json::from_value(value) {
            Ok(notification) => notification,
            Err(_) => {
                return PumpAction::Malformed(format!(
                    "{} notification did not match its JSON-RPC envelope",
                    C::SHORT
                ));
            }
        };
        if !C::consumes_notification(&notification.method) {
            return PumpAction::Skip;
        }
        PumpAction::Emit(NativeFrame::Notification {
            method: notification.method,
            params: notification.params.unwrap_or_else(empty_object),
        })
    }
}

fn valid_jsonrpc_version(value: &Value) -> bool {
    value
        .get("jsonrpc")
        .is_none_or(|version| version.as_str() == Some("2.0"))
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}
