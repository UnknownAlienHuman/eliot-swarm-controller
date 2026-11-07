//! Antigravity native transport: AGY ACP over the shared JSON-RPC client.
//!
//! The stdio NDJSON JSON-RPC transport lives in
//! [`eliot_plugin::native_stdio`]; this module only binds the Antigravity
//! host labels, the ACP response-error mapping, and the notification filter.
//! Only `session/update` notifications are consumed by the event task;
//! server requests and responses always pass through untouched.

use eliot_core::{Error, Result};
use eliot_plugin::native_stdio::{
    NativeJsonRpcErrorBody, NativeStdioConfig, StdioClient, StdioProcess,
};
use serde_json::Value;

#[derive(Clone, Debug)]
pub(crate) struct AgyStdioConfig;

impl NativeStdioConfig for AgyStdioConfig {
    const SERVER: &'static str = "Antigravity ACP server";
    const SHORT: &'static str = "Antigravity";
    const ID_PREFIX: &'static str = "agy";

    fn map_response_error(error: NativeJsonRpcErrorBody) -> Result<Value> {
        // Standard JSON-RPC validation errors prove the request never
        // reached admission. Anything else — internal errors, cancellation,
        // extension codes — leaves an admitted write unruled-out, so it is
        // unknown, never rejected. The native message is never surfaced.
        if matches!(error.code, -32602..=-32600) {
            Err(Error::new("native_rejected", error.message))
        } else {
            Err(Error::new(
                "unknown",
                format!(
                    "Antigravity ACP call failed with native code {}",
                    error.code
                ),
            ))
        }
    }

    /// The exact notification methods the Antigravity event task consumes
    /// today (`session/update` in `events.rs`). Every other well-formed
    /// notification is already ignored there, so dropping it at the pump
    /// preserves semantics while keeping ignored traffic out of the bounded
    /// queue. Server requests and responses always pass through untouched.
    fn consumes_notification(method: &str) -> bool {
        method == "session/update"
    }
}

pub(crate) type NativeClient = StdioClient<AgyStdioConfig>;
pub(crate) type NativeProcess = StdioProcess<AgyStdioConfig>;
pub(crate) use eliot_plugin::native_stdio::{NativeFrame, StreamState};
