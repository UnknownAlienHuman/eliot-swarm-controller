//! Codex native transport: labels and error mapping over the shared client.
//!
//! The stdio NDJSON JSON-RPC transport lives in
//! [`eliot_plugin::native_stdio`]; this module only binds the Codex host
//! labels, the Codex response-error mapping (every native request error proves
//! non-admission), and the stderr regression test.

use eliot_core::{Error, Result};
use eliot_plugin::native_stdio::{
    NativeJsonRpcErrorBody, NativeStdioConfig, StdioClient, StdioProcess,
};
use serde_json::Value;

#[derive(Clone, Debug)]
pub(crate) struct CodexStdioConfig;

impl NativeStdioConfig for CodexStdioConfig {
    const SERVER: &'static str = "Codex app-server";
    const SHORT: &'static str = "Codex";
    const ID_PREFIX: &'static str = "codex";
    /// Inbound methods the Codex host can legitimately send: the
    /// notifications the event task consumes plus the three server-request
    /// families Eliot answers (`events.rs`, `native/requests.rs`).
    /// Oversized-frame diagnosis records only these exact names.
    const OVERSIZED_KNOWN_METHODS: &'static [&'static str] = &[
        "turn/started",
        "turn/completed",
        "item/started",
        "item/completed",
        "serverRequest/resolved",
        "thread/started",
        "thread/tokenUsage/updated",
        "error",
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
        "item/tool/requestUserInput",
    ];

    fn map_response_error(error: NativeJsonRpcErrorBody) -> Result<Value> {
        let location = if error.code == -32601 {
            "plugin.codex.native.response.method_not_found"
        } else {
            "plugin.codex.native.response"
        };
        Err(Error::new("native_rejected", error.message)
            .with_where(location)
            .with_origin(
                eliot_core::FailureOrigin::Harness,
                "native JSON-RPC refusal; no structured upstream error",
            ))
    }

    /// The exact notification methods the Codex event task consumes today
    /// (`process_notification` / `process_child_notification` arms in `events.rs`,
    /// including `thread/started` child confirmation and the `error` validation
    /// arm). Every other well-formed notification is already ignored there
    /// (`_ => {}`), so dropping it at the pump preserves semantics while keeping
    /// ignored traffic out of the bounded queue. Server requests and responses
    /// always pass through untouched.
    fn consumes_notification(method: &str) -> bool {
        matches!(
            method,
            "turn/started"
                | "turn/completed"
                | "item/started"
                | "item/completed"
                | "serverRequest/resolved"
                | "thread/started"
                | "thread/tokenUsage/updated"
                | "error"
        )
    }
}

pub(crate) type NativeClient = StdioClient<CodexStdioConfig>;
pub(crate) type NativeProcess = StdioProcess<CodexStdioConfig>;
pub(crate) use eliot_plugin::native_stdio::{NativeFrame, NativeLaunch, StreamState};
