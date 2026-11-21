//! Muse native transport: labels and error mapping over the shared client.
//!
//! The stdio NDJSON JSON-RPC transport lives in
//! [`eliot_plugin::native_stdio`]; this module only binds the Muse host
//! labels, the Muse response-error mapping (only pre-admission/validation
//! errors prove non-admission), and the stderr regression test.

use eliot_core::{Error, Result};
use eliot_plugin::native_stdio::{
    NativeJsonRpcErrorBody as SharedErrorBody, NativeStdioConfig, StdioClient, StdioProcess,
};
use serde_json::Value;

use crate::native::types::is_not_admitted;

#[derive(Clone, Debug)]
pub(crate) struct MuseStdioConfig;

impl MuseStdioConfig {
    const NATIVE_ERROR_PREFIX: &'static str = "Muse request error ";
    const NATIVE_ERROR_SUFFIX: &'static str = " left its write outcome uncertain";
}

/// The numeric native JSON-RPC error code carried by an uncertain-call error,
/// when the error is a native error response (transport failures and timeouts
/// carry none). Integers only: the native message itself is never surfaced.
pub(crate) fn native_error_code(error: &Error) -> Option<i64> {
    error
        .message
        .strip_prefix(MuseStdioConfig::NATIVE_ERROR_PREFIX)?
        .strip_suffix(MuseStdioConfig::NATIVE_ERROR_SUFFIX)?
        .parse()
        .ok()
}

impl NativeStdioConfig for MuseStdioConfig {
    const SERVER: &'static str = "Muse session host";
    const SHORT: &'static str = "Muse";
    const ID_PREFIX: &'static str = "muse";
    /// Inbound methods the Muse host can legitimately send: the
    /// notifications the event task consumes plus the two server-request
    /// families Eliot answers (`events.rs`, `native/requests.rs`).
    /// Oversized-frame diagnosis records only these exact names.
    const OVERSIZED_KNOWN_METHODS: &'static [&'static str] = &[
        "turn/started",
        "turn/completed",
        "item/started",
        "item/updated",
        "item/completed",
        "approval/resolved",
        "approval/updated",
        "userInput/settled",
        "session/started",
        "session/closed",
        "session/viewHealthChanged",
        "session/reasoningEffortChanged",
        "view/gap",
        "approval/request",
        "userInput/request",
    ];

    fn map_response_error(error: SharedErrorBody) -> Result<Value> {
        // Schema errors table: only pre-admission/validation errors prove the
        // command was not admitted. Anything else — `interrupted`/`cancelled`,
        // `-32603` internal, all other MSP kinds, unknown codes — leaves an
        // admitted write unruled-out, so it is unknown, never rejected.
        if is_not_admitted(error.code) {
            let location = if error.code == -32601 {
                "plugin.muse.native.response.method_not_found"
            } else {
                "plugin.muse.native.response"
            };
            Err(Error::new("native_rejected", error.message)
                .with_where(location)
                .with_origin(
                    eliot_core::FailureOrigin::Harness,
                    "MSP pre-admission refusal",
                ))
        } else {
            Err(Error::new(
                "unknown",
                format!(
                    "{}{}{}",
                    MuseStdioConfig::NATIVE_ERROR_PREFIX,
                    error.code,
                    MuseStdioConfig::NATIVE_ERROR_SUFFIX,
                ),
            )
            .with_where("plugin.muse.native.response")
            .with_origin(
                eliot_core::FailureOrigin::Harness,
                "MSP native error; upstream error class absent",
            ))
        }
    }
}

pub(crate) type NativeClient = StdioClient<MuseStdioConfig>;
pub(crate) type NativeProcess = StdioProcess<MuseStdioConfig>;
pub(crate) use eliot_plugin::native_stdio::{NativeFrame, NativeLaunch, StreamState};
