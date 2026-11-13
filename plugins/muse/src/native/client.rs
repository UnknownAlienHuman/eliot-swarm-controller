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

#[cfg(test)]
use eliot_core::{SecretPolicy, StderrSnapshot, StderrSource};
#[cfg(test)]
use std::{ffi::OsString, time::Duration};
#[cfg(test)]
use tokio::time::Instant;

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
    // The 8 MiB cap covers `view/page` readback (Eliot requests
    // `limit: 1000`) plus envelope overhead; the installed schema's text
    // budget is otherwise unpublished, so a live item notification beyond
    // it stays an explicit oversized gap. Eliot makes no `item/readOutput`
    // calls, so that surface (6 MiB stored bytes per page, exactly 8 MiB
    // of base64 before envelope overhead) does not size this cap.
    const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

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
            Err(Error::new("native_rejected", error.message))
        } else {
            Err(Error::new(
                "unknown",
                format!(
                    "{}{}{}",
                    MuseStdioConfig::NATIVE_ERROR_PREFIX,
                    error.code,
                    MuseStdioConfig::NATIVE_ERROR_SUFFIX,
                ),
            ))
        }
    }
}

pub(crate) type NativeClient = StdioClient<MuseStdioConfig>;
pub(crate) type NativeProcess = StdioProcess<MuseStdioConfig>;
pub(crate) use eliot_plugin::native_stdio::{NativeFrame, NativeLaunch, StreamState};

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn stderr_fixture_child() {
        if std::env::var_os("ELIOT_MUSE_NATIVE_STDERR_FIXTURE").as_deref()
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
                OsString::from("ELIOT_MUSE_NATIVE_STDERR_FIXTURE"),
                OsString::from("1"),
            )],
        };
        let mut native = NativeProcess::spawn(launch, SecretPolicy::new(["vendor-secret", "xy"]))
            .await
            .expect("start synthetic child");
        let client = native.client();
        // The test harness's stdout is deliberately not a native protocol frame.
        let error = client
            .call(
                "initialize",
                serde_json::json!({}),
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .expect_err("malformed/closed transport has unknown outcome");
        let _ = native
            .terminate(false, Instant::now() + Duration::from_secs(5))
            .await;
        let error = client.diagnostic_error(error.with_stderr(
            StderrSource::Plugin,
            StderrSnapshot {
                text: "plugin context".to_owned(),
                truncated: false,
                eof: true,
                read_error: false,
            },
        ));
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
        assert_eq!(error.diagnostics.len(), 2);
        let error = crate::host::contextual_error(error, "unknown", "native cleanup readback");
        assert_eq!(error.diagnostics.len(), 2);
        let encoded = serde_json::to_string(&error).expect("bounded error wire");
        assert!(!encoded.contains("vendor-secret"));
        assert!(!encoded.contains("xy"));
        let restored: Error = serde_json::from_str(&encoded).expect("decode error diagnostics");
        assert_eq!(restored, error);
    }
}
