//! Vendor command-hook helper and private authenticated IPC for Google Antigravity.
//!
//! Antigravity (AGY 1.5.2) supports lifecycle hooks declared in `hooks.json`.
//! During `PreInvocation`, AGY invokes the configured command hook over stdio,
//! passing camelCase JSON (`conversationId`, `invocationNum`, `initialNumSteps`)
//! and expecting JSON output containing `injectSteps` with `ephemeralMessage`.
//!
//! This module provides:
//! 1. Typed wire representations for Antigravity hook I/O (`HookInput`, `HookOutput`).
//! 2. Bounded private authenticated IPC between the command hook helper and the
//!    owning plugin session.
//! 3. `HookServer`: Session-owned private listener that yields `HookCallback` on
//!    `accept()`. The session consumes `HookCallback::respond()` after validating
//!    session/conversation binding and emitting before-model events.
//! 4. `run_helper`: Command-hook helper invoked by the hidden CLI, which captures
//!    immutable process origin metadata, queries the owning plugin over private IPC,
//!    writes native stdout JSON, and reports the response-write result back to the plugin.

use std::{ffi::OsString, sync::Arc, time::Duration};

use eliot_core::{Error, Result};
use eliot_plugin::{
    MAX_FRAME_BYTES,
    native_transport::{ProcessBirthIdentity, current_process_birth_identity},
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    time::timeout,
};
use ulid::Ulid;

/// Environment variable specifying the private IPC endpoint address.
pub const ENV_HOOK_ENDPOINT: &str = "ELIOT_AGY_HOOK_ENDPOINT";

/// Environment variable carrying the scoped authentication token for private hook IPC.
pub const ENV_HOOK_TOKEN: &str = "ELIOT_AGY_HOOK_TOKEN";

/// Environment variable specifying the path to the Eliot hook helper executable.
pub const ENV_HOOK_HELPER: &str = "ELIOT_AGY_HOOK_HELPER";

/// Exact serialized byte overhead of the `IpcQueryResponse` wrapper and framing newline
/// around the constituent `HookOutput` payload:
/// `{"accepted":true,"output":...}\n` (26 bytes prefix + 1 byte closing brace + 1 byte newline).
pub const IPC_WRAPPER_OVERHEAD_BYTES: usize = br#"{"accepted":true,"output":}"#.len() + 1;

/// Maximum allowable line length for framed hook IPC messages.
///
/// Derived compositionally from the two shared constituent payload bounds
/// (`eliot_plugin::MAX_FRAME_BYTES` for claimed context and steer output)
/// plus the exact fixed `IpcQueryResponse` JSON wrapper and framing newline.
pub const MAX_HOOK_FRAME_BYTES: usize = 2 * MAX_FRAME_BYTES + IPC_WRAPPER_OVERHEAD_BYTES;

/// Timeout for helper IPC query operations.
const IPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Hook invocation payload received on stdin from the vendor runner.
///
/// Matches official Antigravity specification (https://antigravity.google/docs/hooks/)
/// and installed AGY 1.5.2 protojson camelCase encoding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HookInput {
    pub conversation_id: String,
    pub invocation_num: u64,
    pub initial_num_steps: u64,
}

/// Helper origin identity alias carried through IPC queries.
pub type HelperOrigin = ProcessBirthIdentity;

/// Vendor hook response output to stdout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct HookOutput {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inject_steps: Vec<HookStep>,
}

impl HookOutput {
    pub fn empty() -> Self {
        Self {
            inject_steps: Vec::new(),
        }
    }

    pub fn with_ephemeral(message: impl Into<String>) -> Self {
        Self {
            inject_steps: vec![HookStep::ephemeral(message)],
        }
    }
}

/// Individual step injected before model invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HookStep {
    EphemeralMessage {
        #[serde(rename = "ephemeralMessage")]
        ephemeral_message: String,
    },
}

impl HookStep {
    pub fn ephemeral(message: impl Into<String>) -> Self {
        Self::EphemeralMessage {
            ephemeral_message: message.into(),
        }
    }
}

/// Helper query sent by the command hook helper to the owning plugin session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IpcQuery {
    pub token: String,
    pub conversation_id: String,
    pub invocation_num: u64,
    pub initial_num_steps: u64,
    pub origin: HelperOrigin,
}

/// Response returned by the owning plugin session to the helper carrying full HookOutput.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IpcQueryResponse {
    pub accepted: bool,
    pub output: HookOutput,
}

/// Report returned by the helper to the plugin recording stdout write outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IpcWriteReport {
    pub token: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Acknowledgment of the write report sent by the plugin to the helper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IpcWriteAck {
    pub acknowledged: bool,
}

/// Session-owned private listener for Antigravity lifecycle hooks.
pub struct HookServer {
    endpoint: String,
    token: Arc<String>,
    listener: TcpListener,
}

impl HookServer {
    /// Bind a new private loopback IPC listener on an ephemeral port.
    pub async fn bind() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
            Error::new("ipc_bind", format!("Failed to bind hook IPC listener: {e}"))
        })?;
        let local_addr = listener.local_addr().map_err(|e| {
            Error::new(
                "ipc_bind",
                format!("Failed to read local socket address: {e}"),
            )
        })?;
        let endpoint = local_addr.to_string();
        let token = Arc::new(Ulid::new().to_string());

        Ok(Self {
            endpoint,
            token,
            listener,
        })
    }

    /// Read the scoped random token string.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Produce scoped environment pairs to pass to the native child process.
    pub fn endpoint_env(&self) -> Vec<(OsString, OsString)> {
        vec![
            (ENV_HOOK_ENDPOINT.into(), self.endpoint.clone().into()),
            (ENV_HOOK_TOKEN.into(), (*self.token).clone().into()),
        ]
    }

    /// Accept the next authenticated hook invocation connection from the helper.
    pub async fn accept(&self) -> Result<HookCallback> {
        loop {
            let (stream, _) = self.listener.accept().await.map_err(|e| {
                Error::new(
                    "ipc_accept",
                    format!("Failed to accept hook connection: {e}"),
                )
            })?;

            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);

            let query_line = match timeout(
                IPC_TIMEOUT,
                read_bounded_line(&mut reader, MAX_HOOK_FRAME_BYTES),
            )
            .await
            {
                Ok(Ok(line)) => line,
                _ => continue,
            };

            let query: IpcQuery = match serde_json::from_str(&query_line) {
                Ok(q) => q,
                Err(_) => continue,
            };

            if query.token != *self.token {
                let resp = IpcQueryResponse {
                    accepted: false,
                    output: HookOutput::empty(),
                };
                if let Ok(mut resp_bytes) = serde_json::to_vec(&resp) {
                    resp_bytes.push(b'\n');
                    let _ = timeout(IPC_TIMEOUT, async {
                        write_half.write_all(&resp_bytes).await?;
                        write_half.flush().await?;
                        Ok::<(), std::io::Error>(())
                    })
                    .await;
                }
                continue;
            }

            let input = HookInput {
                conversation_id: query.conversation_id,
                invocation_num: query.invocation_num,
                initial_num_steps: query.initial_num_steps,
            };

            return Ok(HookCallback {
                input,
                origin: query.origin,
                parts: Some((reader, write_half)),
                token: Arc::clone(&self.token),
            });
        }
    }
}

/// Active hook callback handle held by the owning plugin session.
pub struct HookCallback {
    pub input: HookInput,
    pub origin: HelperOrigin,
    parts: Option<(BufReader<OwnedReadHalf>, OwnedWriteHalf)>,
    token: Arc<String>,
}

impl HookCallback {
    /// Read the incoming hook input DTO.
    pub fn input(&self) -> &HookInput {
        &self.input
    }

    /// Read the immutable origin metadata captured at helper process creation.
    pub fn origin(&self) -> &HelperOrigin {
        &self.origin
    }

    /// Send the hook response output to the helper and await confirmed stdout write acknowledgment.
    ///
    /// Success guarantees that the helper successfully wrote and flushed the output JSON
    /// to AGY's stdout. An absent, negative, or broken acknowledgment returns an error.
    pub async fn respond(mut self, output: HookOutput) -> Result<()> {
        let (mut reader, mut write_half) = self
            .parts
            .take()
            .ok_or_else(|| Error::new("internal", "HookCallback already responded"))?;

        let resp = IpcQueryResponse {
            accepted: true,
            output,
        };

        let mut resp_bytes =
            serde_json::to_vec(&resp).map_err(|e| Error::new("json_encode", e.to_string()))?;
        resp_bytes.push(b'\n');

        timeout(IPC_TIMEOUT, async {
            write_half.write_all(&resp_bytes).await?;
            write_half.flush().await?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|_| Error::new("timeout", "Timed out sending query response over IPC"))?
        .map_err(|e| Error::new("ipc_write", format!("Failed to send query response: {e}")))?;

        let report_line = timeout(
            IPC_TIMEOUT,
            read_bounded_line(&mut reader, MAX_HOOK_FRAME_BYTES),
        )
        .await
        .map_err(|_| {
            Error::new(
                "timeout",
                "Timed out waiting for helper stdout write confirmation",
            )
        })?
        .map_err(|e| Error::new("ipc_read", format!("Failed to read write report: {e}")))?;

        let report: IpcWriteReport = serde_json::from_str(&report_line)
            .map_err(|e| Error::new("ipc_decode", format!("Malformed write report: {e}")))?;

        if report.token != *self.token {
            return Err(Error::new("auth_rejected", "Unauthorized report token"));
        }

        if !report.success {
            let err_msg = report
                .error
                .unwrap_or_else(|| "Helper reported stdout write failure".to_string());
            return Err(Error::new("write_failed", err_msg));
        }

        let ack = IpcWriteAck { acknowledged: true };
        let mut ack_bytes =
            serde_json::to_vec(&ack).map_err(|e| Error::new("json_encode", e.to_string()))?;
        ack_bytes.push(b'\n');

        timeout(IPC_TIMEOUT, async {
            write_half.write_all(&ack_bytes).await?;
            write_half.flush().await?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|_| Error::new("timeout", "Timed out sending write ack over IPC"))?
        .map_err(|e| Error::new("ipc_write", format!("Failed to send write ack: {e}")))?;

        Ok(())
    }
}

impl Drop for HookCallback {
    fn drop(&mut self) {
        drop(self.parts.take());
    }
}

/// Hidden CLI helper entrypoint invoked by `swarm plugin antigravity-hook`.
pub async fn run_helper() -> Result<()> {
    // If managed endpoint/token are absent, emit an empty JSON object and exit successfully.
    // This check runs before process birth query so outside-session invocations don't query OS identity.
    let (Ok(endpoint), Ok(token)) = (
        std::env::var(ENV_HOOK_ENDPOINT),
        std::env::var(ENV_HOOK_TOKEN),
    ) else {
        write_stdout_json(&HookOutput::empty()).await?;
        return Ok(());
    };

    let origin = current_process_birth_identity().map_err(|e| {
        Error::new(
            "process_identity",
            format!("Could not read helper process birth identity: {e}"),
        )
    })?;

    let mut stdin = tokio::io::stdin();
    let input = match timeout(IPC_TIMEOUT, read_hook_input(&mut stdin)).await {
        Ok(Ok(input)) => input,
        _ => {
            let _ = write_stdout_json(&HookOutput::empty()).await;
            return Ok(());
        }
    };

    let stream = match timeout(IPC_TIMEOUT, TcpStream::connect(&endpoint)).await {
        Ok(Ok(stream)) => stream,
        _ => {
            let _ = write_stdout_json(&HookOutput::empty()).await;
            return Ok(());
        }
    };

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let query = IpcQuery {
        token: token.clone(),
        conversation_id: input.conversation_id,
        invocation_num: input.invocation_num,
        initial_num_steps: input.initial_num_steps,
        origin,
    };

    let mut query_bytes =
        serde_json::to_vec(&query).map_err(|e| Error::new("json_encode", e.to_string()))?;
    query_bytes.push(b'\n');

    let query_write_res = timeout(IPC_TIMEOUT, async {
        write_half.write_all(&query_bytes).await?;
        write_half.flush().await?;
        Ok::<(), std::io::Error>(())
    })
    .await;

    if query_write_res.is_err() || query_write_res.unwrap().is_err() {
        let _ = write_stdout_json(&HookOutput::empty()).await;
        return Ok(());
    }

    let query_resp: IpcQueryResponse = match timeout(
        IPC_TIMEOUT,
        read_bounded_line(&mut reader, MAX_HOOK_FRAME_BYTES),
    )
    .await
    {
        Ok(Ok(line)) => match serde_json::from_str(&line) {
            Ok(resp) => resp,
            Err(_) => {
                let _ = write_stdout_json(&HookOutput::empty()).await;
                return Ok(());
            }
        },
        _ => {
            let _ = write_stdout_json(&HookOutput::empty()).await;
            return Ok(());
        }
    };

    let output = if query_resp.accepted {
        query_resp.output
    } else {
        HookOutput::empty()
    };

    let write_res = write_stdout_json(&output).await;
    let (success, error) = match &write_res {
        Ok(()) => (true, None),
        Err(e) => (false, Some(e.to_string())),
    };

    let report = IpcWriteReport {
        token,
        success,
        error,
    };
    if let Ok(mut report_bytes) = serde_json::to_vec(&report) {
        report_bytes.push(b'\n');
        let _ = timeout(IPC_TIMEOUT, async {
            write_half.write_all(&report_bytes).await?;
            write_half.flush().await?;
            Ok::<(), std::io::Error>(())
        })
        .await;

        let _ = timeout(
            IPC_TIMEOUT,
            read_bounded_line(&mut reader, MAX_HOOK_FRAME_BYTES),
        )
        .await;
    }

    write_res
}

/// Read a single framed line with an enforced hard maximum byte cap on allocations.
async fn read_bounded_line<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<String> {
    let mut line = String::new();
    let mut take = reader.take(max_bytes as u64);
    let bytes_read = take
        .read_line(&mut line)
        .await
        .map_err(|e| Error::new("ipc_read", format!("Failed to read framed line: {e}")))?;
    if bytes_read == 0 {
        return Err(Error::new("ipc_eof", "IPC stream closed unexpectedly"));
    }
    if !line.ends_with('\n') {
        return Err(Error::new(
            "ipc_frame_oversized",
            "Framed line exceeded maximum byte limit or was not terminated with a newline",
        ));
    }
    Ok(line)
}

async fn read_hook_input<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<HookInput> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    while buffer.len() < MAX_HOOK_FRAME_BYTES {
        let max_to_read = (MAX_HOOK_FRAME_BYTES - buffer.len()).min(chunk.len());
        let n = reader.read(&mut chunk[..max_to_read]).await.map_err(|e| {
            Error::new(
                "stdin_read",
                format!("Failed to read hook input from stdin: {e}"),
            )
        })?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Ok(input) = serde_json::from_slice::<HookInput>(&buffer) {
            return Ok(input);
        }
    }
    serde_json::from_slice::<HookInput>(&buffer).map_err(|e| {
        Error::new(
            "stdin_parse",
            format!("Failed to parse HookInput JSON: {e}"),
        )
    })
}

async fn write_stdout_json(output: &HookOutput) -> Result<()> {
    let json_bytes = serde_json::to_vec(output)
        .map_err(|e| Error::new("json_encode", format!("Failed to encode HookOutput: {e}")))?;
    let mut stdout = tokio::io::stdout();
    timeout(IPC_TIMEOUT, async {
        stdout.write_all(&json_bytes).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(|_| Error::new("timeout", "Timed out writing hook output to stdout"))?
    .map_err(|e| Error::new("stdout_write", format!("Failed to write to stdout: {e}")))?;
    Ok(())
}
