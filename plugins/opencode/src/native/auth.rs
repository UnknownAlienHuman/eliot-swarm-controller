//! Canonical-to-isolated credential transfer for one selected integration.
//!
//! Installed boundary (opencode v2.0.32 `auth --help`): `auth export
//! [<target>] --standalone` prints stored credentials, including secrets, as
//! JSON to stdout (`<target>` is an integration ID or name; omitting it
//! exports every integration, which this module never does — there is no
//! output-file flag), and `auth import [<file>] --standalone` imports
//! credentials exported by `auth export`, reading stdin when `<file>` is
//! omitted. Both argv vectors below are derived from that help output; no
//! `--server`, login, switch, or global flag is ever passed.
//!
//! The per-Agent server runs under isolated `XDG_CONFIG_HOME` (the instance
//! `config` directory) and `XDG_DATA_HOME` (the per-Agent `data` directory)
//! and therefore sees no canonical credential. The transfer reuses the
//! owner's explicitly selected canonical integration by running export
//! against the canonical store (inherited environment, no XDG overrides)
//! and piping its stdout opaquely, in memory only, into the import child's
//! stdin under the exact isolated roots the server will use. The hook calls
//! this before `serve`; isolation is never disabled and no `auth.json` is
//! ever copied by hand.
//!
//! Secrecy discipline: the secret document is held in one owned buffer that
//! is never logged, never printed, never placed in argv/env/files, and never
//! attached to errors or diagnostics; both children run with stderr
//! discarded because native error text may echo secrets. Only the validated
//! nonsecret selector and static error strings leave this module. No second
//! executor or ledger exists: one async function on the caller's runtime,
//! two bounded owned children, no spawned tasks, no retry, no fallback.

use std::{ffi::OsStr, ffi::OsString, path::Path, time::Duration};

use eliot_core::{Error, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    time::{Instant, timeout, timeout_at},
};

/// Largest accepted export document. One integration's credential JSON is
/// small; anything larger is refused before import, never truncated.
const MAX_AUTH_BYTES: usize = 256 * 1024;

/// Longest accepted integration selector, in bytes.
const MAX_SELECTOR_BYTES: usize = 128;

/// Grace to reap a killed helper after a deadline before reporting. The
/// caller owns the deadline; this only bounds our own cleanup.
const KILL_GRACE: Duration = Duration::from_secs(2);

/// One explicitly selected native integration (ID or name), validated but
/// never defaulted. Deliberately without `Debug`: a caller bug must not be
/// able to log whatever was passed here.
pub struct IntegrationSelector {
    inner: String,
}

impl IntegrationSelector {
    /// Accept exactly one explicit selector. Empty, overlong, control-byte,
    /// or flag-shaped (`-...`) input is rejected; there is no fallback to
    /// another integration and never an export of every integration.
    pub fn new(selector: &str) -> Result<Self> {
        let trimmed = selector.trim();
        if trimmed.is_empty()
            || trimmed.len() > MAX_SELECTOR_BYTES
            || trimmed.starts_with('-')
            || trimmed.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(Error::new(
                "invalid",
                "OpenCode auth integration selector is invalid",
            ));
        }
        Ok(Self {
            inner: trimmed.to_owned(),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.inner
    }
}

/// Exact transfer inputs. Deliberately without `Debug`: the environment
/// lists may carry secret values. The hook derives every field from the
/// same launch the server uses: `program` is the frozen native executable
/// (never a wrapper), `workspace` the Agent workspace both children run
/// in, and the isolated roots the exact `XDG_CONFIG_HOME` / `XDG_DATA_HOME`
/// the server will receive.
pub struct AuthTransfer<'a> {
    pub program: &'a OsStr,
    pub selector: &'a IntegrationSelector,
    pub workspace: &'a Path,
    /// Extra variables for the export child on top of inheritance. Normally
    /// empty: export must resolve the canonical store exactly as the
    /// owner's CLI does, with no XDG overrides.
    pub export_environment: &'a [(OsString, OsString)],
    /// Extra variables for the import child on top of inheritance: the same
    /// target environment the server receives. The explicit isolated roots
    /// below always win over any same-named entry here.
    pub import_environment: &'a [(OsString, OsString)],
    pub isolated_config_home: &'a Path,
    pub isolated_data_home: &'a Path,
}

/// `auth export <selector> --standalone`, in the installed help's order.
/// The selector is positional and mandatory: omitting it would export every
/// integration.
pub fn export_argv(selector: &IntegrationSelector) -> [&str; 4] {
    ["auth", "export", selector.as_str(), "--standalone"]
}

/// `auth import --standalone` with no file argument, so the child reads the
/// piped export document from stdin.
pub fn import_argv() -> [&'static str; 3] {
    ["auth", "import", "--standalone"]
}

/// Move the selected canonical credential into the isolated native store:
/// export it to one in-memory buffer, validate only that the buffer is a
/// non-trivial JSON document, and pipe it opaquely into import under the
/// isolated roots. `deadline` covers both children and is owned by the
/// host's deadline plumbing. All failures are static strings; the secret
/// document and native stderr never appear in them.
pub async fn transfer_selected_integration(
    transfer: &AuthTransfer<'_>,
    deadline: Instant,
) -> Result<()> {
    // An empty XDG root would silently fall back to the canonical store and
    // import into owner state: refuse before spawning. Relative roots stay
    // consistent because both children share the server's workspace cwd.
    if transfer.isolated_config_home.as_os_str().is_empty()
        || transfer.isolated_data_home.as_os_str().is_empty()
    {
        return Err(Error::new(
            "invalid",
            "OpenCode auth isolated store roots must not be empty",
        ));
    }
    let secret = export_selected(transfer, deadline).await?;
    import_selected(transfer, &secret, deadline).await
    // `secret` is zeroed on drop below.
}

/// Owned secret bytes. No `Debug`/`Display` by design; the best-effort
/// zeroing on drop keeps the document out of reused heap pages.
struct SecretBytes(Vec<u8>);

impl Drop for SecretBytes {
    fn drop(&mut self) {
        for byte in self.0.iter_mut() {
            *byte = 0;
        }
    }
}

/// Run export against the canonical store and capture its stdout, bounded.
/// Stderr is discarded: native error text may echo secrets, so failures
/// surface as static strings only.
async fn export_selected(transfer: &AuthTransfer<'_>, deadline: Instant) -> Result<SecretBytes> {
    let mut child = Command::new(transfer.program)
        .args(export_argv(transfer.selector))
        .current_dir(transfer.workspace)
        .envs(transfer.export_environment.iter().map(|(k, v)| (k, v)))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| Error::new("native_spawn", "OpenCode auth export could not start"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("native_spawn", "OpenCode auth export could not start"))?;
    let mut buffer = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((MAX_AUTH_BYTES + 1) as u64);
        limited.read_to_end(&mut buffer).await
    };
    match timeout_at(deadline, read).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode auth export did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_readback",
                "OpenCode auth export returned no usable credential",
            ));
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > MAX_AUTH_BYTES {
        kill_and_reap(&mut child).await;
        return Err(Error::new(
            "native_readback",
            "OpenCode auth export returned no usable credential",
        ));
    }
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode auth export did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            return Err(Error::new(
                "native_readback",
                "OpenCode auth export returned no usable credential",
            ));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new(
            "native_readback",
            "OpenCode auth export was refused",
        ));
    }
    if !is_usable_document(&buffer) {
        return Err(Error::new(
            "native_readback",
            "OpenCode auth export returned no usable credential",
        ));
    }
    Ok(SecretBytes(buffer))
}

/// Pipe the export document into import under the isolated roots. Exit
/// status is the only proof read: stderr is discarded for the same reason
/// as export, so any non-zero or ambiguous outcome is `unknown` rather
/// than a proven rejection.
async fn import_selected(
    transfer: &AuthTransfer<'_>,
    secret: &SecretBytes,
    deadline: Instant,
) -> Result<()> {
    let mut child = Command::new(transfer.program)
        .args(import_argv())
        .current_dir(transfer.workspace)
        .envs(transfer.import_environment.iter().map(|(k, v)| (k, v)))
        .env("XDG_CONFIG_HOME", transfer.isolated_config_home)
        .env("XDG_DATA_HOME", transfer.isolated_data_home)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| Error::new("native_spawn", "OpenCode auth import could not start"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::new("native_spawn", "OpenCode auth import could not start"))?;
    let write = async {
        stdin.write_all(&secret.0).await?;
        stdin.shutdown().await
    };
    match timeout_at(deadline, write).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "unknown",
                "OpenCode auth import did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            // The child died or the pipe broke mid-write: whatever landed
            // in the isolated store is unproven.
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "unknown",
                "OpenCode auth import outcome is unknown",
            ));
        }
        Ok(Ok(())) => {}
    }
    drop(stdin);
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "unknown",
                "OpenCode auth import did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            return Err(Error::new(
                "unknown",
                "OpenCode auth import outcome is unknown",
            ));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new("unknown", "OpenCode auth import was refused"));
    }
    Ok(())
}

/// The export contract is one JSON document holding the selected
/// credential. Shape-agnostic check only: valid JSON that is not trivially
/// empty. Nonsecret IDs and counts inside stay in memory, never logged.
fn is_usable_document(buffer: &[u8]) -> bool {
    if buffer.is_empty() {
        return false;
    }
    match serde_json::from_slice::<serde_json::Value>(buffer) {
        Ok(serde_json::Value::Null) => false,
        Ok(serde_json::Value::Object(map)) => !map.is_empty(),
        Ok(serde_json::Value::Array(items)) => !items.is_empty(),
        Ok(serde_json::Value::String(text)) => !text.is_empty(),
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Best-effort kill plus bounded reap. `kill_on_drop` remains the backstop
/// if this future is itself cancelled.
async fn kill_and_reap(child: &mut Child) {
    let _ = child.kill().await;
    let _ = timeout(KILL_GRACE, child.wait()).await;
}
