//! Canonical-to-isolated credential transfer for one selected integration.
//!
//! Installed boundary (opencode v2.0.32 `auth --help`): `auth export
//! [<target>] --standalone` prints stored credentials, including secrets, as
//! JSON to stdout (`<target>` is an integration ID or name; omitting it
//! exports every integration, which this module never does — there is no
//! output-file flag), and `auth import [<file>] --standalone` imports
//! credentials exported by `auth export`, reading stdin when `<file>` is
//! omitted. `auth list --format json --standalone` reports nonsecret
//! metadata observed on the installed CLI as an array of
//! `{id, name, connections: [...]}` entries. All argv vectors below are
//! derived from that help output; no `--server`, login, switch, or global
//! flag is ever passed.
//!
//! The per-Agent server runs under isolated `XDG_CONFIG_HOME` (the instance
//! `config` directory) and `XDG_DATA_HOME` (the per-Agent `data` directory)
//! and therefore sees no canonical credential. Before `serve`, the hook
//! prepares only the owner's explicitly selected integration in that
//! private store: the canonical list must show the selection exactly once
//! with exactly one credential for this current path (anything else refuses
//! without fallback), then export pipes opaquely, in memory only, into
//! import under the exact isolated roots the server will use. On explicit
//! resume the isolated list is read first and a correctly present
//! integration is reused without re-import; an unknown import outcome
//! settles to success only after the original child is proven exited and an
//! isolated-list readback proves the selected state — never by retry.
//! Isolation is never disabled and no `auth.json` is ever copied by hand.
//!
//! Secrecy discipline: the secret document is held in one owned buffer that
//! is never logged, never printed, never placed in argv/env/files, never
//! parsed into extra copies, and never attached to errors or diagnostics;
//! the native importer owns its format. All children run with stderr
//! discarded because native error text may echo secrets. Only the validated
//! nonsecret selector, nonsecret list IDs/counts, and static error strings
//! leave this module. No second executor or ledger exists: one async
//! function on the caller's runtime, bounded owned children, no spawned
//! tasks, no import retry.

use std::{ffi::OsStr, ffi::OsString, path::Path, time::Duration};

use eliot_core::{Error, Result};
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    time::{Instant, timeout, timeout_at},
};

/// Largest accepted export document. One integration's credential is small;
/// anything larger is refused before import, never truncated.
const MAX_AUTH_BYTES: usize = 256 * 1024;

/// Largest accepted list document. List output is metadata (hundreds of
/// bytes observed); anything larger is unreadable, never truncated.
const MAX_LIST_BYTES: usize = 64 * 1024;

/// Longest accepted integration selector, in bytes.
const MAX_SELECTOR_BYTES: usize = 128;

/// Grace to reap a killed helper after a deadline before reporting. The
/// caller owns the deadline; this only bounds our own cleanup.
const KILL_GRACE: Duration = Duration::from_secs(2);

/// One explicitly selected native integration (ID or name), validated but
/// never defaulted. Deliberately without `Debug`: a caller bug must not be
/// able to log whatever was passed here.
#[derive(Clone)]
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

/// The server-start hook's auth config: which canonical integration to
/// prepare, and whether an explicitly resumed generation may reuse it when
/// the isolated store already holds it correctly.
pub struct AuthSetup {
    pub selector: IntegrationSelector,
    pub reuse_existing: bool,
}

/// Exact transfer inputs. Deliberately without `Debug`: the environment
/// lists may carry secret values. The hook derives every field from the
/// same launch the server uses: `program` is the frozen native executable
/// (never a wrapper), `workspace` the Agent workspace every child runs
/// in, and the isolated roots the exact `XDG_CONFIG_HOME` / `XDG_DATA_HOME`
/// the server will receive.
pub struct AuthTransfer<'a> {
    pub program: &'a OsStr,
    pub selector: &'a IntegrationSelector,
    pub workspace: &'a Path,
    /// Extra variables for canonical-store children (list, export) on top
    /// of inheritance. Normally empty: they must resolve the canonical
    /// store exactly as the owner's CLI does, with no XDG overrides.
    pub export_environment: &'a [(OsString, OsString)],
    /// Extra variables for isolated-store children (list, import) on top of
    /// inheritance: the same target environment the server receives. The
    /// explicit isolated roots below always win over any same-named entry.
    pub import_environment: &'a [(OsString, OsString)],
    pub isolated_config_home: &'a Path,
    pub isolated_data_home: &'a Path,
    /// True only on explicit resume: reuse the integration without
    /// re-import when the isolated store already holds it correctly.
    pub reuse_existing: bool,
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

/// `auth list --format json --standalone`: nonsecret integration metadata
/// on stdout. Only IDs and connection counts are ever interpreted.
pub fn list_argv() -> [&'static str; 5] {
    ["auth", "list", "--format", "json", "--standalone"]
}

/// Move the selected canonical credential into the isolated native store.
/// On explicit resume a correctly present integration is reused without
/// I/O beyond the isolated list; otherwise the canonical list must show
/// the selection exactly once with exactly one credential, export runs,
/// and an unknown import settles only by proven child exit plus readback
/// proof, never by retry. `deadline` covers every child and is owned by the host's
/// deadline plumbing. All failures are static strings; the secret document
/// and native stderr never appear in them.
pub async fn transfer_selected_integration(
    transfer: &AuthTransfer<'_>,
    deadline: Instant,
) -> Result<()> {
    // An empty XDG root would silently fall back to the canonical store and
    // import into owner state: refuse before spawning. Relative roots stay
    // consistent because every child shares the server's workspace cwd.
    if transfer.isolated_config_home.as_os_str().is_empty()
        || transfer.isolated_data_home.as_os_str().is_empty()
    {
        return Err(Error::new(
            "invalid",
            "OpenCode auth isolated store roots must not be empty",
        ));
    }
    if transfer.reuse_existing {
        match isolated_gate(transfer, deadline).await? {
            Gate::ExactlyOne => return Ok(()),
            Gate::Absent => {}
            Gate::Ambiguous => {
                return Err(Error::new(
                    "unknown",
                    "OpenCode auth isolated store holds an ambiguous credential for the selected integration",
                ));
            }
        }
    }
    match canonical_gate(transfer, deadline).await? {
        Gate::ExactlyOne => {}
        Gate::Absent | Gate::Ambiguous => {
            return Err(Error::new(
                "profile_error",
                "OpenCode auth integration selection does not hold exactly one canonical credential",
            ));
        }
    }
    let secret = export_selected(transfer, deadline).await?;
    import_with_reconcile(transfer, &secret, deadline).await
}

/// Owned secret bytes. No `Debug`/`Display` by design, and no parsing into
/// further copies: the bytes travel from export stdout to import stdin
/// untouched. No secure-zeroization claim is made: that would need
/// volatile writes, which need `unsafe`, which is forbidden in this crate,
/// so an ordinary fill loop is not presented as one.
struct SecretBytes(Vec<u8>);

/// How the selected integration stands in one store's list: exactly one
/// entry with exactly one connection, absent, or ambiguous in any way.
enum Gate {
    ExactlyOne,
    Absent,
    Ambiguous,
}

/// One `auth list` entry. Only the nonsecret ID, name, and connection
/// count are interpreted; connection bodies are ignored without copies
/// and unknown fields stay unread.
#[derive(Deserialize)]
struct ListEntry {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    connections: Vec<serde::de::IgnoredAny>,
}

/// The canonical gate: the selection must name exactly one integration
/// entry (by ID or name) holding exactly one connection.
async fn canonical_gate(transfer: &AuthTransfer<'_>, deadline: Instant) -> Result<Gate> {
    let mut command = canonical_command(transfer);
    command
        .args(list_argv())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    let entries = run_list(command, deadline).await?;
    Ok(gate(&entries, transfer.selector.as_str()))
}

/// The isolated gate: same rule, read under the isolated roots the server
/// will use, so the read observes exactly the store `serve` will read.
async fn isolated_gate(transfer: &AuthTransfer<'_>, deadline: Instant) -> Result<Gate> {
    let mut command = isolated_command(transfer);
    command
        .args(list_argv())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    let entries = run_list(command, deadline).await?;
    Ok(gate(&entries, transfer.selector.as_str()))
}

/// Pure gate rule: exactly one entry matching by ID or name, holding
/// exactly one connection. Matching is exact; zero or several matches, or
/// any connection count but one, refuses without fallback.
fn gate(entries: &[ListEntry], selector: &str) -> Gate {
    let mut matches = entries
        .iter()
        .filter(|entry| entry.id == selector || entry.name == selector);
    match (matches.next(), matches.next()) {
        (Some(entry), None) if entry.connections.len() == 1 => Gate::ExactlyOne,
        (None, None) => Gate::Absent,
        _ => Gate::Ambiguous,
    }
}

/// Base command for canonical-store children: inherit, plus extras, never
/// XDG overrides, so the child resolves the owner's store.
fn canonical_command(transfer: &AuthTransfer<'_>) -> Command {
    let mut command = Command::new(transfer.program);
    command
        .current_dir(transfer.workspace)
        .envs(transfer.export_environment.iter().map(|(k, v)| (k, v)))
        .kill_on_drop(true);
    command
}

/// Base command for isolated-store children: inherit, plus the target
/// environment, plus the exact isolated roots the server will receive.
fn isolated_command(transfer: &AuthTransfer<'_>) -> Command {
    let mut command = Command::new(transfer.program);
    command
        .current_dir(transfer.workspace)
        .envs(transfer.import_environment.iter().map(|(k, v)| (k, v)))
        .env("XDG_CONFIG_HOME", transfer.isolated_config_home)
        .env("XDG_DATA_HOME", transfer.isolated_data_home)
        .kill_on_drop(true);
    command
}

/// Run one list child and parse its metadata, bounded. Stderr is discarded
/// like every other helper's; failures are static strings only.
async fn run_list(mut command: Command, deadline: Instant) -> Result<Vec<ListEntry>> {
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("native_spawn", "OpenCode auth list could not start"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("native_spawn", "OpenCode auth list could not start"))?;
    let mut buffer = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((MAX_LIST_BYTES + 1) as u64);
        limited.read_to_end(&mut buffer).await
    };
    match timeout_at(deadline, read).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode auth list did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_readback",
                "OpenCode auth list metadata was unreadable",
            ));
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > MAX_LIST_BYTES {
        kill_and_reap(&mut child).await;
        return Err(Error::new(
            "native_readback",
            "OpenCode auth list metadata was unreadable",
        ));
    }
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode auth list did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            return Err(Error::new(
                "native_readback",
                "OpenCode auth list metadata was unreadable",
            ));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new(
            "native_readback",
            "OpenCode auth list metadata was unreadable",
        ));
    }
    serde_json::from_slice(&buffer).map_err(|_| {
        Error::new(
            "native_readback",
            "OpenCode auth list metadata was unreadable",
        )
    })
}

/// Run export against the canonical store and capture its stdout, bounded
/// and opaque: non-empty and within bound is all that is checked, because
/// the native importer owns the document format. Stderr is discarded:
/// native error text may echo secrets, so failures surface as static
/// strings only.
async fn export_selected(transfer: &AuthTransfer<'_>, deadline: Instant) -> Result<SecretBytes> {
    let mut command = canonical_command(transfer);
    command
        .args(export_argv(transfer.selector))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    let mut child = command
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
    if buffer.is_empty() || buffer.len() > MAX_AUTH_BYTES {
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
    if buffer.is_empty() {
        return Err(Error::new(
            "native_readback",
            "OpenCode auth export returned no usable credential",
        ));
    }
    Ok(SecretBytes(buffer))
}

/// Outcome of one import child: applied, ambiguous (reconcile by isolated
/// readback; the child-exit proof travels with the report), or never
/// started (the child never ran, so no readback is needed).
enum ImportAttempt {
    Applied,
    Unknown { error: Error, reaped: bool },
    NotStarted(Error),
}

/// Pipe the export document into import under the isolated roots, with
/// readback reconciliation for an unknown outcome, never a retry. Exit
/// status is the only proof read: stderr is discarded for the same reason
/// as export, so any non-zero or ambiguous outcome reconciles rather than
/// assuming.
async fn import_with_reconcile(
    transfer: &AuthTransfer<'_>,
    secret: &SecretBytes,
    deadline: Instant,
) -> Result<()> {
    match import_once(transfer, secret, deadline).await {
        ImportAttempt::Applied => Ok(()),
        ImportAttempt::NotStarted(error) => Err(error),
        ImportAttempt::Unknown { error, reaped } => {
            reconcile_unknown(transfer, deadline, error, reaped).await
        }
    }
}

/// Reconcile an unknown import without retry: success requires the original
/// child proven exited first — a still-running child could write after the
/// readback — and then an isolated-list readback proving the selected
/// state. Anything unproven preserves Unknown and starts no serve.
async fn reconcile_unknown(
    transfer: &AuthTransfer<'_>,
    deadline: Instant,
    triggered: Error,
    reaped: bool,
) -> Result<()> {
    if !reaped {
        return Err(triggered);
    }
    match isolated_gate(transfer, deadline).await {
        Ok(Gate::ExactlyOne) => Ok(()),
        Ok(Gate::Ambiguous) => Err(Error::new(
            "unknown",
            "OpenCode auth isolated store holds an ambiguous credential for the selected integration",
        )),
        Ok(Gate::Absent) | Err(_) => Err(triggered),
    }
}

/// Run one import child to completion. Only `spawn` failing proves the
/// child never ran; everything after a successful spawn that is not a
/// clean applied exit is ambiguous, because the discarded stderr cannot
/// prove no effect.
async fn import_once(
    transfer: &AuthTransfer<'_>,
    secret: &SecretBytes,
    deadline: Instant,
) -> ImportAttempt {
    let mut command = isolated_command(transfer);
    command
        .args(import_argv())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            return ImportAttempt::NotStarted(Error::new(
                "native_spawn",
                "OpenCode auth import could not start",
            ));
        }
    };
    let mut stdin = match child.stdin.take() {
        Some(stdin) => stdin,
        None => {
            let reaped = kill_and_reap(&mut child).await;
            return ImportAttempt::Unknown {
                error: Error::new("unknown", "OpenCode auth import outcome is unknown"),
                reaped,
            };
        }
    };
    let write = async {
        stdin.write_all(&secret.0).await?;
        stdin.shutdown().await
    };
    match timeout_at(deadline, write).await {
        Err(_) => {
            let reaped = kill_and_reap(&mut child).await;
            return ImportAttempt::Unknown {
                error: Error::new(
                    "unknown",
                    "OpenCode auth import did not finish before its deadline",
                ),
                reaped,
            };
        }
        Ok(Err(_)) => {
            // The child died or the pipe broke mid-write: whatever landed
            // in the isolated store is unproven.
            let reaped = kill_and_reap(&mut child).await;
            return ImportAttempt::Unknown {
                error: Error::new("unknown", "OpenCode auth import outcome is unknown"),
                reaped,
            };
        }
        Ok(Ok(())) => {}
    }
    drop(stdin);
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            let reaped = kill_and_reap(&mut child).await;
            return ImportAttempt::Unknown {
                error: Error::new(
                    "unknown",
                    "OpenCode auth import did not finish before its deadline",
                ),
                reaped,
            };
        }
        Ok(Err(_)) => {
            let reaped = kill_and_reap(&mut child).await;
            return ImportAttempt::Unknown {
                error: Error::new("unknown", "OpenCode auth import outcome is unknown"),
                reaped,
            };
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        // `wait` returned, so the exit is proven; the discarded stderr
        // still cannot prove no effect, hence Unknown rather than refused.
        return ImportAttempt::Unknown {
            error: Error::new("unknown", "OpenCode auth import was refused"),
            reaped: true,
        };
    }
    ImportAttempt::Applied
}

/// Best-effort kill plus bounded reap, reporting positive exit proof: true
/// only when the child's exit was actually reaped. `kill_on_drop` remains
/// the backstop if this future is itself cancelled.
async fn kill_and_reap(child: &mut Child) -> bool {
    let _ = child.kill().await;
    timeout(KILL_GRACE, child.wait())
        .await
        .is_ok_and(|wait| wait.is_ok())
}
