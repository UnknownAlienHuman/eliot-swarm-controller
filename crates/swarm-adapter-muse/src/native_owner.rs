//! Spawn the caller-selected Muse client inside the already verified module
//! owner group and attach its stdio to the bounded MSP transport.
//!
//! This module owns no process group, Job, retry, or shutdown policy. The
//! module owner remains responsible for the native process family. A direct
//! child exit and either stdio EOF are observations about that stream/process
//! only; none proves family departure.

use crate::msp::{MspTransport, Notification, Options, TransportEnd};
use jsonrpsee_core::server::RpcModule;
use std::{
    error::Error,
    ffi::OsString,
    fmt,
    io,
    path::PathBuf,
    process::{ExitStatus, Stdio},
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, ChildStderr, Command},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

const MAX_STDERR_BYTES: usize = 4_096;
const MAX_DIAGNOSTIC_CODE_BYTES: usize = 64;

/// Caller-selected executable and argv. serve() supplies exactly the native
/// client's serve subcommand; any further arguments remain caller-owned.
/// The process inherits the native harness environment and existing settings.
#[derive(Clone, Debug)]
pub struct NativeClientSpec {
    executable: PathBuf,
    args: Vec<OsString>,
    current_dir: Option<PathBuf>,
}

impl NativeClientSpec {
    pub fn serve(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            args: vec![OsString::from("serve")],
            current_dir: None,
        }
    }

    pub fn arg(mut self, argument: impl Into<OsString>) -> Self {
        self.args.push(argument.into());
        self
    }

    pub fn current_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(path.into());
        self
    }
}

/// Safe, bounded stderr observation. Raw stderr bytes are never returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeStderrResult {
    /// The pipe reached EOF. error_code is present only for the exact bounded
    /// {"error_code":"UPPERCASE_CODE"} diagnostic record.
    Closed { error_code: Option<String> },
    ReadFailed,
    DrainTaskFailed,
    Unavailable,
}

struct StderrDrain {
    task: JoinHandle<()>,
    result: oneshot::Receiver<NativeStderrResult>,
}

/// Direct-child handle retained by the caller. Waiting has no timeout and
/// never kills the process. Its result says nothing about owner-group state.
pub struct NativeDirectChild {
    child: Child,
    stderr_drain: Option<StderrDrain>,
    stderr_result: Option<NativeStderrResult>,
}

impl fmt::Debug for NativeDirectChild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeDirectChild")
            .field("pid", &self.child.id())
            .field(
                "stderr_drain_finished",
                &self
                    .stderr_drain
                    .as_ref()
                    .is_some_and(|drain| drain.task.is_finished()),
            )
            .finish()
    }
}

impl NativeDirectChild {
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    /// Observe only this process. No timeout or kill is applied.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait().await
    }

    /// Wait for stderr EOF and return only closed, allowlisted metadata.
    /// Descendants may keep the pipe open after this direct child exits; this
    /// method is diagnostic and is not a family-departure check.
    pub async fn wait_stderr_closed(&mut self) -> NativeStderrResult {
        if let Some(result) = &self.stderr_result {
            return result.clone();
        }
        let result = match self.stderr_drain.as_mut() {
            Some(drain) => match (&mut drain.result).await {
                Ok(result) => result,
                Err(_) => NativeStderrResult::DrainTaskFailed,
            },
            None => NativeStderrResult::Unavailable,
        };
        self.stderr_drain = None;
        self.stderr_result = Some(result.clone());
        result
    }
}

/// An attached child. transport.close() drains admitted submissions before
/// it closes child stdin. The outer module owner retains group and Job custody.
pub struct NativeOwnedChild {
    pub transport: MspTransport,
    pub notifications: mpsc::Receiver<Notification>,
    pub direct_child: NativeDirectChild,
}

/// Results from an explicit transport close followed by an unbounded wait for
/// the direct child. Neither field proves that descendants have departed.
#[derive(Debug)]
pub struct NativeCloseObservation {
    pub transport: Result<(), TransportEnd>,
    pub direct_child: io::Result<ExitStatus>,
}

impl NativeOwnedChild {
    /// Apply the MSP submission barrier, close stdin through MspTransport,
    /// then wait for the direct child without timeout or kill escalation.
    pub async fn close_and_wait_direct(&mut self) -> NativeCloseObservation {
        let transport = self.transport.close().await;
        let direct_child = self.direct_child.wait().await;
        NativeCloseObservation {
            transport,
            direct_child,
        }
    }
}

/// A post-spawn attachment failure keeps the direct-child handle available so
/// the caller can continue observing it. The outer owner still owns the family.
#[derive(Debug)]
pub enum NativeOwnerStartError {
    AdapterOwnerUnverified,
    Spawn(io::Error),
    PipedStdioUnavailable { direct_child: NativeDirectChild },
    TransportUnavailable {
        reason: TransportEnd,
        direct_child: NativeDirectChild,
    },
}

impl fmt::Display for NativeOwnerStartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdapterOwnerUnverified => f.write_str("MUSE_ADAPTER_OWNER_UNVERIFIED"),
            Self::Spawn(error) => write!(f, "MUSE_NATIVE_SPAWN_{:?}", error.kind()),
            Self::PipedStdioUnavailable { .. } => f.write_str("MUSE_NATIVE_STDIO_UNAVAILABLE"),
            Self::TransportUnavailable { reason, .. } => {
                write!(f, "MUSE_NATIVE_TRANSPORT_{reason:?}")
            }
        }
    }
}

impl Error for NativeOwnerStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Spawn(error) => Some(error),
            Self::TransportUnavailable { reason, .. } => Some(reason),
            Self::AdapterOwnerUnverified | Self::PipedStdioUnavailable { .. } => None,
        }
    }
}

/// Launch the supplied client with caller-selected argv and inherited native
/// settings, then attach its pipes to MSP. The normal service invocation is
/// NativeClientSpec::serve(installed_executable); no provider/model switches,
/// authentication setup, HOME/XDG changes, or environment copying are added.
pub async fn spawn_owned(
    spec: NativeClientSpec,
    server: RpcModule<()>,
) -> Result<NativeOwnedChild, NativeOwnerStartError> {
    swarm_process::module_owner::verify_current_adapter_from_env()
        .map_err(|_| NativeOwnerStartError::AdapterOwnerUnverified)?;

    let mut child = build_command(&spec)
        .spawn()
        .map_err(NativeOwnerStartError::Spawn)?;
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr_drain = child.stderr.take().map(drain_stderr);
    let direct_child = NativeDirectChild {
        child,
        stderr_drain,
        stderr_result: None,
    };

    let (Some(stdin), Some(stdout)) = (stdin, stdout) else {
        return Err(NativeOwnerStartError::PipedStdioUnavailable { direct_child });
    };

    match MspTransport::attach_owned_stdio(stdout, stdin, server, Options::default()) {
        Ok((transport, notifications)) => Ok(NativeOwnedChild {
            transport,
            notifications,
            direct_child,
        }),
        Err(reason) => Err(NativeOwnerStartError::TransportUnavailable {
            reason,
            direct_child,
        }),
    }
}

fn build_command(spec: &NativeClientSpec) -> Command {
    let mut command = Command::new(&spec.executable);
    command
        .args(&spec.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // The verified outer module owner retains non-killing group custody.
        .kill_on_drop(false);
    if let Some(current_dir) = &spec.current_dir {
        command.current_dir(current_dir);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
    }
    command
}

fn drain_stderr(stderr: ChildStderr) -> StderrDrain {
    let (sender, result) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = sender.send(read_closed_stderr(stderr).await);
    });
    StderrDrain { task, result }
}

async fn read_closed_stderr(mut stderr: ChildStderr) -> NativeStderrResult {
    let mut captured = Vec::with_capacity(MAX_STDERR_BYTES);
    let mut overflowed = false;
    let mut chunk = [0_u8; 1_024];
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) => break,
            Ok(count) => {
                if !overflowed && captured.len().saturating_add(count) <= MAX_STDERR_BYTES {
                    captured.extend_from_slice(&chunk[..count]);
                } else {
                    overflowed = true;
                    captured.clear();
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return NativeStderrResult::ReadFailed,
        }
    }
    NativeStderrResult::Closed {
        error_code: (!overflowed)
            .then(|| parse_closed_error_code(&captured))
            .flatten(),
    }
}

fn parse_closed_error_code(stderr: &[u8]) -> Option<String> {
    let record = stderr
        .strip_suffix(b"\r\n")
        .or_else(|| stderr.strip_suffix(b"\n"))
        .unwrap_or(stderr);
    let code = record
        .strip_prefix(b"{\"error_code\":\"")?
        .strip_suffix(b"\"}")?;
    if code.is_empty()
        || code.len() > MAX_DIAGNOSTIC_CODE_BYTES
        || !code
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'_')
    {
        return None;
    }
    String::from_utf8(code.to_vec()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const FIXTURE_ENV: &str = "ELIOT_SWARM_MUSE_SYNTHETIC_CHILD_CASE";
    const FIXTURE_TEST: &str = "native_owner::tests::synthetic_native_child_fixture";

    #[tokio::test]
    async fn synthetic_process_eof_and_stderr_are_observed_without_raw_logs() {
        for (case, expected_code) in [
            ("closed_code", Some("SYNTHETIC_CHILD_ERROR")),
            ("raw_prompt", None),
            ("oversized", None),
        ] {
            let executable = std::env::current_exe().expect("test executable path");
            let spec = NativeClientSpec {
                executable,
                args: vec![
                    OsString::from("--exact"),
                    OsString::from(FIXTURE_TEST),
                    OsString::from("--ignored"),
                    OsString::from("--nocapture"),
                ],
                current_dir: None,
            };
            let mut command = build_command(&spec);
            command.env(FIXTURE_ENV, case);
            let mut child = command.spawn().expect("synthetic child starts");
            let mut stdin = child.stdin.take().expect("piped stdin");
            let mut stdout = child.stdout.take().expect("piped stdout");
            let stderr = child.stderr.take().expect("piped stderr");
            let mut stderr_drain = drain_stderr(stderr);

            stdin
                .write_all(b"accepted-before-eof\n")
                .await
                .expect("write synthetic payload");
            stdin.shutdown().await.expect("close after payload");
            drop(stdin);

            let mut output = Vec::new();
            stdout
                .read_to_end(&mut output)
                .await
                .expect("read synthetic stdout to EOF");
            let status = child.wait().await.expect("observe direct child exit");
            let stderr = (&mut stderr_drain.result)
                .await
                .expect("stderr drain task result");
            stderr_drain.task.await.expect("stderr drain task");

            assert!(status.success());
            assert_eq!(output, b"stdin-eof\n");
            match (stderr, expected_code) {
                (NativeStderrResult::Closed { error_code: Some(actual) }, Some(expected)) => {
                    assert_eq!(actual, expected);
                }
                (NativeStderrResult::Closed { error_code: None }, None) => {}
                (other, expected) => {
                    panic!("unexpected stderr result {other:?}, expected {expected:?}")
                }
            }
        }
    }

    #[test]
    #[ignore = "spawned only by the synthetic owned-child pipe fixture"]
    fn synthetic_native_child_fixture() {
        let Some(case) = std::env::var_os(FIXTURE_ENV) else {
            return;
        };
        let mut input = Vec::new();
        std::io::stdin()
            .read_to_end(&mut input)
            .expect("read until parent closes stdin");
        if input != b"accepted-before-eof\n" {
            std::process::exit(91);
        }
        std::io::stdout()
            .write_all(b"stdin-eof\n")
            .expect("write synthetic stdout");
        let case = case.to_string_lossy();
        match case.as_ref() {
            "closed_code" => std::io::stderr()
                .write_all(b"{\"error_code\":\"SYNTHETIC_CHILD_ERROR\"}\n")
                .expect("write bounded diagnostic record"),
            "raw_prompt" => std::io::stderr()
                .write_all(b"synthetic prompt and API_KEY=fixture-only\n")
                .expect("write synthetic raw log"),
            "oversized" => std::io::stderr()
                .write_all(&vec![b'x'; MAX_STDERR_BYTES + 1])
                .expect("write oversized synthetic stderr"),
            _ => std::process::exit(92),
        }
        std::io::stdout().flush().expect("flush synthetic stdout");
        std::io::stderr().flush().expect("flush synthetic stderr");
        std::process::exit(0);
    }
}
