//! Spawn the caller-selected Muse client inside the already verified module
//! owner group and attach its stdio to the bounded MSP transport.
//!
//! This module owns no process group, Job, restart, or shutdown policy. The
//! module owner remains responsible for the native process family. A direct
//! child exit and either stdio EOF are observations about that stream/process
//! only; none proves family departure.
//!
//! Driver lifetime contract (including start errors, cancellation and unwind):
//! retain NativeChildCustodian outside the consumer task before spawning. Join
//! or abort-and-join that consumer, then join the custodian while the runtime
//! is still alive. MSP Drop only queues Close; its coordinator/writer and the
//! stderr drain must remain schedulable until that join completes. Never move
//! the custodian into the consumer or drop it during runtime teardown. Like
//! Host ShutdownCustody, this is a driver-owned shutdown obligation, not a Drop
//! fallback or a detached reaper. The generic module owner separately retains
//! its non-killing group/Job until actual family departure.
//! Retain start/consumer errors until after join; propagating either with ?
//! before draining custody would violate this contract.

use crate::msp::{MspTransport, Notification, Options, TransportEnd};
use jsonrpsee_core::server::RpcModule;
use std::{
    error::Error,
    ffi::OsString,
    fmt, io,
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{ChildStderr, ChildStdin, ChildStdout},
    sync::mpsc,
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
    Closed {
        error_code: Option<String>,
    },
    ReadFailed,
    DrainTaskFailed,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeWaitFailures {
    pub first: Option<io::ErrorKind>,
    pub count: u64,
}

#[derive(Default)]
struct DirectState {
    child: Option<Child>,
    observed_exit: Option<ExitStatus>,
    wait_failures: NativeWaitFailures,
    #[cfg(test)]
    injected_wait_errors: usize,
}

impl DirectState {
    fn poll_exit(&mut self) -> io::Result<Option<ExitStatus>> {
        if self.observed_exit.is_some() {
            return Ok(self.observed_exit);
        }
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("MUSE_NATIVE_CHILD_NOT_STARTED"))?;
        #[cfg(test)]
        let result = if self.injected_wait_errors > 0 {
            self.injected_wait_errors -= 1;
            Err(io::Error::other("fixture-local wait observation failure"))
        } else {
            child.try_wait()
        };
        #[cfg(not(test))]
        let result = child.try_wait();
        match result {
            Ok(status) => {
                self.observed_exit = status;
                Ok(status)
            }
            Err(error) => {
                self.wait_failures.first.get_or_insert(error.kind());
                self.wait_failures.count = self.wait_failures.count.saturating_add(1);
                // Keep the actual Child even when its exit cannot be observed.
                Err(error)
            }
        }
    }
}

/// Single-use direct-child custody retained by the outer adapter driver.
///
/// Create before spawn, outside every abortable consumer task. On every exit
/// path, first join the consumer, then await join() with IO/time still enabled.
/// Cancelling join borrows this owner; it never transfers its Child or drain.
/// There is deliberately no blocking Drop, kill, or unregistered reaper. A
/// caller that drops this owner before joining violates the lifetime contract.
#[must_use = "retain outside the consumer and join before runtime/module exit"]
pub struct NativeChildCustodian {
    state: Arc<Mutex<DirectState>>,
    stderr_drain: Option<JoinHandle<NativeStderrResult>>,
    stderr_result: Option<NativeStderrResult>,
}

impl Default for NativeChildCustodian {
    fn default() -> Self {
        Self::new()
    }
}

/// Direct exit and joined stderr only. Wait failures remain visible even if a
/// later poll succeeds. This is neither native-effect nor family-exit proof.
#[derive(Clone, Debug)]
pub struct NativeCustodianObservation {
    pub direct_child: ExitStatus,
    pub stderr: NativeStderrResult,
    pub wait_failures: NativeWaitFailures,
}

impl NativeChildCustodian {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(DirectState::default())),
            stderr_drain: None,
            stderr_result: None,
        }
    }

    /// Recover an observer even after the consumer/start error was dropped.
    pub fn direct_child(&self) -> Option<NativeDirectChild> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.child.as_ref()?;
        Some(NativeDirectChild {
            state: Arc::downgrade(&self.state),
        })
    }

    /// Observation failures remain inspectable while join is still pending.
    pub fn wait_failures(&self) -> NativeWaitFailures {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .wait_failures
    }

    /// None means no successful OS spawn in this custodian. Otherwise wait
    /// without timeout/kill, retrying observation errors with the same handle.
    /// The driver must retain this owner and keep the runtime alive if waiting
    /// remains uncertain. Descendants may hold stderr open after direct exit;
    /// this join intentionally retains its drain until EOF/task completion.
    pub async fn join(&mut self) -> Option<NativeCustodianObservation> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .child
            .as_ref()?;
        let status = loop {
            let observed = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .poll_exit();
            if let Ok(Some(status)) = observed {
                break status;
            }
            // Never block the executor that owns MSP Close and stderr IO.
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        if self.stderr_result.is_none() {
            // Borrow the retained JoinHandle, as PendingSupervisorReaper does.
            // No await exists between completion, caching and taking it.
            let result = match self.stderr_drain.as_mut() {
                Some(task) => match task.await {
                    Ok(result) => result,
                    Err(_) => NativeStderrResult::DrainTaskFailed,
                },
                None => NativeStderrResult::Unavailable,
            };
            self.stderr_result = Some(result);
            self.stderr_drain = None;
        }
        Some(NativeCustodianObservation {
            direct_child: status,
            stderr: self
                .stderr_result
                .as_ref()
                .expect("cached drain result")
                .clone(),
            wait_failures: self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .wait_failures,
        })
    }
}

/// Non-owning direct-child observer. Dropping it never waits, closes stdin or
/// releases process custody. NativeChildCustodian must outlive this consumer.
pub struct NativeDirectChild {
    state: Weak<Mutex<DirectState>>,
}

impl fmt::Debug for NativeDirectChild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeDirectChild")
            .field("pid", &self.id())
            .finish()
    }
}

impl NativeDirectChild {
    /// None is not departure proof (it also covers a lost driver custodian).
    pub fn id(&self) -> Option<u32> {
        let state = self.state.upgrade()?;
        let state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.observed_exit.is_some() {
            None
        } else {
            state.child.as_ref().map(Child::id)
        }
    }

    /// Observe this process only. Errors/cancellation retain driver custody.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        loop {
            let observed = {
                let state = self
                    .state
                    .upgrade()
                    .ok_or_else(|| io::Error::other("MUSE_NATIVE_CUSTODIAN_LOST"))?;
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .poll_exit()
            };
            if let Some(status) = observed? {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

/// An attached consumer, not the process owner. transport.close() drains
/// admitted submissions before closing stdin. Dropping this consumer queues
/// the same barrier; the driver must keep Tokio alive and join its custodian.
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
    /// The driver must still join its custodian afterward, including when this
    /// future is cancelled or its direct-child observation returns an error.
    pub async fn close_and_wait_direct(&mut self) -> NativeCloseObservation {
        let transport = self.transport.close().await;
        let direct_child = self.direct_child.wait().await;
        NativeCloseObservation {
            transport,
            direct_child,
        }
    }
}

/// Post-spawn errors carry an observer; the driver custodian retains the actual
/// handle even when this error is dropped. Always join it after start failures.
#[derive(Debug)]
pub enum NativeOwnerStartError {
    AdapterOwnerUnverified,
    CustodianAlreadyUsed,
    Spawn(io::Error),
    AsyncStdioUnavailable {
        kind: io::ErrorKind,
        direct_child: NativeDirectChild,
    },
    PipedStdioUnavailable {
        direct_child: NativeDirectChild,
    },
    TransportUnavailable {
        reason: TransportEnd,
        direct_child: NativeDirectChild,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeStartEffect {
    NotStarted,
    PossiblyStarted,
}

impl NativeOwnerStartError {
    /// Effect of this launch attempt only. CustodianAlreadyUsed does not make
    /// a no-effect claim about the earlier child retained by that custodian.
    pub fn native_effect(&self) -> NativeStartEffect {
        match self {
            Self::AdapterOwnerUnverified | Self::CustodianAlreadyUsed | Self::Spawn(_) => {
                NativeStartEffect::NotStarted
            }
            Self::AsyncStdioUnavailable { .. }
            | Self::PipedStdioUnavailable { .. }
            | Self::TransportUnavailable { .. } => NativeStartEffect::PossiblyStarted,
        }
    }
}

impl fmt::Display for NativeOwnerStartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdapterOwnerUnverified => f.write_str("MUSE_ADAPTER_OWNER_UNVERIFIED"),
            Self::CustodianAlreadyUsed => f.write_str("MUSE_NATIVE_CUSTODIAN_ALREADY_USED"),
            Self::Spawn(error) => write!(f, "MUSE_NATIVE_SPAWN_{:?}", error.kind()),
            Self::AsyncStdioUnavailable { kind, .. } => {
                write!(f, "MUSE_NATIVE_POST_SPAWN_STDIO_{kind:?}")
            }
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
            Self::AdapterOwnerUnverified
            | Self::CustodianAlreadyUsed
            | Self::AsyncStdioUnavailable { .. }
            | Self::PipedStdioUnavailable { .. } => None,
        }
    }
}

/// Launch the supplied client with caller-selected argv and inherited native
/// settings, then attach its pipes to MSP. The normal service invocation is
/// NativeClientSpec::serve(installed_executable); no provider/model switches,
/// authentication setup, HOME/XDG changes, or environment copying are added.
/// The supplied custodian must remain in the outer driver, outside the task
/// consuming the returned transport. Join it even when this function errors.
pub async fn spawn_owned(
    custodian: &mut NativeChildCustodian,
    spec: NativeClientSpec,
    server: RpcModule<()>,
) -> Result<NativeOwnedChild, NativeOwnerStartError> {
    swarm_process::module_owner::verify_current_adapter_from_env()
        .map_err(|_| NativeOwnerStartError::AdapterOwnerUnverified)?;

    let (direct_child, stdin, stdout) = spawn_piped(custodian, build_command(&spec), attach_stdio)?;

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

type AsyncStdio = (ChildStdin, ChildStdout, ChildStderr);

fn attach_stdio(
    stdin: std::process::ChildStdin,
    stdout: std::process::ChildStdout,
    stderr: std::process::ChildStderr,
) -> io::Result<AsyncStdio> {
    Ok((
        ChildStdin::from_std(stdin)?,
        ChildStdout::from_std(stdout)?,
        ChildStderr::from_std(stderr)?,
    ))
}

fn spawn_piped(
    custodian: &mut NativeChildCustodian,
    mut command: Command,
    attach: impl FnOnce(
        std::process::ChildStdin,
        std::process::ChildStdout,
        std::process::ChildStderr,
    ) -> io::Result<AsyncStdio>,
) -> Result<(NativeDirectChild, ChildStdin, ChildStdout), NativeOwnerStartError> {
    // std Command::spawn is the OS-creation boundary. Unlike Tokio 1.53.1
    // Command::spawn, it performs no subsequent reactor/pidfd registration.
    // Install direct custody before the first fallible async pipe conversion.
    let mut state = custodian
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.child.is_some() {
        return Err(NativeOwnerStartError::CustodianAlreadyUsed);
    }
    state.child = Some(command.spawn().map_err(NativeOwnerStartError::Spawn)?);
    let direct_child = NativeDirectChild {
        state: Arc::downgrade(&custodian.state),
    };
    let child = state.child.as_mut().expect("installed OS child");
    let (Some(stdin), Some(stdout), Some(stderr)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        return Err(NativeOwnerStartError::PipedStdioUnavailable { direct_child });
    };
    drop(state);
    let (stdin, stdout, stderr) = match attach(stdin, stdout, stderr) {
        Ok(pipes) => pipes,
        Err(error) => {
            return Err(NativeOwnerStartError::AsyncStdioUnavailable {
                kind: error.kind(),
                direct_child,
            });
        }
    };
    custodian.stderr_drain = Some(tokio::spawn(read_closed_stderr(stderr)));
    Ok((direct_child, stdin, stdout))
}

fn build_command(spec: &NativeClientSpec) -> Command {
    let mut command = Command::new(&spec.executable);
    command
        .args(&spec.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(current_dir) = &spec.current_dir {
        command.current_dir(current_dir);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
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
    use std::{
        future::Future,
        io::{Read as _, Write as _},
        path::Path,
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        task::Poll,
        time::{Instant, SystemTime, UNIX_EPOCH},
    };
    use swarm_process::module_owner::{ModuleOwnerPlan, ProtectedResolverMap, run_module};
    use tokio::io::{AsyncBufReadExt, BufReader};

    const CASE_ENV: &str = "MUSE_OWNED_FIXTURE_CASE";
    const ROOT_ENV: &str = "MUSE_OWNED_FIXTURE_ROOT";
    const OWNER_TEST: &str = "native_owner::tests::synthetic_module_owner_fixture";
    const WORKER_TEST: &str = "native_owner::tests::synthetic_adapter_worker_fixture";
    const CLIENT_TEST: &str = "native_owner::tests::synthetic_native_child_fixture";
    const PAYLOAD_BYTES: usize = 512 * 1_024;

    fn fixture_args(test: &str) -> Vec<OsString> {
        [
            "--exact",
            test,
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ]
        .into_iter()
        .map(OsString::from)
        .collect()
    }

    fn fixture_spec(test: &str) -> NativeClientSpec {
        NativeClientSpec {
            executable: std::env::current_exe().expect("fixture executable"),
            args: fixture_args(test),
            current_dir: None,
        }
    }

    fn fixture_context() -> Option<(PathBuf, String)> {
        Some((
            PathBuf::from(std::env::var_os(ROOT_ENV)?),
            std::env::var(CASE_ENV).ok()?,
        ))
    }

    // Ordinary OS threads, independent of the watched Tokio executor. Only the
    // two synthetic fixture processes arm this abort path. They exit themselves
    // on watchdog failure; production has no kill, fault flag or Drop fallback.
    struct FixtureAbortWatch {
        stop: Arc<AtomicBool>,
        thread: std::thread::JoinHandle<()>,
    }

    impl FixtureAbortWatch {
        fn arm(root: &Path, role: &'static str) -> Self {
            let root = root.to_owned();
            let stop = Arc::new(AtomicBool::new(false));
            let stopped = stop.clone();
            let (ready, armed) = std::sync::mpsc::sync_channel(0);
            let thread = std::thread::spawn(move || {
                ready.send(()).expect("watchdog handshake");
                while !stopped.load(Ordering::Acquire) {
                    if root.join("abort").exists() {
                        let _ = std::fs::write(root.join(format!("{role}-aborted")), b"fixture");
                        std::process::exit(86);
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
            armed.recv().expect("independent watchdog is armed");
            Self { stop, thread }
        }

        fn stop(self) {
            self.stop.store(true, Ordering::Release);
            self.thread.join().expect("fixture watchdog thread joined");
        }
    }

    /// Parent has no Tokio runtime. Regressing to 80a blocks the worker runtime,
    /// but cannot freeze this watchdog or the fixture-only abort threads.
    #[test]
    fn current_thread_drop_paths_external_watchdog() {
        for case in [
            "return",
            "cancel_close",
            "abort_task",
            "unwind",
            "wait_failure",
            "attach_failure",
            "raw_prompt",
            "oversized",
        ] {
            let parent = std::fs::canonicalize(std::env::temp_dir()).expect("fixture parent");
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = parent.join(format!(
                "muse-custody-{}-{nonce}-{case}",
                std::process::id()
            ));
            std::fs::create_dir(&root).expect("fresh fixture scope");
            let root = std::fs::canonicalize(root).expect("canonical fixture scope");
            assert_eq!(root.parent(), Some(parent.as_path()));
            let mut owner = Command::new(std::env::current_exe().unwrap())
                .args(fixture_args(OWNER_TEST))
                .env(ROOT_ENV, &root)
                .env(CASE_ENV, case)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("controlled module-owner test process");
            let started = Instant::now();
            let mut expired = false;
            let mut abort_started = None;
            let status = loop {
                if let Some(status) = owner.try_wait().expect("watch exact fixture owner") {
                    break status;
                }
                if !expired && started.elapsed() > Duration::from_secs(30) {
                    expired = true;
                    std::fs::write(root.join("abort"), b"external watchdog expired").unwrap();
                    abort_started = Some(Instant::now());
                }
                if abort_started.is_some_and(|at| at.elapsed() > Duration::from_secs(10)) {
                    // Last-resort fixture cleanup affects only the directly
                    // spawned helper. Worker/client have their independent abort
                    // marker; retain the scope for diagnosis, never remove it.
                    let _ = owner.kill();
                    let _ = owner.wait();
                    panic!(
                        "fixture owner cleanup failed for {case}: {}",
                        root.display()
                    );
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            assert!(
                !expired,
                "external watchdog caught {case} deadlock: {}",
                root.display()
            );
            assert!(
                status.success(),
                "fixture owner failed for {case}: {}",
                root.display()
            );
            assert!(
                root.join("worker-joined").is_file(),
                "driver must join custody"
            );
            assert!(
                root.join("owner-family-empty").is_file(),
                "outer owner must finish separately"
            );
            assert!(
                root.join("native-observed").is_file(),
                "native EOF/frame check must run"
            );
            // Only this fresh, canonical, direct child of the temp directory.
            assert_eq!(std::fs::canonicalize(&root).unwrap(), root);
            assert_eq!(root.parent(), Some(parent.as_path()));
            std::fs::remove_dir_all(&root).expect("remove completed synthetic scope");
        }
    }

    #[test]
    #[ignore = "invoked only by the external watchdog parent"]
    fn synthetic_module_owner_fixture() {
        let Some((root, case)) = fixture_context() else {
            return;
        };
        let plan = ModuleOwnerPlan {
            state_dir: root.join("module-state"),
            executable: std::env::current_exe().unwrap(),
            argv: fixture_args(WORKER_TEST)
                .into_iter()
                .map(|arg| arg.into_string().unwrap())
                .collect(),
            protected_refs: Vec::new(),
            environment: vec![
                (ROOT_ENV.to_owned(), root.to_str().unwrap().to_owned()),
                (CASE_ENV.to_owned(), case),
            ],
            module: "fixture.muse".into(),
            binding: "fixture.muse.binding".into(),
            generation: "1".into(),
            artifact_id: "fixture.muse.artifact".into(),
            artifact_version: "1".into(),
            build_id: Some("synthetic-only".into()),
            protocol: "1.0".into(),
            module_contract: "{}".into(),
            module_client_id: "fixture.muse.client".into(),
            boot_id: "00000000-0000-4000-8000-000000000001".into(),
        };
        // Empty private resolver map: no credentials, account or native model.
        let resolver_path = root.join("resolver.json");
        let resolver = serde_json::json!({
            "version": 1, "module": plan.module, "binding": plan.binding,
            "generation": plan.generation, "artifact_id": plan.artifact_id,
            "artifact_version": plan.artifact_version, "build_id": plan.build_id,
            "protocol": plan.protocol, "module_contract": plan.module_contract,
            "refs": [],
        });
        std::fs::write(&resolver_path, serde_json::to_vec(&resolver).unwrap()).unwrap();
        let status = run_module(plan, ProtectedResolverMap::load(&resolver_path).unwrap())
            .expect("actual generic owner retained group through departure");
        assert!(status.success());
        std::fs::write(root.join("owner-family-empty"), b"generic owner returned").unwrap();
    }

    async fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
        std::future::poll_fn(|context| {
            assert!(future.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    #[test]
    #[ignore = "verified worker launched by the real generic module owner"]
    fn synthetic_adapter_worker_fixture() {
        let Some((root, case)) = fixture_context() else {
            return;
        };
        let watchdog = FixtureAbortWatch::arm(&root, "worker");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("watched single-thread runtime");
        runtime.block_on(async {
            // The helper publishes worker.json just after spawn. Wait for that
            // real proof; never substitute a test-only membership bypass.
            while swarm_process::module_owner::verify_current_adapter_from_env().is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let mut custody = NativeChildCustodian::new(); // Outside the consumer!
            let command = build_command(&fixture_spec(CLIENT_TEST));
            if case == "attach_failure" {
                let error = match spawn_piped(&mut custody, command, |stdin, stdout, stderr| {
                    // Keep the synthetic child's pipes open through libtest's
                    // banner. Otherwise injected attachment failure can make
                    // that banner fail before the fixture body even starts.
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while !root.join("native-entry-ready").is_file() {
                        assert!(
                            Instant::now() < deadline,
                            "synthetic native entry did not start"
                        );
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    drop((stdin, stdout, stderr));
                    Err(io::Error::other("fixture-local post-spawn attach failure"))
                }) {
                    Err(error) => error,
                    Ok(_) => panic!("attachment injection did not fail"),
                };
                assert_eq!(error.native_effect(), NativeStartEffect::PossiblyStarted);
                let pid = match &error {
                    NativeOwnerStartError::AsyncStdioUnavailable { direct_child, .. } => {
                        direct_child.id()
                    }
                    _ => panic!("post-spawn attachment failure must carry an observer"),
                };
                assert!(pid.is_some());
                drop(error); // Ordinary error return/drop cannot release custody.
                let mut recovered = custody
                    .direct_child()
                    .expect("recover observer after error drop");
                assert_eq!(recovered.id(), pid);
                assert_eq!(recovered.wait().await.unwrap().code(), Some(23));
                let joined = custody.join().await.unwrap();
                assert_eq!(joined.direct_child.code(), Some(23));
                assert_eq!(joined.stderr, NativeStderrResult::Unavailable);
                assert_eq!(joined.wait_failures, NativeWaitFailures::default());
            } else {
                let (mut direct_child, stdin, stdout) =
                    spawn_piped(&mut custody, command, attach_stdio).expect("fixture child");
                // libtest emits a banner before entering a test. Consume only
                // this bounded fixture prelude, leaving actual pipes to MSP.
                // Production spawn_owned has no prelude/filter/bypass.
                let mut stdout = BufReader::new(stdout);
                let mut prelude = Vec::new();
                loop {
                    assert_ne!(stdout.read_until(b'\n', &mut prelude).await.unwrap(), 0);
                    assert!(prelude.len() <= 4_096);
                    if prelude.ends_with(b"MUSE_FIXTURE_READY\n") {
                        break;
                    }
                }
                let pid = direct_child.id();
                let mut waiting = Box::pin(direct_child.wait());
                poll_pending(waiting.as_mut()).await;
                drop(waiting);
                assert_eq!(direct_child.id(), pid);
                if case == "wait_failure" {
                    custody.state.lock().unwrap().injected_wait_errors = 2;
                    let pid = direct_child.id();
                    assert_eq!(
                        direct_child.wait().await.unwrap_err().kind(),
                        io::ErrorKind::Other
                    );
                    assert_eq!(direct_child.id(), pid);
                }
                let (transport, notifications) = MspTransport::attach_owned_stdio(
                    stdout,
                    stdin,
                    RpcModule::new(()),
                    Options::default(),
                )
                .expect("actual owner-verified MSP attachment");
                let consumer = NativeOwnedChild {
                    transport,
                    notifications,
                    direct_child,
                };
                let (armed, started) = tokio::sync::oneshot::channel();
                let mode = case.clone();
                let mut task = tokio::spawn(async move {
                    let mut consumer = consumer;
                    for data in ["x".repeat(PAYLOAD_BYTES), "second".into()] {
                        let params = serde_json::json!({ "data": data })
                            .as_object()
                            .unwrap()
                            .clone();
                        // Cancelling a receipt does not cancel the admitted frame.
                        drop(consumer.transport.notify("fixture.frame", params).unwrap());
                    }
                    let _ = armed.send(());
                    match mode.as_str() {
                        "cancel_close" => {
                            let mut closing = Box::pin(consumer.close_and_wait_direct());
                            poll_pending(closing.as_mut()).await;
                            drop(closing);
                        }
                        "abort_task" => std::future::pending::<()>().await,
                        "unwind" => panic!("intentional fixture consumer unwind"),
                        _ => {}
                    }
                    drop(consumer); // 80a freezes here, before MSP can close stdin.
                });
                started
                    .await
                    .expect("consumer admitted both ordered frames");
                if case == "abort_task" {
                    task.abort();
                }
                let completion = (&mut task).await;
                match case.as_str() {
                    "abort_task" => assert!(completion.unwrap_err().is_cancelled()),
                    "unwind" => assert!(completion.unwrap_err().is_panic()),
                    _ => completion.expect("consumer returned without blocking Drop"),
                }
                // Join cancellation must retain both the Child and stderr join.
                // The native fixture cannot exit until the driver releases read.
                let mut joining = Box::pin(custody.join());
                poll_pending(joining.as_mut()).await;
                drop(joining);
                assert!(custody.direct_child().unwrap().id().is_some());
                if case == "wait_failure" {
                    assert_eq!(custody.wait_failures().count, 2);
                    assert_eq!(custody.wait_failures().first, Some(io::ErrorKind::Other));
                }
                std::fs::write(root.join("read"), b"consumer joined; runtime stays alive").unwrap();
                let joined = custody.join().await.expect("retained direct child");
                assert!(joined.direct_child.success());
                assert_eq!(
                    joined.wait_failures.count,
                    if case == "wait_failure" { 2 } else { 0 }
                );
                let expected = if matches!(case.as_str(), "raw_prompt" | "oversized") {
                    None
                } else {
                    Some("SYNTHETIC_CHILD_ERROR".to_owned())
                };
                assert_eq!(
                    joined.stderr,
                    NativeStderrResult::Closed {
                        error_code: expected
                    }
                );
                assert_eq!(custody.direct_child().unwrap().id(), None);
                assert_eq!(custody.join().await.unwrap().stderr, joined.stderr);
            }
            std::fs::write(root.join("worker-joined"), b"direct child and drain joined").unwrap();
        });
        watchdog.stop();
    }

    #[test]
    #[ignore = "synthetic native client; never an account/model/harness call"]
    fn synthetic_native_child_fixture() {
        let Some((root, case)) = fixture_context() else {
            return;
        };
        let watchdog = FixtureAbortWatch::arm(&root, "native");
        std::fs::write(root.join("native-entry-ready"), b"fixture body entered").unwrap();
        if case != "attach_failure" {
            std::io::stdout()
                .write_all(b"MUSE_FIXTURE_READY\n")
                .unwrap();
            std::io::stdout().flush().unwrap();
            match case.as_str() {
                "raw_prompt" => std::io::stderr()
                    .write_all(b"synthetic prompt and API_KEY=fixture-only\n")
                    .unwrap(),
                "oversized" => std::io::stderr()
                    .write_all(&vec![b'x'; 128 * 1_024])
                    .unwrap(),
                _ => std::io::stderr()
                    .write_all(b"{\"error_code\":\"SYNTHETIC_CHILD_ERROR\"}\n")
                    .unwrap(),
            }
            std::io::stderr().flush().unwrap();
            while !root.join("read").exists() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        let mut input = Vec::new();
        std::io::stdin()
            .read_to_end(&mut input)
            .expect("EOF after MSP submission barrier");
        if case == "attach_failure" {
            assert!(input.is_empty());
        } else {
            assert_eq!(input.last(), Some(&b'\n'));
            let frames: Vec<serde_json::Value> = input
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice(line).expect("complete admitted MSP frame"))
                .collect();
            assert_eq!(frames.len(), 2);
            assert_eq!(frames[0]["method"], "fixture.frame");
            assert_eq!(frames[1]["method"], "fixture.frame");
            assert_eq!(
                frames[0]["params"]["data"].as_str().unwrap(),
                "x".repeat(PAYLOAD_BYTES)
            );
            assert_eq!(frames[1]["params"]["data"], "second");
        }
        std::fs::write(
            root.join("native-observed"),
            b"ordered frames then stdin EOF",
        )
        .unwrap();
        watchdog.stop();
        // Avoid libtest's trailing stdout/stderr, which is not MSP traffic.
        std::process::exit(if case == "attach_failure" { 23 } else { 0 });
    }
}
