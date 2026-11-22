use crate::platform::{CustodyGuarantee, CustodyObserver, OwnedChild, custody::AgentSpawnError};
use eliot_core::{Error, SecretPolicy, StderrSnapshot, limits::MAX_FRAME_BYTES};
use eliot_plugin::{StderrCapture, spawn_stderr, spawn_stderr_prefix};
use std::{ffi::OsStr, io, process::ExitStatus, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{ChildStdin, ChildStdout, Command},
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep_until, timeout_at},
};

const CLEANUP_GRACE: Duration = Duration::from_secs(5);

#[cfg(windows)]
const LOCAL_BASE_ENVIRONMENT: &[&str] = &[
    "PATH",
    "PATHEXT",
    "SystemRoot",
    "SystemDrive",
    "WINDIR",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "HOME",
    "APPDATA",
    "LOCALAPPDATA",
    "COMSPEC",
];

#[cfg(unix)]
const LOCAL_BASE_ENVIRONMENT: &[&str] = &[
    "PATH", "HOME", "TMPDIR", "TMP", "TEMP", "USER", "LOGNAME", "LANG", "LC_ALL", "LC_CTYPE",
];

#[cfg(not(any(windows, unix)))]
const LOCAL_BASE_ENVIRONMENT: &[&str] = &[];

/// Build a local child command with the shared, explicit platform environment.
///
/// These checks and scripts need installed developer tools from PATH, while
/// ambient credentials and unrelated daemon variables must stay out.
pub(crate) fn local_process_command(program: impl AsRef<OsStr>) -> eliot_core::Result<Command> {
    let mut command = Command::new(program);
    command.env_clear();

    #[cfg(windows)]
    for (name, message) in [
        ("SystemRoot", "Windows SystemRoot is unavailable"),
        ("PATHEXT", "Windows PATHEXT is unavailable"),
    ] {
        if std::env::var_os(name).is_none() {
            return Err(Error::new("unavailable", message));
        }
    }

    for &name in LOCAL_BASE_ENVIRONMENT {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    Ok(command)
}

#[derive(Debug)]
pub(crate) struct ProcessCapture {
    pub(crate) stdout: Vec<u8>,
    pub(crate) stdout_complete: bool,
    pub(crate) stderr: StderrSnapshot,
    pub(crate) status: ExitStatus,
    pub(crate) custody: CustodyGuarantee,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProcessFailure {
    InputLimit,
    InvalidTimeout,
    Exit,
    Timeout,
    Cancelled,
    OutputLimit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProcessUncertainty {
    Spawn,
    Custody,
    PipeSetup,
    Stdin,
    Stdout,
    Exit,
    Quiescence,
    InterruptedSuccess,
    Supervisor,
}

impl ProcessUncertainty {
    pub(crate) const fn stable_code(self) -> &'static str {
        match self {
            Self::Spawn => "script_spawn_unknown",
            Self::Custody => "script_custody_unknown",
            Self::PipeSetup => "script_pipe_setup_unknown",
            Self::Stdin => "script_stdin_unknown",
            Self::Stdout => "script_stdout_unknown",
            Self::Exit => "script_exit_unknown",
            Self::Quiescence => "script_quiescence_unknown",
            Self::InterruptedSuccess => "script_interrupted_success",
            Self::Supervisor => "script_supervisor_unknown",
        }
    }
}

#[derive(Debug)]
pub(crate) enum ProcessOutcome {
    Completed(ProcessCapture),
    NotSpawned(Error),
    Failed {
        reason: ProcessFailure,
        capture: Option<ProcessCapture>,
    },
    Unknown {
        reason: ProcessUncertainty,
        quiescent: bool,
    },
}

struct ProcessIo {
    stdin_task: JoinHandle<io::Result<()>>,
    stdin_result: Option<Result<io::Result<()>, tokio::task::JoinError>>,
    stdout_task: JoinHandle<io::Result<StdoutCapture>>,
    stdout_result: Option<Result<io::Result<StdoutCapture>, tokio::task::JoinError>>,
    stderr_task: StderrCapture,
}

struct InterruptedProcess<'a> {
    child: &'a mut OwnedChild,
    observer: Option<&'a CustodyObserver>,
    custody: CustodyGuarantee,
    status: Option<ExitStatus>,
    root_exit_observed: bool,
}

/// Run one already-resolved command. Output is exposed only after the direct
/// exit, all required pipe drainage and owned-custody quiescence are known.
pub(crate) async fn run_owned_process(
    command: Command,
    stdin: Vec<u8>,
    timeout: Duration,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
) -> ProcessOutcome {
    run_owned_process_with_stderr(
        command,
        stdin,
        timeout,
        policy,
        cancellation,
        StderrExcerpt::Tail,
    )
    .await
}

#[derive(Clone, Copy)]
pub(crate) enum StderrExcerpt {
    Tail,
    Prefix,
}

pub(crate) async fn run_owned_process_with_stderr(
    command: Command,
    stdin: Vec<u8>,
    timeout: Duration,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
    stderr: StderrExcerpt,
) -> ProcessOutcome {
    if stdin.len() > MAX_FRAME_BYTES {
        return ProcessOutcome::Failed {
            reason: ProcessFailure::InputLimit,
            capture: None,
        };
    }
    if timeout.is_zero() || Instant::now().checked_add(timeout).is_none() {
        return ProcessOutcome::Failed {
            reason: ProcessFailure::InvalidTimeout,
            capture: None,
        };
    }
    if cancellation_requested(&cancellation) {
        return ProcessOutcome::Failed {
            reason: ProcessFailure::Cancelled,
            capture: None,
        };
    }

    // Keep process ownership alive if the caller drops this future. The
    // cancellation receiver and timeout remain the supervisor's stop paths.
    match tokio::spawn(run_owned_process_inner(
        command,
        stdin,
        timeout,
        policy,
        cancellation,
        stderr,
    ))
    .await
    {
        Ok(outcome) => outcome,
        Err(_) => ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Supervisor,
            quiescent: false,
        },
    }
}

enum RootExitWait {
    #[cfg(not(target_os = "linux"))]
    Reaped(ExitStatus),
    #[cfg(target_os = "linux")]
    Unreaped,
}

async fn wait_root_exit(child: &mut OwnedChild) -> eliot_core::Result<RootExitWait> {
    #[cfg(target_os = "linux")]
    {
        child.wait_exit_unreaped().await?;
        Ok(RootExitWait::Unreaped)
    }
    #[cfg(not(target_os = "linux"))]
    {
        child.wait().await.map(RootExitWait::Reaped)
    }
}

async fn run_owned_process_inner(
    mut command: Command,
    stdin: Vec<u8>,
    timeout: Duration,
    policy: Arc<SecretPolicy>,
    mut cancellation: watch::Receiver<bool>,
    stderr: StderrExcerpt,
) -> ProcessOutcome {
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return ProcessOutcome::Failed {
            reason: ProcessFailure::InvalidTimeout,
            capture: None,
        };
    };
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = match OwnedChild::spawn_report(command) {
        Ok(child) => child,
        Err(AgentSpawnError::NotSpawned(error)) => return ProcessOutcome::NotSpawned(error),
        Err(AgentSpawnError::Uncertain(_)) => {
            return ProcessOutcome::Unknown {
                reason: ProcessUncertainty::Spawn,
                quiescent: false,
            };
        }
    };
    let custody = child.guarantee();
    #[cfg(unix)]
    let observer = child.observer().ok();
    #[cfg(not(unix))]
    let observer: Option<CustodyObserver> = None;

    let (stdin_pipe, stdout_pipe, stderr_pipe) =
        (child.take_stdin(), child.take_stdout(), child.take_stderr());
    let (Some(stdin_pipe), Some(stdout_pipe), Some(stderr_pipe)) =
        (stdin_pipe, stdout_pipe, stderr_pipe)
    else {
        let quiescent = terminate_and_wait(&mut child, observer.as_ref()).await;
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::PipeSetup,
            quiescent,
        };
    };

    let mut io = ProcessIo {
        stdin_task: tokio::spawn(write_stdin(stdin_pipe, stdin)),
        stdin_result: None,
        stdout_task: tokio::spawn(capture_stdout(stdout_pipe)),
        stdout_result: None,
        stderr_task: match stderr {
            StderrExcerpt::Tail => spawn_stderr(stderr_pipe, policy),
            StderrExcerpt::Prefix => spawn_stderr_prefix(stderr_pipe, policy),
        },
    };
    #[cfg(target_os = "linux")]
    let status = None;
    #[cfg(not(target_os = "linux"))]
    let mut status = None;
    let mut root_exit_observed = false;
    let custody_unavailable = {
        #[cfg(windows)]
        {
            false
        }
        #[cfg(unix)]
        {
            custody == CustodyGuarantee::Unknown || observer.is_none()
        }
        #[cfg(not(any(windows, unix)))]
        {
            true
        }
    };
    let mut stop = custody_unavailable.then_some(StopReason::Unknown(ProcessUncertainty::Custody));

    while stop.is_none() {
        if io.stdin_result.is_some() && io.stdout_result.is_some() && root_exit_observed {
            break;
        }

        tokio::select! {
            biased;
            _ = wait_for_cancellation(&mut cancellation) => {
                stop = Some(StopReason::Failure(ProcessFailure::Cancelled));
            }
            _ = sleep_until(deadline) => {
                stop = Some(StopReason::Failure(ProcessFailure::Timeout));
            }
            result = &mut io.stdin_task, if io.stdin_result.is_none() => {
                let complete = matches!(&result, Ok(Ok(())));
                io.stdin_result = Some(result);
                if !complete {
                    stop = Some(StopReason::Unknown(ProcessUncertainty::Stdin));
                }
            }
            result = &mut io.stdout_task, if io.stdout_result.is_none() => {
                let output_limit = matches!(&result, Ok(Ok(capture)) if capture.exceeded);
                let complete = matches!(&result, Ok(Ok(_)));
                io.stdout_result = Some(result);
                if output_limit {
                    stop = Some(StopReason::Failure(ProcessFailure::OutputLimit));
                } else if !complete {
                    stop = Some(StopReason::Unknown(ProcessUncertainty::Stdout));
                }
            }
            result = wait_root_exit(&mut child), if !root_exit_observed => {
                match result {
                    #[cfg(not(target_os = "linux"))]
                    Ok(RootExitWait::Reaped(exit)) => {
                        status = Some(exit);
                        root_exit_observed = true;
                    }
                    #[cfg(target_os = "linux")]
                    Ok(RootExitWait::Unreaped) => root_exit_observed = true,
                    Err(_) => stop = Some(StopReason::Unknown(ProcessUncertainty::Exit)),
                }
            }
        }
    }

    let interrupted = InterruptedProcess {
        child: &mut child,
        observer: observer.as_ref(),
        custody,
        status,
        root_exit_observed,
    };
    if let Some(stop) = stop {
        return settle_interrupted(interrupted, stop, io).await;
    }

    if !root_exit_observed {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Exit,
            quiescent: false,
        };
    }
    let stdout = match io.stdout_result.take() {
        Some(Ok(Ok(stdout))) => stdout,
        _ => unreachable!("stdout errors exit through the cleanup path"),
    };
    if !matches!(io.stdin_result.as_ref(), Some(Ok(Ok(())))) || !stdout.complete {
        io.stdout_result = Some(Ok(Ok(stdout)));
        return settle_interrupted(
            interrupted,
            StopReason::Unknown(ProcessUncertainty::Stdin),
            io,
        )
        .await;
    }

    let quiescence = tokio::select! {
        biased;
        _ = wait_for_cancellation(&mut cancellation) => {
            Err(StopReason::Failure(ProcessFailure::Cancelled))
        }
        _ = sleep_until(deadline) => {
            Err(StopReason::Failure(ProcessFailure::Timeout))
        }
        result = wait_for_quiescence(interrupted.child, interrupted.observer, deadline) => {
            result.map_err(|_| StopReason::Unknown(ProcessUncertainty::Quiescence))
        }
    };
    match quiescence {
        Ok(true) => {}
        Ok(false) => {
            io.stdout_result = Some(Ok(Ok(stdout)));
            return settle_interrupted(
                interrupted,
                StopReason::Failure(ProcessFailure::Timeout),
                io,
            )
            .await;
        }
        Err(stop) => {
            io.stdout_result = Some(Ok(Ok(stdout)));
            return settle_interrupted(interrupted, stop, io).await;
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Some(stop) = pending_interruption(deadline, &cancellation) {
            io.stdout_result = Some(Ok(Ok(stdout)));
            return settle_interrupted(interrupted, stop, io).await;
        }

        // The pre-reap observation is read-only. Reuse the validated owner
        // signal, then rescan while the exact leader still anchors the PGID.
        let _ = interrupted.child.terminate();
        let final_quiescent = matches!(
            interrupted
                .child
                .wait_group_empty_except_leader(CLEANUP_GRACE)
                .await,
            Ok(true)
        );
        if !final_quiescent {
            io.stdout_result = Some(Ok(Ok(stdout)));
            return settle_interrupted(
                interrupted,
                StopReason::Unknown(ProcessUncertainty::Quiescence),
                io,
            )
            .await;
        }
        if let Some(stop) = pending_interruption(deadline, &cancellation) {
            io.stdout_result = Some(Ok(Ok(stdout)));
            return settle_interrupted(interrupted, stop, io).await;
        }
    }

    #[cfg(target_os = "linux")]
    let status = match timeout_at(Instant::now() + CLEANUP_GRACE, child.wait()).await {
        Ok(Ok(status)) => status,
        _ => {
            return ProcessOutcome::Unknown {
                reason: ProcessUncertainty::Exit,
                quiescent: false,
            };
        }
    };
    #[cfg(target_os = "linux")]
    if !matches!(
        observer
            .as_ref()
            .and_then(|observer| observer.known_process_group_empty().ok()),
        Some(true)
    ) {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Quiescence,
            quiescent: false,
        };
    }
    #[cfg(not(target_os = "linux"))]
    let Some(status) = status else {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Exit,
            quiescent: false,
        };
    };

    let stderr = io.stderr_task.finish(Instant::now() + CLEANUP_GRACE).await;
    #[cfg(target_os = "linux")]
    if let Some(stop) = pending_interruption(deadline, &cancellation) {
        return classify_settled_process(SettledProcess {
            status,
            exit_was_known_before_cleanup: true,
            stop,
            quiescent: true,
            stdin_finished: true,
            stdout: Some(stdout),
            stderr,
            custody,
        });
    }
    classify_quiescent_capture(ProcessCapture {
        stdout: stdout.bytes,
        stdout_complete: true,
        stderr,
        status,
        custody,
    })
}

#[derive(Clone, Copy)]
enum StopReason {
    Failure(ProcessFailure),
    Unknown(ProcessUncertainty),
}

#[cfg(target_os = "linux")]
fn pending_interruption(
    deadline: Instant,
    cancellation: &watch::Receiver<bool>,
) -> Option<StopReason> {
    if cancellation_requested(cancellation) {
        Some(StopReason::Failure(ProcessFailure::Cancelled))
    } else if Instant::now() >= deadline {
        Some(StopReason::Failure(ProcessFailure::Timeout))
    } else {
        None
    }
}

struct StdoutCapture {
    bytes: Vec<u8>,
    complete: bool,
    exceeded: bool,
}

struct SettledProcess {
    status: ExitStatus,
    exit_was_known_before_cleanup: bool,
    stop: StopReason,
    quiescent: bool,
    stdin_finished: bool,
    stdout: Option<StdoutCapture>,
    stderr: StderrSnapshot,
    custody: CustodyGuarantee,
}

/// Classify after stdin/stdout completion and custody quiescence are known.
/// Stderr is diagnostic-only and never changes the primary process outcome.
fn classify_quiescent_capture(capture: ProcessCapture) -> ProcessOutcome {
    if !capture.stdout_complete {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Stdout,
            quiescent: true,
        };
    }
    if !capture.status.success() {
        return ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(capture),
        };
    }
    ProcessOutcome::Completed(capture)
}

async fn write_stdin(mut stdin: ChildStdin, bytes: Vec<u8>) -> io::Result<()> {
    stdin.write_all(&bytes).await?;
    drop(stdin);
    Ok(())
}

async fn capture_stdout(stdout: ChildStdout) -> io::Result<StdoutCapture> {
    capture_bounded(stdout, MAX_FRAME_BYTES).await
}

async fn capture_bounded<R>(mut reader: R, limit: usize) -> io::Result<StdoutCapture>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(limit.min(8192));
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(StdoutCapture {
                bytes,
                complete: true,
                exceeded: false,
            });
        }
        let remaining = limit.saturating_sub(bytes.len());
        let retained = remaining.min(count);
        bytes.extend_from_slice(&buffer[..retained]);
        if retained != count {
            return Ok(StdoutCapture {
                bytes,
                complete: false,
                exceeded: true,
            });
        }
    }
}

async fn settle_interrupted(
    interrupted: InterruptedProcess<'_>,
    stop: StopReason,
    io: ProcessIo,
) -> ProcessOutcome {
    let InterruptedProcess {
        child,
        observer,
        custody,
        mut status,
        root_exit_observed,
    } = interrupted;
    let ProcessIo {
        mut stdin_task,
        stdin_result,
        mut stdout_task,
        mut stdout_result,
        stderr_task,
    } = io;
    #[cfg(target_os = "linux")]
    let root_exit_observed = root_exit_observed || child.try_wait_exit_unreaped().unwrap_or(false);
    let exit_was_known_before_cleanup = root_exit_observed;
    // Signal before wait() reaps a Unix group leader and drops its signal identity.
    let _ = child.terminate();
    #[cfg(target_os = "linux")]
    let quiescent = {
        let mut root_exit_observed = root_exit_observed;
        settle_linux_custody(child, observer, &mut root_exit_observed, &mut status).await
    };
    #[cfg(not(target_os = "linux"))]
    let quiescent = {
        let cleanup_deadline = Instant::now() + CLEANUP_GRACE;
        if status.is_none() {
            status = match timeout_at(cleanup_deadline, child.wait()).await {
                Ok(Ok(exit)) => Some(exit),
                _ => None,
            };
        }

        let quiescent = if status.is_some() {
            matches!(
                wait_for_quiescence(child, observer, cleanup_deadline).await,
                Ok(true)
            )
        } else {
            false
        };
        #[cfg(windows)]
        let quiescent = if quiescent {
            true
        } else {
            // The Job remains a safe family handle after the direct process exits.
            let _ = child.terminate();
            matches!(
                wait_for_quiescence(child, observer, Instant::now() + CLEANUP_GRACE).await,
                Ok(true)
            )
        };
        quiescent
    };

    let capture_deadline = Instant::now() + CLEANUP_GRACE;
    let stdin_result = match stdin_result {
        Some(result) => Some(result),
        None => join_bounded(&mut stdin_task, capture_deadline).await,
    };
    let stdin_finished = matches!(stdin_result.as_ref(), Some(Ok(_)));
    if stdout_result.is_none() {
        stdout_result = join_bounded(&mut stdout_task, capture_deadline).await;
    }
    let stderr = stderr_task.finish(capture_deadline).await;

    let clean_stdout = match stdout_result {
        Some(Ok(Ok(capture))) => Some(capture),
        _ => None,
    };
    let Some(status) = status else {
        return ProcessOutcome::Unknown {
            reason: stop.unknown_reason().unwrap_or(ProcessUncertainty::Exit),
            quiescent,
        };
    };
    classify_settled_process(SettledProcess {
        status,
        exit_was_known_before_cleanup,
        stop,
        quiescent,
        stdin_finished,
        stdout: clean_stdout,
        stderr,
        custody,
    })
}

fn classify_settled_process(settled: SettledProcess) -> ProcessOutcome {
    let SettledProcess {
        status,
        exit_was_known_before_cleanup,
        stop,
        quiescent,
        stdin_finished,
        stdout,
        stderr,
        custody,
    } = settled;
    if !quiescent {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Quiescence,
            quiescent: false,
        };
    }
    if exit_was_known_before_cleanup && !status.success() {
        let capture = stdout.map(|stdout| ProcessCapture {
            stdout: stdout.bytes,
            stdout_complete: stdout.complete,
            stderr,
            status,
            custody,
        });
        return ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture,
        };
    }
    if let Some(reason) = stop.unknown_reason() {
        return ProcessOutcome::Unknown {
            reason,
            quiescent: true,
        };
    }
    if !stdin_finished {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Stdin,
            quiescent: true,
        };
    }

    let StopReason::Failure(reason) = stop else {
        unreachable!("unknown stop reasons returned above");
    };
    let output_limit = reason == ProcessFailure::OutputLimit;
    let Some(stdout) = stdout else {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Stdout,
            quiescent: true,
        };
    };
    if output_limit && !stdout.exceeded {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Stdout,
            quiescent: true,
        };
    }
    if !output_limit && !stdout.complete {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Stdout,
            quiescent: true,
        };
    }
    if !output_limit && !status.success() {
        return ProcessOutcome::Failed {
            reason,
            capture: Some(ProcessCapture {
                stdout: stdout.bytes,
                stdout_complete: true,
                stderr,
                status,
                custody,
            }),
        };
    }
    if !output_limit && status.success() {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::InterruptedSuccess,
            quiescent: true,
        };
    }

    ProcessOutcome::Failed {
        reason,
        capture: Some(ProcessCapture {
            stdout: stdout.bytes,
            stdout_complete: stdout.complete,
            stderr,
            status,
            custody,
        }),
    }
}

#[cfg(target_os = "linux")]
async fn settle_linux_custody(
    child: &mut OwnedChild,
    observer: Option<&CustodyObserver>,
    root_exit_observed: &mut bool,
    status: &mut Option<ExitStatus>,
) -> bool {
    let first_deadline = Instant::now() + CLEANUP_GRACE;
    if !*root_exit_observed {
        *root_exit_observed = matches!(
            timeout_at(first_deadline, child.wait_exit_unreaped()).await,
            Ok(Ok(()))
        );
    }
    let mut group_empty = *root_exit_observed
        && matches!(
            child
                .wait_group_empty_except_leader(
                    first_deadline.saturating_duration_since(Instant::now(),)
                )
                .await,
            Ok(true)
        );

    if !group_empty {
        // Revalidate and signal while the exact exited leader is still waitable.
        let _ = child.terminate();
        let retry_deadline = Instant::now() + CLEANUP_GRACE;
        if !*root_exit_observed {
            *root_exit_observed = matches!(
                timeout_at(retry_deadline, child.wait_exit_unreaped()).await,
                Ok(Ok(()))
            );
        }
        if *root_exit_observed {
            group_empty = matches!(
                child
                    .wait_group_empty_except_leader(
                        retry_deadline.saturating_duration_since(Instant::now(),)
                    )
                    .await,
                Ok(true)
            );
        }
    }

    if group_empty {
        // An empty observation grants no signal authority by itself. Reuse the
        // retained leader's birth check, then observe the group again pre-reap.
        let _ = child.terminate();
        group_empty = matches!(
            child.wait_group_empty_except_leader(Duration::ZERO).await,
            Ok(true)
        );
    }
    if !group_empty {
        let _ = child.terminate();
        group_empty = matches!(
            child.wait_group_empty_except_leader(CLEANUP_GRACE).await,
            Ok(true)
        );
    }

    // Reap the direct child only after the final birth-checked group signal and
    // pre-reap group observation have completed.
    if status.is_none() {
        *status = match timeout_at(Instant::now() + CLEANUP_GRACE, child.wait()).await {
            Ok(Ok(exit)) => Some(exit),
            _ => None,
        };
    }
    let group_empty_after_reap = status.is_some()
        && matches!(
            observer.and_then(|observer| observer.known_process_group_empty().ok()),
            Some(true)
        );
    group_empty && group_empty_after_reap
}

impl StopReason {
    fn unknown_reason(self) -> Option<ProcessUncertainty> {
        match self {
            Self::Unknown(reason) => Some(reason),
            Self::Failure(_) => None,
        }
    }
}

async fn terminate_and_wait(child: &mut OwnedChild, observer: Option<&CustodyObserver>) -> bool {
    let _ = child.terminate();
    #[cfg(target_os = "linux")]
    {
        let mut root_exit_observed = false;
        let mut status = None;
        return settle_linux_custody(child, observer, &mut root_exit_observed, &mut status).await;
    }
    #[cfg(not(target_os = "linux"))]
    {
        let deadline = Instant::now() + CLEANUP_GRACE;
        let status = matches!(timeout_at(deadline, child.wait()).await, Ok(Ok(_)));
        status
            && matches!(
                wait_for_quiescence(child, observer, deadline).await,
                Ok(true)
            )
    }
}

async fn wait_for_quiescence(
    child: &mut OwnedChild,
    observer: Option<&CustodyObserver>,
    deadline: Instant,
) -> Result<bool, ()> {
    #[cfg(target_os = "linux")]
    {
        let _ = observer;
        let remaining = deadline.saturating_duration_since(Instant::now());
        child
            .wait_group_empty_except_leader(remaining)
            .await
            .map_err(|_| ())
    }
    #[cfg(windows)]
    {
        let _ = observer;
        let remaining = deadline.saturating_duration_since(Instant::now());
        child.wait_family_empty(remaining).await.map_err(|_| ())
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let _ = child;
        let Some(observer) = observer else {
            return Ok(false);
        };
        loop {
            match observer.known_process_group_empty().map_err(|_| ())? {
                true => return Ok(true),
                false if Instant::now() >= deadline => return Ok(false),
                false => {
                    sleep_until((Instant::now() + Duration::from_millis(50)).min(deadline)).await;
                }
            }
        }
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = (child, observer, deadline);
        Ok(false)
    }
}

async fn wait_for_cancellation(cancellation: &mut watch::Receiver<bool>) {
    loop {
        if *cancellation.borrow_and_update() {
            return;
        }
        if cancellation.changed().await.is_err() {
            return;
        }
    }
}

fn cancellation_requested(cancellation: &watch::Receiver<bool>) -> bool {
    *cancellation.borrow() || cancellation.has_changed().is_err()
}

async fn join_bounded<T>(
    task: &mut JoinHandle<T>,
    deadline: Instant,
) -> Option<Result<T, tokio::task::JoinError>> {
    match timeout_at(deadline, &mut *task).await {
        Ok(result) => Some(result),
        Err(_) => {
            task.abort();
            let _ = task.await;
            None
        }
    }
}
