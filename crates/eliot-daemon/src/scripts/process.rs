use crate::platform::{CustodyGuarantee, CustodyObserver, OwnedChild};
use eliot_core::{SecretPolicy, StderrSnapshot, limits::MAX_FRAME_BYTES};
use eliot_plugin::{StderrCapture, spawn_stderr};
use std::{io, process::ExitStatus, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{ChildStdin, ChildStdout, Command},
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep_until, timeout_at},
};

const CLEANUP_GRACE: Duration = Duration::from_secs(5);

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
    Failed {
        reason: ProcessFailure,
        capture: Option<ProcessCapture>,
    },
    Unknown {
        reason: ProcessUncertainty,
        quiescent: bool,
    },
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
    let mut child = match OwnedChild::spawn(command) {
        Ok(child) => child,
        Err(_) => {
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

    let mut stdin_task = tokio::spawn(write_stdin(stdin_pipe, stdin));
    let mut stdout_task = tokio::spawn(capture_stdout(stdout_pipe));
    let stderr_task = spawn_stderr(stderr_pipe, policy);

    let mut stdin_result = None;
    let mut stdout_result = None;
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
        if stdin_result.is_some() && stdout_result.is_some() && root_exit_observed {
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
            result = &mut stdin_task, if stdin_result.is_none() => {
                let complete = matches!(&result, Ok(Ok(())));
                stdin_result = Some(result);
                if !complete {
                    stop = Some(StopReason::Unknown(ProcessUncertainty::Stdin));
                }
            }
            result = &mut stdout_task, if stdout_result.is_none() => {
                let output_limit = matches!(&result, Ok(Ok(capture)) if capture.exceeded);
                let complete = matches!(&result, Ok(Ok(_)));
                stdout_result = Some(result);
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

    if let Some(stop) = stop {
        return settle_interrupted(
            &mut child,
            observer.as_ref(),
            custody,
            stop,
            status,
            root_exit_observed,
            &mut stdin_task,
            stdin_result,
            &mut stdout_task,
            stdout_result,
            stderr_task,
        )
        .await;
    }

    if !root_exit_observed {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Exit,
            quiescent: false,
        };
    }
    let stdout = match stdout_result {
        Some(Ok(Ok(stdout))) => stdout,
        _ => unreachable!("stdout errors exit through the cleanup path"),
    };
    if !matches!(stdin_result.as_ref(), Some(Ok(Ok(())))) || !stdout.complete {
        return settle_interrupted(
            &mut child,
            observer.as_ref(),
            custody,
            StopReason::Unknown(ProcessUncertainty::Stdin),
            status,
            root_exit_observed,
            &mut stdin_task,
            stdin_result,
            &mut stdout_task,
            Some(Ok(Ok(stdout))),
            stderr_task,
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
        result = wait_for_quiescence(&mut child, observer.as_ref(), deadline) => {
            result.map_err(|_| StopReason::Unknown(ProcessUncertainty::Quiescence))
        }
    };
    match quiescence {
        Ok(true) => {}
        Ok(false) => {
            return settle_interrupted(
                &mut child,
                observer.as_ref(),
                custody,
                StopReason::Failure(ProcessFailure::Timeout),
                status,
                root_exit_observed,
                &mut stdin_task,
                stdin_result,
                &mut stdout_task,
                Some(Ok(Ok(stdout))),
                stderr_task,
            )
            .await;
        }
        Err(stop) => {
            return settle_interrupted(
                &mut child,
                observer.as_ref(),
                custody,
                stop,
                status,
                root_exit_observed,
                &mut stdin_task,
                stdin_result,
                &mut stdout_task,
                Some(Ok(Ok(stdout))),
                stderr_task,
            )
            .await;
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

    let stderr = stderr_task.finish(Instant::now() + CLEANUP_GRACE).await;
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

struct StdoutCapture {
    bytes: Vec<u8>,
    complete: bool,
    exceeded: bool,
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
    child: &mut OwnedChild,
    observer: Option<&CustodyObserver>,
    custody: CustodyGuarantee,
    stop: StopReason,
    mut status: Option<ExitStatus>,
    root_exit_observed: bool,
    stdin_task: &mut JoinHandle<io::Result<()>>,
    stdin_result: Option<Result<io::Result<()>, tokio::task::JoinError>>,
    stdout_task: &mut JoinHandle<io::Result<StdoutCapture>>,
    mut stdout_result: Option<Result<io::Result<StdoutCapture>, tokio::task::JoinError>>,
    stderr_task: StderrCapture,
) -> ProcessOutcome {
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
        None => join_bounded(stdin_task, capture_deadline).await,
    };
    let stdin_finished = matches!(stdin_result.as_ref(), Some(Ok(_)));
    let stdin_complete = matches!(stdin_result.as_ref(), Some(Ok(Ok(()))));
    if stdout_result.is_none() {
        stdout_result = join_bounded(stdout_task, capture_deadline).await;
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
    if !quiescent {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Quiescence,
            quiescent: false,
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
    if exit_was_known_before_cleanup && !stdin_complete {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Stdin,
            quiescent: true,
        };
    }

    let StopReason::Failure(reason) = stop else {
        unreachable!("unknown stop reasons returned above");
    };
    let output_limit = reason == ProcessFailure::OutputLimit;
    let Some(stdout) = clean_stdout else {
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
        let failure = if exit_was_known_before_cleanup {
            ProcessFailure::Exit
        } else {
            reason
        };
        return ProcessOutcome::Failed {
            reason: failure,
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

    let capture = ProcessCapture {
        stdout: stdout.bytes,
        stdout_complete: stdout.complete,
        stderr,
        status,
        custody,
    };
    ProcessOutcome::Failed {
        reason,
        capture: Some(capture),
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

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::process::Child as StdChild;
    use std::{
        io::Write,
        process::{Command as StdCommand, Stdio as StdStdio},
        thread,
    };

    const FIXTURE_ENV: &str = "ELIOT_PROCESS_TEST_FIXTURE";
    const FIXTURE_TEST: &str = "scripts::process::tests::process_fixture";

    fn fixture_command(mode: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .args(["--exact", FIXTURE_TEST, "--nocapture"])
            .env(FIXTURE_ENV, mode);
        command
    }

    fn cancellation() -> (watch::Sender<bool>, watch::Receiver<bool>) {
        watch::channel(false)
    }

    #[cfg(unix)]
    fn unsuccessful_exit_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(7 << 8)
    }

    #[cfg(windows)]
    fn unsuccessful_exit_status() -> ExitStatus {
        use std::os::windows::process::ExitStatusExt;
        ExitStatus::from_raw(7)
    }

    #[cfg(unix)]
    fn successful_exit_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(0)
    }

    #[cfg(windows)]
    fn successful_exit_status() -> ExitStatus {
        use std::os::windows::process::ExitStatusExt;
        ExitStatus::from_raw(0)
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn known_nonzero_exit_stays_failed_when_stderr_drain_is_incomplete() {
        #[cfg(unix)]
        let custody = CustodyGuarantee::Degraded;
        #[cfg(windows)]
        let custody = CustodyGuarantee::Exact;

        let outcome = classify_quiescent_capture(ProcessCapture {
            stdout: br#"{"action":"send"}"#.to_vec(),
            stdout_complete: true,
            stderr: StderrSnapshot {
                text: "[REDACTED]".to_owned(),
                truncated: true,
                eof: false,
                read_error: true,
            },
            status: unsuccessful_exit_status(),
            custody,
        });

        let ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(capture),
        } = outcome
        else {
            panic!("known nonzero exit must remain a failure");
        };
        assert!(capture.stdout_complete);
        assert_eq!(capture.stderr.text, "[REDACTED]");
        assert!(capture.stderr.truncated);
        assert!(!capture.stderr.eof);
        assert!(capture.stderr.read_error);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn known_success_stays_completed_when_stderr_drain_is_incomplete() {
        #[cfg(unix)]
        let custody = CustodyGuarantee::Degraded;
        #[cfg(windows)]
        let custody = CustodyGuarantee::Exact;

        let outcome = classify_quiescent_capture(ProcessCapture {
            stdout: br#"{"action":"send"}"#.to_vec(),
            stdout_complete: true,
            stderr: StderrSnapshot {
                text: "[REDACTED]".to_owned(),
                truncated: true,
                eof: false,
                read_error: true,
            },
            status: successful_exit_status(),
            custody,
        });

        let ProcessOutcome::Completed(capture) = outcome else {
            panic!("known success must remain completed");
        };
        assert!(capture.stdout_complete);
        assert_eq!(capture.stdout, br#"{"action":"send"}"#);
        assert_eq!(capture.stderr.text, "[REDACTED]");
        assert!(capture.stderr.truncated);
        assert!(!capture.stderr.eof);
        assert!(capture.stderr.read_error);
    }

    #[tokio::test]
    async fn captures_complete_stdout_and_redacted_stderr() {
        let (_sender, receiver) = cancellation();
        let outcome = run_owned_process(
            fixture_command("complete"),
            b"bounded input".to_vec(),
            Duration::from_secs(5),
            Arc::new(SecretPolicy::new(["fixture-secret"])),
            receiver,
        )
        .await;
        let ProcessOutcome::Completed(capture) = outcome else {
            panic!("expected completed output");
        };
        assert!(capture.stdout_complete);
        assert!(capture.stdout.ends_with(b"fixture-output"));
        assert!(capture.stderr.eof);
        assert!(!capture.stderr.read_error);
        assert!(!capture.stderr.text.contains("fixture-secret"));
        assert!(capture.stderr.text.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn stdout_limit_is_a_known_failure_and_never_retains_more_than_one_frame() {
        let (_sender, receiver) = cancellation();
        let outcome = run_owned_process(
            fixture_command("large"),
            Vec::new(),
            Duration::from_secs(5),
            Arc::new(SecretPolicy::default()),
            receiver,
        )
        .await;
        let ProcessOutcome::Failed {
            reason: ProcessFailure::OutputLimit,
            capture: Some(capture),
        } = outcome
        else {
            panic!("expected output-limit failure");
        };
        assert_eq!(capture.stdout.len(), MAX_FRAME_BYTES);
        assert!(!capture.stdout_complete);
    }

    #[tokio::test]
    async fn timeout_cleans_owned_children_and_keeps_success_after_interrupt_unknown() {
        let (_sender, receiver) = cancellation();
        let outcome = run_owned_process(
            fixture_command("sleep"),
            Vec::new(),
            Duration::from_millis(100),
            Arc::new(SecretPolicy::default()),
            receiver,
        )
        .await;
        assert!(matches!(
            outcome,
            ProcessOutcome::Failed {
                reason: ProcessFailure::Timeout,
                capture: Some(_),
            }
        ));

        let (sender, receiver) = cancellation();
        let cancel = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = sender.send(true);
        });
        let outcome = run_owned_process(
            fixture_command("sleep"),
            Vec::new(),
            Duration::from_secs(5),
            Arc::new(SecretPolicy::default()),
            receiver,
        )
        .await;
        let _ = cancel.await;
        assert!(matches!(
            outcome,
            ProcessOutcome::Failed {
                reason: ProcessFailure::Cancelled,
                capture: Some(_),
            }
        ));

        let (_sender, receiver) = cancellation();
        let outcome = run_owned_process(
            fixture_command("descendant"),
            Vec::new(),
            Duration::from_millis(100),
            Arc::new(SecretPolicy::default()),
            receiver,
        )
        .await;
        assert!(matches!(
            outcome,
            ProcessOutcome::Unknown {
                reason: ProcessUncertainty::InterruptedSuccess,
                quiescent: true,
            }
        ));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn timeout_after_root_exit_kills_owned_group_and_preserves_unrelated_group() {
        use std::os::unix::process::CommandExt;

        struct ChildGuard(StdChild);

        impl Drop for ChildGuard {
            fn drop(&mut self) {
                if !matches!(self.0.try_wait(), Ok(Some(_))) {
                    let _ = self.0.kill();
                    let _ = self.0.wait();
                }
            }
        }

        let mut unrelated = StdCommand::new(std::env::current_exe().expect("test executable"));
        unrelated
            .args(["--exact", FIXTURE_TEST, "--nocapture"])
            .env(FIXTURE_ENV, "hold")
            .stdin(StdStdio::null())
            .stdout(StdStdio::null())
            .stderr(StdStdio::null())
            .process_group(0);
        let mut unrelated = ChildGuard(unrelated.spawn().expect("spawn unrelated group"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            unrelated
                .0
                .try_wait()
                .expect("observe unrelated group")
                .is_none(),
            "unrelated process must be live before the script run"
        );

        let (_sender, receiver) = cancellation();
        let started = Instant::now();
        let outcome = run_owned_process(
            fixture_command("descendant-hold"),
            Vec::new(),
            Duration::from_millis(100),
            Arc::new(SecretPolicy::default()),
            receiver,
        )
        .await;
        assert!(matches!(
            outcome,
            ProcessOutcome::Unknown {
                reason: ProcessUncertainty::InterruptedSuccess,
                quiescent: true,
            }
        ));
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "owned-group cleanup must not wait for the fixture descendant's natural exit"
        );
        assert!(
            unrelated
                .0
                .try_wait()
                .expect("recheck unrelated group")
                .is_none(),
            "timeout cleanup must preserve a different process group"
        );
    }

    #[tokio::test]
    async fn process_fixture() {
        let Ok(mode) = std::env::var(FIXTURE_ENV) else {
            return;
        };
        match mode.as_str() {
            "complete" => {
                io::stdout()
                    .write_all(b"fixture-output")
                    .expect("write fixture stdout");
                io::stdout().flush().expect("flush fixture stdout");
                io::stderr()
                    .write_all(b"fixture-secret")
                    .expect("write fixture stderr");
                io::stderr().flush().expect("flush fixture stderr");
                std::process::exit(0);
            }
            "large" => {
                let mut stdout = io::stdout().lock();
                stdout
                    .write_all(&vec![b'x'; MAX_FRAME_BYTES + 1])
                    .expect("write oversized fixture stdout");
                stdout.flush().expect("flush oversized fixture stdout");
                std::process::exit(0);
            }
            "sleep" => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                std::process::exit(0);
            }
            "descendant" => {
                let mut descendant =
                    StdCommand::new(std::env::current_exe().expect("test executable"));
                descendant
                    .args(["--exact", FIXTURE_TEST, "--nocapture"])
                    .env(FIXTURE_ENV, "brief")
                    .stdin(StdStdio::inherit())
                    .stdout(StdStdio::inherit())
                    .stderr(StdStdio::inherit())
                    .spawn()
                    .expect("spawn bounded descendant fixture");
                std::process::exit(0);
            }
            "descendant-hold" => {
                let mut descendant =
                    StdCommand::new(std::env::current_exe().expect("test executable"));
                descendant
                    .args(["--exact", FIXTURE_TEST, "--nocapture"])
                    .env(FIXTURE_ENV, "hold")
                    .stdin(StdStdio::inherit())
                    .stdout(StdStdio::inherit())
                    .stderr(StdStdio::inherit())
                    .spawn()
                    .expect("spawn retained descendant fixture");
                std::process::exit(0);
            }
            "brief" => {
                thread::sleep(Duration::from_millis(300));
                std::process::exit(0);
            }
            "hold" => {
                thread::sleep(Duration::from_secs(6));
                std::process::exit(0);
            }
            _ => std::process::exit(2),
        }
    }
}
