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
    Stderr,
    Exit,
    Quiescence,
    InterruptedSuccess,
    Supervisor,
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
    let mut status = None;
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
        if stdin_result.is_some() && stdout_result.is_some() && status.is_some() {
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
            result = child.wait(), if status.is_none() => {
                match result {
                    Ok(exit) => status = Some(exit),
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
            &mut stdin_task,
            stdin_result,
            &mut stdout_task,
            stdout_result,
            stderr_task,
        )
        .await;
    }

    let Some(status) = status else {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Exit,
            quiescent: false,
        };
    };
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
            Some(status),
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
    let quiescent = match quiescence {
        Ok(true) => true,
        Ok(false) => {
            return settle_interrupted(
                &mut child,
                observer.as_ref(),
                custody,
                StopReason::Failure(ProcessFailure::Timeout),
                Some(status),
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
                Some(status),
                &mut stdin_task,
                stdin_result,
                &mut stdout_task,
                Some(Ok(Ok(stdout))),
                stderr_task,
            )
            .await;
        }
    };

    let stderr = stderr_task.finish(Instant::now() + CLEANUP_GRACE).await;
    if !stderr.eof || stderr.read_error {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Stderr,
            quiescent,
        };
    }

    let capture = ProcessCapture {
        stdout: stdout.bytes,
        stdout_complete: true,
        stderr,
        status,
        custody,
    };
    if capture.status.success() {
        ProcessOutcome::Completed(capture)
    } else {
        ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(capture),
        }
    }
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
    stdin_task: &mut JoinHandle<io::Result<()>>,
    stdin_result: Option<Result<io::Result<()>, tokio::task::JoinError>>,
    stdout_task: &mut JoinHandle<io::Result<StdoutCapture>>,
    mut stdout_result: Option<Result<io::Result<StdoutCapture>, tokio::task::JoinError>>,
    stderr_task: StderrCapture,
) -> ProcessOutcome {
    // Signal before wait() reaps a Unix group leader and drops its signal identity.
    let _ = child.terminate();
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

    let capture_deadline = Instant::now() + CLEANUP_GRACE;
    let stdin_finished = match stdin_result {
        Some(Ok(_)) => true,
        Some(Err(_)) => false,
        None => matches!(
            join_bounded(stdin_task, capture_deadline).await,
            Some(Ok(_))
        ),
    };
    if stdout_result.is_none() {
        stdout_result = join_bounded(stdout_task, capture_deadline).await;
    }
    let stderr = stderr_task.finish(capture_deadline).await;

    let clean_stdout = match stdout_result {
        Some(Ok(Ok(capture))) => Some(capture),
        _ => None,
    };
    let stderr_complete = stderr.eof && !stderr.read_error;
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
    if !stderr_complete {
        return ProcessOutcome::Unknown {
            reason: ProcessUncertainty::Stderr,
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
    let deadline = Instant::now() + CLEANUP_GRACE;
    let status = matches!(timeout_at(deadline, child.wait()).await, Ok(Ok(_)));
    status
        && matches!(
            wait_for_quiescence(child, observer, deadline).await,
            Ok(true)
        )
}

async fn wait_for_quiescence(
    child: &mut OwnedChild,
    observer: Option<&CustodyObserver>,
    deadline: Instant,
) -> Result<bool, ()> {
    #[cfg(windows)]
    {
        let _ = observer;
        let remaining = deadline.saturating_duration_since(Instant::now());
        child.wait_family_empty(remaining).await.map_err(|_| ())
    }
    #[cfg(unix)]
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
            "brief" => {
                thread::sleep(Duration::from_millis(300));
                std::process::exit(0);
            }
            _ => std::process::exit(2),
        }
    }
}
