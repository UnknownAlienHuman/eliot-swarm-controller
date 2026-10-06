use std::{
    process::{Command, ExitCode, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};
use swarm_process::child_error::{
    BoundedStderrSink, StderrForwardResult, forward_stderr, wrapper_error_json,
};

const POST_EXIT_DRAIN_TIMEOUT: Duration = Duration::from_millis(300);

fn main() -> ExitCode {
    let sink = BoundedStderrSink::spawn().ok();
    let mut executable = match std::env::current_exe() {
        Ok(path) => path,
        Err(_) => {
            let envelope = wrapper_error_json(
                "KERNEL_HOST_PATH_FAILED",
                "could not locate the public host executable",
                false,
                None,
                None,
            );
            emit_wrapper_error(
                sink.as_ref(),
                &envelope,
                Instant::now() + POST_EXIT_DRAIN_TIMEOUT,
            );
            return ExitCode::FAILURE;
        }
    };
    executable.set_file_name(if cfg!(windows) {
        "swarm-kernel-host.exe"
    } else {
        "swarm-kernel-host"
    });
    let sibling_is_file = executable.is_file();
    let mut child = match Command::new(&executable)
        .args(std::env::args_os().skip(1))
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return report_start_failure(sibling_is_file, sink.as_ref()),
    };
    let Some(stderr) = child.stderr.take() else {
        let _ = child.kill();
        return report_start_failure(sibling_is_file, sink.as_ref());
    };

    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    let reader_sink = sink.clone();
    if thread::Builder::new()
        .name("swarm-child-stderr".to_owned())
        .spawn(move || {
            let result = forward_stderr(stderr, reader_sink);
            let _ = result_sender.send(result);
        })
        .is_err()
    {
        let _ = child.kill();
        return report_start_failure(sibling_is_file, sink.as_ref());
    }

    let status = match child.wait() {
        Ok(status) => status,
        Err(_) => {
            let _ = child.kill();
            return report_wait_failure(sibling_is_file, sink.as_ref(), &result_receiver);
        }
    };
    let deadline = Instant::now() + POST_EXIT_DRAIN_TIMEOUT;
    let stream = match result_receiver.recv_timeout(remaining(deadline)) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
            return status_to_exit_code(status);
        }
    };
    if !stream.pipe_drained {
        return status_to_exit_code(status);
    }
    let Some(sink) = sink.as_ref() else {
        return status_to_exit_code(status);
    };
    if !sink.flush_until(deadline) {
        return status_to_exit_code(status);
    }

    if status.success() {
        ExitCode::SUCCESS
    } else {
        let envelope = wrapper_error_json(
            "KERNEL_HOST_COMMAND_FAILED",
            "the relocated kernel host exited unsuccessfully",
            true,
            status.code(),
            stream.child_error.as_ref(),
        );
        emit_wrapper_error(Some(sink), &envelope, deadline);
        status_to_exit_code(status)
    }
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

fn emit_wrapper_error(sink: Option<&BoundedStderrSink>, envelope: &str, deadline: Instant) {
    if let Some(sink) = sink {
        let _ = sink.write_envelope_until(envelope, deadline);
    }
}

fn report_wait_failure(
    sibling_is_file: bool,
    sink: Option<&BoundedStderrSink>,
    result_receiver: &Receiver<StderrForwardResult>,
) -> ExitCode {
    let Some(sink) = sink else {
        return ExitCode::FAILURE;
    };
    let deadline = Instant::now() + POST_EXIT_DRAIN_TIMEOUT;
    let Ok(stream) = result_receiver.recv_timeout(remaining(deadline)) else {
        return ExitCode::FAILURE;
    };
    if !stream.pipe_drained || !sink.flush_until(deadline) {
        return ExitCode::FAILURE;
    }
    report_start_failure_until(sibling_is_file, Some(sink), deadline)
}

fn report_start_failure(sibling_is_file: bool, sink: Option<&BoundedStderrSink>) -> ExitCode {
    report_start_failure_until(
        sibling_is_file,
        sink,
        Instant::now() + POST_EXIT_DRAIN_TIMEOUT,
    )
}

fn report_start_failure_until(
    sibling_is_file: bool,
    sink: Option<&BoundedStderrSink>,
    deadline: Instant,
) -> ExitCode {
    let code = if sibling_is_file {
        "KERNEL_HOST_START_FAILED"
    } else {
        "KERNEL_HOST_BINARY_MISSING"
    };
    let envelope = wrapper_error_json(
        code,
        "install the swarm-kernel-host sibling beside swarm-host and retry",
        false,
        None,
        None,
    );
    emit_wrapper_error(sink, &envelope, deadline);
    ExitCode::FAILURE
}

fn status_to_exit_code(status: ExitStatus) -> ExitCode {
    if status.success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(
            status
                .code()
                .and_then(|code| u8::try_from(code).ok())
                .filter(|code| *code != 0)
                .unwrap_or(1),
        )
    }
}
