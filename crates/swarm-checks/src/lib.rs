//! Bounded native check execution for plans already resolved and admitted by
//! the Swarm kernel. This crate owns no Store, Task, selector, or result DB.
//!
//! A kernel adapter implements [`CheckControl`]. It must publish the exact
//! process identity through the existing CheckRun path and return `Start` only
//! after the Store has durably acknowledged that owner. The executor does not
//! retry a command when that acknowledgement is missing or uncertain.

mod status;

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde_json::Value;
use swarm_contracts::{Error, Result};
use swarm_process::Group;

pub use status::read_check_status;

/// Per-stream disk cap for retained check output. Excess bytes are drained
/// from the pipe but not written, and the returned stream evidence is truncated.
pub const MAX_CAPTURE_BYTES_PER_STREAM: u64 = 64 * 1024 * 1024;
const CHECK_POLL_INTERVAL: Duration = Duration::from_millis(25);
const LONG_DRAIN_DIAGNOSTIC_AFTER: Duration = Duration::from_secs(30);

/// The exact operation identity reserved by the kernel before a check worker
/// is launched. IDs are checked again at the process boundary.
#[derive(Debug, Clone)]
pub struct CheckIdentity {
    pub check_id: String,
    pub operation_id: String,
    pub token: String,
}

/// A resolved command, not a user-facing profile or selector. The Store's
/// trusted profile and input resolver remain the only source of its argv,
/// environment, working directory, and executable.
#[derive(Debug, Clone)]
pub struct ResolvedCheckPlan {
    pub identity: CheckIdentity,
    pub executable: PathBuf,
    pub argv: Vec<String>,
    pub working_directory: PathBuf,
    pub environment: BTreeMap<String, String>,
    /// Existing directory created for this admitted CheckRun. Output filenames
    /// are fixed to `stdout` and `stderr` beneath this directory.
    pub output_directory: PathBuf,
    /// Maximum bytes retained per stream; values above the package cap reject.
    pub output_limit_bytes_per_stream: u64,
    /// No command timeout is implied by this module. The kernel may pass one
    /// only when its existing admission policy supplies that bound.
    pub timeout: Option<Duration>,
}

/// Exact OS process identity reported by the existing process owner primitive.
#[derive(Debug, Clone)]
pub struct OwnedCheckProcess {
    pub check_id: String,
    pub operation_id: String,
    pub token: String,
    pub process: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartDecision {
    /// The kernel durably acknowledged this exact owner and admitted execution.
    Start,
    /// A durable cancellation request arrived before native command spawn.
    CancelBeforeStart,
}

/// Narrow adapter to the existing CheckRun supervisor. `wait_for_start` must
/// persist/read the existing identity and go/cancel receipts; it is not a new
/// RPC method. A transport or persistence error returns before command spawn.
pub trait CheckControl {
    fn wait_for_start(&mut self, owner: &OwnedCheckProcess) -> Result<StartDecision>;

    /// Read the current durable cancellation request for this exact CheckRun.
    /// Errors after spawn do not trigger a replay; execution remains owned
    /// until the process group is proven empty.
    fn cancellation_requested(&mut self, owner: &OwnedCheckProcess) -> Result<bool>;

    /// Publish a one-shot, non-terminal process/control diagnostic through the
    /// host's existing CheckRun status path. Returning an error keeps the owned
    /// Group retained and causes the adapter to retry publication.
    fn process_group_drain_pending(
        &mut self,
        owner: &OwnedCheckProcess,
        elapsed: Duration,
        observation_error: bool,
        control_read_unknown: bool,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    Exited,
    Cancelled,
    TimedOut,
    CancelledBeforeStart,
    ControlReadUnknownBeforeStart,
    ProcessObservationUnknown,
}

#[derive(Debug, Clone)]
pub struct CapturedStream {
    pub path: PathBuf,
    pub bytes_written: u64,
    pub truncated: bool,
    pub capture_complete: bool,
}

/// Process-level evidence for the kernel's existing receipt validators. This
/// is not a CheckRun verdict: Cargo coverage, source identity, artifacts, and
/// the final `Completion` remain validated by the kernel.
#[derive(Debug, Clone)]
pub struct CheckExecution {
    pub check_id: String,
    pub operation_id: String,
    pub process: Value,
    pub child_pid: Option<u32>,
    pub termination: Termination,
    pub exit_code: Option<i32>,
    /// Exact sum returned by the owned Group's cancellation primitive.
    pub termination_requests: u64,
    pub stdout: CapturedStream,
    pub stderr: CapturedStream,
    /// True only after the owned Group reports empty and disarms successfully.
    pub resource_released: bool,
    /// The Store cancellation read failed. Do not infer that no cancellation
    /// was requested from this flag.
    pub control_read_unknown: bool,
    /// Process signaling was attempted against this exact group but one or
    /// more requests failed. Group emptiness is still checked before return.
    pub termination_request_unconfirmed: bool,
}

#[derive(Debug, Default)]
struct CaptureStats {
    bytes_written: u64,
    truncated: bool,
}

/// Executes exactly one kernel-resolved command, after the host adapter has
/// acknowledged the process owner. Uses direct argv (never a shell), a fresh
/// check-owned process group, bounded stdout/stderr files, and no retries.
pub fn execute(
    plan: &ResolvedCheckPlan,
    control: &mut impl CheckControl,
) -> Result<CheckExecution> {
    let identity = plan.identity.clone();
    let output_directory = plan.output_directory.clone();
    execute_with_plan(identity, output_directory, control, || Ok(plan.clone()))
}

/// Establish the existing CheckRun process owner and wait for Store's durable
/// go-ahead before resolving execution inputs. `build_plan` runs only after
/// that handshake and must return the same admitted identity.
pub fn execute_with_plan(
    identity: CheckIdentity,
    output_directory: PathBuf,
    control: &mut impl CheckControl,
    build_plan: impl FnOnce() -> Result<ResolvedCheckPlan>,
) -> Result<CheckExecution> {
    validate_identity(&identity)?;

    let group = Group::enter(&identity.token)?;
    let owner = OwnedCheckProcess {
        check_id: identity.check_id.clone(),
        operation_id: identity.operation_id.clone(),
        token: identity.token.clone(),
        process: group.identity.clone(),
    };

    let start = match control.wait_for_start(&owner) {
        Ok(start) => start,
        Err(error) => return reject_before_command(&group, error),
    };
    match start {
        StartDecision::CancelBeforeStart => {
            group.disarm()?;
            return Ok(not_started(
                &identity,
                &output_directory,
                owner,
                Termination::CancelledBeforeStart,
                false,
            ));
        }
        StartDecision::Start => {}
    }

    let plan = match build_plan() {
        Ok(plan) => plan,
        Err(error) => return reject_before_command(&group, error),
    };
    if plan.identity.check_id != identity.check_id
        || plan.identity.operation_id != identity.operation_id
        || plan.identity.token != identity.token
    {
        return reject_before_command(
            &group,
            Error::conflict("resolved check plan differs from the admitted process owner"),
        );
    }
    if let Err(error) = validate_plan(&plan) {
        return reject_before_command(&group, error);
    }

    // Compute the actual deadline immediately before any native side effect.
    // Never turn a configured timeout into an unbounded run on clock overflow.
    let deadline = match plan.timeout {
        Some(timeout) => match Instant::now().checked_add(timeout) {
            Some(deadline) => Some(deadline),
            None => {
                return reject_before_command(
                    &group,
                    Error::invalid("check timeout deadline overflows the process clock"),
                );
            }
        },
        None => None,
    };

    let stdout_path = plan.output_directory.join("stdout");
    let stderr_path = plan.output_directory.join("stderr");
    let stdout_file = match create_output(&stdout_path) {
        Ok(file) => file,
        Err(error) => return reject_before_command(&group, error),
    };
    let stderr_file = match create_output(&stderr_path) {
        Ok(file) => file,
        Err(error) => return reject_before_command(&group, error),
    };

    match control.cancellation_requested(&owner) {
        Ok(false) => {}
        Ok(true) => {
            group.disarm()?;
            return Ok(not_started(
                &plan.identity,
                &plan.output_directory,
                owner,
                Termination::CancelledBeforeStart,
                false,
            ));
        }
        Err(_) => {
            group.disarm()?;
            return Ok(not_started(
                &plan.identity,
                &plan.output_directory,
                owner,
                Termination::ControlReadUnknownBeforeStart,
                true,
            ));
        }
    }

    let mut command = Command::new(&plan.executable);
    command
        .args(&plan.argv)
        .current_dir(&plan.working_directory)
        .env_clear()
        .envs(&plan.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            return reject_before_command(
                &group,
                Error::new(
                    "CHECK_EXECUTABLE_START_FAILED",
                    "could not start the admitted check command",
                ),
            );
        }
    };
    let child_pid = Some(child.id());

    let Some(stdout_pipe) = child.stdout.take() else {
        abort_owned_child(&group, &mut child, control, &owner);
        group.disarm()?;
        return Err(capture_setup_error());
    };
    let Some(stderr_pipe) = child.stderr.take() else {
        abort_owned_child(&group, &mut child, control, &owner);
        group.disarm()?;
        return Err(capture_setup_error());
    };
    let stdout_reader = match capture_thread(
        "swarm-check-stdout",
        stdout_pipe,
        stdout_file,
        plan.output_limit_bytes_per_stream,
    ) {
        Ok(reader) => reader,
        Err(_) => {
            abort_owned_child(&group, &mut child, control, &owner);
            group.disarm()?;
            return Err(capture_setup_error());
        }
    };
    let stderr_reader = match capture_thread(
        "swarm-check-stderr",
        stderr_pipe,
        stderr_file,
        plan.output_limit_bytes_per_stream,
    ) {
        Ok(reader) => reader,
        Err(_) => {
            abort_owned_child(&group, &mut child, control, &owner);
            let _ = stdout_reader.join();
            group.disarm()?;
            return Err(capture_setup_error());
        }
    };

    let mut exit_code = None;
    let mut process_observation_unknown = false;
    let mut cancel_observed = false;
    let mut timed_out = false;
    let mut control_read_unknown = false;
    let mut control_read_unknown_since = None;
    let mut control_diagnostic_published = false;
    let mut last_control_diagnostic_attempt = None;
    let command_started = Instant::now();
    let mut termination_request_unconfirmed = false;
    let mut termination_requests = 0u64;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                exit_code = status.code();
                break;
            }
            Ok(None) => {}
            Err(_) => {
                process_observation_unknown = true;
                let _ = control.process_group_drain_pending(
                    &owner,
                    command_started.elapsed(),
                    true,
                    control_read_unknown,
                );
                let _ = child.kill(); // This exact Child was spawned by this check.
                request_termination(
                    &group,
                    &mut termination_requests,
                    &mut termination_request_unconfirmed,
                );
                match child.wait() {
                    Ok(status) => exit_code = status.code(),
                    Err(_) => {}
                }
                break;
            }
        }

        if !control_read_unknown {
            match control.cancellation_requested(&owner) {
                Ok(true) => cancel_observed = true,
                Ok(false) => {}
                Err(_) => {
                    control_read_unknown = true;
                    control_read_unknown_since.get_or_insert_with(Instant::now);
                }
            }
        }
        if !control_diagnostic_published
            && control_read_unknown_since
                .is_some_and(|since: Instant| since.elapsed() >= LONG_DRAIN_DIAGNOSTIC_AFTER)
            && last_control_diagnostic_attempt
                .is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(1))
        {
            last_control_diagnostic_attempt = Some(Instant::now());
            if control
                .process_group_drain_pending(
                    &owner,
                    control_read_unknown_since
                        .map_or(Duration::ZERO, |since: Instant| since.elapsed()),
                    process_observation_unknown,
                    true,
                )
                .is_ok()
            {
                control_diagnostic_published = true;
            }
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            timed_out = true;
        }
        if cancel_observed || timed_out {
            request_termination(
                &group,
                &mut termination_requests,
                &mut termination_request_unconfirmed,
            );
        }
        thread::sleep(CHECK_POLL_INTERVAL);
    }

    // Release the direct process handle before checking exact Job membership
    // on Windows. Descendants still belong to this check Group.
    drop(child);
    wait_until_empty(
        &group,
        deadline,
        control,
        &owner,
        &mut cancel_observed,
        &mut timed_out,
        &mut control_read_unknown,
        &mut process_observation_unknown,
        &mut termination_request_unconfirmed,
        &mut termination_requests,
    );

    let stdout = finish_capture(stdout_path, stdout_reader);
    let stderr = finish_capture(stderr_path, stderr_reader);
    group.disarm()?;

    let termination = if process_observation_unknown {
        Termination::ProcessObservationUnknown
    } else if cancel_observed {
        Termination::Cancelled
    } else if timed_out {
        Termination::TimedOut
    } else {
        Termination::Exited
    };

    Ok(CheckExecution {
        check_id: plan.identity.check_id.clone(),
        operation_id: plan.identity.operation_id.clone(),
        process: owner.process,
        child_pid,
        termination,
        exit_code,
        termination_requests,
        stdout,
        stderr,
        resource_released: true,
        control_read_unknown,
        termination_request_unconfirmed,
    })
}

fn wait_until_empty(
    group: &Group,
    deadline: Option<Instant>,
    control: &mut impl CheckControl,
    owner: &OwnedCheckProcess,
    cancel_observed: &mut bool,
    timed_out: &mut bool,
    control_read_unknown: &mut bool,
    process_observation_unknown: &mut bool,
    termination_request_unconfirmed: &mut bool,
    termination_requests: &mut u64,
) {
    let drain_started = Instant::now();
    let mut drain_diagnostic_published = false;
    let mut observation_diagnostic_published = false;
    let mut control_diagnostic_published = false;
    let mut last_diagnostic_attempt = None;
    loop {
        match group.children_empty() {
            Ok(true) => return,
            Ok(false) => {}
            Err(_) => *process_observation_unknown = true,
        }
        let needs_diagnostic = (*process_observation_unknown && !observation_diagnostic_published)
            || (*control_read_unknown && !control_diagnostic_published)
            || (!*process_observation_unknown
                && !*control_read_unknown
                && !drain_diagnostic_published);
        if needs_diagnostic
            && drain_started.elapsed() >= LONG_DRAIN_DIAGNOSTIC_AFTER
            && last_diagnostic_attempt
                .is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(1))
        {
            last_diagnostic_attempt = Some(Instant::now());
            match control.process_group_drain_pending(
                owner,
                drain_started.elapsed(),
                *process_observation_unknown,
                *control_read_unknown,
            ) {
                Ok(()) => {
                    if *process_observation_unknown {
                        observation_diagnostic_published = true;
                    }
                    if *control_read_unknown {
                        control_diagnostic_published = true;
                    }
                    if !*process_observation_unknown && !*control_read_unknown {
                        drain_diagnostic_published = true;
                    }
                }
                Err(_) => {}
            }
        }
        if !*control_read_unknown {
            match control.cancellation_requested(owner) {
                Ok(true) => *cancel_observed = true,
                Ok(false) => {}
                Err(_) => *control_read_unknown = true,
            }
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            *timed_out = true;
        }
        if *cancel_observed || *timed_out {
            request_termination(group, termination_requests, termination_request_unconfirmed);
        }
        thread::sleep(CHECK_POLL_INTERVAL);
    }
}

fn request_termination(
    group: &Group,
    termination_requests: &mut u64,
    termination_request_unconfirmed: &mut bool,
) {
    match group.cancel_children() {
        Ok(sent) => {
            *termination_requests = (*termination_requests).saturating_add(sent);
        }
        Err(_) => *termination_request_unconfirmed = true,
    }
}

fn validate_plan(plan: &ResolvedCheckPlan) -> Result<()> {
    validate_identity(&plan.identity)?;
    if plan.executable.as_os_str().is_empty()
        || !plan.executable.is_absolute()
        || !plan.working_directory.is_absolute()
        || !plan.output_directory.is_absolute()
        || plan.output_limit_bytes_per_stream == 0
        || plan.output_limit_bytes_per_stream > MAX_CAPTURE_BYTES_PER_STREAM
        || plan.timeout.is_some_and(|timeout| timeout.is_zero())
        || plan
            .timeout
            .is_some_and(|timeout| Instant::now().checked_add(timeout).is_none())
        || plan.argv.iter().any(|arg| arg.contains('\0'))
        || plan
            .environment
            .iter()
            .any(|(key, value)| key.is_empty() || key.contains(['=', '\0']) || value.contains('\0'))
    {
        return Err(Error::invalid("invalid resolved check plan"));
    }
    if !plan.output_directory.is_dir() {
        return Err(Error::invalid("CheckRun output directory is unavailable"));
    }
    #[cfg(windows)]
    if !plan
        .executable
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"))
    {
        return Err(Error::invalid(
            "check executable must be a native executable, not a batch shim",
        ));
    }
    Ok(())
}

fn validate_identity(identity: &CheckIdentity) -> Result<()> {
    for id in [
        identity.check_id.as_str(),
        identity.operation_id.as_str(),
        identity.token.as_str(),
    ] {
        uuid::Uuid::parse_str(id).map_err(|_| Error::invalid("invalid CheckRun identity"))?;
    }
    Ok(())
}

fn reject_before_command<T>(group: &Group, error: Error) -> Result<T> {
    group.disarm()?;
    Err(error)
}

fn create_output(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| {
            Error::new(
                "CHECK_OUTPUT_CREATE_FAILED",
                "could not create check output",
            )
        })
}

fn capture_setup_error() -> Error {
    Error::new(
        "CHECK_CAPTURE_SETUP_FAILED",
        "could not prepare bounded check output capture",
    )
}

fn capture_thread<R: Read + Send + 'static>(
    name: &str,
    mut reader: R,
    mut file: File,
    limit: u64,
) -> io::Result<JoinHandle<io::Result<CaptureStats>>> {
    thread::Builder::new().name(name.into()).spawn(move || {
        let mut stats = CaptureStats::default();
        let mut buffer = [0u8; 16 * 1024];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let remaining = limit.saturating_sub(stats.bytes_written);
            let keep = usize::try_from(remaining.min(read as u64)).unwrap_or(read);
            if keep > 0 {
                file.write_all(&buffer[..keep])?;
                stats.bytes_written = stats.bytes_written.saturating_add(keep as u64);
            }
            if keep < read {
                stats.truncated = true;
            }
        }
        file.sync_all()?;
        Ok(stats)
    })
}

fn finish_capture(path: PathBuf, reader: JoinHandle<io::Result<CaptureStats>>) -> CapturedStream {
    match reader.join() {
        Ok(Ok(stats)) => CapturedStream {
            path,
            bytes_written: stats.bytes_written,
            truncated: stats.truncated,
            capture_complete: true,
        },
        Ok(Err(_)) | Err(_) => CapturedStream {
            path,
            bytes_written: 0,
            truncated: false,
            capture_complete: false,
        },
    }
}

fn abort_owned_child(
    group: &Group,
    child: &mut Child,
    control: &mut impl CheckControl,
    owner: &OwnedCheckProcess,
) {
    let _ = child.kill();
    let mut child_reaped = false;
    let abort_started = Instant::now();
    let mut process_observation_unknown = false;
    let mut drain_diagnostic_published = false;
    let mut observation_diagnostic_published = false;
    let mut last_diagnostic_attempt = None;
    loop {
        if !child_reaped {
            match child.try_wait() {
                Ok(Some(_)) => child_reaped = true,
                Ok(None) => {
                    let _ = child.kill();
                }
                Err(_) => {
                    process_observation_unknown = true;
                    let _ = child.kill();
                    observation_diagnostic_published = control
                        .process_group_drain_pending(owner, abort_started.elapsed(), true, false)
                        .is_ok();
                }
            }
        }
        match group.children_empty() {
            Ok(true) => {
                if !child_reaped {
                    let _ = child.wait();
                }
                break;
            }
            Ok(false) => {}
            Err(_) => process_observation_unknown = true,
        }
        let needs_diagnostic = if process_observation_unknown {
            !observation_diagnostic_published
        } else {
            !drain_diagnostic_published
        };
        if needs_diagnostic
            && abort_started.elapsed() >= LONG_DRAIN_DIAGNOSTIC_AFTER
            && last_diagnostic_attempt
                .is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(1))
        {
            last_diagnostic_attempt = Some(Instant::now());
            if control
                .process_group_drain_pending(
                    owner,
                    abort_started.elapsed(),
                    process_observation_unknown,
                    false,
                )
                .is_ok()
            {
                if process_observation_unknown {
                    observation_diagnostic_published = true;
                } else {
                    drain_diagnostic_published = true;
                }
            }
        }
        let _ = group.cancel_children();
        thread::sleep(CHECK_POLL_INTERVAL);
    }
}

fn not_started(
    identity: &CheckIdentity,
    output_directory: &Path,
    owner: OwnedCheckProcess,
    termination: Termination,
    control_read_unknown: bool,
) -> CheckExecution {
    CheckExecution {
        check_id: identity.check_id.clone(),
        operation_id: identity.operation_id.clone(),
        process: owner.process,
        child_pid: None,
        termination,
        exit_code: None,
        termination_requests: 0,
        stdout: CapturedStream {
            path: output_directory.join("stdout"),
            bytes_written: 0,
            truncated: false,
            capture_complete: false,
        },
        stderr: CapturedStream {
            path: output_directory.join("stderr"),
            bytes_written: 0,
            truncated: false,
            capture_complete: false,
        },
        resource_released: true,
        control_read_unknown,
        termination_request_unconfirmed: false,
    }
}
