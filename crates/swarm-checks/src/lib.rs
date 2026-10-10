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
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
const TERMINATION_GRACE: Duration = Duration::from_secs(5);
const CAPTURE_DRAIN_GRACE: Duration = Duration::from_secs(5);
const CAPTURE_STOP_GRACE: Duration = Duration::from_millis(250);
const TERMINATION_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const MAX_TERMINATION_ATTEMPTS: u8 = 3;

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
    /// The admitted owner received no Go before the bounded start gate expired.
    StartGateTimedOut,
}

/// Narrow adapter to the existing CheckRun supervisor. `wait_for_start` must
/// persist/read the existing identity and go/cancel receipts; it is not a new
/// RPC method. The adapter must bound its start gate and return
/// [`StartDecision::StartGateTimedOut`] when no Go arrives. A transport or
/// persistence error returns before command spawn.
pub trait CheckControl {
    fn wait_for_start(&mut self, owner: &OwnedCheckProcess) -> Result<StartDecision>;

    /// Read the current durable cancellation request for this exact CheckRun.
    /// Errors after spawn do not trigger a replay; execution remains owned
    /// until the process group is proven empty.
    fn cancellation_requested(&mut self, owner: &OwnedCheckProcess) -> Result<bool>;

    /// Publish a non-terminal process/control diagnostic through the host's
    /// existing CheckRun status path. Diagnostic failure does not change the
    /// family fact returned with the exact owner identity.
    fn process_group_drain_pending(
        &mut self,
        owner: &OwnedCheckProcess,
        elapsed: Duration,
        observation_error: bool,
        control_read_unknown: bool,
    ) -> Result<()>;

    /// Publish a fixed, token-bound diagnostic while output readers remain
    /// unfinished after the owned process Group was observed empty.
    fn output_capture_drain_pending(
        &mut self,
        owner: &OwnedCheckProcess,
        elapsed: Duration,
        stdout_pending: bool,
        stderr_pending: bool,
    ) -> Result<()>;

    /// Re-read cancellation after the owned Group is empty without treating a
    /// late request as a signal or evidence that cancellation caused exit.
    fn cancellation_requested_after_group_empty(
        &mut self,
        owner: &OwnedCheckProcess,
    ) -> Result<bool>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    Exited,
    Cancelled,
    TimedOut,
    CancelledBeforeStart,
    StartGateTimedOut,
    ControlReadUnknownBeforeStart,
    ProcessObservationUnknown,
}

/// Evidence about the direct child, independent of its process family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectExit {
    NotStarted,
    Observed { exit_code: Option<i32> },
    ObservationUnknown,
}

/// Evidence about the exact owned process group. Pending/unknown results carry
/// the identity needed by the host reaper; neither is resource release.
#[derive(Debug, Clone)]
pub enum FamilyDeparture {
    Confirmed,
    CleanupPending { process: Value },
    ObservationUnknown { process: Value },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureDisposition {
    NotStarted,
    Complete,
    Incomplete,
}

#[derive(Debug, Clone)]
pub struct CapturedStream {
    pub path: PathBuf,
    pub bytes_written: u64,
    pub bytes_observed: u64,
    pub truncated: bool,
    pub capture_complete: bool,
    pub capture_disposition: CaptureDisposition,
    pub capture_error: Option<String>,
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
    pub direct_exit: DirectExit,
    pub family_departure: FamilyDeparture,
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

#[derive(Debug, Default, Clone)]
struct CaptureStats {
    bytes: Vec<u8>,
    bytes_observed: u64,
    truncated: bool,
    capture_complete: bool,
    capture_error: Option<String>,
}

struct CaptureTask {
    path: PathBuf,
    file: File,
    reader: JoinHandle<()>,
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<CaptureStats>>,
}

#[derive(Default)]
struct TerminationState {
    started: Option<Instant>,
    last_request: Option<Instant>,
    attempts: u8,
    requests: u64,
    request_unconfirmed: bool,
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
        StartDecision::StartGateTimedOut => {
            group.disarm()?;
            return Ok(not_started(
                &identity,
                &output_directory,
                owner,
                Termination::StartGateTimedOut,
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

    let stdout_reader = child.stdout.take().and_then(|pipe| {
        capture_thread(
            "swarm-check-stdout",
            stdout_path.clone(),
            pipe,
            stdout_file,
            plan.output_limit_bytes_per_stream,
        )
        .ok()
    });
    let stderr_reader = child.stderr.take().and_then(|pipe| {
        capture_thread(
            "swarm-check-stderr",
            stderr_path.clone(),
            pipe,
            stderr_file,
            plan.output_limit_bytes_per_stream,
        )
        .ok()
    });

    let mut exit_code = None;
    let mut process_observation_unknown = false;
    let mut direct_exit = None;
    let mut cancel_observed = false;
    let mut timed_out = false;
    let mut control_read_unknown = false;
    let mut control_read_unknown_since = None;
    let mut control_diagnostic_published = false;
    let mut last_control_diagnostic_attempt = None;
    let mut termination = TerminationState::default();
    if stdout_reader.is_none() || stderr_reader.is_none() {
        request_termination(&group, &mut termination);
    }

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                exit_code = status.code();
                direct_exit = Some(DirectExit::Observed { exit_code });
                break;
            }
            Ok(None) => {}
            Err(_) => {
                process_observation_unknown = true;
                direct_exit = Some(DirectExit::ObservationUnknown);
                request_termination(&group, &mut termination);
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
            if termination.started.is_none() {
                request_termination(&group, &mut termination);
            } else {
                retry_termination(&group, &mut termination);
            }
        } else {
            retry_termination(&group, &mut termination);
        }
        if termination
            .started
            .is_some_and(|started| started.elapsed() >= TERMINATION_GRACE)
        {
            break;
        }
        thread::sleep(CHECK_POLL_INTERVAL);
    }

    // Dropping Child does not prove family departure. The exact Group remains
    // the sole source of that proof.
    drop(child);
    let family_departure = wait_until_empty(
        &group,
        control,
        &owner,
        &mut termination,
        &mut cancel_observed,
        &mut control_read_unknown,
    );
    let resource_released = if matches!(family_departure, FamilyDeparture::Confirmed) {
        group.disarm().is_ok()
    } else {
        false
    };
    // On Windows this closes the exact Job owner (and may request kill-on-close);
    // on every platform the serialized identity remains available for readback.
    drop(group);
    let (stdout, stderr) = finish_captures_bounded(
        stdout_reader,
        stderr_reader,
        stdout_path,
        stderr_path,
        control,
        &owner,
        &mut control_read_unknown,
    );

    let termination_reason = if process_observation_unknown {
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
        termination: termination_reason,
        direct_exit: direct_exit.unwrap_or(DirectExit::ObservationUnknown),
        family_departure,
        exit_code,
        termination_requests: termination.requests,
        stdout,
        stderr,
        resource_released,
        control_read_unknown,
        termination_request_unconfirmed: termination.request_unconfirmed,
    })
}

fn wait_until_empty(
    group: &Group,
    control: &mut impl CheckControl,
    owner: &OwnedCheckProcess,
    termination: &mut TerminationState,
    cancel_observed: &mut bool,
    control_read_unknown: &mut bool,
) -> FamilyDeparture {
    let drain_started = termination.started.unwrap_or_else(Instant::now);
    loop {
        let last_observation_unknown = match group.children_empty() {
            Ok(true) => return FamilyDeparture::Confirmed,
            Ok(false) => false,
            Err(_) => true,
        };
        if termination.started.is_none() {
            request_termination(group, termination);
        } else {
            retry_termination(group, termination);
        }
        if !*control_read_unknown {
            match control.cancellation_requested(owner) {
                Ok(true) => *cancel_observed = true,
                Ok(false) => {}
                Err(_) => *control_read_unknown = true,
            }
        }
        if termination
            .started
            .is_some_and(|started| started.elapsed() >= TERMINATION_GRACE)
        {
            let _ = control.process_group_drain_pending(
                owner,
                drain_started.elapsed(),
                last_observation_unknown,
                *control_read_unknown,
            );
            let process = owner.process.clone();
            return if last_observation_unknown {
                FamilyDeparture::ObservationUnknown { process }
            } else {
                FamilyDeparture::CleanupPending { process }
            };
        }
        thread::sleep(CHECK_POLL_INTERVAL);
    }
}

fn request_termination(group: &Group, state: &mut TerminationState) {
    state.started.get_or_insert_with(Instant::now);
    if state.attempts >= MAX_TERMINATION_ATTEMPTS {
        return;
    }
    state.attempts += 1;
    state.last_request = Some(Instant::now());
    match group.cancel_children() {
        Ok(sent) => state.requests = state.requests.saturating_add(sent),
        Err(_) => state.request_unconfirmed = true,
    }
}

fn retry_termination(group: &Group, state: &mut TerminationState) {
    if state.attempts < MAX_TERMINATION_ATTEMPTS
        && state
            .last_request
            .is_some_and(|last| last.elapsed() >= TERMINATION_RETRY_INTERVAL)
    {
        request_termination(group, state);
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

fn capture_thread<R: PollableRead + Send + 'static>(
    name: &str,
    path: PathBuf,
    mut reader: R,
    file: File,
    limit: u64,
) -> io::Result<CaptureTask> {
    reader.make_nonblocking()?;
    let capacity = usize::try_from(limit)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "capture limit overflow"))?;
    let stats = Arc::new(Mutex::new(CaptureStats {
        bytes: Vec::with_capacity(capacity.min(64 * 1024)),
        ..CaptureStats::default()
    }));
    let thread_stats = Arc::clone(&stats);
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let reader = thread::Builder::new().name(name.into()).spawn(move || {
        let mut buffer = [0u8; 16 * 1024];
        loop {
            if thread_stop.load(Ordering::Acquire) {
                break;
            }
            match reader.read_available(&mut buffer) {
                Ok(PipeRead::Pending) => thread::sleep(CHECK_POLL_INTERVAL),
                Ok(PipeRead::Eof) => {
                    capture_stats_lock(&thread_stats).capture_complete = true;
                    return;
                }
                Ok(PipeRead::Data(read)) => {
                    let mut current = capture_stats_lock(&thread_stats);
                    current.bytes_observed = current.bytes_observed.saturating_add(read as u64);
                    let remaining = capacity.saturating_sub(current.bytes.len());
                    let keep = remaining.min(read);
                    current.bytes.extend_from_slice(&buffer[..keep]);
                    if keep < read {
                        current.truncated = true;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => {
                    capture_error(&thread_stats, "capture_read_failed");
                    return;
                }
            }
        }
        capture_error(&thread_stats, "capture_drain_timeout");
    })?;
    Ok(CaptureTask {
        path,
        file,
        reader,
        stop,
        stats,
    })
}

fn capture_stats_lock(stats: &Mutex<CaptureStats>) -> std::sync::MutexGuard<'_, CaptureStats> {
    stats
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn capture_error(stats: &Mutex<CaptureStats>, code: &str) {
    let mut current = capture_stats_lock(stats);
    if current.capture_error.is_none() {
        current.capture_error = Some(code.to_owned());
    }
}

fn finish_capture_task(mut task: CaptureTask, drain_grace: Duration) -> CapturedStream {
    let deadline = Instant::now() + drain_grace;
    while !task.reader.is_finished() && Instant::now() < deadline {
        thread::sleep(CHECK_POLL_INTERVAL);
    }
    let mut stopped = !task.reader.is_finished();
    if stopped {
        task.stop.store(true, Ordering::Release);
        let stop_deadline = Instant::now() + CAPTURE_STOP_GRACE;
        while !task.reader.is_finished() && Instant::now() < stop_deadline {
            thread::sleep(CHECK_POLL_INTERVAL);
        }
        stopped = !task.reader.is_finished();
    }
    let join_panicked = if task.reader.is_finished() {
        task.reader.join().is_err()
    } else {
        false
    };
    let mut stats = if stopped {
        capture_stats_lock(&task.stats).clone()
    } else {
        let mut current = capture_stats_lock(&task.stats);
        std::mem::take(&mut *current)
    };
    if join_panicked && stats.capture_error.is_none() {
        stats.capture_error = Some("capture_reader_panicked".to_owned());
    }
    if stopped && stats.capture_error.is_none() {
        stats.capture_error = Some("capture_reader_stop_pending".to_owned());
    }
    let mut bytes_written = 0u64;
    while bytes_written < stats.bytes.len() as u64 {
        let start = usize::try_from(bytes_written).unwrap_or(stats.bytes.len());
        match task.file.write(&stats.bytes[start..]) {
            Ok(0) => {
                if stats.capture_error.is_none() {
                    stats.capture_error = Some("capture_write_failed".to_owned());
                }
                break;
            }
            Ok(count) => bytes_written = bytes_written.saturating_add(count as u64),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => {
                if stats.capture_error.is_none() {
                    stats.capture_error = Some("capture_write_failed".to_owned());
                }
                break;
            }
        }
    }
    if task.file.sync_all().is_err() && stats.capture_error.is_none() {
        stats.capture_error = Some("capture_sync_failed".to_owned());
    }
    let complete = stats.capture_complete
        && stats.capture_error.is_none()
        && !stopped
        && bytes_written == stats.bytes.len() as u64;
    CapturedStream {
        path: task.path,
        bytes_written,
        bytes_observed: stats.bytes_observed,
        truncated: stats.truncated,
        capture_complete: complete,
        capture_disposition: if complete {
            CaptureDisposition::Complete
        } else {
            CaptureDisposition::Incomplete
        },
        capture_error: stats.capture_error,
    }
}

fn capture_setup_failed(path: PathBuf) -> CapturedStream {
    CapturedStream {
        path,
        bytes_written: 0,
        bytes_observed: 0,
        truncated: false,
        capture_complete: false,
        capture_disposition: CaptureDisposition::Incomplete,
        capture_error: Some("capture_setup_failed".to_owned()),
    }
}

fn finish_captures_bounded(
    stdout: Option<CaptureTask>,
    stderr: Option<CaptureTask>,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    control: &mut impl CheckControl,
    owner: &OwnedCheckProcess,
    control_read_unknown: &mut bool,
) -> (CapturedStream, CapturedStream) {
    let drain_started = Instant::now();
    let deadline = drain_started + CAPTURE_DRAIN_GRACE;
    while Instant::now() < deadline
        && (stdout
            .as_ref()
            .is_some_and(|task| !task.reader.is_finished())
            || stderr
                .as_ref()
                .is_some_and(|task| !task.reader.is_finished()))
    {
        if control
            .cancellation_requested_after_group_empty(owner)
            .is_err()
        {
            *control_read_unknown = true;
        }
        thread::sleep(CHECK_POLL_INTERVAL);
    }
    let stdout_pending = stdout
        .as_ref()
        .is_some_and(|task| !task.reader.is_finished());
    let stderr_pending = stderr
        .as_ref()
        .is_some_and(|task| !task.reader.is_finished());
    if stdout_pending || stderr_pending {
        let _ = control.output_capture_drain_pending(
            owner,
            drain_started.elapsed(),
            stdout_pending,
            stderr_pending,
        );
    }
    (
        stdout.map_or_else(
            || capture_setup_failed(stdout_path),
            |task| finish_capture_task(task, Duration::ZERO),
        ),
        stderr.map_or_else(
            || capture_setup_failed(stderr_path),
            |task| finish_capture_task(task, Duration::ZERO),
        ),
    )
}

trait PollableRead: Read {
    fn make_nonblocking(&self) -> io::Result<()>;
    fn read_available(&mut self, buffer: &mut [u8]) -> io::Result<PipeRead>;
}

enum PipeRead {
    Data(usize),
    Pending,
    Eof,
}

#[cfg(target_os = "linux")]
impl<T: Read + std::os::fd::AsRawFd> PollableRead for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        const F_GETFL: i32 = 3;
        const F_SETFL: i32 = 4;
        const O_NONBLOCK: i32 = 0x800;
        unsafe extern "C" {
            fn fcntl(fd: i32, command: i32, ...) -> i32;
        }
        // SAFETY: fcntl reads and updates flags on this live pipe descriptor.
        let flags = unsafe { fcntl(self.as_raw_fd(), F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: F_SETFL accepts the current flags plus O_NONBLOCK.
        if unsafe { fcntl(self.as_raw_fd(), F_SETFL, flags | O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn read_available(&mut self, buffer: &mut [u8]) -> io::Result<PipeRead> {
        match self.read(buffer) {
            Ok(0) => Ok(PipeRead::Eof),
            Ok(read) => Ok(PipeRead::Data(read)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(PipeRead::Pending),
            Err(error) => Err(error),
        }
    }
}

#[cfg(windows)]
impl<T: Read + std::os::windows::io::AsRawHandle> PollableRead for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        Ok(())
    }

    fn read_available(&mut self, buffer: &mut [u8]) -> io::Result<PipeRead> {
        use std::ffi::c_void;
        unsafe extern "system" {
            fn PeekNamedPipe(
                pipe: *mut c_void,
                buffer: *mut c_void,
                buffer_size: u32,
                bytes_read: *mut u32,
                total_available: *mut u32,
                bytes_left: *mut u32,
            ) -> i32;
            fn GetLastError() -> u32;
        }
        let mut available = 0u32;
        // SAFETY: the handle is a live child pipe and output points to local storage.
        let ok = unsafe {
            PeekNamedPipe(
                self.as_raw_handle().cast(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                &mut available,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            let code = unsafe { GetLastError() };
            if code == 109 {
                return Ok(PipeRead::Eof);
            }
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        if available == 0 {
            return Ok(PipeRead::Pending);
        }
        let bound = buffer.len().min(available as usize);
        match self.read(&mut buffer[..bound]) {
            Ok(0) => Ok(PipeRead::Eof),
            Ok(read) => Ok(PipeRead::Data(read)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(PipeRead::Pending),
            Err(error) => Err(error),
        }
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
impl<T: Read> PollableRead for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded pipe capture is unsupported on this platform",
        ))
    }

    fn read_available(&mut self, _buffer: &mut [u8]) -> io::Result<PipeRead> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded pipe capture is unsupported on this platform",
        ))
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
        direct_exit: DirectExit::NotStarted,
        family_departure: FamilyDeparture::Confirmed,
        exit_code: None,
        termination_requests: 0,
        stdout: CapturedStream {
            path: output_directory.join("stdout"),
            bytes_written: 0,
            bytes_observed: 0,
            truncated: false,
            capture_complete: false,
            capture_disposition: CaptureDisposition::NotStarted,
            capture_error: Some("capture_not_started".to_owned()),
        },
        stderr: CapturedStream {
            path: output_directory.join("stderr"),
            bytes_written: 0,
            bytes_observed: 0,
            truncated: false,
            capture_complete: false,
            capture_disposition: CaptureDisposition::NotStarted,
            capture_error: Some("capture_not_started".to_owned()),
        },
        resource_released: true,
        control_read_unknown,
        termination_request_unconfirmed: false,
    }
}
