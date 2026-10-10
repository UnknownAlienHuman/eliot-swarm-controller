//! Bounded native check execution for plans already resolved and admitted by
//! the Swarm kernel. This crate owns no Store, Task, selector, or result DB.
//!
//! A kernel adapter implements [`CheckControl`]. It must publish the exact
//! process identity through the existing CheckRun path and return `Start` only
//! after the Store has durably acknowledged that owner. The executor does not
//! retry a command when that acknowledgement is missing or uncertain.

mod capture;
mod status;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use capture::{CapturePair, CaptureReceipt, CaptureStream, PollableRead, create_output};
use serde_json::Value;
use swarm_contracts::{Error, Result};
use swarm_process::{Group, MAX_CAPTURE_BYTES_PER_STREAM as PROCESS_CAPTURE_LIMIT};

pub use status::read_check_status;

/// Maximum bytes retained per stdout/stderr file. Excess bytes are drained and
/// counted as observed, while `truncated` reports that the disk cap was hit.
pub const MAX_CAPTURE_BYTES_PER_STREAM: u64 = PROCESS_CAPTURE_LIMIT;
const CHECK_POLL_INTERVAL: Duration = Duration::from_millis(25);
const LONG_DRAIN_DIAGNOSTIC_AFTER: Duration = Duration::from_secs(30);
const TERMINATION_GRACE: Duration = Duration::from_secs(5);
const CAPTURE_DRAIN_GRACE: Duration = Duration::from_secs(5);
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
    /// Exact number of bytes accepted by the output file writer.
    pub bytes_written: u64,
    /// Exact number of bytes read from the child pipe.
    pub bytes_observed: u64,
    /// SHA-256 of exactly the `bytes_written` bytes accepted by the file.
    pub sha256: String,
    /// True when observed output exceeded the retained file prefix.
    pub truncated: bool,
    pub capture_complete: bool,
    pub capture_disposition: CaptureDisposition,
    pub capture_error: Option<String>,
}

impl From<CaptureReceipt> for CapturedStream {
    fn from(receipt: CaptureReceipt) -> Self {
        let capture_disposition = if !receipt.started {
            CaptureDisposition::NotStarted
        } else if receipt.capture_complete {
            CaptureDisposition::Complete
        } else {
            CaptureDisposition::Incomplete
        };
        Self {
            path: receipt.path,
            bytes_written: receipt.bytes_written,
            bytes_observed: receipt.bytes_observed,
            sha256: receipt.sha256,
            truncated: receipt.truncated,
            capture_complete: receipt.capture_complete,
            capture_disposition,
            capture_error: receipt.capture_error,
        }
    }
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
pub fn execute<C: CheckControl>(
    plan: &ResolvedCheckPlan,
    control: &mut C,
    publish_execution: impl FnMut(&CheckExecution, &mut C) -> Result<()>,
) -> Result<CheckExecution> {
    let identity = plan.identity.clone();
    let output_directory = plan.output_directory.clone();
    execute_with_plan(
        identity,
        output_directory,
        control,
        || Ok(plan.clone()),
        publish_execution,
    )
}

/// Establish the existing CheckRun process owner and wait for Store's durable
/// go-ahead before resolving execution inputs. `build_plan` runs only after
/// that handshake and must return the same admitted identity.
pub fn execute_with_plan<C: CheckControl>(
    identity: CheckIdentity,
    output_directory: PathBuf,
    control: &mut C,
    build_plan: impl FnOnce() -> Result<ResolvedCheckPlan>,
    mut publish_execution: impl FnMut(&CheckExecution, &mut C) -> Result<()>,
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
            let execution = not_started(
                &identity,
                &output_directory,
                owner,
                Termination::CancelledBeforeStart,
                false,
            );
            publish_execution(&execution, control)?;
            return Ok(execution);
        }
        StartDecision::StartGateTimedOut => {
            group.disarm()?;
            let execution = not_started(
                &identity,
                &output_directory,
                owner,
                Termination::StartGateTimedOut,
                false,
            );
            publish_execution(&execution, control)?;
            return Ok(execution);
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
            let execution = not_started(
                &plan.identity,
                &plan.output_directory,
                owner,
                Termination::CancelledBeforeStart,
                false,
            );
            publish_execution(&execution, control)?;
            return Ok(execution);
        }
        Err(_) => {
            group.disarm()?;
            let execution = not_started(
                &plan.identity,
                &plan.output_directory,
                owner,
                Termination::ControlReadUnknownBeforeStart,
                true,
            );
            publish_execution(&execution, control)?;
            return Ok(execution);
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

    let stdout_capture = child.stdout.take().and_then(|pipe| {
        CaptureStream::new(
            stdout_path.clone(),
            stdout_file,
            pipe,
            plan.output_limit_bytes_per_stream,
        )
        .ok()
    });
    let stderr_capture = child.stderr.take().and_then(|pipe| {
        CaptureStream::new(
            stderr_path.clone(),
            stderr_file,
            pipe,
            plan.output_limit_bytes_per_stream,
        )
        .ok()
    });
    let mut captures = CapturePair::new(
        stdout_path.clone(),
        stdout_capture,
        stderr_path.clone(),
        stderr_capture,
    );

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
    if captures.has_setup_failure() {
        request_termination(&group, &mut termination);
    }

    loop {
        captures.poll();
        if captures.has_reader_failure() && termination.started.is_none() {
            request_termination(&group, &mut termination);
        }
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
        &mut captures,
    );
    let resource_released = if matches!(family_departure, FamilyDeparture::Confirmed) {
        group.disarm().is_ok()
    } else {
        false
    };
    let (stdout, stderr) =
        finish_captures_bounded(captures, control, &owner, &mut control_read_unknown);
    // Keep the process owner alive until output files have been synced. On
    // Windows, dropping a pending kill-on-close Job can terminate this worker;
    // the caller still publishes the final execution receipt after return.
    let termination_reason = if process_observation_unknown {
        Termination::ProcessObservationUnknown
    } else if cancel_observed {
        Termination::Cancelled
    } else if timed_out {
        Termination::TimedOut
    } else {
        Termination::Exited
    };

    let execution = CheckExecution {
        check_id: plan.identity.check_id.clone(),
        operation_id: plan.identity.operation_id.clone(),
        process: owner.process.clone(),
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
    };
    let publication = publish_execution(&execution, control);
    if publication.is_err()
        || !publication_release_is_safe(&execution.family_departure, execution.resource_released)
    {
        // The caller's worker remains alive (and Store retains its Child) while
        // this exact Group stays in custody. The callback is never retried.
        retain_group_until_released(
            &group,
            control,
            &owner,
            &mut termination,
            &mut cancel_observed,
            &mut control_read_unknown,
        );
    }
    publication?;
    // The receipt was published before this disarmed owner is released.
    drop(group);
    Ok(execution)
}

fn publication_release_is_safe(
    family_departure: &FamilyDeparture,
    resource_released: bool,
) -> bool {
    matches!(family_departure, FamilyDeparture::Confirmed) && resource_released
}

fn retain_group_until_released(
    group: &Group,
    control: &mut impl CheckControl,
    owner: &OwnedCheckProcess,
    termination: &mut TerminationState,
    cancel_observed: &mut bool,
    control_read_unknown: &mut bool,
) {
    loop {
        let family_departed = match group.children_empty() {
            Ok(true) => {
                if group.disarm().is_ok() {
                    return;
                }
                true
            }
            Ok(false) | Err(_) => false,
        };

        if !family_departed {
            if termination.started.is_none() {
                request_termination(group, termination);
            } else {
                retry_termination(group, termination);
            }
        }
        if !*control_read_unknown {
            match control.cancellation_requested(owner) {
                Ok(true) => *cancel_observed = true,
                Ok(false) => {}
                Err(_) => *control_read_unknown = true,
            }
        }
        thread::sleep(CHECK_POLL_INTERVAL);
    }
}

fn wait_until_empty<O: PollableRead, E: PollableRead>(
    group: &Group,
    control: &mut impl CheckControl,
    owner: &OwnedCheckProcess,
    termination: &mut TerminationState,
    cancel_observed: &mut bool,
    control_read_unknown: &mut bool,
    captures: &mut CapturePair<O, E>,
) -> FamilyDeparture {
    let drain_started = termination.started.unwrap_or_else(Instant::now);
    loop {
        captures.poll();
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

fn finish_captures_bounded<O: PollableRead, E: PollableRead>(
    mut captures: CapturePair<O, E>,
    control: &mut impl CheckControl,
    owner: &OwnedCheckProcess,
    control_read_unknown: &mut bool,
) -> (CapturedStream, CapturedStream) {
    let drain_started = Instant::now();
    let deadline = drain_started + CAPTURE_DRAIN_GRACE;
    while Instant::now() < deadline && (!captures.stdout_finished() || !captures.stderr_finished())
    {
        let progressed = captures.poll();
        if control
            .cancellation_requested_after_group_empty(owner)
            .is_err()
        {
            *control_read_unknown = true;
        }
        if !progressed {
            thread::sleep(CHECK_POLL_INTERVAL);
        }
    }
    let stdout_pending = !captures.stdout_finished();
    let stderr_pending = !captures.stderr_finished();
    if stdout_pending || stderr_pending {
        let _ = control.output_capture_drain_pending(
            owner,
            drain_started.elapsed(),
            stdout_pending,
            stderr_pending,
        );
    }
    let (stdout, stderr) = captures.finish();
    (stdout.into(), stderr.into())
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
        stdout: CaptureReceipt::not_started(output_directory.join("stdout")).into(),
        stderr: CaptureReceipt::not_started(output_directory.join("stderr")).into(),
        resource_released: true,
        control_read_unknown,
        termination_request_unconfirmed: false,
    }
}

#[cfg(test)]
mod publication_failure_fault_fixture {
    use super::*;

    #[test]
    fn writer_error_requires_confirmed_departure_and_released_resource() {
        let publication: Result<()> = Err(Error::new("FIXTURE_WRITE_FAILED", "injected"));
        assert!(publication.is_err());
        assert!(!publication_release_is_safe(
            &FamilyDeparture::CleanupPending {
                process: Value::Null,
            },
            false,
        ));
        assert!(!publication_release_is_safe(
            &FamilyDeparture::ObservationUnknown {
                process: Value::Null,
            },
            false,
        ));
        assert!(!publication_release_is_safe(
            &FamilyDeparture::Confirmed,
            false,
        ));
        assert!(publication_release_is_safe(
            &FamilyDeparture::Confirmed,
            true
        ));
    }
}
