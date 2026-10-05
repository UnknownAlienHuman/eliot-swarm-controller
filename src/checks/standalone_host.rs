//! Root-host adapter for the independently built swarm-checks process.
//!
//! The root process retains CheckRun admission, profile/input resolution,
//! source/artifact verification and final settlement. The child receives a
//! private owner bootstrap and a one-use, digest-bound plan only after Store Go.

use super::{
    inputs,
    model::ExecutorPin,
    source,
    worker::{self, Completion, Work},
};
use crate::{
    artifacts::ArtifactFiles,
    error::{Error, Result},
    model::{canonical, digest, now_ms},
    platform::process_group::{departed_empty, process_image_identity, spawned_identity},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};
use swarm_checks::{CheckIdentity, MAX_CAPTURE_BYTES_PER_STREAM, ResolvedCheckPlan};

const BOOTSTRAP_FILE: &str = "worker-bootstrap.json";
const PLAN_FILE: &str = "execution-plan.json";
const PLAN_READY_FILE: &str = "plan-ready.json";
const PLAN_FAILURE_FILE: &str = "plan-failure.json";
const EXECUTOR_FAILURE_FILE: &str = "executor-failure.json";
const EXECUTION_FILE: &str = "execution.json";
const MAX_RECEIPT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_PLAN_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
struct VerifiedExecutor {
    executable: PathBuf,
    sha256: String,
    artifact_id: String,
    version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PlanContext {
    candidate_ref: String,
    candidate_content_sha256: String,
    input_fingerprint: Option<String>,
    executable_path: String,
    executable_sha256: String,
    profile_identity_sha256: String,
    scope_plan_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanReceipt {
    version: u32,
    check_id: String,
    operation_id: String,
    token: String,
    process: Value,
    plan_sha256: String,
    context: PlanContext,
    materialized_at_ms: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionReceipt {
    version: u32,
    check_id: String,
    operation_id: String,
    token: String,
    process: Value,
    plan_sha256: Option<String>,
    context: Option<PlanContext>,
    started_at_ms: Option<i64>,
    finished_at_ms: i64,
    execution: ExecutionEvidence,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionEvidence {
    check_id: String,
    operation_id: String,
    process: Value,
    child_pid: Option<u32>,
    termination: String,
    exit_code: Option<i32>,
    termination_requests: u64,
    stdout: StreamEvidence,
    stderr: StreamEvidence,
    resource_released: bool,
    control_read_unknown: bool,
    termination_request_unconfirmed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamEvidence {
    path: PathBuf,
    bytes_written: u64,
    truncated: bool,
    capture_complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminationKind {
    Exited,
    Cancelled,
    TimedOut,
    CancelledBeforeStart,
    ControlReadUnknownBeforeStart,
    ProcessObservationUnknown,
}

pub(crate) fn prepare_and_spawn(work: &Work, pin: &ExecutorPin) -> Result<Value> {
    let executor = verify_executor(pin)?;
    let directory = check_directory(work, true)?;
    let data_dir = fs::canonicalize(&work.data_dir)?;
    let bootstrap = json!({
        "version": 1,
        "check_id": work.check_id,
        "operation_id": work.operation_id,
        "token": work.token,
        "data_dir": path_text(&data_dir)?
    });
    write_private_once(
        &directory.join(BOOTSTRAP_FILE),
        canonical(&bootstrap)?.as_bytes(),
    )?;

    let log_path = directory.join("worker.stderr");
    swarm_process::write_private_new(&log_path, &[])?;
    let log = OpenOptions::new().append(true).open(log_path)?;
    let control_file = directory.join(BOOTSTRAP_FILE);
    let mut command = Command::new(&executor.executable);
    command
        .arg("--control-file")
        .arg(&control_file)
        .current_dir(&directory)
        .env_clear()
        .envs(minimal_os_environment())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command.spawn().map_err(|_| {
        Error::new(
            "CHECK_EXECUTOR_START_FAILED",
            "could not start the configured standalone checks executor",
        )
    })?;
    let pid = child.id();
    let process = match spawned_identity(pid) {
        Ok(process) => process,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(Error::new(
                "CHECK_LAUNCH_UNKNOWN",
                format!("checks executor started without a recordable launch identity: {error}"),
            ));
        }
    };
    let image = match process_image_identity(pid) {
        Ok(image) => image,
        Err(_) => {
            let killed = child.kill().is_ok();
            let reaped = child.wait().is_ok();
            return Err(if killed && reaped {
                Error::new(
                    "CHECK_EXECUTOR_MISMATCH",
                    "checks executor image could not be verified before Store Go",
                )
            } else {
                Error::new(
                    "CHECK_LAUNCH_UNKNOWN",
                    "checks executor image verification failed and its process may remain active",
                )
            });
        }
    };
    let image_path = match canonical_path_text(&image["image_path"]) {
        Ok(path) => path,
        Err(_) => {
            let killed = child.kill().is_ok();
            let reaped = child.wait().is_ok();
            return Err(if killed && reaped {
                Error::new(
                    "CHECK_EXECUTOR_MISMATCH",
                    "the running checks executor image could not be verified",
                )
            } else {
                Error::new(
                    "CHECK_LAUNCH_UNKNOWN",
                    "the checks executor image could not be verified and its process may remain active",
                )
            });
        }
    };
    if image["image_sha256"].as_str() != Some(executor.sha256.as_str())
        || image_path != path_text(&executor.executable)?
    {
        let killed = child.kill().is_ok();
        let reaped = child.wait().is_ok();
        return Err(if killed && reaped {
            Error::new(
                "CHECK_EXECUTOR_MISMATCH",
                "the running checks executor differs from the configured artifact pin",
            )
        } else {
            Error::new(
                "CHECK_LAUNCH_UNKNOWN",
                "the checks executor image differed and its process could not be confirmed stopped",
            )
        });
    }
    let launched_at = match now_ms() {
        Ok(value) => value,
        Err(error) => {
            let killed = child.kill().is_ok();
            let reaped = child.wait().is_ok();
            return Err(if killed && reaped {
                error
            } else {
                Error::new(
                    "CHECK_LAUNCH_UNKNOWN",
                    "the checks executor started but its launch receipt could not be timestamped or stopped",
                )
            });
        }
    };
    let launch = json!({
        "spawned_at_ms": launched_at,
        "process": process,
        "executor": {
            "artifact_id": executor.artifact_id,
            "version": executor.version,
            "sha256": executor.sha256,
            "image": image
        }
    });
    let child_slot = Arc::new(Mutex::new(Some(child)));
    let reaper_slot = Arc::clone(&child_slot);
    if std::thread::Builder::new()
        .name("checks-executor-reaper".into())
        .spawn(move || {
            if let Ok(mut child) = reaper_slot.lock()
                && let Some(mut child) = child.take()
            {
                let _ = child.wait();
            }
        })
        .is_err()
    {
        if let Ok(mut child) = child_slot.lock()
            && let Some(mut child) = child.take()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
        return Err(Error::new(
            "CHECK_LAUNCH_UNKNOWN",
            "checks executor reaper was unavailable before Store Go",
        ));
    }
    Ok(launch)
}

/// Called only after the existing Store ready transaction succeeded and the
/// host wrote the existing go.json. Reconciliation never reconstructs a plan.
pub(crate) fn materialize_plan(
    work: &Work,
    files: &ArtifactFiles,
    accepted_worker: &Value,
) -> Result<()> {
    let directory = check_directory(work, false)?;
    let current_worker = read_value(&directory.join("worker.json"), MAX_RECEIPT_BYTES)?;
    if &current_worker != accepted_worker
        || current_worker["token"] != work.token
        || current_worker["control_version"] != 2
        || current_worker["process"].is_null()
    {
        return Err(Error::conflict(
            "cannot plan for an unaccepted CheckRun owner",
        ));
    }
    let go = read_value(&directory.join("go.json"), MAX_RECEIPT_BYTES)?;
    if go["token"] != work.token {
        return Err(Error::conflict("cannot materialize before exact Store Go"));
    }
    if directory.join(PLAN_FAILURE_FILE).try_exists()? {
        return Err(Error::new(
            "CHECK_PLAN_MATERIALIZATION_FAILED",
            "the admitted CheckRun plan already has an immutable failure receipt",
        ));
    }
    let ready_path = directory.join(PLAN_READY_FILE);
    if ready_path.try_exists()? {
        let receipt: PlanReceipt =
            serde_json::from_value(read_value(&ready_path, MAX_RECEIPT_BYTES)?)?;
        if receipt.version != 1
            || receipt.check_id != work.check_id
            || receipt.operation_id != work.operation_id
            || receipt.token != work.token
            || receipt.process != current_worker["process"]
        {
            return Err(Error::conflict("retained plan receipt has another owner"));
        }
        return Ok(());
    }
    if directory.join("cancel.json").try_exists()? {
        // The child will settle pre-start cancellation without plan material.
        return Ok(());
    }

    let verified = source::verified_content(files, &work.data_dir, &work.candidate)?;
    let context = plan_context(work, &verified)?;
    let (argv, _expected_targets, executable) = if let Some(resolved) = &work.resolved_inputs {
        if work.input_fingerprint.as_deref() != resolved["input_fingerprint"].as_str()
            || resolved["candidate_content_sha256"] != verified.content_sha256
            || resolved["execution_workspace"]
                != inputs::execution_workspace_identity(&work.profile, &verified)?
        {
            return Err(Error::new(
                "CHECK_INPUTS_STALE",
                "candidate or workspace differs from the admitted CheckRun plan",
            ));
        }
        inputs::verify_runtime_environment(&work.profile, &verified, &resolved["environment"])?;
        let argv = serde_json::from_value(resolved["argv"].clone())
            .map_err(|_| Error::new("CHECK_INPUTS_STALE", "resolved argv is invalid"))?;
        let targets = serde_json::from_value(resolved["expected_targets"].clone())
            .map_err(|_| Error::new("CHECK_INPUTS_STALE", "resolved targets are invalid"))?;
        (argv, targets, inputs::verify_executable(resolved)?)
    } else {
        let environment = inputs::effective_environment(&work.profile);
        (
            work.profile.args.clone(),
            work.profile.expected_targets.clone(),
            worker::executable(&work.profile.executable, &environment)?,
        )
    };
    if path_text(&fs::canonicalize(&executable)?)? != context.executable_path
        || sha256_file(&executable)? != context.executable_sha256
    {
        return Err(Error::new(
            "CHECK_INPUTS_STALE",
            "resolved executable differs from its digest-bound context",
        ));
    }
    let (source_dir, candidate_file, _) = worker::ensure_execution_inputs(
        &work.data_dir,
        files,
        &work.candidate,
        &verified,
        &work.profile,
    )?;
    let mut environment = inputs::effective_environment(&work.profile);
    let data_root = fs::canonicalize(&work.data_dir)?;
    let target = worker::resolve_target_directory(&data_root, &work.profile, &environment)?;
    environment.insert("CARGO_TARGET_DIR".into(), path_text(&target)?);
    environment.insert("SWARM_CANDIDATE_FILE".into(), path_text(&candidate_file)?);
    #[cfg(windows)]
    if !executable
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"))
    {
        return Err(Error::invalid("check command must be a native executable"));
    }

    let plan = ResolvedCheckPlan {
        identity: CheckIdentity {
            check_id: work.check_id.clone(),
            operation_id: work.operation_id.clone(),
            token: work.token.clone(),
        },
        executable,
        argv,
        working_directory: fs::canonicalize(source_dir)?,
        environment,
        output_directory: fs::canonicalize(&directory)?,
        output_limit_bytes_per_stream: MAX_CAPTURE_BYTES_PER_STREAM,
        timeout: None,
    };
    let envelope = json!({
        "version": 1,
        "identity": {
            "check_id": work.check_id,
            "operation_id": work.operation_id,
            "token": work.token
        },
        "process": current_worker["process"],
        "context": context,
        "plan": {
            "executable": path_text(&plan.executable)?,
            "argv": plan.argv,
            "working_directory": path_text(&plan.working_directory)?,
            "environment": plan.environment,
            "output_directory": path_text(&plan.output_directory)?,
            "output_limit_bytes_per_stream": plan.output_limit_bytes_per_stream,
            "timeout_ms": Value::Null
        }
    });
    let bytes = canonical(&envelope)?.into_bytes();
    if bytes.len() as u64 > MAX_PLAN_BYTES {
        return Err(Error::invalid(
            "resolved CheckRun plan exceeds its size bound",
        ));
    }
    let receipt = PlanReceipt {
        version: 1,
        check_id: work.check_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        process: current_worker["process"].clone(),
        plan_sha256: digest(&bytes),
        context,
        materialized_at_ms: now_ms()?,
    };
    write_private_once(&directory.join(PLAN_FILE), &bytes)?;
    write_private_once(
        &ready_path,
        canonical(&serde_json::to_value(receipt)?)?.as_bytes(),
    )?;
    Ok(())
}

pub(crate) fn publish_plan_failure(work: &Work, accepted_worker: &Value) -> Result<()> {
    let directory = check_directory(work, false)?;
    let current = read_value(&directory.join("worker.json"), MAX_RECEIPT_BYTES)?;
    if &current != accepted_worker
        || current["token"] != work.token
        || current["control_version"] != 2
    {
        return Err(Error::conflict("plan failure owner was not accepted"));
    }
    let failure = json!({
        "version": 1,
        "check_id": work.check_id,
        "operation_id": work.operation_id,
        "token": work.token,
        "process": current["process"],
        "code": "CHECK_PLAN_MATERIALIZATION_FAILED"
    });
    worker::write_once(&directory.join(PLAN_FAILURE_FILE), &failure)
}

/// Creates the same host-authored Completion that the former in-root worker
/// wrote, but only after the independent executor has exited and its exact
/// CheckRun Group is independently observed empty.
pub(crate) fn finalize_execution(work: &Work, files: &ArtifactFiles) -> Result<Option<Completion>> {
    if work.expected_worker.is_none() {
        // The Store has not committed this exact process owner yet.
        return Ok(None);
    }
    let directory = check_directory(work, false)?;
    // Preserve the existing late-cancellation evidence behavior. This only
    // publishes a token-bound receipt and never signals a departed process.
    worker::deliver_cancel(work)?;
    let identity = read_value(&directory.join("worker.json"), MAX_RECEIPT_BYTES)?;
    if Some(&identity) != work.expected_worker.as_ref()
        || identity["token"] != work.token
        || identity["control_version"] != 2
        || identity["process"].is_null()
    {
        return Err(Error::conflict(
            "executor identity differs from the accepted CheckRun owner",
        ));
    }
    let execution_path = directory.join(EXECUTION_FILE);
    let plan_failure_path = directory.join(PLAN_FAILURE_FILE);
    let executor_failure_path = directory.join(EXECUTOR_FAILURE_FILE);
    if execution_path.try_exists()?
        && (plan_failure_path.try_exists()? || executor_failure_path.try_exists()?)
    {
        return Err(Error::conflict(
            "CheckRun has both execution and failure receipts",
        ));
    }
    let lock_path = directory.join("worker.lock");
    let lock_metadata = fs::symlink_metadata(&lock_path)?;
    if is_link_or_reparse(&lock_metadata) || !lock_metadata.is_file() {
        return Err(Error::invalid("CheckRun worker lock is not a regular file"));
    }
    let lock = OpenOptions::new().read(true).write(true).open(lock_path)?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
        Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
    }
    if !departed_empty(&identity["process"], &work.token)? {
        return Ok(None);
    }

    if execution_path.try_exists()? {
        let receipt: ExecutionReceipt =
            serde_json::from_value(read_value(&execution_path, MAX_RECEIPT_BYTES)?)?;
        validate_execution_receipt(work, &identity, &directory, &receipt)?;
        if let Some(plan_sha256) = &receipt.plan_sha256 {
            let plan_ready: PlanReceipt = serde_json::from_value(read_value(
                &directory.join(PLAN_READY_FILE),
                MAX_RECEIPT_BYTES,
            )?)?;
            let expected_context = expected_context(work, files, &plan_ready.context)?;
            if plan_ready.version != 1
                || plan_ready.check_id != work.check_id
                || plan_ready.operation_id != work.operation_id
                || plan_ready.token != work.token
                || plan_ready.process != identity["process"]
                || plan_ready.plan_sha256 != *plan_sha256
                || receipt.context.as_ref() != Some(&expected_context)
                || plan_ready.context != expected_context
            {
                return Err(Error::conflict(
                    "execution plan differs from the admitted candidate, profile or process",
                ));
            }
        } else if receipt.context.is_some() || receipt.started_at_ms.is_some() {
            return Err(Error::conflict(
                "execution without a plan contains plan-dependent evidence",
            ));
        }
        return build_completion(work, files, &directory, &identity, receipt).map(Some);
    }

    let failure_path = plan_failure_path;
    if failure_path.try_exists()? {
        let failure = read_value(&failure_path, MAX_RECEIPT_BYTES)?;
        if failure["version"] != 1
            || failure["check_id"] != work.check_id
            || failure["operation_id"] != work.operation_id
            || failure["token"] != work.token
            || failure["process"] != identity["process"]
            || failure["code"] != "CHECK_PLAN_MATERIALIZATION_FAILED"
        {
            return Err(Error::conflict("plan failure receipt identity mismatch"));
        }
        let error = Error::new(
            "CHECK_PLAN_MATERIALIZATION_FAILED",
            "the host could not materialize the admitted CheckRun plan",
        );
        return worker::failure_with_process(
            work,
            files,
            json!(error),
            identity["process"].clone(),
        )
        .map(Some);
    }
    if executor_failure_path.try_exists()? {
        let failure = read_value(&executor_failure_path, MAX_RECEIPT_BYTES)?;
        if failure.as_object().is_none_or(|fields| {
            fields.len() != 6
                || ![
                    "version",
                    "check_id",
                    "operation_id",
                    "token",
                    "process",
                    "code",
                ]
                .iter()
                .all(|field| fields.contains_key(*field))
        }) || failure["version"] != 1
            || failure["check_id"] != work.check_id
            || failure["operation_id"] != work.operation_id
            || failure["token"] != work.token
            || failure["process"] != identity["process"]
        {
            return Err(Error::conflict(
                "executor failure receipt identity mismatch",
            ));
        }
        let (code, message) = match failure["code"].as_str() {
            Some("CHECK_EXECUTOR_FAILED") => (
                "CHECK_EXECUTOR_FAILED",
                "standalone checks executor failed before producing process evidence",
            ),
            Some("CHECK_PLAN_MATERIALIZATION_FAILED") => (
                "CHECK_PLAN_MATERIALIZATION_FAILED",
                "standalone executor could not load the admitted CheckRun plan",
            ),
            Some("CHECK_PROCESS_DIAGNOSTIC_WRITE_FAILED") => (
                "CHECK_PROCESS_DIAGNOSTIC_WRITE_FAILED",
                "the CheckRun process diagnostic could not be persisted",
            ),
            Some("CHECK_OUTPUT_DIAGNOSTIC_WRITE_FAILED") => (
                "CHECK_OUTPUT_DIAGNOSTIC_WRITE_FAILED",
                "the CheckRun output-capture diagnostic could not be persisted",
            ),
            _ => return Err(Error::conflict("executor failure code is not recognized")),
        };
        let error = Error::new(code, message);
        return worker::failure_with_process(
            work,
            files,
            json!(error),
            identity["process"].clone(),
        )
        .map(Some);
    }
    Ok(None)
}

fn build_completion(
    work: &Work,
    files: &ArtifactFiles,
    directory: &Path,
    identity: &Value,
    receipt: ExecutionReceipt,
) -> Result<Completion> {
    let evidence = receipt.execution;
    let termination = parse_termination(&evidence.termination)?;
    let cancellation_request = cancellation_request(work, directory)?;
    let cancellation_applied = matches!(
        termination,
        TerminationKind::Cancelled | TerminationKind::CancelledBeforeStart
    );
    if cancellation_applied && cancellation_request.is_none() {
        return Err(Error::conflict(
            "executor reports cancellation without its durable cancellation receipt",
        ));
    }
    let cancellation = cancellation_request.as_ref().map(|request| {
        json!({
            "operation_id": request["operation_id"],
            "reason": request["reason"],
            "disposition": if termination == TerminationKind::CancelledBeforeStart {
                "cancelled_before_command"
            } else if cancellation_applied {
                "cancel_attempted_group_empty"
            } else {
                "completed_before_termination"
            },
            "termination_requests": evidence.termination_requests,
            "last_error": if evidence.termination_request_unconfirmed {
                json!({
                    "code":"CHECK_TERMINATION_UNCONFIRMED",
                    "message":"one or more requests to stop this check process group were not confirmed"
                })
            } else {
                Value::Null
            }
        })
    });

    let mut source_verified = false;
    let outcome = (|| -> Result<Value> {
        match termination {
            TerminationKind::CancelledBeforeStart => {
                return Err(Error::new(
                    "CHECK_CANCELLED",
                    "cancelled before command execution",
                ));
            }
            TerminationKind::ControlReadUnknownBeforeStart => {
                return Err(Error::new(
                    "CHECK_CONTROL_READ_UNKNOWN",
                    "the CheckRun cancellation receipt could not be read before command execution",
                ));
            }
            TerminationKind::ProcessObservationUnknown => {
                return Err(Error::new(
                    "CHECK_PROCESS_OBSERVATION_UNKNOWN",
                    "the check process exit could not be observed",
                ));
            }
            TerminationKind::TimedOut => {
                return Err(Error::new(
                    "CHECK_TIMEOUT_UNEXPECTED",
                    "the checks executor timed out without an admitted timeout policy",
                ));
            }
            TerminationKind::Exited | TerminationKind::Cancelled => {}
        }
        if evidence.control_read_unknown {
            return Err(Error::new(
                "CHECK_CONTROL_READ_UNKNOWN",
                "the CheckRun cancellation receipt could not be read while the process was active",
            ));
        }
        if !evidence.resource_released {
            return Err(Error::new(
                "CHECK_RESOURCE_RELEASE_UNKNOWN",
                "the standalone executor did not confirm release of its process Group",
            ));
        }
        if evidence.child_pid.is_none() {
            return Err(Error::new(
                "CHECK_EXECUTION_PLAN_MISSING",
                "the executor returned no command process for this admitted CheckRun",
            ));
        }

        let verified = source::verified_content(files, &work.data_dir, &work.candidate)?;
        let data_root = fs::canonicalize(&work.data_dir)?;
        let (source_dir, _) = inputs::execution_paths(&data_root, &work.profile, &verified)?;
        let source_dir = fs::canonicalize(source_dir).map_err(|_| {
            Error::new(
                "CHECK_INPUTS_STALE",
                "the materialized CheckRunner source workspace is unavailable",
            )
        })?;
        let expected_targets: Vec<String> = match &work.resolved_inputs {
            Some(resolved) => serde_json::from_value(resolved["expected_targets"].clone())
                .map_err(|_| Error::new("CHECK_INPUTS_STALE", "resolved targets are invalid"))?,
            None => work.profile.expected_targets.clone(),
        };
        let empty_scope_plan = Value::Null;
        let mut coverage = if work.profile.parser == super::model::Parser::CargoJson {
            worker::parse_cargo(
                &directory.join("stdout"),
                &expected_targets,
                work.scope_plan.as_ref().unwrap_or(&empty_scope_plan),
                &source_dir,
            )?
        } else {
            json!({"requested":["process_exit"],"checked":["process_exit"],"gaps":[]})
        };
        add_stream_gaps(&mut coverage, &evidence)?;
        match source::verify_directory(&source_dir, &verified.manifest) {
            Ok(()) => source_verified = true,
            Err(error) => coverage["gaps"]
                .as_array_mut()
                .ok_or_else(|| Error::invalid("coverage gaps are missing"))?
                .push(json!(format!("source_changed:{}", error.code))),
        }
        Ok(coverage)
    })();

    let mut outputs = Vec::new();
    for (name, stream) in [("stdout", &evidence.stdout), ("stderr", &evidence.stderr)] {
        verify_stream_path(stream, &directory.join(name), evidence.child_pid.is_some())?;
        if stream.path.try_exists()? {
            outputs.push(files.seal_file(
                &format!("{}:{name}", work.operation_id),
                &stream.path,
                json!({
                    "check_id":work.check_id,
                    "stream":name,
                    "bytes_written":stream.bytes_written,
                    "truncated":stream.truncated,
                    "capture_complete":stream.capture_complete
                }),
            )?);
        }
    }
    let (mut coverage, error) = match outcome {
        Ok(coverage) => (coverage, None),
        Err(error) => (
            json!({
                "requested":work.profile.expected_targets,
                "checked":[],
                "gaps":[error.code]
            }),
            Some(error),
        ),
    };
    if let Some(gaps) = work
        .scope_plan
        .as_ref()
        .and_then(|plan| plan["coverage_gaps"].as_array())
        && let Some(coverage_gaps) = coverage["gaps"].as_array_mut()
    {
        coverage_gaps.extend(gaps.iter().filter(|gap| gap.is_string()).cloned());
        coverage_gaps.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        coverage_gaps.dedup();
    }
    let state = if cancellation_applied {
        "cancelled"
    } else if error.is_some() {
        "error"
    } else if evidence.exit_code != Some(0) {
        "failed"
    } else if coverage["gaps"]
        .as_array()
        .is_none_or(|gaps| !gaps.is_empty())
        || (work.profile.parser == super::model::Parser::CargoJson
            && (coverage["build_finished"] != true || coverage["errors"].as_u64().unwrap_or(0) > 0))
    {
        "incomplete"
    } else {
        "passed"
    };
    let report = json!({
        "version":1,
        "check_id":work.check_id,
        "operation_id":work.operation_id,
        "candidate_ref":work.candidate.artifact_id,
        "candidate_sha256":work.candidate.content_digest,
        "input_fingerprint":work.input_fingerprint,
        "resolved_inputs":work.resolved_inputs,
        "scope_plan":work.scope_plan,
        "cache_reusable":work.resolved_inputs.as_ref().is_some_and(|inputs|inputs["cache_reusable"]==true),
        "profile":inputs::profile_identity(&work.profile)?,
        "cancellation":cancellation,
        "worker_version":env!("CARGO_PKG_VERSION"),
        "process":identity["process"],
        "state":state,
        "exit_code":evidence.exit_code,
        "resource_released":evidence.resource_released,
        "source_checkout_verified":source_verified,
        "coverage":coverage,
        "error":error,
        "outputs":outputs.iter().map(|record|json!({
            "artifact_ref":record.artifact_id,
            "stream":record.metadata["stream"],
            "sha256":record.content_digest,
            "length":record.byte_length,
            "bytes_written":record.metadata["bytes_written"],
            "truncated":record.metadata["truncated"],
            "capture_complete":record.metadata["capture_complete"]
        })).collect::<Vec<_>>(),
        "started_at_ms":receipt.started_at_ms.unwrap_or(now_ms()?),
        "finished_at_ms":receipt.finished_at_ms
    });
    let result_id = format!("check-{}", digest(work.operation_id.as_bytes()));
    let (result, bytes) = ArtifactFiles::document(
        "check_result",
        &result_id,
        &report,
        json!({
            "check_id":work.check_id,
            "candidate_ref":work.candidate.artifact_id,
            "state":state
        }),
    )?;
    files.publish(&result, &bytes)?;
    let completion = Completion {
        check_id: work.check_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        state: state.into(),
        exit_code: evidence.exit_code,
        resource_released: evidence.resource_released,
        coverage,
        result,
        outputs,
        cancellation,
    };
    worker::write_once(&directory.join("terminal.json"), &json!(completion))?;
    worker::write_once(&directory.join("completion.json"), &json!(completion))?;
    Ok(completion)
}

fn expected_context(
    work: &Work,
    files: &ArtifactFiles,
    receipt: &PlanContext,
) -> Result<PlanContext> {
    let verified = source::verified_content(files, &work.data_dir, &work.candidate)?;
    let profile_identity_sha256 = inputs::profile_identity_sha256(&work.profile)?;
    let null_scope_plan = Value::Null;
    let scope_plan = work.scope_plan.as_ref().unwrap_or(&null_scope_plan);
    let scope_plan_sha256 = digest(canonical(scope_plan)?.as_bytes());
    if receipt.candidate_ref != work.candidate.artifact_id
        || receipt.candidate_content_sha256 != verified.content_sha256
        || receipt.input_fingerprint != work.input_fingerprint
        || receipt.profile_identity_sha256 != profile_identity_sha256
        || receipt.scope_plan_sha256 != scope_plan_sha256
    {
        return Err(Error::conflict(
            "plan receipt differs from the admitted candidate, inputs or profile",
        ));
    }
    if let Some(resolved) = &work.resolved_inputs {
        let executable = inputs::verify_executable(resolved)?;
        if resolved["executable"]["sha256"] != receipt.executable_sha256 {
            return Err(Error::conflict(
                "plan executable digest differs from resolved inputs",
            ));
        }
        if path_text(&fs::canonicalize(executable)?)? != receipt.executable_path {
            return Err(Error::conflict(
                "plan executable path differs from resolved inputs",
            ));
        }
    } else {
        let environment = inputs::effective_environment(&work.profile);
        let executable = worker::executable(&work.profile.executable, &environment)?;
        if path_text(&fs::canonicalize(executable)?)? != receipt.executable_path
            || sha256_file(&PathBuf::from(&receipt.executable_path))? != receipt.executable_sha256
        {
            return Err(Error::conflict(
                "plan executable differs from the admitted check profile",
            ));
        }
    }
    let executable = PathBuf::from(&receipt.executable_path);
    if !executable.is_absolute()
        || !valid_sha256(&receipt.executable_sha256)
        || sha256_file(&executable)? != receipt.executable_sha256
    {
        return Err(Error::conflict("plan executable path or digest is stale"));
    }
    Ok(receipt.clone())
}

fn validate_execution_receipt(
    work: &Work,
    identity: &Value,
    directory: &Path,
    receipt: &ExecutionReceipt,
) -> Result<()> {
    let execution = &receipt.execution;
    if receipt.version != 1
        || receipt.check_id != work.check_id
        || receipt.operation_id != work.operation_id
        || receipt.token != work.token
        || receipt.process != identity["process"]
        || receipt.finished_at_ms < 0
        || receipt.started_at_ms.is_some_and(|value| value < 0)
        || execution.check_id != work.check_id
        || execution.operation_id != work.operation_id
        || execution.process != identity["process"]
        || !execution.resource_released
    {
        return Err(Error::conflict(
            "executor receipt differs from the accepted CheckRun owner",
        ));
    }
    if receipt
        .plan_sha256
        .as_ref()
        .is_some_and(|digest| !valid_sha256(digest))
    {
        return Err(Error::invalid("execution plan digest is invalid"));
    }
    verify_stream_path(
        &execution.stdout,
        &directory.join("stdout"),
        execution.child_pid.is_some(),
    )?;
    verify_stream_path(
        &execution.stderr,
        &directory.join("stderr"),
        execution.child_pid.is_some(),
    )?;
    let has_plan = receipt.plan_sha256.is_some();
    let has_context = receipt.context.is_some();
    let termination = parse_termination(&execution.termination)?;
    if execution.stdout.bytes_written > MAX_CAPTURE_BYTES_PER_STREAM
        || execution.stderr.bytes_written > MAX_CAPTURE_BYTES_PER_STREAM
        || has_plan != has_context
        || has_plan != receipt.started_at_ms.is_some()
        || (execution.child_pid.is_some() && !has_plan)
    {
        return Err(Error::invalid(
            "execution stream or plan evidence is invalid",
        ));
    }
    if !has_plan
        && !matches!(
            termination,
            TerminationKind::CancelledBeforeStart | TerminationKind::ControlReadUnknownBeforeStart
        )
    {
        return Err(Error::invalid(
            "execution without a plan is not a pre-start disposition",
        ));
    }
    Ok(())
}

fn add_stream_gaps(coverage: &mut Value, execution: &ExecutionEvidence) -> Result<()> {
    let gaps = coverage["gaps"]
        .as_array_mut()
        .ok_or_else(|| Error::invalid("coverage gaps are missing"))?;
    for (stream, name) in [(&execution.stdout, "stdout"), (&execution.stderr, "stderr")] {
        if stream.truncated {
            gaps.push(json!(format!("{name}_truncated")));
        }
        if !stream.capture_complete {
            gaps.push(json!(format!("{name}_capture_incomplete")));
        }
    }
    gaps.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    gaps.dedup();
    Ok(())
}

fn verify_stream_path(evidence: &StreamEvidence, expected: &Path, required: bool) -> Result<()> {
    if evidence.path != expected {
        return Err(Error::conflict(
            "output path differs from the fixed CheckRun stream",
        ));
    }
    if evidence.path.try_exists()? {
        let metadata = fs::symlink_metadata(&evidence.path)?;
        if is_link_or_reparse(&metadata)
            || !metadata.is_file()
            || metadata.len() > MAX_CAPTURE_BYTES_PER_STREAM
            || (evidence.capture_complete && metadata.len() != evidence.bytes_written)
            || metadata.len() < evidence.bytes_written
        {
            return Err(Error::conflict(
                "output file differs from the bounded capture receipt",
            ));
        }
    } else if required || evidence.bytes_written != 0 || evidence.capture_complete {
        return Err(Error::conflict("captured output file is missing"));
    }
    Ok(())
}

fn cancellation_request(work: &Work, directory: &Path) -> Result<Option<Value>> {
    let path = directory.join("cancel.json");
    if !path.try_exists()? {
        return Ok(None);
    }
    let value = read_value(&path, MAX_RECEIPT_BYTES)?;
    if value["check_id"] != work.check_id
        || value["token"] != work.token
        || value["request"]["operation_id"] != work.operation_id
        || value["request"]["reason"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return Err(Error::conflict("cancellation receipt identity mismatch"));
    }
    Ok(Some(value["request"].clone()))
}

fn parse_termination(value: &str) -> Result<TerminationKind> {
    match value {
        "exited" => Ok(TerminationKind::Exited),
        "cancelled" => Ok(TerminationKind::Cancelled),
        "timed_out" => Ok(TerminationKind::TimedOut),
        "cancelled_before_start" => Ok(TerminationKind::CancelledBeforeStart),
        "control_read_unknown_before_start" => Ok(TerminationKind::ControlReadUnknownBeforeStart),
        "process_observation_unknown" => Ok(TerminationKind::ProcessObservationUnknown),
        _ => Err(Error::invalid("unknown standalone termination disposition")),
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn verify_executor(pin: &ExecutorPin) -> Result<VerifiedExecutor> {
    pin.validate_shape()?;
    let metadata = fs::symlink_metadata(&pin.executable)?;
    if is_link_or_reparse(&metadata) || !metadata.is_file() {
        return Err(Error::invalid(
            "checks executor is not a regular executable file",
        ));
    }
    let executable = fs::canonicalize(&pin.executable)?;
    let sha256 = sha256_file(&executable)?;
    if sha256 != pin.sha256 {
        return Err(Error::conflict(
            "checks executor differs from its configured digest",
        ));
    }
    Ok(VerifiedExecutor {
        executable,
        sha256,
        artifact_id: pin.artifact_id.clone(),
        version: pin.version.clone(),
    })
}

/// The private bootstrap is the durable discriminator between the standalone
/// protocol and an already-running legacy worker during optional migration.
pub(crate) fn owns_work(work: &Work) -> Result<bool> {
    let data_dir = fs::canonicalize(&work.data_dir)?;
    let _ = worker::directory(&data_dir, &work.check_id)?;
    let checks = data_dir.join("checks");
    if !checks.try_exists()? {
        return Ok(false);
    }
    let directory = checks.join(&work.check_id);
    if !directory.try_exists()? {
        return Ok(false);
    }
    let directory = check_directory(work, false)?;
    Ok(directory.join(BOOTSTRAP_FILE).try_exists()?)
}

fn check_directory(work: &Work, create_job: bool) -> Result<PathBuf> {
    let data_dir = fs::canonicalize(&work.data_dir)?;
    let _ = worker::directory(&data_dir, &work.check_id)?;
    let checks = data_dir.join("checks");
    if create_job && !checks.try_exists()? {
        fs::create_dir(&checks)?;
    }
    let checks_metadata = fs::symlink_metadata(&checks)?;
    if is_link_or_reparse(&checks_metadata) || !checks_metadata.is_dir() {
        return Err(Error::invalid("CheckRun root is not a regular directory"));
    }
    let checks = fs::canonicalize(checks)?;
    if checks.parent() != Some(data_dir.as_path()) {
        return Err(Error::invalid(
            "CheckRun root escaped the controller data directory",
        ));
    }
    let directory = checks.join(&work.check_id);
    if create_job && let Err(error) = fs::create_dir(&directory) {
        return Err(if error.kind() == std::io::ErrorKind::AlreadyExists {
            Error::new(
                "CHECK_LAUNCH_UNKNOWN",
                "existing CheckRun directory requires reconciliation, not another executor",
            )
        } else {
            error.into()
        });
    }
    let metadata = fs::symlink_metadata(&directory)?;
    if is_link_or_reparse(&metadata) || !metadata.is_dir() {
        return Err(Error::invalid("CheckRun job is not a regular directory"));
    }
    let directory = fs::canonicalize(directory)?;
    if directory.parent() != Some(checks.as_path()) {
        return Err(Error::invalid("CheckRun job escaped its owned directory"));
    }
    Ok(directory)
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn minimal_os_environment() -> Vec<(OsString, OsString)> {
    #[cfg(windows)]
    let names = ["SystemRoot", "WINDIR", "ComSpec", "TEMP", "TMP"];
    #[cfg(not(windows))]
    let names: [&str; 0] = [];
    names
        .into_iter()
        .filter_map(|name| std::env::var_os(name).map(|value| (name.into(), value)))
        .collect()
}

fn plan_context(work: &Work, verified: &source::VerifiedSource) -> Result<PlanContext> {
    let (executable, executable_sha256) = if let Some(resolved) = &work.resolved_inputs {
        let executable = inputs::verify_executable(resolved)?;
        let digest = resolved["executable"]["sha256"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::new("CHECK_INPUTS_STALE", "executable digest is missing"))?;
        (executable, digest)
    } else {
        let environment = inputs::effective_environment(&work.profile);
        let executable = worker::executable(&work.profile.executable, &environment)?;
        let digest = sha256_file(&executable)?;
        (executable, digest)
    };
    if !valid_sha256(&executable_sha256) {
        return Err(Error::new(
            "CHECK_INPUTS_STALE",
            "executable digest is invalid",
        ));
    }
    if let Some(fingerprint) = &work.input_fingerprint
        && !valid_sha256(fingerprint)
    {
        return Err(Error::new(
            "CHECK_INPUTS_STALE",
            "input fingerprint is invalid",
        ));
    }
    let null_scope_plan = Value::Null;
    let scope_plan = work.scope_plan.as_ref().unwrap_or(&null_scope_plan);
    Ok(PlanContext {
        candidate_ref: work.candidate.artifact_id.clone(),
        candidate_content_sha256: verified.content_sha256.clone(),
        input_fingerprint: work.input_fingerprint.clone(),
        executable_path: path_text(&fs::canonicalize(executable)?)?,
        executable_sha256,
        profile_identity_sha256: inputs::profile_identity_sha256(&work.profile)?,
        scope_plan_sha256: digest(canonical(scope_plan)?.as_bytes()),
    })
}

fn canonical_path_text(value: &Value) -> Result<String> {
    let path = value
        .as_str()
        .ok_or_else(|| Error::invalid("process image path is missing"))?;
    path_text(&fs::canonicalize(path)?)
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::invalid("CheckRun path is not Unicode"))
}

fn read_value(path: &Path, maximum: u64) -> Result<Value> {
    Ok(serde_json::from_slice(&read_bytes(path, maximum)?)?)
}

fn read_bytes(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if is_link_or_reparse(&metadata) || !metadata.is_file() {
        return Err(Error::invalid("CheckRun receipt is not a regular file"));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::invalid("CheckRun receipt exceeds its size limit"));
    }
    Ok(bytes)
}

fn write_private_once(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.try_exists()? {
        if read_bytes(path, MAX_PLAN_BYTES)? == bytes {
            return Ok(());
        }
        return Err(Error::conflict("retained private plan differs"));
    }
    swarm_process::write_private_new(path, bytes).map_err(Into::into)
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
