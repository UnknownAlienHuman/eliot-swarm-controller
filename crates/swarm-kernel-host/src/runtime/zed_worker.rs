//! Isolated one-shot owner for one native Zed `eval-cli` invocation.
//!
//! The host binary dispatches this hidden command before opening the Store.
//! The helper creates its process Group before the native child, publishes the
//! exact Group identity, and starts no native work until the parent binds a
//! one-use go gate to that identity and the exact private plan.

use super::{
    BatchWorkerPlan, CLEANUP_GRACE_SECONDS, HOST_GRACE_SECONDS, MAX_OUTPUT_BYTES, POLL_INTERVAL,
    WORKER_MAX_TERMINATION_ATTEMPTS, WORKER_START_GATE_SECONDS, WORKER_TERMINATION_RETRY,
    read_json_bounded, replace_json_durable, write_json_new,
};
use crate::{
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

const READY_FILE: &str = "worker-ready.json";
const GO_FILE: &str = "worker-go.json";
const DENY_FILE: &str = "worker-deny.json";
const CANCEL_FILE: &str = "worker-cancel.json";
const STATE_FILE: &str = "worker-state.json";
const RESULT_FILE: &str = "worker-result.json";
const MAX_CONTROL_BYTES: u64 = 8 * 1024 * 1024;
const MAX_CAPTURE_BYTES: u64 = MAX_OUTPUT_BYTES;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerGate {
    version: u8,
    decision: String,
    operation_id: String,
    run_id: String,
    plan_sha256: String,
    worker_pid: u32,
    worker_identity: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureFact {
    file: String,
    bytes_total: Option<u64>,
    bytes_hashed: u64,
    sha256: Option<String>,
    truncated: bool,
    complete: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerEvidence {
    version: u8,
    operation_id: String,
    run_id: String,
    plan_sha256: String,
    worker_pid: u32,
    worker_identity: Value,
    launch_state: String,
    launch_error: Option<String>,
    native_pid: Option<u32>,
    native_identity: Option<Value>,
    native_identity_error: Option<String>,
    direct_exit: String,
    exit_code: Option<i32>,
    signal: Option<i32>,
    direct_exit_error: Option<String>,
    family_departure: String,
    family_error: Option<String>,
    capture: String,
    capture_error: Option<String>,
    stdout: Option<CaptureFact>,
    stderr: Option<CaptureFact>,
    cleanup: String,
    termination_requests: u64,
    host_termination_requested: bool,
    family_termination_requested: bool,
    parent_termination_requested: bool,
    failure: Option<Value>,
}

enum GateDecision {
    Go,
    Deny,
    TimedOut,
}

/// Entry point wired by the host's hidden `zed-batch-worker --file` command.
pub fn run_worker(plan_path: &Path) -> Result<()> {
    let plan_value = read_json_bounded(plan_path, MAX_CONTROL_BYTES)?;
    let plan: BatchWorkerPlan = serde_json::from_value(plan_value.clone())
        .map_err(|_| Error::new("BATCH_WORKER_PLAN_INVALID", "worker plan schema is invalid"))?;
    validate_plan(plan_path, &plan, &plan_value)?;

    // The worker is its own OS process, so this Group contains no unrelated
    // host children. The native command inherits only this worker's Job/PGID.
    let group = swarm_process::Group::enter(&plan.run_id)?;
    let identity = group.identity.clone();
    let worker_pid = std::process::id();
    let plan_sha256 = model::digest(model::canonical(&plan_value)?.as_bytes());
    let ready = json!({
        "version": 1,
        "operation_id": plan.operation_id,
        "run_id": plan.run_id,
        "plan_sha256": plan_sha256,
        "worker_pid": worker_pid,
        "worker_identity": identity
    });
    write_json_new(&plan.output_dir.join(READY_FILE), &ready)?;

    let mut evidence = WorkerEvidence {
        version: 1,
        operation_id: plan.operation_id.clone(),
        run_id: plan.run_id.clone(),
        plan_sha256,
        worker_pid,
        worker_identity: identity,
        launch_state: "waiting_for_parent_gate".to_owned(),
        launch_error: None,
        native_pid: None,
        native_identity: None,
        native_identity_error: None,
        direct_exit: "not_started".to_owned(),
        exit_code: None,
        signal: None,
        direct_exit_error: None,
        family_departure: "not_observed".to_owned(),
        family_error: None,
        capture: "not_started".to_owned(),
        capture_error: None,
        stdout: None,
        stderr: None,
        cleanup: "not_started".to_owned(),
        termination_requests: 0,
        host_termination_requested: false,
        family_termination_requested: false,
        parent_termination_requested: false,
        failure: None,
    };
    persist_state(&plan.output_dir, &evidence)?;

    match wait_for_gate(&plan, &evidence.worker_identity)? {
        GateDecision::Deny => {
            evidence.launch_state = "denied_before_native_start".to_owned();
            evidence.cleanup = close_empty_group(&group, &mut evidence);
            finish_capture(&plan.output_dir, &mut evidence, None, None)?;
            persist_result(&plan.output_dir, &evidence)?;
            return Ok(());
        }
        GateDecision::TimedOut => {
            evidence.launch_state = "start_gate_timed_out".to_owned();
            evidence.failure = Some(json!({
                "code": "BATCH_WORKER_GATE_TIMEOUT",
                "message": "parent did not publish a matching go or deny gate"
            }));
            evidence.cleanup = close_empty_group(&group, &mut evidence);
            finish_capture(&plan.output_dir, &mut evidence, None, None)?;
            persist_result(&plan.output_dir, &evidence)?;
            return Ok(());
        }
        GateDecision::Go => {}
    }

    let (stdout_writer, stdout_sync) = open_capture(&plan.output_dir.join("stdout.log"))?;
    let (stderr_writer, stderr_sync) = open_capture(&plan.output_dir.join("stderr.log"))?;
    let mut command = Command::new(&plan.executable);
    command
        .arg("--workdir")
        .arg(&plan.workdir)
        .arg("--model")
        .arg(&plan.model)
        .arg("--instruction")
        .arg(&plan.instruction)
        .arg("--timeout")
        .arg(plan.timeout_seconds.to_string())
        .arg("--output-dir")
        .arg(&plan.output_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_writer))
        .stderr(Stdio::from(stderr_writer))
        .env_clear();
    for key in ["PATH", "HOME"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    for key in &plan.env_keys {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            evidence.launch_state = "not_started".to_owned();
            evidence.launch_error = Some(error.to_string());
            evidence.failure = Some(json!({
                "code": "NATIVE_LAUNCH_FAILED",
                "message": error.to_string()
            }));
            evidence.family_departure = match group.children_empty() {
                Ok(true) => "confirmed".to_owned(),
                Ok(false) => "observed_active".to_owned(),
                Err(observe_error) => {
                    evidence.family_error = Some(observe_error.to_string());
                    "observation_unknown".to_owned()
                }
            };
            evidence.cleanup = close_empty_group(&group, &mut evidence);
            finish_capture(
                &plan.output_dir,
                &mut evidence,
                Some(stdout_sync),
                Some(stderr_sync),
            )?;
            persist_result(&plan.output_dir, &evidence)?;
            return Ok(());
        }
    };
    evidence.launch_state = "spawned".to_owned();
    evidence.native_pid = Some(child.id());
    match swarm_process::spawned_identity(child.id()) {
        Ok(identity) => evidence.native_identity = Some(identity),
        Err(error) => {
            evidence.native_identity_error = Some(format!("{}: {}", error.code, error.message))
        }
    }
    evidence.direct_exit = "pending".to_owned();
    evidence.family_departure = "observed_active".to_owned();
    evidence.capture = "in_progress".to_owned();
    persist_state(&plan.output_dir, &evidence)?;

    observe_native(
        &plan,
        &group,
        &mut child,
        stdout_sync,
        stderr_sync,
        &mut evidence,
    )?;
    persist_result(&plan.output_dir, &evidence)
}

pub(super) fn validate_plan(plan_path: &Path, plan: &BatchWorkerPlan, value: &Value) -> Result<()> {
    let expected_path = plan.output_dir.join("worker-plan.json");
    let metadata = fs::symlink_metadata(plan_path)?;
    let output_metadata = fs::symlink_metadata(&plan.output_dir)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || !output_metadata.is_dir()
        || output_metadata.file_type().is_symlink()
        || plan_path != expected_path
        || plan.version != 1
        || plan.run_id != super::run_id(&plan.operation_id)
        || plan.intent.operation_id != plan.operation_id
        || plan.intent.run_id != plan.run_id
        || !plan.executable.is_absolute()
        || !plan.workdir.is_absolute()
        || !(1..=86_400).contains(&plan.timeout_seconds)
        || plan.instruction.trim().is_empty()
        || plan.instruction.len() > super::MAX_INSTRUCTION_BYTES
        || plan.env_keys.iter().any(|key| !super::valid_env_key(key))
    {
        return Err(Error::new(
            "BATCH_WORKER_PLAN_INVALID",
            "worker plan does not identify one valid private batch run",
        ));
    }
    let digest = model::digest(model::canonical(value)?.as_bytes());
    if digest.is_empty() {
        return Err(Error::new(
            "BATCH_WORKER_PLAN_INVALID",
            "worker plan digest is empty",
        ));
    }
    Ok(())
}

fn wait_for_gate(plan: &BatchWorkerPlan, identity: &Value) -> Result<GateDecision> {
    let go_path = plan.output_dir.join(GO_FILE);
    let deny_path = plan.output_dir.join(DENY_FILE);
    let deadline = Instant::now() + Duration::from_secs(WORKER_START_GATE_SECONDS);
    loop {
        let go_exists = go_path.try_exists()?;
        let deny_exists = deny_path.try_exists()?;
        if go_exists && deny_exists {
            return Err(Error::conflict("both worker go and deny gates exist"));
        }
        if go_exists || deny_exists {
            let gate_path = if go_exists { &go_path } else { &deny_path };
            let value = read_json_bounded(gate_path, MAX_CONTROL_BYTES)?;
            let gate: WorkerGate = serde_json::from_value(value).map_err(|_| {
                Error::new("BATCH_WORKER_GATE_INVALID", "worker gate schema is invalid")
            })?;
            if gate.version != 1
                || gate.operation_id != plan.operation_id
                || gate.run_id != plan.run_id
                || gate.plan_sha256
                    != model::digest(
                        model::canonical(&serde_json::to_value(plan).map_err(|error| {
                            Error::new("BATCH_WORKER_PLAN_INVALID", error.to_string())
                        })?)?
                        .as_bytes(),
                    )
                || gate.worker_pid != std::process::id()
                || &gate.worker_identity != identity
                || (go_exists && gate.decision != "go")
                || (deny_exists && gate.decision != "deny")
            {
                return Err(Error::conflict(
                    "worker gate does not bind this exact run and owner",
                ));
            }
            return Ok(if go_exists {
                GateDecision::Go
            } else {
                GateDecision::Deny
            });
        }
        if Instant::now() >= deadline {
            return Ok(GateDecision::TimedOut);
        }
        std::thread::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

fn observe_native(
    plan: &BatchWorkerPlan,
    group: &swarm_process::Group,
    child: &mut std::process::Child,
    stdout_sync: File,
    stderr_sync: File,
    evidence: &mut WorkerEvidence,
) -> Result<()> {
    let execution_deadline = Instant::now()
        + Duration::from_secs(plan.timeout_seconds.saturating_add(HOST_GRACE_SECONDS));
    let mut family_drain_started = None;
    let mut termination_started = None;
    let mut last_termination_request = None;
    let mut termination_attempts = 0u8;
    let mut direct_status: Option<ExitStatus> = None;
    let mut direct_observation_failed = false;
    let mut cancel_observed = false;
    let mut containment_lost = false;
    let mut containment_loss_started = None;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if direct_status.is_none() {
                    direct_status = Some(status);
                    record_exit(status, evidence);
                }
            }
            Ok(None) => {}
            Err(error) => {
                direct_observation_failed = true;
                evidence.direct_exit = "observation_unknown".to_owned();
                evidence.direct_exit_error = Some(error.to_string());
            }
        }

        let family_empty = match group.children_empty() {
            Ok(true) => {
                if !containment_lost {
                    evidence.family_departure = "confirmed".to_owned();
                    evidence.family_error = None;
                }
                true
            }
            Ok(false) => {
                evidence.family_departure = "observed_active".to_owned();
                false
            }
            Err(error) => {
                evidence.family_departure = "observation_unknown".to_owned();
                evidence.family_error = Some(error.to_string());
                false
            }
        };

        if family_empty
            && direct_status.is_none()
            && !direct_observation_failed
            && !containment_lost
        {
            containment_lost = true;
            containment_loss_started = Some(Instant::now());
            evidence.family_departure = "observation_unknown".to_owned();
            evidence.family_error = Some(
                "owned Group reported empty while the exact native child remained active"
                    .to_owned(),
            );
            evidence.cleanup = "cleanup_pending".to_owned();
            evidence.failure = Some(json!({
                "code": "BATCH_NATIVE_CONTAINMENT_LOST",
                "message": "the exact native child was outside its worker Group; family departure cannot be claimed"
            }));
            match child.kill() {
                Ok(()) => {
                    evidence.termination_requests = evidence.termination_requests.saturating_add(1);
                }
                Err(error) => {
                    evidence.family_error = Some(format!(
                        "owned Group was empty with native child active; exact-child termination failed: {error}"
                    ));
                }
            }
        }

        if family_empty && direct_status.is_some() && !containment_lost {
            evidence.cleanup = if group.disarm().is_ok() {
                "complete".to_owned()
            } else {
                evidence.family_error =
                    Some("process Group disarm failed after empty observation".to_owned());
                "cleanup_pending".to_owned()
            };
            break;
        }
        if family_empty && direct_observation_failed && !containment_lost {
            evidence.cleanup = if group.disarm().is_ok() {
                "family_empty_direct_exit_unknown".to_owned()
            } else {
                "cleanup_pending".to_owned()
            };
            break;
        }

        let cancel_path = plan.output_dir.join(CANCEL_FILE);
        if !cancel_observed {
            match cancel_path.try_exists() {
                Ok(true) => {
                    cancel_observed = true;
                    match read_json_bounded(&cancel_path, MAX_CONTROL_BYTES) {
                        Ok(cancel)
                            if cancel["version"] == 1
                                && cancel["decision"] == "cancel"
                                && cancel["operation_id"] == plan.operation_id
                                && cancel["run_id"] == plan.run_id
                                && cancel["plan_sha256"] == evidence.plan_sha256
                                && cancel["worker_pid"] == evidence.worker_pid
                                && cancel["worker_identity"] == evidence.worker_identity =>
                        {
                            evidence.parent_termination_requested = true;
                            start_group_cancellation(
                                group,
                                evidence,
                                &mut termination_started,
                                &mut last_termination_request,
                                &mut termination_attempts,
                            );
                        }
                        Ok(_) => {
                            evidence.failure = Some(json!({
                                "code": "BATCH_WORKER_CANCEL_INVALID",
                                "message": "parent cancellation does not bind this worker"
                            }));
                        }
                        Err(error) => {
                            evidence.failure = Some(json!({
                                "code": "BATCH_WORKER_CANCEL_UNREADABLE",
                                "message": error.to_string()
                            }));
                        }
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    cancel_observed = true;
                    evidence.failure = Some(json!({
                        "code": "BATCH_WORKER_CANCEL_UNREADABLE",
                        "message": error.to_string()
                    }));
                }
            }
        }

        if Instant::now() >= execution_deadline && !evidence.host_termination_requested {
            evidence.host_termination_requested = true;
            start_group_cancellation(
                group,
                evidence,
                &mut termination_started,
                &mut last_termination_request,
                &mut termination_attempts,
            );
        } else if direct_status.is_some() && !family_empty {
            let started = family_drain_started.get_or_insert_with(Instant::now);
            if started.elapsed() >= Duration::from_secs(CLEANUP_GRACE_SECONDS)
                && termination_started.is_none()
            {
                evidence.family_termination_requested = true;
                start_group_cancellation(
                    group,
                    evidence,
                    &mut termination_started,
                    &mut last_termination_request,
                    &mut termination_attempts,
                );
            }
        }

        if termination_started.is_some()
            && termination_started.is_some_and(|started| {
                started.elapsed() >= Duration::from_secs(CLEANUP_GRACE_SECONDS)
            })
        {
            evidence.cleanup = "cleanup_pending".to_owned();
            break;
        }
        if containment_loss_started
            .is_some_and(|started| started.elapsed() >= Duration::from_secs(CLEANUP_GRACE_SECONDS))
        {
            break;
        }
        if last_termination_request.is_some_and(|last| last.elapsed() >= WORKER_TERMINATION_RETRY)
            && termination_attempts < WORKER_MAX_TERMINATION_ATTEMPTS
        {
            start_group_cancellation(
                group,
                evidence,
                &mut termination_started,
                &mut last_termination_request,
                &mut termination_attempts,
            );
        }
        persist_state(&plan.output_dir, evidence)?;
        std::thread::sleep(POLL_INTERVAL);
    }

    let family_empty = group.children_empty().unwrap_or(false);
    if family_empty && evidence.direct_exit == "pending" {
        // The exact Group is empty, but Child::try_wait did not produce an exit
        // status. Preserve both facts independently for same-run readback.
        evidence.direct_exit = "observation_unknown".to_owned();
        evidence.direct_exit_error.get_or_insert_with(|| {
            "direct exit was not observed before Group departure".to_owned()
        });
    }
    if !family_empty {
        evidence.family_departure = "observed_active".to_owned();
        evidence.cleanup = "cleanup_pending".to_owned();
    }
    evidence.capture = "draining".to_owned();
    persist_state(&plan.output_dir, evidence)?;
    finish_capture(
        &plan.output_dir,
        evidence,
        Some(stdout_sync),
        Some(stderr_sync),
    )?;
    if evidence.cleanup == "not_started" {
        evidence.cleanup = if family_empty {
            "complete"
        } else {
            "cleanup_pending"
        }
        .to_owned();
    }
    Ok(())
}

fn record_exit(status: ExitStatus, evidence: &mut WorkerEvidence) {
    evidence.direct_exit = "observed".to_owned();
    evidence.exit_code = status.code();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        evidence.signal = status.signal();
    }
    evidence.direct_exit_error = None;
}

fn start_group_cancellation(
    group: &swarm_process::Group,
    evidence: &mut WorkerEvidence,
    started: &mut Option<Instant>,
    last_request: &mut Option<Instant>,
    attempts: &mut u8,
) {
    started.get_or_insert_with(Instant::now);
    if *attempts >= WORKER_MAX_TERMINATION_ATTEMPTS {
        return;
    }
    *attempts += 1;
    *last_request = Some(Instant::now());
    match group.cancel_children() {
        Ok(requested) => {
            evidence.termination_requests = evidence.termination_requests.saturating_add(requested);
        }
        Err(error) => {
            evidence.family_error = Some(format!("{}: {}", error.code, error.message));
        }
    }
}

fn open_capture(path: &Path) -> Result<(File, File)> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "BATCH_CAPTURE_INVALID",
            "capture target is not a regular file",
        ));
    }
    let writer = OpenOptions::new().append(true).open(path)?;
    let sync = writer.try_clone()?;
    Ok((writer, sync))
}

fn finish_capture(
    output_dir: &Path,
    evidence: &mut WorkerEvidence,
    stdout_sync: Option<File>,
    stderr_sync: Option<File>,
) -> Result<()> {
    let family_complete =
        evidence.family_departure == "confirmed" && evidence.cleanup.starts_with("complete");
    let stdout = capture_fact(
        output_dir.join("stdout.log"),
        "stdout.log",
        stdout_sync,
        family_complete,
    );
    let stderr = capture_fact(
        output_dir.join("stderr.log"),
        "stderr.log",
        stderr_sync,
        family_complete,
    );
    let errors = [&stdout, &stderr]
        .into_iter()
        .filter_map(|fact| fact.error.as_deref())
        .collect::<Vec<_>>();
    evidence.capture = if family_complete && stdout.complete && stderr.complete {
        "complete".to_owned()
    } else {
        "incomplete".to_owned()
    };
    evidence.capture_error = if errors.is_empty() {
        None
    } else {
        Some(errors.join(";"))
    };
    evidence.stdout = Some(stdout);
    evidence.stderr = Some(stderr);
    persist_state(output_dir, evidence)
}

fn capture_fact(
    path: PathBuf,
    name: &str,
    sync_file: Option<File>,
    family_complete: bool,
) -> CaptureFact {
    let mut fact = CaptureFact {
        file: name.to_owned(),
        bytes_total: None,
        bytes_hashed: 0,
        sha256: None,
        truncated: false,
        complete: false,
        error: None,
    };
    if let Some(file) = sync_file
        && file.sync_all().is_err()
    {
        fact.error = Some("capture_sync_failed".to_owned());
    }
    let metadata_before = match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => metadata,
        Ok(_) => {
            fact.error
                .get_or_insert_with(|| "capture_target_invalid".to_owned());
            return fact;
        }
        Err(error) => {
            fact.error
                .get_or_insert_with(|| format!("capture_stat_failed:{error}"));
            return fact;
        }
    };
    fact.bytes_total = Some(metadata_before.len());
    fact.truncated = metadata_before.len() > MAX_CAPTURE_BYTES;
    let mut bytes = Vec::with_capacity(metadata_before.len().min(MAX_CAPTURE_BYTES) as usize);
    match File::open(&path)
        .and_then(|file| file.take(MAX_CAPTURE_BYTES + 1).read_to_end(&mut bytes))
    {
        Ok(_) => {
            if bytes.len() as u64 > MAX_CAPTURE_BYTES {
                bytes.truncate(MAX_CAPTURE_BYTES as usize);
                fact.truncated = true;
            }
            fact.bytes_hashed = bytes.len() as u64;
            fact.sha256 = Some(model::digest(&bytes));
        }
        Err(error) => {
            fact.error
                .get_or_insert_with(|| format!("capture_read_failed:{error}"));
        }
    }
    match fs::symlink_metadata(&path) {
        Ok(after) if after.len() == metadata_before.len() => {}
        Ok(_) => {
            fact.error
                .get_or_insert_with(|| "capture_changed_during_read".to_owned());
        }
        Err(error) => {
            fact.error
                .get_or_insert_with(|| format!("capture_restat_failed:{error}"));
        }
    }
    if fact.truncated {
        fact.error
            .get_or_insert_with(|| "capture_hash_limit_reached".to_owned());
    }
    fact.complete =
        family_complete && fact.error.is_none() && fact.bytes_total == Some(fact.bytes_hashed);
    fact
}

fn close_empty_group(group: &swarm_process::Group, evidence: &mut WorkerEvidence) -> String {
    match group.children_empty() {
        Ok(true) => {
            evidence.family_departure = "confirmed".to_owned();
            evidence.family_error = None;
            if group.disarm().is_ok() {
                "complete".to_owned()
            } else {
                evidence.family_error = Some("process Group disarm failed".to_owned());
                "cleanup_pending".to_owned()
            }
        }
        Ok(false) => {
            evidence.family_departure = "observed_active".to_owned();
            "cleanup_pending".to_owned()
        }
        Err(error) => {
            evidence.family_departure = "observation_unknown".to_owned();
            evidence.family_error = Some(format!("{}: {}", error.code, error.message));
            "cleanup_pending".to_owned()
        }
    }
}

fn persist_state(output_dir: &Path, evidence: &WorkerEvidence) -> Result<()> {
    let value = serde_json::to_value(evidence)
        .map_err(|error| Error::new("BATCH_WORKER_STATE_INVALID", error.to_string()))?;
    let bytes = model::canonical(&value)?;
    replace_json_durable(&output_dir.join(STATE_FILE), &value)?;
    let _ = bytes;
    Ok(())
}

fn persist_result(output_dir: &Path, evidence: &WorkerEvidence) -> Result<()> {
    let value = serde_json::to_value(evidence)
        .map_err(|error| Error::new("BATCH_WORKER_RESULT_INVALID", error.to_string()))?;
    write_json_new(&output_dir.join(RESULT_FILE), &value)
}
