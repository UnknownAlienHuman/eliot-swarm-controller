//! Standalone process boundary for one already-admitted CheckRun.
//!
//! The root host writes a private bootstrap before launch, persists the exact
//! process owner through its existing CheckRun `ready` path, writes `go.json`,
//! and only then materializes the resolved plan. This executable has no Store,
//! IPC client, Manager credential, profile selector, or retry loop.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use swarm_checks::{
    CheckControl, CheckExecution, CheckIdentity, OwnedCheckProcess, ResolvedCheckPlan,
    StartDecision, Termination,
};
use swarm_contracts::{Error, Result};

const BOOTSTRAP_FILE: &str = "worker-bootstrap.json";
const PLAN_FILE: &str = "execution-plan.json";
const PLAN_READY_FILE: &str = "plan-ready.json";
const PLAN_FAILURE_FILE: &str = "plan-failure.json";
const EXECUTOR_FAILURE_FILE: &str = "executor-failure.json";
const EXECUTION_FILE: &str = "execution.json";
const MAX_CONTROL_BYTES: u64 = 8 * 1024 * 1024;
const MAX_PLAN_BYTES: u64 = 4 * 1024 * 1024;
const CONTROL_POLL: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct Bootstrap {
    identity: CheckIdentity,
    job_dir: PathBuf,
}

#[derive(Clone)]
struct PlanContext {
    candidate_ref: String,
    candidate_content_sha256: String,
    input_fingerprint: Option<String>,
    executable_path: String,
    executable_sha256: String,
    profile_identity_sha256: String,
    scope_plan_sha256: String,
}

#[derive(Clone)]
struct LoadedPlan {
    plan: ResolvedCheckPlan,
    plan_sha256: String,
    context: PlanContext,
}

struct FileCheckControl {
    bootstrap: Bootstrap,
    owner_process: Option<Value>,
    started_at_ms: Option<i64>,
    group_diagnostic_write_failed: bool,
    output_diagnostic_write_failed: bool,
    group_diagnostics: BTreeMap<&'static str, Value>,
    output_diagnostic: Option<Value>,
}

impl FileCheckControl {
    fn new(bootstrap: Bootstrap) -> Self {
        Self {
            bootstrap,
            owner_process: None,
            started_at_ms: None,
            group_diagnostic_write_failed: false,
            output_diagnostic_write_failed: false,
            group_diagnostics: BTreeMap::new(),
            output_diagnostic: None,
        }
    }

    fn validate_owner(&self, owner: &OwnedCheckProcess) -> Result<()> {
        let identity = &self.bootstrap.identity;
        if owner.check_id != identity.check_id
            || owner.operation_id != identity.operation_id
            || owner.token != identity.token
        {
            return Err(Error::conflict(
                "check process owner differs from the bootstrap identity",
            ));
        }
        if self
            .owner_process
            .as_ref()
            .is_some_and(|process| process != &owner.process)
        {
            return Err(Error::conflict(
                "check process identity changed after publication",
            ));
        }
        Ok(())
    }

    fn cancellation_requested(&mut self, owner: &OwnedCheckProcess) -> Result<bool> {
        self.validate_owner(owner)?;
        let path = self.bootstrap.job_dir.join("cancel.json");
        if !path.try_exists()? {
            return Ok(false);
        }
        let value = read_value(&path, MAX_CONTROL_BYTES)?;
        let request = &value["request"];
        if value["check_id"] != owner.check_id
            || value["token"] != owner.token
            || request["operation_id"] != owner.operation_id
            || request["reason"].as_str().is_none_or(str::is_empty)
        {
            return Err(Error::conflict(
                "cancellation receipt targets another CheckRun",
            ));
        }
        Ok(true)
    }

    fn diagnostic_event(
        &self,
        owner: &OwnedCheckProcess,
        code: &str,
        elapsed: Duration,
    ) -> Result<Value> {
        Ok(json!({
            "version": 1,
            "check_id": owner.check_id,
            "operation_id": owner.operation_id,
            "token": owner.token,
            "process": owner.process,
            "code": code,
            "elapsed_ms": elapsed.as_millis().min(u64::MAX as u128) as u64,
            "observed_at_ms": now_ms()?
        }))
    }
}

impl CheckControl for FileCheckControl {
    fn wait_for_start(&mut self, owner: &OwnedCheckProcess) -> Result<StartDecision> {
        if owner.check_id != self.bootstrap.identity.check_id
            || owner.operation_id != self.bootstrap.identity.operation_id
            || owner.token != self.bootstrap.identity.token
        {
            return Err(Error::conflict(
                "check process owner differs from the bootstrap identity",
            ));
        }
        let identity = json!({
            "token": owner.token,
            "process": owner.process,
            "ready_at_ms": now_ms()?,
            "control_version": 2
        });
        write_once(&self.bootstrap.job_dir.join("worker.json"), &identity)?;
        self.owner_process = Some(owner.process.clone());

        loop {
            if self.cancellation_requested(owner)? {
                return Ok(StartDecision::CancelBeforeStart);
            }

            let go_path = self.bootstrap.job_dir.join("go.json");
            if go_path.try_exists()? {
                let go = read_value(&go_path, MAX_CONTROL_BYTES)?;
                if go["token"] != owner.token {
                    return Err(Error::conflict("check start token mismatch"));
                }

                let failed = self.bootstrap.job_dir.join(PLAN_FAILURE_FILE);
                if failed.try_exists()? {
                    let failure = read_value(&failed, MAX_CONTROL_BYTES)?;
                    if failure["version"] != 1
                        || failure["check_id"] != owner.check_id
                        || failure["operation_id"] != owner.operation_id
                        || failure["token"] != owner.token
                        || failure["process"] != owner.process
                    {
                        return Err(Error::conflict(
                            "check plan failure receipt targets another owner",
                        ));
                    }
                    return Err(Error::new(
                        "CHECK_PLAN_MATERIALIZATION_FAILED",
                        "the host could not materialize the admitted check plan",
                    ));
                }

                let ready_path = self.bootstrap.job_dir.join(PLAN_READY_FILE);
                let plan_path = self.bootstrap.job_dir.join(PLAN_FILE);
                if ready_path.try_exists()? && plan_path.try_exists()? {
                    self.started_at_ms = Some(now_ms()?);
                    return Ok(StartDecision::Start);
                }
            }
            thread::sleep(CONTROL_POLL);
        }
    }

    fn cancellation_requested(&mut self, owner: &OwnedCheckProcess) -> Result<bool> {
        FileCheckControl::cancellation_requested(self, owner)
    }

    fn process_group_drain_pending(
        &mut self,
        owner: &OwnedCheckProcess,
        elapsed: Duration,
        observation_error: bool,
        control_read_unknown: bool,
    ) -> Result<()> {
        self.validate_owner(owner)?;
        let mut result = Ok(());
        for (selected, filename, code) in [
            (
                observation_error,
                "observation.json",
                "CHECK_PROCESS_OBSERVATION_UNKNOWN",
            ),
            (
                control_read_unknown,
                "control.json",
                "CHECK_CONTROL_READ_UNKNOWN",
            ),
            (
                !observation_error && !control_read_unknown,
                "drain.json",
                "CHECK_PROCESS_DRAIN_PENDING",
            ),
        ] {
            if !selected {
                continue;
            }
            let event = if let Some(event) = self.group_diagnostics.get(filename) {
                event.clone()
            } else {
                let event = self.diagnostic_event(owner, code, elapsed)?;
                self.group_diagnostics.insert(filename, event.clone());
                event
            };
            if let Err(error) = write_once(&self.bootstrap.job_dir.join(filename), &event) {
                result = Err(error);
            }
        }
        self.group_diagnostic_write_failed = result.is_err();
        result
    }

    fn output_capture_drain_pending(
        &mut self,
        owner: &OwnedCheckProcess,
        elapsed: Duration,
        stdout_pending: bool,
        stderr_pending: bool,
    ) -> Result<()> {
        self.validate_owner(owner)?;
        if !stdout_pending && !stderr_pending {
            return Err(Error::invalid(
                "output capture diagnostic has no unfinished reader",
            ));
        }
        let event = if let Some(event) = &self.output_diagnostic {
            event.clone()
        } else {
            let mut event = self.diagnostic_event(owner, "CHECK_OUTPUT_DRAIN_PENDING", elapsed)?;
            event["stdout_pending"] = json!(stdout_pending);
            event["stderr_pending"] = json!(stderr_pending);
            self.output_diagnostic = Some(event.clone());
            event
        };
        let result = write_once(&self.bootstrap.job_dir.join("output-capture.json"), &event);
        self.output_diagnostic_write_failed = result.is_err();
        result
    }

    fn cancellation_requested_after_group_empty(
        &mut self,
        owner: &OwnedCheckProcess,
    ) -> Result<bool> {
        self.cancellation_requested(owner)
    }
}

fn execute(control_file: &Path) -> Result<()> {
    let bootstrap = load_bootstrap(control_file)?;
    let lock = open_worker_lock(&bootstrap.job_dir.join("worker.lock"))?;
    lock.try_lock().map_err(|_| {
        Error::new(
            "CHECK_WORKER_EXISTS",
            "this CheckRun already has an owning executor",
        )
    })?;
    if bootstrap.job_dir.join("worker.json").try_exists()?
        || bootstrap.job_dir.join(EXECUTION_FILE).try_exists()?
        || bootstrap.job_dir.join("completion.json").try_exists()?
    {
        return Err(Error::new(
            "CHECK_RECOVERY_REQUIRED",
            "a prior executor published state; the command will not be replayed",
        ));
    }

    let plan_context: Arc<Mutex<Option<LoadedPlan>>> = Arc::new(Mutex::new(None));
    let captured_context = Arc::clone(&plan_context);
    let identity = bootstrap.identity.clone();
    let expected_identity = identity.clone();
    let job_dir = bootstrap.job_dir.clone();
    let mut control = FileCheckControl::new(bootstrap.clone());
    let execution_result = swarm_checks::execute_with_plan(
        identity.clone(),
        job_dir.clone(),
        &mut control,
        move || {
            let owner = identity_from_worker(&job_dir, &expected_identity)?;
            let loaded = load_execution_plan(&job_dir, &owner)?;
            *captured_context
                .lock()
                .map_err(|_| Error::new("CHECK_PLAN_LOCK_FAILED", "plan lock is unavailable"))? =
                Some(loaded.clone());
            Ok(loaded.plan)
        },
    );
    let execution = match execution_result {
        Ok(_execution)
            if control.group_diagnostic_write_failed || control.output_diagnostic_write_failed =>
        {
            let code = executor_failure_code(None, &control);
            if record_executor_failure(&bootstrap, &job_dir, &control, code)? {
                drop(lock);
                return Ok(());
            }
            return Err(Error::new(
                code,
                "CheckRun diagnostics could not be persisted",
            ));
        }
        Ok(execution) => execution,
        Err(error) => {
            // The root has already published an exact, host-authored plan
            // failure after Store Go. Reusing that receipt avoids creating a
            // contradictory second failure record for the same run.
            if job_dir.join(PLAN_FAILURE_FILE).try_exists()? {
                drop(lock);
                return Ok(());
            }
            let code = executor_failure_code(Some(&error.code), &control);
            if record_executor_failure(&bootstrap, &job_dir, &control, code)? {
                drop(lock);
                return Ok(());
            }
            return Err(Error::new(code, "standalone checks executor failed"));
        }
    };

    let loaded = plan_context
        .lock()
        .map_err(|_| Error::new("CHECK_PLAN_LOCK_FAILED", "plan lock is unavailable"))?
        .clone();
    let plan_sha256 = loaded.as_ref().map(|value| value.plan_sha256.as_str());
    let plan_context = loaded.as_ref().map(|value| &value.context);

    if let Some(pid) = execution.child_pid {
        if let (Some(started_at_ms), Some(loaded)) = (control.started_at_ms, loaded.as_ref()) {
            write_once(
                &job_dir.join("started.json"),
                &json!({
                    "pid": pid,
                    "program": loaded.plan.executable.to_string_lossy(),
                    "started_at_ms": started_at_ms,
                    "token": bootstrap.identity.token
                }),
            )?;
        }
    }

    let receipt = execution_json(
        &bootstrap,
        &execution,
        plan_sha256,
        plan_context,
        control.started_at_ms,
        now_ms()?,
    );
    write_once(&job_dir.join(EXECUTION_FILE), &receipt)?;
    drop(lock);
    Ok(())
}

fn load_bootstrap(control_file: &Path) -> Result<Bootstrap> {
    let value = read_value(control_file, MAX_CONTROL_BYTES)?;
    exact_fields(
        &value,
        &["version", "check_id", "operation_id", "token", "data_dir"],
        "worker bootstrap",
    )?;
    if value["version"] != 1 {
        return Err(Error::invalid("unsupported worker bootstrap version"));
    }
    let identity = CheckIdentity {
        check_id: required_string(&value, "check_id")?,
        operation_id: required_string(&value, "operation_id")?,
        token: required_string(&value, "token")?,
    };
    for id in [
        identity.check_id.as_str(),
        identity.operation_id.as_str(),
        identity.token.as_str(),
    ] {
        uuid::Uuid::parse_str(id)
            .map_err(|_| Error::invalid("invalid worker bootstrap identity"))?;
    }
    let raw_data_dir = PathBuf::from(required_string(&value, "data_dir")?);
    if !raw_data_dir.is_absolute() {
        return Err(Error::invalid("worker data root must be absolute"));
    }
    let data_dir = fs::canonicalize(&raw_data_dir)?;
    let checks_dir = data_dir.join("checks");
    let checks_metadata = fs::symlink_metadata(&checks_dir)?;
    if is_link_or_reparse(&checks_metadata) || !checks_metadata.is_dir() {
        return Err(Error::invalid(
            "CheckRun directory root is not a regular directory",
        ));
    }
    let canonical_checks = fs::canonicalize(&checks_dir)?;
    if canonical_checks.parent() != Some(data_dir.as_path()) {
        return Err(Error::invalid(
            "CheckRun root escaped the controller data directory",
        ));
    }
    let job_dir = canonical_checks.join(&identity.check_id);
    let canonical_job_dir = fs::canonicalize(&job_dir)?;
    let job_metadata = fs::symlink_metadata(&job_dir)?;
    if canonical_job_dir != job_dir
        || is_link_or_reparse(&job_metadata)
        || !job_metadata.is_dir()
        || canonical_job_dir.parent() != Some(canonical_checks.as_path())
    {
        return Err(Error::invalid(
            "worker job directory is not a direct owned path",
        ));
    }
    let expected_control = canonical_job_dir.join(BOOTSTRAP_FILE);
    let control_metadata = fs::symlink_metadata(control_file)?;
    if fs::canonicalize(control_file)? != expected_control
        || is_link_or_reparse(&control_metadata)
        || !control_metadata.is_file()
    {
        return Err(Error::invalid(
            "worker control path does not match its generated CheckRun directory",
        ));
    }
    Ok(Bootstrap {
        identity,
        job_dir: canonical_job_dir,
    })
}

fn identity_from_worker(job_dir: &Path, identity: &CheckIdentity) -> Result<OwnedCheckProcess> {
    let value = read_value(&job_dir.join("worker.json"), MAX_CONTROL_BYTES)?;
    exact_fields(
        &value,
        &["token", "process", "ready_at_ms", "control_version"],
        "worker identity",
    )?;
    if value["token"] != identity.token {
        return Err(Error::conflict("worker token differs from its bootstrap"));
    }
    if value["control_version"] != 2
        || value["ready_at_ms"].as_i64().is_none_or(|value| value < 0)
        || value["process"].is_null()
    {
        return Err(Error::invalid("worker identity receipt is invalid"));
    }
    Ok(OwnedCheckProcess {
        check_id: identity.check_id.clone(),
        operation_id: identity.operation_id.clone(),
        token: identity.token.clone(),
        process: value["process"].clone(),
    })
}

fn has_store_go(bootstrap: &Bootstrap) -> Result<bool> {
    let path = bootstrap.job_dir.join("go.json");
    if !path.try_exists()? {
        return Ok(false);
    }
    let value = read_value(&path, MAX_CONTROL_BYTES)?;
    exact_fields(&value, &["token"], "Store go receipt")?;
    if value["token"] != bootstrap.identity.token {
        return Err(Error::conflict("Store go receipt targets another CheckRun"));
    }
    Ok(true)
}

fn executor_failure_code(error_code: Option<&str>, control: &FileCheckControl) -> &'static str {
    if control.group_diagnostic_write_failed {
        "CHECK_PROCESS_DIAGNOSTIC_WRITE_FAILED"
    } else if control.output_diagnostic_write_failed {
        "CHECK_OUTPUT_DIAGNOSTIC_WRITE_FAILED"
    } else if matches!(
        error_code,
        Some("CHECK_PLAN_MATERIALIZATION_FAILED" | "CHECK_PLAN_CLEANUP_FAILED")
    ) {
        "CHECK_PLAN_MATERIALIZATION_FAILED"
    } else {
        "CHECK_EXECUTOR_FAILED"
    }
}

fn record_executor_failure(
    bootstrap: &Bootstrap,
    job_dir: &Path,
    control: &FileCheckControl,
    code: &str,
) -> Result<bool> {
    let Some(process) = control.owner_process.as_ref() else {
        return Ok(false);
    };
    if !has_store_go(bootstrap)? {
        return Ok(false);
    }
    let failure = json!({
        "version": 1,
        "check_id": bootstrap.identity.check_id,
        "operation_id": bootstrap.identity.operation_id,
        "token": bootstrap.identity.token,
        "process": process,
        "code": code
    });
    write_once(&job_dir.join(EXECUTOR_FAILURE_FILE), &failure)?;
    Ok(true)
}

fn load_execution_plan(job_dir: &Path, owner: &OwnedCheckProcess) -> Result<LoadedPlan> {
    let receipt = read_value(&job_dir.join(PLAN_READY_FILE), MAX_CONTROL_BYTES)?;
    exact_fields(
        &receipt,
        &[
            "version",
            "check_id",
            "operation_id",
            "token",
            "process",
            "plan_sha256",
            "context",
            "materialized_at_ms",
        ],
        "plan-ready receipt",
    )?;
    if receipt["version"] != 1
        || receipt["check_id"] != owner.check_id
        || receipt["operation_id"] != owner.operation_id
        || receipt["token"] != owner.token
        || receipt["process"] != owner.process
    {
        return Err(Error::conflict(
            "resolved plan receipt differs from the acknowledged CheckRun owner",
        ));
    }
    let plan_path = job_dir.join(PLAN_FILE);
    let plan_bytes = read_bytes(&plan_path, MAX_PLAN_BYTES)?;
    let plan_sha256 = sha256(&plan_bytes);
    if receipt["plan_sha256"].as_str() != Some(plan_sha256.as_str()) {
        return Err(Error::conflict("resolved plan digest mismatch"));
    }
    validate_sha256(&plan_sha256)?;
    let value: Value = serde_json::from_slice(&plan_bytes)?;
    exact_fields(
        &value,
        &["version", "identity", "process", "context", "plan"],
        "resolved plan envelope",
    )?;
    exact_fields(
        &value["identity"],
        &["check_id", "operation_id", "token"],
        "resolved plan identity",
    )?;
    if value["version"] != 1
        || value["identity"]["check_id"] != owner.check_id
        || value["identity"]["operation_id"] != owner.operation_id
        || value["identity"]["token"] != owner.token
        || value["process"] != owner.process
    {
        return Err(Error::conflict(
            "resolved plan identity differs from the CheckRun owner",
        ));
    }
    if value["context"] != receipt["context"] {
        return Err(Error::conflict(
            "resolved plan context differs from its receipt",
        ));
    }
    if receipt["materialized_at_ms"]
        .as_i64()
        .is_none_or(|value| value < 0)
    {
        return Err(Error::invalid("plan-ready timestamp is invalid"));
    }
    let context = parse_context(&value["context"])?;
    let plan = parse_plan(&value["plan"], owner)?;
    if plan.executable.to_string_lossy() != context.executable_path
        || sha256_file(&plan.executable)? != context.executable_sha256
    {
        return Err(Error::conflict(
            "resolved executable differs from its digest-bound plan context",
        ));
    }
    if fs::canonicalize(&plan.output_directory)? != job_dir {
        return Err(Error::conflict(
            "resolved output directory differs from the generated CheckRun directory",
        ));
    }
    fs::remove_file(plan_path).map_err(|_| {
        Error::new(
            "CHECK_PLAN_CLEANUP_FAILED",
            "the one-use resolved plan could not be removed before native execution",
        )
    })?;
    Ok(LoadedPlan {
        plan,
        plan_sha256,
        context,
    })
}

fn parse_context(value: &Value) -> Result<PlanContext> {
    exact_fields(
        value,
        &[
            "candidate_ref",
            "candidate_content_sha256",
            "input_fingerprint",
            "executable_path",
            "executable_sha256",
            "profile_identity_sha256",
            "scope_plan_sha256",
        ],
        "resolved plan context",
    )?;
    let context = PlanContext {
        candidate_ref: required_string(value, "candidate_ref")?,
        candidate_content_sha256: required_string(value, "candidate_content_sha256")?,
        input_fingerprint: optional_string(value, "input_fingerprint")?,
        executable_path: required_string(value, "executable_path")?,
        executable_sha256: required_string(value, "executable_sha256")?,
        profile_identity_sha256: required_string(value, "profile_identity_sha256")?,
        scope_plan_sha256: required_string(value, "scope_plan_sha256")?,
    };
    for digest in [
        context.candidate_content_sha256.as_str(),
        context.executable_sha256.as_str(),
        context.profile_identity_sha256.as_str(),
        context.scope_plan_sha256.as_str(),
    ] {
        validate_sha256(digest)?;
    }
    if let Some(digest) = &context.input_fingerprint {
        validate_sha256(digest)?;
    }
    if !PathBuf::from(&context.executable_path).is_absolute() {
        return Err(Error::invalid("resolved executable path is not absolute"));
    }
    Ok(context)
}

fn parse_plan(value: &Value, owner: &OwnedCheckProcess) -> Result<ResolvedCheckPlan> {
    exact_fields(
        value,
        &[
            "executable",
            "argv",
            "working_directory",
            "environment",
            "output_directory",
            "output_limit_bytes_per_stream",
            "timeout_ms",
        ],
        "resolved check plan",
    )?;
    let argv: Vec<String> = serde_json::from_value(value["argv"].clone())
        .map_err(|_| Error::invalid("resolved argv is invalid"))?;
    if argv.len() > 4096 || argv.iter().any(|arg| arg.len() > 1_048_576) {
        return Err(Error::invalid("resolved argv exceeds its bounds"));
    }
    let environment: BTreeMap<String, String> =
        serde_json::from_value(value["environment"].clone())
            .map_err(|_| Error::invalid("resolved environment is invalid"))?;
    let environment_bytes = environment
        .iter()
        .map(|(key, value)| key.len().saturating_add(value.len()))
        .sum::<usize>();
    if environment.len() > 4096 || environment_bytes > 2 * 1024 * 1024 {
        return Err(Error::invalid("resolved environment exceeds its bounds"));
    }
    let timeout = match &value["timeout_ms"] {
        Value::Null => None,
        Value::Number(number) => number
            .as_u64()
            .map(Duration::from_millis)
            .filter(|timeout| !timeout.is_zero()),
        _ => None,
    };
    if !value["timeout_ms"].is_null() && timeout.is_none() {
        return Err(Error::invalid("resolved timeout is invalid"));
    }
    let plan = ResolvedCheckPlan {
        identity: CheckIdentity {
            check_id: owner.check_id.clone(),
            operation_id: owner.operation_id.clone(),
            token: owner.token.clone(),
        },
        executable: PathBuf::from(required_string(value, "executable")?),
        argv,
        working_directory: PathBuf::from(required_string(value, "working_directory")?),
        environment,
        output_directory: PathBuf::from(required_string(value, "output_directory")?),
        output_limit_bytes_per_stream: value["output_limit_bytes_per_stream"]
            .as_u64()
            .ok_or_else(|| Error::invalid("resolved output cap is invalid"))?,
        timeout,
    };
    Ok(plan)
}

fn execution_json(
    bootstrap: &Bootstrap,
    execution: &CheckExecution,
    plan_sha256: Option<&str>,
    context: Option<&PlanContext>,
    started_at_ms: Option<i64>,
    finished_at_ms: i64,
) -> Value {
    json!({
        "version": 1,
        "check_id": bootstrap.identity.check_id,
        "operation_id": bootstrap.identity.operation_id,
        "token": bootstrap.identity.token,
        "process": execution.process,
        "plan_sha256": plan_sha256,
        "context": context.map(|context| json!({
            "candidate_ref": context.candidate_ref,
            "candidate_content_sha256": context.candidate_content_sha256,
            "input_fingerprint": context.input_fingerprint,
            "executable_path": context.executable_path,
            "executable_sha256": context.executable_sha256,
            "profile_identity_sha256": context.profile_identity_sha256,
            "scope_plan_sha256": context.scope_plan_sha256
        })),
        "started_at_ms": started_at_ms,
        "finished_at_ms": finished_at_ms,
        "execution": {
            "check_id": execution.check_id,
            "operation_id": execution.operation_id,
            "process": execution.process,
            "child_pid": execution.child_pid,
            "termination": termination_name(execution.termination),
            "exit_code": execution.exit_code,
            "termination_requests": execution.termination_requests,
            "stdout": stream_json(&execution.stdout),
            "stderr": stream_json(&execution.stderr),
            "resource_released": execution.resource_released,
            "control_read_unknown": execution.control_read_unknown,
            "termination_request_unconfirmed": execution.termination_request_unconfirmed
        }
    })
}

fn stream_json(stream: &swarm_checks::CapturedStream) -> Value {
    json!({
        "path": stream.path.to_string_lossy(),
        "bytes_written": stream.bytes_written,
        "truncated": stream.truncated,
        "capture_complete": stream.capture_complete
    })
}

fn termination_name(termination: Termination) -> &'static str {
    match termination {
        Termination::Exited => "exited",
        Termination::Cancelled => "cancelled",
        Termination::TimedOut => "timed_out",
        Termination::CancelledBeforeStart => "cancelled_before_start",
        Termination::ControlReadUnknownBeforeStart => "control_read_unknown_before_start",
        Termination::ProcessObservationUnknown => "process_observation_unknown",
    }
}

fn exact_fields(value: &Value, fields: &[&str], label: &str) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid(format!("{label} must be an object")))?;
    if object.len() != fields.len() || fields.iter().any(|field| !object.contains_key(*field)) {
        return Err(Error::invalid(format!("{label} has an invalid shape")));
    }
    Ok(())
}

fn required_string(value: &Value, field: &str) -> Result<String> {
    value[field]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::invalid(format!("{field} is missing or invalid")))
}

fn optional_string(value: &Value, field: &str) -> Result<Option<String>> {
    match &value[field] {
        Value::Null => Ok(None),
        Value::String(value) if !value.is_empty() => Ok(Some(value.clone())),
        _ => Err(Error::invalid(format!("{field} is invalid"))),
    }
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
        return Err(Error::invalid(
            "CheckRun control envelope exceeds its limit",
        ));
    }
    Ok(bytes)
}

fn write_once(path: &Path, value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if path.try_exists()? {
        if read_bytes(path, MAX_CONTROL_BYTES)? == bytes {
            return Ok(());
        }
        return Err(Error::conflict("retained CheckRun receipt differs"));
    }
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| Error::invalid("CheckRun receipt filename is invalid"))?;
    let temp = path.with_file_name(format!(".{filename}.{}.tmp", std::process::id()));
    let result = (|| -> Result<()> {
        swarm_process::write_private_new(&temp, &bytes)?;
        match fs::hard_link(&temp, path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if read_bytes(path, MAX_CONTROL_BYTES)? == bytes {
                    Ok(())
                } else {
                    Err(Error::conflict("CheckRun receipt raced different bytes"))
                }
            }
            Err(error) => Err(error.into()),
        }
    })();
    let _ = fs::remove_file(temp);
    result
}

fn sha256(bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(bytes);
    format!("{:x}", hash.finalize())
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

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::invalid("SHA-256 value is not canonical"));
    }
    Ok(())
}

fn open_worker_lock(path: &Path) -> Result<File> {
    if path.try_exists()? {
        let metadata = fs::symlink_metadata(path)?;
        if is_link_or_reparse(&metadata) || !metadata.is_file() {
            return Err(Error::invalid("CheckRun worker lock is not a regular file"));
        }
        return Ok(OpenOptions::new().read(true).write(true).open(path)?);
    }
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)?;
            if is_link_or_reparse(&metadata) || !metadata.is_file() {
                return Err(Error::invalid("CheckRun worker lock is not a regular file"));
            }
            Ok(OpenOptions::new().read(true).write(true).open(path)?)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::invalid("system clock predates the Unix epoch"))?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| Error::invalid("system clock timestamp exceeds the supported range"))
}

fn main() {
    if let Err(error) = run_cli() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run_cli() -> Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    let mut control_file = None;
    while let Some(argument) = arguments.next() {
        if argument == "--control-file" && control_file.is_none() {
            control_file = arguments.next().map(PathBuf::from);
        } else {
            return Err(Error::invalid("unsupported swarm-checks argument"));
        }
    }
    let control_file =
        control_file.ok_or_else(|| Error::invalid("missing --control-file argument"))?;
    execute(&control_file)
}
