//! One short-lived worker per check. It owns no DB or model session and executes
//! at most one configured command, after the host durably acknowledges its identity.
use super::{
    inputs,
    model::{CheckProfile, Parser},
    scope::CargoTargetIdentity,
    source,
};
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    error::{Error, Result},
    model,
    platform::process_group::{departed_empty, spawned_departed, spawned_identity},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use swarm_checks::{
    CheckControl, CheckExecution, CheckIdentity, MAX_CAPTURE_BYTES_PER_STREAM, OwnedCheckProcess,
    ResolvedCheckPlan, StartDecision, Termination,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Work {
    pub check_id: String,
    pub operation_id: String,
    pub token: String,
    pub data_dir: PathBuf,
    pub candidate: ArtifactRecord,
    pub profile: CheckProfile,
    #[serde(default)]
    pub resolved_inputs: Option<Value>,
    #[serde(default)]
    pub scope_plan: Option<Value>,
    #[serde(default)]
    pub input_fingerprint: Option<String>,
    #[serde(default)]
    pub preflight_error: Option<Value>,
    #[serde(default)]
    pub cancel_request: Option<CancelRequest>,
    #[serde(default)]
    pub expected_worker: Option<Value>,
    /// Host-recorded evidence of the spawned worker process, captured by the
    /// launcher at spawn time. Present only for workers spawned by a host that
    /// records launch receipts; the sole evidence available if the worker dies
    /// before publishing its own identity.
    #[serde(default)]
    pub launch: Option<Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelRequest {
    pub operation_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub check_id: String,
    pub operation_id: String,
    pub token: String,
    pub state: String,
    pub exit_code: Option<i32>,
    pub resource_released: bool,
    pub coverage: Value,
    pub result: ArtifactRecord,
    pub outputs: Vec<ArtifactRecord>,
    #[serde(default)]
    pub cancellation: Option<Value>,
}
pub fn directory(root: &Path, id: &str) -> Result<PathBuf> {
    if uuid::Uuid::parse_str(id).is_err() {
        return Err(Error::invalid("invalid CheckRun ID"));
    }
    Ok(root.join("checks").join(id))
}
pub fn write_once(path: &Path, body: &Value) -> Result<()> {
    let bytes = model::canonical(body)?.into_bytes();
    if path.exists() {
        if fs::read(path)? == bytes {
            return Ok(());
        }
        return Err(Error::conflict("retained check control file differs"));
    }
    let temp = path.with_file_name(format!(".{}.tmp", model::new_id()));
    let result = (|| -> Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        drop(f);
        match fs::hard_link(&temp, path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read(path)? == bytes {
                    Ok(())
                } else {
                    Err(Error::conflict(
                        "check control publication raced different bytes",
                    ))
                }
            }
            Err(e) => Err(e.into()),
        }
    })();
    let _ = fs::remove_file(temp);
    result
}
fn read_value(path: &Path) -> Result<Value> {
    use std::io::Read;
    let mut bytes = Vec::new();
    File::open(path)?.take(8_388_609).read_to_end(&mut bytes)?;
    if bytes.len() > 8_388_608 {
        return Err(Error::invalid(
            "check control file exceeds metadata envelope",
        ));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn profile_report_matches(work: &Work, reported: &Value) -> Result<bool> {
    if reported == &inputs::profile_identity(&work.profile)? {
        return Ok(true);
    }
    // Accept a previously admitted pre-resolver worker during rolling upgrades.
    // Its legacy report contains the original profile; compare only in memory
    // and never copy that raw profile into a newly published report.
    let Ok(previous) = serde_json::from_value::<CheckProfile>(reported.clone()) else {
        return Ok(false);
    };
    Ok(model::canonical(&serde_json::to_value(previous)?)?
        == model::canonical(&serde_json::to_value(&work.profile)?)?)
}
pub fn prepare_and_spawn(work: &Work) -> Result<Value> {
    let dir = directory(&work.data_dir, &work.check_id)?;
    fs::create_dir_all(
        dir.parent()
            .ok_or_else(|| Error::invalid("missing check parent"))?,
    )?;
    if let Err(e) = fs::create_dir(&dir) {
        return Err(if e.kind() == std::io::ErrorKind::AlreadyExists {
            Error::new(
                "CHECK_LAUNCH_UNKNOWN",
                "existing job directory requires reconciliation, not another worker",
            )
        } else {
            e.into()
        });
    }
    write_once(&dir.join("work.json"), &json!(work))?;
    let log = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join("worker.stderr"))?;
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("check-worker")
        .arg("--file")
        .arg(dir.join("work.json"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000);
    }
    let mut child = cmd.spawn()?;
    let pid = child.id();
    // Record the launch before anything else can happen to the worker. A bare
    // PID proves nothing; the platform record pins this process instance. If
    // the process already exited, keep the partial record: departure of that
    // PID/group may still be provable, absence never is.
    let process = match spawned_identity(pid) {
        Ok(v) => v,
        Err(e) if e.code == "PROCESS_GONE" => {
            json!({"pid":pid,"scope":"launcher_spawned_process","purpose":"check"})
        }
        Err(e) => {
            return Err(Error::new(
                "CHECK_LAUNCH_UNKNOWN",
                format!("worker spawned without a recordable launch identity: {e}"),
            ));
        }
    };
    let launch = json!({"spawned_at_ms":model::now_ms()?,"process":process});
    // Reap our worker on Unix without tying its life to an async task/host link.
    std::thread::Builder::new()
        .name("check-reaper".into())
        .spawn(move || {
            let _ = child.wait();
        })
        .map_err(|e| {
            Error::new(
                "CHECK_LAUNCH_UNKNOWN",
                format!("worker started; reaper unavailable: {e}"),
            )
        })?;
    Ok(launch) // Deliberately independent of a host/CLI disconnect.
}
pub fn ready(work: &Work) -> Result<Option<Value>> {
    let p = directory(&work.data_dir, &work.check_id)?.join("worker.json");
    if !p.try_exists()? {
        return Ok(None);
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(p.with_file_name("worker.lock"))?;
    match lock.try_lock() {
        Ok(()) => {
            return Err(Error::new(
                "CHECK_WORKER_LOST",
                "worker lock is free without a completion receipt; resource is retained",
            ));
        }
        Err(std::fs::TryLockError::WouldBlock) => {}
        Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
    }
    let v = read_value(&p)?;
    if v["token"] != work.token {
        return Err(Error::conflict("worker identity token mismatch"));
    }
    Ok(Some(v))
}
pub fn allow(work: &Work) -> Result<()> {
    write_once(
        &directory(&work.data_dir, &work.check_id)?.join("go.json"),
        &json!({"token":work.token}),
    )
}
/// Delivery is idempotent and never creates another worker or a missing job directory.
pub fn deliver_cancel(work: &Work) -> Result<()> {
    let Some(request) = &work.cancel_request else {
        return Ok(());
    };
    let dir = directory(&work.data_dir, &work.check_id)?;
    let identity = dir.join("worker.json");
    if !identity.try_exists()? {
        return Ok(());
    }
    let v = read_value(&identity)?;
    if v["token"] != work.token {
        return Err(Error::conflict("cancellation worker token mismatch"));
    }
    if v["control_version"] != 2 {
        return Err(Error::new(
            "CHECK_CANCEL_UNSUPPORTED",
            "this already running worker predates active cancellation",
        ));
    }
    write_once(
        &dir.join("cancel.json"),
        &json!({"check_id":work.check_id,"token":work.token,"request":request}),
    )
}

/// Read and validate the executor's one-shot process-group diagnostic. The
/// token and full process identity stay in the private Store update envelope;
/// only the fixed, sanitized `public` projection reaches `check.get`.
pub fn process_diagnostic(work: &Work) -> Result<Option<Value>> {
    let Some(expected_worker) = &work.expected_worker else {
        // The Store has not durably accepted this process owner yet.
        return Ok(None);
    };
    let dir = directory(&work.data_dir, &work.check_id)?;
    let identity = read_value(&dir.join("worker.json"))?;
    if &identity != expected_worker
        || identity["token"] != work.token
        || identity["control_version"] != 2
    {
        return Err(Error::conflict(
            "process diagnostic worker differs from the accepted CheckRun owner",
        ));
    }
    let mut selected: Option<(u8, &'static str, &'static str, u64, i64)> = None;
    let mut control_read_unknown = false;
    for (filename, expected_code, priority, cause) in [
        (
            "drain.json",
            "CHECK_PROCESS_DRAIN_PENDING",
            1,
            "drain_pending",
        ),
        (
            "control.json",
            "CHECK_CONTROL_READ_UNKNOWN",
            2,
            "control_read_error",
        ),
        (
            "observation.json",
            "CHECK_PROCESS_OBSERVATION_UNKNOWN",
            3,
            "observation_error",
        ),
    ] {
        let path = dir.join(filename);
        if !path.try_exists()? {
            continue;
        }
        let diagnostic = read_value(&path)?;
        if diagnostic["version"] != 1
            || diagnostic["check_id"] != work.check_id
            || diagnostic["operation_id"] != work.operation_id
            || diagnostic["token"] != work.token
            || diagnostic["process"] != identity["process"]
            || diagnostic["code"] != expected_code
        {
            return Err(Error::conflict(
                "process diagnostic differs from the accepted CheckRun owner",
            ));
        }
        if filename == "control.json" {
            control_read_unknown = true;
        }
        let elapsed_ms = diagnostic["elapsed_ms"]
            .as_u64()
            .ok_or_else(|| Error::invalid("process diagnostic elapsed time is invalid"))?;
        let observed_at_ms = diagnostic["observed_at_ms"]
            .as_i64()
            .filter(|value| *value >= 0)
            .ok_or_else(|| Error::invalid("process diagnostic timestamp is invalid"))?;
        if selected
            .as_ref()
            .is_none_or(|(existing, ..)| priority > *existing)
        {
            selected = Some((priority, expected_code, cause, elapsed_ms, observed_at_ms));
        }
    }
    let mut output_capture_pending = None;
    let capture_path = dir.join("output-capture.json");
    if capture_path.try_exists()? {
        let diagnostic = read_value(&capture_path)?;
        if diagnostic["version"] != 1
            || diagnostic["check_id"] != work.check_id
            || diagnostic["operation_id"] != work.operation_id
            || diagnostic["token"] != work.token
            || diagnostic["process"] != identity["process"]
            || diagnostic["code"] != "CHECK_OUTPUT_DRAIN_PENDING"
        {
            return Err(Error::conflict(
                "output capture diagnostic differs from the accepted CheckRun owner",
            ));
        }
        let stdout_pending = diagnostic["stdout_pending"]
            .as_bool()
            .ok_or_else(|| Error::invalid("stdout capture diagnostic is invalid"))?;
        let stderr_pending = diagnostic["stderr_pending"]
            .as_bool()
            .ok_or_else(|| Error::invalid("stderr capture diagnostic is invalid"))?;
        if !stdout_pending && !stderr_pending {
            return Err(Error::invalid(
                "output capture diagnostic has no pending reader",
            ));
        }
        let elapsed_ms = diagnostic["elapsed_ms"]
            .as_u64()
            .ok_or_else(|| Error::invalid("output capture elapsed time is invalid"))?;
        let observed_at_ms = diagnostic["observed_at_ms"]
            .as_i64()
            .filter(|value| *value >= 0)
            .ok_or_else(|| Error::invalid("output capture timestamp is invalid"))?;
        output_capture_pending = Some((stdout_pending, stderr_pending));
        if selected.as_ref().is_none_or(|(existing, ..)| 4 > *existing) {
            selected = Some((
                4,
                "CHECK_OUTPUT_DRAIN_PENDING",
                "output_capture_pending",
                elapsed_ms,
                observed_at_ms,
            ));
        }
    }
    let Some((_, code, cause, elapsed_ms, observed_at_ms)) = selected else {
        return Ok(None);
    };
    let message = match cause {
        "observation_error" => {
            "the owned process group could not be observed as empty; the resource remains held"
        }
        "control_read_error" => {
            "the CheckRun cancellation receipt could not be read while its process group remains held"
        }
        "drain_pending" => {
            "the owned process group remains active after the drain grace period; the resource remains held"
        }
        "output_capture_pending" => {
            "the owned process group is empty but output capture has not finished; the CheckRun remains unresolved"
        }
        _ => return Err(Error::invalid("process diagnostic cause is invalid")),
    };
    let mut public = json!({
        "code": code,
        "status": "unresolved",
        "cause": cause,
        "message": message,
        "control_read_unknown": control_read_unknown,
        "elapsed_ms": elapsed_ms,
        "observed_at_ms": observed_at_ms,
        "resolved_at_ms": Value::Null
    });
    if cause == "output_capture_pending" {
        let (stdout_pending, stderr_pending) = output_capture_pending
            .ok_or_else(|| Error::invalid("output capture status was not retained"))?;
        public["stdout_pending"] = json!(stdout_pending);
        public["stderr_pending"] = json!(stderr_pending);
    }
    Ok(Some(json!({
        "version": 1,
        "check_id": work.check_id,
        "operation_id": work.operation_id,
        "token": work.token,
        "process": identity["process"],
        "public": public
    })))
}

#[derive(Default)]
struct Cancellation {
    request: Option<CancelRequest>,
    signals_sent: u64,
    termination_attempted: bool,
    skipped_start: bool,
    last_error: Option<Error>,
}
impl Cancellation {
    fn read(&mut self, work: &Work, dir: &Path) -> Result<bool> {
        if self.request.is_none() && dir.join("cancel.json").try_exists()? {
            let v = read_value(&dir.join("cancel.json"))?;
            if v["token"] != work.token || v["check_id"] != work.check_id {
                return Err(Error::conflict("cancellation targets another worker"));
            }
            self.request = Some(serde_json::from_value(v["request"].clone())?);
        }
        Ok(self.request.is_some())
    }
    fn applied(&self) -> bool {
        self.skipped_start || self.termination_attempted
    }
    fn evidence(&self) -> Option<Value> {
        self.request.as_ref().map(|r| json!({"operation_id":r.operation_id,"reason":r.reason,
            "disposition":if self.skipped_start {"cancelled_before_command"} else if self.termination_attempted {"cancel_attempted_group_empty"} else {"completed_before_termination"},
            "termination_requests":self.signals_sent,"last_error":self.last_error}))
    }
}

/// Adapts the existing CheckRun identity/go/cancel files to the executor.
/// It owns no Store handle and cannot broaden the admitted command or scope.
struct HostCheckControl<'a> {
    work: &'a Work,
    dir: &'a Path,
    cancellation: Cancellation,
    identity: Option<Value>,
    cancellation_observed: bool,
    started_at_ms: Option<i64>,
    drain_diagnostic: Option<Value>,
    output_capture_diagnostic: Option<Value>,
    control_read_diagnostic: Option<Value>,
    process_observation_diagnostic: Option<Value>,
    drain_diagnostic_write_failed: bool,
    output_capture_diagnostic_write_failed: bool,
}

impl<'a> HostCheckControl<'a> {
    fn new(work: &'a Work, dir: &'a Path) -> Self {
        Self {
            work,
            dir,
            cancellation: Cancellation::default(),
            identity: None,
            cancellation_observed: false,
            started_at_ms: None,
            drain_diagnostic: None,
            output_capture_diagnostic: None,
            control_read_diagnostic: None,
            process_observation_diagnostic: None,
            drain_diagnostic_write_failed: false,
            output_capture_diagnostic_write_failed: false,
        }
    }

    fn validate_owner(&self, owner: &OwnedCheckProcess) -> Result<()> {
        if owner.check_id != self.work.check_id
            || owner.operation_id != self.work.operation_id
            || owner.token != self.work.token
        {
            return Err(Error::conflict(
                "check process owner differs from the admitted CheckRun",
            ));
        }
        Ok(())
    }

    fn record_execution(&mut self, execution: &CheckExecution) {
        if execution.termination == Termination::CancelledBeforeStart {
            self.cancellation.skipped_start = true;
        }
        if self.cancellation_observed {
            self.cancellation.termination_attempted = true;
            self.cancellation.signals_sent = self
                .cancellation
                .signals_sent
                .saturating_add(execution.termination_requests);
        }
        if execution.termination_request_unconfirmed && self.cancellation_observed {
            self.cancellation.last_error = Some(Error::new(
                "CHECK_TERMINATION_UNCONFIRMED",
                "one or more requests to stop this check process group were not confirmed",
            ));
        }
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
            "observed_at_ms": model::now_ms()?
        }))
    }
}

impl CheckControl for HostCheckControl<'_> {
    fn wait_for_start(
        &mut self,
        owner: &OwnedCheckProcess,
    ) -> swarm_contracts::Result<StartDecision> {
        self.validate_owner(owner)
            .map_err(into_swarm_checks_error)?;
        let identity = json!({
            "token": owner.token,
            "process": owner.process,
            "ready_at_ms": model::now_ms().unwrap_or(0),
            "control_version": 2
        });
        write_once(&self.dir.join("worker.json"), &identity).map_err(into_swarm_checks_error)?;
        self.identity = Some(identity);

        loop {
            if self
                .cancellation
                .read(self.work, self.dir)
                .map_err(into_swarm_checks_error)?
            {
                self.cancellation.skipped_start = true;
                return Ok(StartDecision::CancelBeforeStart);
            }
            let go = self.dir.join("go.json");
            if go.try_exists()? {
                let go_receipt = read_value(&go).map_err(into_swarm_checks_error)?;
                if go_receipt["token"] != self.work.token {
                    return Err(into_swarm_checks_error(Error::conflict(
                        "check start token mismatch",
                    )));
                }
                self.started_at_ms = Some(model::now_ms().map_err(into_swarm_checks_error)?);
                return Ok(StartDecision::Start);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn cancellation_requested(
        &mut self,
        owner: &OwnedCheckProcess,
    ) -> swarm_contracts::Result<bool> {
        self.validate_owner(owner)
            .map_err(into_swarm_checks_error)?;
        let requested = self
            .cancellation
            .read(self.work, self.dir)
            .map_err(into_swarm_checks_error)?;
        self.cancellation_observed |= requested;
        Ok(requested)
    }

    fn cancellation_requested_after_group_empty(
        &mut self,
        owner: &OwnedCheckProcess,
    ) -> swarm_contracts::Result<bool> {
        self.validate_owner(owner)
            .map_err(into_swarm_checks_error)?;
        // Retain a late request for the terminal receipt, but do not claim that
        // it caused termination after the owned Group was already empty.
        self.cancellation
            .read(self.work, self.dir)
            .map_err(into_swarm_checks_error)
    }

    fn output_capture_drain_pending(
        &mut self,
        owner: &OwnedCheckProcess,
        elapsed: Duration,
        stdout_pending: bool,
        stderr_pending: bool,
    ) -> swarm_contracts::Result<()> {
        self.validate_owner(owner)
            .map_err(into_swarm_checks_error)?;
        if !stdout_pending && !stderr_pending {
            return Err(into_swarm_checks_error(Error::invalid(
                "output capture diagnostic requires an unfinished reader",
            )));
        }
        let result = (|| -> Result<()> {
            if self.output_capture_diagnostic.is_none() {
                let mut diagnostic =
                    self.diagnostic_event(owner, "CHECK_OUTPUT_DRAIN_PENDING", elapsed)?;
                diagnostic["stdout_pending"] = json!(stdout_pending);
                diagnostic["stderr_pending"] = json!(stderr_pending);
                self.output_capture_diagnostic = Some(diagnostic);
            }
            write_once(
                &self.dir.join("output-capture.json"),
                self.output_capture_diagnostic.as_ref().ok_or_else(|| {
                    Error::invalid("output capture diagnostic was not initialized")
                })?,
            )
        })();
        self.output_capture_diagnostic_write_failed = result.is_err();
        result.map_err(into_swarm_checks_error)
    }

    fn process_group_drain_pending(
        &mut self,
        owner: &OwnedCheckProcess,
        elapsed: Duration,
        observation_error: bool,
        control_read_unknown: bool,
    ) -> swarm_contracts::Result<()> {
        self.validate_owner(owner)
            .map_err(into_swarm_checks_error)?;
        let result = (|| -> Result<()> {
            if observation_error {
                if self.process_observation_diagnostic.is_none() {
                    self.process_observation_diagnostic = Some(self.diagnostic_event(
                        owner,
                        "CHECK_PROCESS_OBSERVATION_UNKNOWN",
                        elapsed,
                    )?);
                }
                write_once(
                    &self.dir.join("observation.json"),
                    self.process_observation_diagnostic
                        .as_ref()
                        .ok_or_else(|| {
                            Error::invalid("process observation diagnostic was not initialized")
                        })?,
                )?;
            }
            if control_read_unknown {
                if self.control_read_diagnostic.is_none() {
                    self.control_read_diagnostic = Some(self.diagnostic_event(
                        owner,
                        "CHECK_CONTROL_READ_UNKNOWN",
                        elapsed,
                    )?);
                }
                write_once(
                    &self.dir.join("control.json"),
                    self.control_read_diagnostic.as_ref().ok_or_else(|| {
                        Error::invalid("control read diagnostic was not initialized")
                    })?,
                )?;
            }
            if !observation_error && !control_read_unknown {
                if self.drain_diagnostic.is_none() {
                    self.drain_diagnostic = Some(self.diagnostic_event(
                        owner,
                        "CHECK_PROCESS_DRAIN_PENDING",
                        elapsed,
                    )?);
                }
                write_once(
                    &self.dir.join("drain.json"),
                    self.drain_diagnostic
                        .as_ref()
                        .ok_or_else(|| Error::invalid("drain diagnostic was not initialized"))?,
                )?;
            }
            Ok(())
        })();
        self.drain_diagnostic_write_failed = result.is_err();
        result.map_err(into_swarm_checks_error)
    }
}

fn into_swarm_checks_error(error: Error) -> swarm_contracts::Error {
    swarm_contracts::Error {
        code: error.code,
        message: error.message,
        rejection_class: error.rejection_class,
        native_http_failure: error.native_http_failure,
    }
}

pub fn completion(work: &Work, files: &ArtifactFiles) -> Result<Option<Completion>> {
    let p = directory(&work.data_dir, &work.check_id)?.join("completion.json");
    if !p.try_exists()? {
        return Ok(None);
    }
    let c: Completion = serde_json::from_value(read_value(&p)?)?;
    validate_completion(work, files, c).map(Some)
}

/// Validate the retained coverage receipt before either Store or a worker may
/// treat it as a pass or a reusable result.
pub(crate) fn validate_passed_coverage(
    parser: &Parser,
    expected_targets: &[String],
    scope_plan: &Value,
    coverage: &Value,
) -> Result<()> {
    let invalid = || {
        Error::new(
            "CHECK_COVERAGE_INVALID",
            "passed CheckRun does not prove its parser-specific required coverage",
        )
    };
    let strings = |field: &str| -> Result<BTreeSet<String>> {
        let values = coverage[field].as_array().ok_or_else(invalid)?;
        let parsed: BTreeSet<String> = values
            .iter()
            .map(|value| value.as_str().map(str::to_owned).ok_or_else(invalid))
            .collect::<Result<_>>()?;
        if parsed.len() != values.len() {
            return Err(invalid());
        }
        Ok(parsed)
    };
    if coverage["gaps"]
        .as_array()
        .is_none_or(|gaps| !gaps.is_empty())
    {
        return Err(invalid());
    }
    let requested = strings("requested")?;
    let checked = strings("checked")?;
    match parser {
        Parser::ExitCode => {
            let process_exit = BTreeSet::from(["process_exit".to_string()]);
            if requested != process_exit || checked != process_exit {
                return Err(invalid());
            }
        }
        Parser::CargoJson => {
            let expected: BTreeSet<String> = expected_targets.iter().cloned().collect();
            let identities: BTreeMap<String, Vec<CargoTargetIdentity>> =
                serde_json::from_value(scope_plan["target_identities"].clone())
                    .map_err(|_| invalid())?;
            let mut expected_identity_keys = BTreeSet::new();
            for target in &expected {
                let Some(target_identities) = identities.get(target) else {
                    return Err(invalid());
                };
                // Profile target names are intentionally simple. Until the
                // profile schema can name package IDs directly, ambiguous names
                // cannot be accepted as complete evidence.
                if target_identities.len() != 1 {
                    return Err(invalid());
                }
                let identity = &target_identities[0];
                if identity.name != *target
                    || identity.package_name.trim().is_empty()
                    || identity.package_version.trim().is_empty()
                    || identity.manifest_path.is_empty()
                    || identity.src_path.is_empty()
                    || identity.manifest_path.starts_with('/')
                    || identity.src_path.starts_with('/')
                    || identity.manifest_path.split('/').any(|part| part == "..")
                    || identity.src_path.split('/').any(|part| part == "..")
                    || identity.kinds.is_empty()
                {
                    return Err(invalid());
                }
                expected_identity_keys.insert(target_identity_key(identity)?);
            }
            let checked_identity_values = coverage["checked_target_identities"]
                .as_array()
                .ok_or_else(invalid)?;
            let checked_identity_keys: BTreeSet<String> = checked_identity_values
                .iter()
                .map(|identity| identity.as_str().map(str::to_owned).ok_or_else(invalid))
                .collect::<Result<_>>()?;
            if expected.is_empty()
                || expected.len() != expected_targets.len()
                || requested != expected
                || expected != checked
                || checked_identity_keys.len() != checked_identity_values.len()
                || expected_identity_keys != checked_identity_keys
                || coverage["build_finished"] != true
                || coverage["errors"].as_u64() != Some(0)
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

fn target_identity_key(identity: &CargoTargetIdentity) -> Result<String> {
    model::canonical(&serde_json::to_value(identity)?)
}

fn workspace_relative_target_source(workspace: &Path, source: &str) -> Option<String> {
    let workspace = fs::canonicalize(workspace).ok()?;
    let source = fs::canonicalize(source).ok()?;
    let relative = source.strip_prefix(workspace).ok()?;
    Some(relative.to_string_lossy().replace('\\', "/"))
}

fn decode_cargo_file_url(source: &str) -> Option<PathBuf> {
    let encoded = source
        .strip_prefix("path+file://")
        .or_else(|| source.strip_prefix("file://"))?;
    if !encoded.starts_with('/') {
        // Do not accept URL authorities (for example, a UNC host) without a
        // platform-specific canonical identity implementation.
        return None;
    }
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = std::str::from_utf8(bytes.get(index + 1..index + 3)?).ok()?;
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    let path = String::from_utf8(decoded).ok()?;
    #[cfg(windows)]
    let path = {
        let mut path = path;
        if path.as_bytes().get(2) == Some(&b':') && path.starts_with('/') {
            path.remove(0);
        }
        path.replace('/', "\\")
    };
    Some(PathBuf::from(path))
}

fn same_canonical_path(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn package_id_matches_target_owner(
    package_id: &str,
    identity: &CargoTargetIdentity,
    workspace: &Path,
) -> bool {
    let Some((source, coordinates)) = package_id.rsplit_once('#') else {
        return false;
    };
    let named_coordinates = format!("{}@{}", identity.package_name, identity.package_version);
    // Cargo emits path package IDs with either the package name and version or
    // just the version (for example, `path+file:///workspace/crate#0.1.0`).
    // In the version-only form, the exact canonical package root below is the
    // owner binding; the name came from metadata for that same manifest.
    if coordinates != named_coordinates && coordinates != identity.package_version {
        return false;
    }
    let Some(package_root) = decode_cargo_file_url(source) else {
        return false;
    };
    let Some(manifest_parent) = Path::new(&identity.manifest_path).parent() else {
        return false;
    };
    let expected_root = workspace.join(manifest_parent);
    let (Ok(package_root), Ok(expected_root)) = (
        fs::canonicalize(package_root),
        fs::canonicalize(expected_root),
    ) else {
        return false;
    };
    same_canonical_path(&package_root, &expected_root)
}

fn artifact_matches_target_identity(
    package_id: &str,
    target: &Value,
    identity: &CargoTargetIdentity,
    workspace: &Path,
) -> bool {
    if !package_id_matches_target_owner(package_id, identity, workspace) {
        return false;
    }
    let Some(name) = target["name"].as_str() else {
        return false;
    };
    if name != identity.name {
        return false;
    }
    let Some(kinds) = target["kind"].as_array() else {
        return false;
    };
    let kinds: BTreeSet<String> = kinds
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    if kinds.len() != target["kind"].as_array().map_or(0, Vec::len) || kinds != identity.kinds {
        return false;
    }
    let Some(src_path) = target["src_path"].as_str() else {
        return false;
    };
    workspace_relative_target_source(workspace, src_path).as_deref()
        == Some(identity.src_path.as_str())
}

fn validate_completion(work: &Work, files: &ArtifactFiles, c: Completion) -> Result<Completion> {
    if c.token != work.token
        || c.operation_id != work.operation_id
        || c.check_id != work.check_id
        || !c.resource_released
        || !matches!(
            c.state.as_str(),
            "passed" | "failed" | "error" | "incomplete" | "cancelled"
        )
    {
        return Err(Error::conflict(
            "check completion identity/disposition mismatch",
        ));
    }
    files.verify(&c.result)?;
    for out in &c.outputs {
        files.verify(out)?;
    }
    let report: Value = serde_json::from_slice(&files.document_bytes(&c.result)?)?;
    if report["check_id"] != work.check_id
        || report["operation_id"] != work.operation_id
        || report["candidate_ref"] != work.candidate.artifact_id
        || report["state"] != c.state
        || report["coverage"] != c.coverage
        || report["exit_code"] != json!(c.exit_code)
        || !profile_report_matches(work, &report["profile"])?
        || report["resource_released"] != true
        || report["cancellation"] != json!(c.cancellation)
    {
        return Err(Error::conflict(
            "check receipt differs from its published report",
        ));
    }
    if work.input_fingerprint.is_some()
        && (report["input_fingerprint"] != json!(work.input_fingerprint)
            || report["resolved_inputs"] != json!(work.resolved_inputs)
            || report["scope_plan"] != json!(work.scope_plan))
    {
        return Err(Error::conflict(
            "check report input plan differs from the admitted CheckRun",
        ));
    }
    if c.state == "passed" && (c.exit_code != Some(0) || report["source_checkout_verified"] != true)
    {
        return Err(Error::conflict(
            "incomplete execution cannot be a passed CheckRun",
        ));
    }
    if c.state == "passed" {
        let expected_targets = match work.resolved_inputs.as_ref() {
            Some(inputs) => serde_json::from_value(inputs["expected_targets"].clone())?,
            None => work.profile.expected_targets.clone(),
        };
        let empty_scope_plan = Value::Null;
        validate_passed_coverage(
            &work.profile.parser,
            &expected_targets,
            work.scope_plan.as_ref().unwrap_or(&empty_scope_plan),
            &c.coverage,
        )?;
    }
    Ok(c)
}
pub fn failure(work: &Work, files: &ArtifactFiles, error: Value) -> Result<Completion> {
    failure_with_process(work, files, error, Value::Null)
}

pub(super) fn failure_with_process(
    work: &Work,
    files: &ArtifactFiles,
    error: Value,
    process: Value,
) -> Result<Completion> {
    let state = if error["code"] == "CHECK_CANCELLED" {
        "cancelled"
    } else {
        "error"
    };
    let coverage = json!({"requested":work.profile.expected_targets,"checked":[],"gaps":["command_not_started"]});
    let report = json!({"version":1,"check_id":work.check_id,"operation_id":work.operation_id,"candidate_ref":work.candidate.artifact_id,
        "input_fingerprint":work.input_fingerprint,"resolved_inputs":work.resolved_inputs,"scope_plan":work.scope_plan,
        "cache_reusable":work.resolved_inputs.as_ref().is_some_and(|inputs|inputs["cache_reusable"]==true),
        "profile":inputs::profile_identity(&work.profile)? ,"state":state,"exit_code":null,"source_checkout_verified":false,"resource_released":true,"coverage":coverage,"error":error,"process":process,"outputs":[]});
    let id = format!("check-{}", model::digest(work.operation_id.as_bytes()));
    let (record, bytes) = ArtifactFiles::document(
        "check_result",
        &id,
        &report,
        json!({"check_id":work.check_id,"candidate_ref":work.candidate.artifact_id,"state":state}),
    )?;
    files.publish(&record, &bytes)?;
    let c = Completion {
        check_id: work.check_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        state: state.into(),
        exit_code: None,
        resource_released: true,
        coverage,
        result: record,
        outputs: Vec::new(),
        cancellation: None,
    };
    let dir = directory(&work.data_dir, &work.check_id)?;
    std::fs::create_dir_all(&dir)?;
    write_once(&dir.join("completion.json"), &json!(c))?;
    Ok(c)
}
/// Read-only recovery after the worker lock is released. Never replays the command
/// or signals a numeric PID from an old snapshot. Unknown/live groups retain ownership.
pub fn recover(work: &Work, files: &ArtifactFiles) -> Result<Option<Completion>> {
    let dir = directory(&work.data_dir, &work.check_id)?;
    // A missing lock file is not proof of departure either; group disposition
    // below is. When the file exists, hold it so no new worker can start.
    let _lock = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("worker.lock"))
    {
        Ok(lock) => {
            match lock.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
                Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
            }
            Some(lock)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    if let Some(c) = completion(work, files)? {
        return Ok(Some(c));
    }
    let identity = read_value(&dir.join("worker.json"))?;
    if identity["token"] != work.token
        || work
            .expected_worker
            .as_ref()
            .is_some_and(|v| v != &identity)
    {
        return Err(Error::conflict(
            "lost worker identity differs from the admitted worker",
        ));
    }
    if !departed_empty(&identity["process"], &work.token)? {
        return Err(Error::new(
            "CHECK_ORPHAN_ACTIVE",
            "worker or descendants may still write; resource remains held",
        ));
    }
    // New workers prepare this exact receipt before final publication. A crash in
    // between need not discard an already verified result or invent another process.
    if dir.join("terminal.json").try_exists()? {
        let c = validate_completion(
            work,
            files,
            serde_json::from_value(read_value(&dir.join("terminal.json"))?)?,
        )?;
        write_once(&dir.join("completion.json"), &json!(c))?;
        return Ok(Some(c));
    }
    let mut outputs = Vec::new();
    for stream in ["stdout", "stderr"] {
        let path = dir.join(stream);
        if path.try_exists()? {
            outputs.push(files.seal_file(
                &format!("{}:{stream}", work.operation_id),
                &path,
                json!({"check_id":work.check_id,"stream":stream}),
            )?);
        }
    }
    let coverage = json!({"requested":work.profile.expected_targets,"checked":[],"gaps":["worker_lost_without_terminal_receipt"]});
    // An absent worker and empty group do not prove success, exit code or coverage.
    let report = json!({"version":1,"check_id":work.check_id,"operation_id":work.operation_id,
        "candidate_ref":work.candidate.artifact_id,"input_fingerprint":work.input_fingerprint,
        "resolved_inputs":work.resolved_inputs,"scope_plan":work.scope_plan,
        "cache_reusable":work.resolved_inputs.as_ref().is_some_and(|inputs|inputs["cache_reusable"]==true),
        "profile":inputs::profile_identity(&work.profile)? ,"state":"incomplete",
        "exit_code":null,"resource_released":true,"source_checkout_verified":false,"coverage":coverage,
        "process":identity["process"],"recovery":{"disposition":"departed_group_observed_empty","command_replayed":false},
        "outputs":outputs.iter().map(|r|json!({"artifact_ref":r.artifact_id,"stream":r.metadata["stream"],"sha256":r.content_digest,"length":r.byte_length})).collect::<Vec<_>>()});
    let id = format!(
        "check-{}",
        model::digest(format!("recovered:{}", work.operation_id).as_bytes())
    );
    let (record, bytes) = ArtifactFiles::document(
        "check_result",
        &id,
        &report,
        json!({"check_id":work.check_id,"candidate_ref":work.candidate.artifact_id,"state":"incomplete"}),
    )?;
    files.publish(&record, &bytes)?;
    let c = Completion {
        check_id: work.check_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        state: "incomplete".into(),
        exit_code: None,
        resource_released: true,
        coverage,
        result: record,
        outputs,
        cancellation: None,
    };
    write_once(&dir.join("completion.json"), &json!(c))?;
    Ok(Some(c))
}

/// Terminal fixation for a launch that never produced a worker identity: the
/// worker died before publishing `worker.json`, so the admitted-worker
/// recovery path has nothing to verify. This is not that path run loosely —
/// it requires the host's own launch receipt and proof that the spawned
/// process and its prospective group are departed. Before identity
/// publication the worker spawns nothing and the tool never runs, so a proven
/// departure closes the launch as incomplete with unknown exit/coverage —
/// never a guessed pass and never a replayed command. Without a launch
/// receipt, or with any doubt about departure, the check stays held.
pub fn recover_pre_identity(work: &Work, files: &ArtifactFiles) -> Result<Option<Completion>> {
    let Some(launch) = &work.launch else {
        return Ok(None);
    };
    let dir = directory(&work.data_dir, &work.check_id)?;
    if !dir.try_exists()? {
        return Ok(None);
    }
    if dir.join("worker.json").try_exists()? || dir.join("completion.json").try_exists()? {
        return Ok(None); // The identity/completion paths own this check.
    }
    if !spawned_departed(&launch["process"], &work.token)? {
        return Ok(None);
    }
    let mut outputs = Vec::new();
    for stream in ["stdout", "stderr"] {
        let path = dir.join(stream);
        if path.try_exists()? {
            outputs.push(files.seal_file(
                &format!("{}:{stream}", work.operation_id),
                &path,
                json!({"check_id":work.check_id,"stream":stream}),
            )?);
        }
    }
    let coverage = json!({"requested":work.profile.expected_targets,"checked":[],"gaps":["worker_lost_before_identity"]});
    let report = json!({"version":1,"check_id":work.check_id,"operation_id":work.operation_id,
        "candidate_ref":work.candidate.artifact_id,"input_fingerprint":work.input_fingerprint,
        "resolved_inputs":work.resolved_inputs,"scope_plan":work.scope_plan,
        "cache_reusable":work.resolved_inputs.as_ref().is_some_and(|inputs|inputs["cache_reusable"]==true),
        "profile":inputs::profile_identity(&work.profile)? ,"state":"incomplete",
        "exit_code":null,"resource_released":true,"source_checkout_verified":false,"coverage":coverage,
        "process":launch["process"],"launch":launch,
        "recovery":{"disposition":"pre_identity_launch_departed","command_replayed":false,"worker_identity_recorded":false},
        "outputs":outputs.iter().map(|r|json!({"artifact_ref":r.artifact_id,"stream":r.metadata["stream"],"sha256":r.content_digest,"length":r.byte_length})).collect::<Vec<_>>()});
    let id = format!(
        "check-{}",
        model::digest(format!("pre-identity:{}", work.operation_id).as_bytes())
    );
    let (record, bytes) = ArtifactFiles::document(
        "check_result",
        &id,
        &report,
        json!({"check_id":work.check_id,"candidate_ref":work.candidate.artifact_id,"state":"incomplete"}),
    )?;
    files.publish(&record, &bytes)?;
    // Recheck at publication time: a worker that published its identity (or a
    // terminal receipt) after the scan above owns this check, not this path.
    if dir.join("worker.json").try_exists()? || dir.join("terminal.json").try_exists()? {
        return Ok(None);
    }
    let c = Completion {
        check_id: work.check_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        state: "incomplete".into(),
        exit_code: None,
        resource_released: true,
        coverage,
        result: record,
        outputs,
        cancellation: None,
    };
    write_once(&dir.join("completion.json"), &json!(c))?;
    Ok(Some(c))
}

fn environment(profile: &CheckProfile) -> BTreeMap<String, String> {
    inputs::effective_environment(profile)
}

pub(super) fn ensure_owned_directories(root: &Path, directory: &Path) -> Result<()> {
    let root = fs::canonicalize(root)?;
    let relative = directory.strip_prefix(&root).map_err(|_| {
        Error::new(
            "CHECK_INPUTS_STALE",
            "content-addressed CheckRunner input escaped its data directory",
        )
    })?;
    let mut current = root;
    for component in relative.components() {
        let std::path::Component::Normal(part) = component else {
            return Err(Error::new(
                "CHECK_INPUTS_STALE",
                "content-addressed CheckRunner path is not normalized",
            ));
        };
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if is_link_or_reparse(&metadata) || !metadata.is_dir() => {
                return Err(Error::new(
                    "CHECK_INPUTS_STALE",
                    "content-addressed CheckRunner directory is not a regular directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Select the exact trusted Cargo target directory for a check. Explicit
/// profile values must name an existing, canonicalizable directory whose path
/// components contain no symlinks or reparse points. With no explicit profile
/// value, retain the existing per-resource directory under the CheckRun data
/// root and its owned-directory creation guard.
pub(super) fn resolve_target_directory(
    data_root: &Path,
    profile: &CheckProfile,
    environment: &BTreeMap<String, String>,
) -> Result<PathBuf> {
    if let Some(configured) = environment.get("CARGO_TARGET_DIR") {
        return verify_existing_target_directory(Path::new(configured));
    }

    let target = data_root
        .join("targets")
        .join(profile.resource.to_ascii_lowercase());
    ensure_owned_directories(data_root, &target)?;
    fs::canonicalize(&target).map_err(|_| invalid_configured_target_directory())
}

fn verify_existing_target_directory(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid_configured_target_directory());
    }

    let mut current = PathBuf::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            std::path::Component::Prefix(_) => current.push(component.as_os_str()),
            std::path::Component::RootDir => {
                current.push(component.as_os_str());
                saw_root = true;
                verify_existing_target_component(&current)?;
            }
            std::path::Component::Normal(_) => {
                if !saw_root {
                    return Err(invalid_configured_target_directory());
                }
                current.push(component.as_os_str());
                verify_existing_target_component(&current)?;
            }
            std::path::Component::CurDir | std::path::Component::ParentDir => {
                return Err(invalid_configured_target_directory());
            }
        }
    }
    if !saw_root {
        return Err(invalid_configured_target_directory());
    }

    let canonical = fs::canonicalize(path).map_err(|_| invalid_configured_target_directory())?;
    verify_existing_target_component(&canonical)?;
    Ok(canonical)
}

fn verify_existing_target_component(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|_| invalid_configured_target_directory())?;
    if is_link_or_reparse(&metadata) || !metadata.is_dir() {
        return Err(invalid_configured_target_directory());
    }
    Ok(())
}

fn invalid_configured_target_directory() -> Error {
    Error::new(
        "CHECK_INPUTS_STALE",
        "Cargo target directory must be an existing absolute regular directory",
    )
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

pub(super) fn ensure_execution_inputs(
    data_dir: &Path,
    files: &ArtifactFiles,
    candidate: &ArtifactRecord,
    verified: &source::VerifiedSource,
    profile: &CheckProfile,
) -> Result<(PathBuf, PathBuf, source::SourceManifest)> {
    let data_root = fs::canonicalize(data_dir)?;
    let (workspace, descriptor) = inputs::execution_paths(&data_root, profile, verified)?;
    let parent = workspace
        .parent()
        .ok_or_else(|| Error::invalid("content-addressed workspace has no parent"))?;
    ensure_owned_directories(&data_root, parent)?;
    match fs::symlink_metadata(&workspace) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(Error::new(
                "CHECK_INPUTS_STALE",
                "content-addressed source workspace is not a regular directory",
            ));
        }
        Ok(_) => source::verify_directory(&workspace, &verified.manifest)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            source::materialize(&data_root, files, candidate, &workspace)?;
        }
        Err(error) => return Err(error.into()),
    }

    let descriptor_bytes = source::content_descriptor(verified)?;
    match fs::symlink_metadata(&descriptor) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(Error::new(
                "CHECK_INPUTS_STALE",
                "content-addressed candidate descriptor is not a regular file",
            ));
        }
        Ok(_) if fs::read(&descriptor)? != descriptor_bytes => {
            return Err(Error::new(
                "CHECK_INPUTS_STALE",
                "content-addressed candidate descriptor changed",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&descriptor)?;
            file.write_all(&descriptor_bytes)?;
            file.sync_all()?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok((workspace, descriptor, verified.manifest.clone()))
}

pub(super) fn executable(program: &Path, env: &BTreeMap<String, String>) -> Result<PathBuf> {
    if program.is_absolute() {
        if program.is_file() {
            return Ok(program.to_path_buf());
        }
        return Err(Error::new(
            "CHECK_EXECUTABLE_MISSING",
            program.display().to_string(),
        ));
    }
    if program.components().count() != 1 {
        return Err(Error::invalid(
            "check executable must be absolute or a PATH program name",
        ));
    }
    let path = env
        .iter()
        .find(|(k, _)| {
            if cfg!(windows) {
                k.eq_ignore_ascii_case("PATH")
            } else {
                k.as_str() == "PATH"
            }
        })
        .map(|(_, v)| v)
        .ok_or_else(|| Error::new("CHECK_EXECUTABLE_MISSING", "PATH is unavailable"))?;
    for dir in std::env::split_paths(path) {
        if !dir.is_absolute() {
            continue;
        }
        let p = dir.join(program);
        #[cfg(windows)]
        let p = if p.extension().is_none() {
            p.with_extension("exe")
        } else {
            p
        };
        if p.is_file() {
            return Ok(p);
        }
    }
    Err(Error::new(
        "CHECK_EXECUTABLE_MISSING",
        program.display().to_string(),
    ))
}
pub(super) fn parse_cargo(
    path: &Path,
    targets: &[String],
    scope_plan: &Value,
    workspace: &Path,
) -> Result<Value> {
    let target_identities: BTreeMap<String, Vec<CargoTargetIdentity>> =
        serde_json::from_value(scope_plan["target_identities"].clone()).unwrap_or_default();
    let mut reader = BufReader::new(File::open(path)?);
    let mut line = Vec::new();
    let mut oversized = false;
    let mut checked = BTreeSet::new();
    let mut checked_target_identities = BTreeSet::new();
    let mut errors = 0u64;
    let mut warnings = 0u64;
    let mut finished = None;
    let mut gaps = Vec::new();
    let mut examples = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() && line.is_empty() && !oversized {
            break;
        }
        let end = available.iter().position(|b| *b == b'\n');
        let take = end.map_or(available.len(), |n| n + 1);
        let eof = available.is_empty();
        if !oversized && line.len() + take <= 1_048_576 {
            line.extend_from_slice(&available[..take]);
        } else {
            oversized = true;
            line.clear();
        }
        reader.consume(take);
        if end.is_none() && !eof {
            continue;
        }
        if oversized {
            gaps.push("oversized_cargo_line".to_string());
        } else if line.first() == Some(&b'{') {
            match serde_json::from_slice::<Value>(&line) {
                Ok(v) => match v["reason"].as_str() {
                    Some("build-finished") => {
                        if finished.is_some() || !v["success"].is_boolean() {
                            gaps.push("invalid_build_finished".into());
                        }
                        finished = v["success"].as_bool();
                    }
                    Some("compiler-artifact") => {
                        if let (Some(name), Some(package_id)) =
                            (v["target"]["name"].as_str(), v["package_id"].as_str())
                            && targets.iter().any(|expected| expected == name)
                            && let Some(identities) = target_identities.get(name)
                        {
                            for identity in identities {
                                if artifact_matches_target_identity(
                                    package_id,
                                    &v["target"],
                                    identity,
                                    workspace,
                                ) {
                                    checked.insert(name.to_string());
                                    checked_target_identities
                                        .insert(target_identity_key(identity)?);
                                }
                            }
                        } else if v["target"]["name"].is_null()
                            || v["package_id"].as_str().is_none()
                        {
                            gaps.push("artifact_without_target".into());
                        }
                    }
                    Some("compiler-message") => {
                        let m = &v["message"];
                        if !m["level"].is_string() || !m["message"].is_string() {
                            gaps.push("invalid_compiler_message".into());
                        }
                        match m["level"].as_str() {
                            Some("error") => errors += 1,
                            Some("warning") => warnings += 1,
                            _ => {}
                        }
                        if examples.len() < 20 {
                            examples.push(json!({"level":m["level"],"message":m["message"].as_str().unwrap_or("").chars().take(500).collect::<String>(),"code":m["code"]["code"]}));
                        }
                    }
                    _ => {}
                },
                Err(_) => gaps.push("unparsed_cargo_json".into()),
            }
        }
        line.clear();
        oversized = false;
        if eof {
            break;
        }
    }
    if finished.is_none() {
        gaps.push("build_finished_missing".into());
    }
    for t in targets {
        if !checked.contains(t) {
            gaps.push(format!("target_not_observed:{t}"));
        }
    }
    for target in targets {
        if !target_identities.contains_key(target) {
            gaps.push(format!("target_identity_unavailable:{target}"));
        }
    }
    gaps.sort();
    gaps.dedup();
    Ok(
        json!({"requested":targets,"checked":checked,"checked_target_identities":checked_target_identities,"gaps":gaps,"build_finished":finished,"errors":errors,"warnings":warnings,"diagnostic_preview":examples}),
    )
}
pub fn run(file: &Path) -> Result<()> {
    let value = read_value(file)?;
    if value.get("check_probe").is_some() {
        return inputs::run_probe(file);
    }
    let work: Work = serde_json::from_value(value)?;
    let dir = directory(&work.data_dir, &work.check_id)?;
    if fs::canonicalize(file)? != fs::canonicalize(dir.join("work.json"))? {
        return Err(Error::invalid(
            "worker path does not match its generated job directory",
        ));
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join("worker.lock"))?;
    lock.try_lock().map_err(|_| {
        Error::new(
            "CHECK_WORKER_EXISTS",
            "this check already has an owning worker",
        )
    })?;
    if dir.join("completion.json").exists() {
        return Ok(());
    }
    if dir.join("worker.json").exists() {
        return Err(Error::new(
            "CHECK_RECOVERY_REQUIRED",
            "a previous worker started; command will not be repeated",
        ));
    }

    let files = ArtifactFiles::new(&work.data_dir)?;
    let mut control = HostCheckControl::new(&work, &dir);
    let mut code = None;
    let mut outputs = Vec::new();
    let mut execution: Option<CheckExecution> = None;
    let mut source_verified = false;
    let mut prepared: Option<(Vec<String>, PathBuf, source::SourceManifest, PathBuf)> = None;
    let mut plan_setup_failed = false;
    let outcome = (|| -> Result<Value> {
        let identity = CheckIdentity {
            check_id: work.check_id.clone(),
            operation_id: work.operation_id.clone(),
            token: work.token.clone(),
        };
        let executed = swarm_checks::execute_with_plan(
            identity,
            dir.clone(),
            &mut control,
            || {
                let setup = (|| -> Result<_> {
                    let verified =
                        source::verified_content(&files, &work.data_dir, &work.candidate)?;
                    let (resolved_argv, expected_targets, program) = if let Some(resolved) =
                        &work.resolved_inputs
                    {
                        if work.input_fingerprint.as_deref()
                            != resolved["input_fingerprint"].as_str()
                        {
                            return Err(Error::new(
                                "CHECK_INPUTS_STALE",
                                "worker input fingerprint does not match its resolved plan",
                            ));
                        }
                        if resolved["candidate_content_sha256"] != verified.content_sha256
                            || resolved["execution_workspace"]
                                != inputs::execution_workspace_identity(&work.profile, &verified)?
                        {
                            return Err(Error::new(
                                "CHECK_INPUTS_STALE",
                                "candidate content or stable workspace differs from its resolved plan",
                            ));
                        }
                        inputs::verify_runtime_environment(
                            &work.profile,
                            &verified,
                            &resolved["environment"],
                        )?;
                        let argv: Vec<String> = serde_json::from_value(resolved["argv"].clone())
                            .map_err(|_| {
                                Error::new("CHECK_INPUTS_STALE", "resolved argv is invalid")
                            })?;
                        let targets: Vec<String> = serde_json::from_value(
                            resolved["expected_targets"].clone(),
                        )
                        .map_err(|_| {
                            Error::new("CHECK_INPUTS_STALE", "resolved targets are invalid")
                        })?;
                        (argv, targets, inputs::verify_executable(resolved)?)
                    } else {
                        let environment = environment(&work.profile);
                        let program = executable(&work.profile.executable, &environment)?;
                        (
                            work.profile.args.clone(),
                            work.profile.expected_targets.clone(),
                            program,
                        )
                    };
                    let (source_dir, candidate_file, manifest) = ensure_execution_inputs(
                        &work.data_dir,
                        &files,
                        &work.candidate,
                        &verified,
                        &work.profile,
                    )?;
                    let mut env = environment(&work.profile);
                    let data_root = fs::canonicalize(&work.data_dir)?;
                    let target = resolve_target_directory(&data_root, &work.profile, &env)?;
                    env.insert(
                        "CARGO_TARGET_DIR".into(),
                        target.to_string_lossy().to_string(),
                    );
                    env.insert(
                        "SWARM_CANDIDATE_FILE".into(),
                        candidate_file.to_string_lossy().to_string(),
                    );
                    #[cfg(windows)]
                    if !program
                        .extension()
                        .and_then(|x| x.to_str())
                        .is_some_and(|x| x.eq_ignore_ascii_case("exe"))
                    {
                        return Err(Error::invalid(
                            "use a native executable, such as pwsh.exe, rather than a batch shim",
                        ));
                    }
                    let plan = ResolvedCheckPlan {
                        identity: CheckIdentity {
                            check_id: work.check_id.clone(),
                            operation_id: work.operation_id.clone(),
                            token: work.token.clone(),
                        },
                        executable: program.clone(),
                        argv: resolved_argv,
                        working_directory: fs::canonicalize(&source_dir)?,
                        environment: env,
                        output_directory: fs::canonicalize(&dir)?,
                        output_limit_bytes_per_stream: MAX_CAPTURE_BYTES_PER_STREAM,
                        // The current trusted CheckProfile has no full-command timeout policy.
                        timeout: None,
                    };
                    Ok((plan, expected_targets, source_dir, manifest, program))
                })();
                match setup {
                    Ok((plan, expected_targets, source_dir, manifest, program)) => {
                        prepared = Some((expected_targets, source_dir, manifest, program));
                        Ok(plan)
                    }
                    Err(error) => {
                        plan_setup_failed = true;
                        Err(into_swarm_checks_error(error))
                    }
                }
            },
        )?;
        control.record_execution(&executed);
        code = executed.exit_code;
        execution = Some(executed.clone());

        if control.drain_diagnostic_write_failed {
            return Err(Error::new(
                "CHECK_PROCESS_DIAGNOSTIC_WRITE_FAILED",
                "the CheckRun process-group diagnostic could not be persisted",
            ));
        }
        if control.output_capture_diagnostic_write_failed {
            return Err(Error::new(
                "CHECK_OUTPUT_DIAGNOSTIC_WRITE_FAILED",
                "the CheckRun output-capture diagnostic could not be persisted",
            ));
        }

        if let Some(pid) = executed.child_pid {
            let started = control.started_at_ms.ok_or_else(|| {
                Error::new(
                    "CHECK_START_RECEIPT_MISSING",
                    "the worker start acknowledgement has no timestamp",
                )
            })?;
            let program = &prepared
                .as_ref()
                .ok_or_else(|| {
                    Error::new(
                        "CHECK_EXECUTION_PLAN_MISSING",
                        "the spawned check has no resolved plan context",
                    )
                })?
                .3;
            write_once(
                &dir.join("started.json"),
                &json!({"pid":pid,"program":program,"started_at_ms":started,"token":work.token}),
            )?;
        }

        // A request noticed after process exit is retained without claiming it
        // caused termination, matching the previous worker's late-read behavior.
        control.cancellation.read(&work, &dir)?;

        match executed.termination {
            Termination::CancelledBeforeStart => {
                return Err(Error::new(
                    "CHECK_CANCELLED",
                    "cancelled before command execution",
                ));
            }
            Termination::ControlReadUnknownBeforeStart => {
                return Err(Error::new(
                    "CHECK_CONTROL_READ_UNKNOWN",
                    "the check cancellation receipt could not be read before command execution",
                ));
            }
            Termination::ProcessObservationUnknown => {
                return Err(Error::new(
                    "CHECK_PROCESS_OBSERVATION_UNKNOWN",
                    "the check process exit could not be observed",
                ));
            }
            Termination::TimedOut => {
                return Err(Error::new(
                    "CHECK_TIMEOUT_UNEXPECTED",
                    "the check executor timed out without an admitted timeout policy",
                ));
            }
            Termination::Exited | Termination::Cancelled => {}
        }
        if executed.control_read_unknown {
            return Err(Error::new(
                "CHECK_CONTROL_READ_UNKNOWN",
                "the check cancellation receipt could not be read while the process was active",
            ));
        }
        if !executed.resource_released {
            return Err(Error::new(
                "CHECK_RESOURCE_RELEASE_UNKNOWN",
                "the check executor did not confirm release of its process group",
            ));
        }

        let (expected_targets, source_dir, manifest, _) = prepared.take().ok_or_else(|| {
            Error::new(
                "CHECK_EXECUTION_PLAN_MISSING",
                "the check executor started without its resolved plan context",
            )
        })?;

        let empty_scope_plan = Value::Null;
        let mut coverage = if work.profile.parser == Parser::CargoJson {
            parse_cargo(
                &dir.join("stdout"),
                &expected_targets,
                work.scope_plan.as_ref().unwrap_or(&empty_scope_plan),
                &source_dir,
            )?
        } else {
            json!({"requested":["process_exit"],"checked":["process_exit"],"gaps":[]})
        };
        let gaps = coverage["gaps"]
            .as_array_mut()
            .ok_or_else(|| Error::invalid("coverage gaps missing"))?;
        if executed.stdout.truncated {
            gaps.push(json!("stdout_truncated"));
        }
        if !executed.stdout.capture_complete {
            gaps.push(json!("stdout_capture_incomplete"));
        }
        if executed.stderr.truncated {
            gaps.push(json!("stderr_truncated"));
        }
        if !executed.stderr.capture_complete {
            gaps.push(json!("stderr_capture_incomplete"));
        }
        gaps.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        gaps.dedup();

        match source::verify_directory(&source_dir, &manifest) {
            Ok(()) => source_verified = true,
            Err(e) => {
                coverage["gaps"]
                    .as_array_mut()
                    .ok_or_else(|| Error::invalid("coverage gaps missing"))?
                    .push(json!(format!("source_changed:{}", e.code)));
            }
        }
        Ok(coverage)
    })();

    if execution.is_none() {
        match outcome {
            Err(error) if control.identity.is_none() || plan_setup_failed => {
                let process = control
                    .identity
                    .as_ref()
                    .map(|identity| identity["process"].clone())
                    .unwrap_or(Value::Null);
                failure_with_process(&work, &files, json!(error), process)?;
                drop(lock);
                return Ok(());
            }
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(Error::new(
                    "CHECK_EXECUTION_RECEIPT_MISSING",
                    "the check executor returned without process evidence",
                ));
            }
        }
    }
    let executed = execution.as_ref().ok_or_else(|| {
        Error::new(
            "CHECK_EXECUTION_RECEIPT_MISSING",
            "the check executor returned without process evidence",
        )
    })?;
    if !executed.resource_released {
        return Err(Error::new(
            "CHECK_RESOURCE_RELEASE_UNKNOWN",
            "the check executor did not confirm release of its process group",
        ));
    }

    for (stream, captured) in [("stdout", &executed.stdout), ("stderr", &executed.stderr)] {
        if captured.path.try_exists()? {
            outputs.push(files.seal_file(
                &format!("{}:{stream}", work.operation_id),
                &captured.path,
                json!({
                    "check_id":work.check_id,
                    "stream":stream,
                    "bytes_written":captured.bytes_written,
                    "truncated":captured.truncated,
                    "capture_complete":captured.capture_complete
                }),
            )?);
        }
    }

    let (mut coverage, error) = match outcome {
        Ok(c) => (c, None),
        Err(e) => (
            json!({"requested":work.profile.expected_targets,"checked":[],"gaps":[e.code]}),
            Some(e),
        ),
    };
    if let Some(gaps) = work
        .scope_plan
        .as_ref()
        .and_then(|plan| plan["coverage_gaps"].as_array())
        && let Some(coverage_gaps) = coverage["gaps"].as_array_mut()
    {
        coverage_gaps.extend(gaps.iter().filter(|gap| gap.is_string()).cloned());
        coverage_gaps.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
        coverage_gaps.dedup();
    }
    let cancellation = std::mem::take(&mut control.cancellation);
    let state = if cancellation.applied() {
        "cancelled"
    } else if error.is_some() {
        "error"
    } else if code != Some(0) {
        "failed"
    } else if coverage["gaps"].as_array().is_none_or(|g| !g.is_empty())
        || (work.profile.parser == Parser::CargoJson
            && (coverage["build_finished"] != true || coverage["errors"].as_u64().unwrap_or(0) > 0))
    {
        "incomplete"
    } else {
        "passed"
    };
    let process = execution
        .as_ref()
        .map(|result| result.process.clone())
        .or_else(|| {
            control
                .identity
                .as_ref()
                .map(|identity| identity["process"].clone())
        })
        .unwrap_or(Value::Null);
    let started = control.started_at_ms.unwrap_or(model::now_ms()?);
    let report = json!({"version":1,"check_id":work.check_id,"operation_id":work.operation_id,"candidate_ref":work.candidate.artifact_id,"candidate_sha256":work.candidate.content_digest,
        "input_fingerprint":work.input_fingerprint,"resolved_inputs":work.resolved_inputs,"scope_plan":work.scope_plan,
        "cache_reusable":work.resolved_inputs.as_ref().is_some_and(|inputs|inputs["cache_reusable"]==true),
        "profile":inputs::profile_identity(&work.profile)? ,"cancellation":cancellation.evidence(),"worker_version":env!("CARGO_PKG_VERSION"),"process":process,"state":state,"exit_code":code,"resource_released":executed.resource_released,"source_checkout_verified":source_verified,"coverage":coverage,"error":error,
        "outputs":outputs.iter().map(|r|json!({"artifact_ref":r.artifact_id,"stream":r.metadata["stream"],"sha256":r.content_digest,"length":r.byte_length,"bytes_written":r.metadata["bytes_written"],"truncated":r.metadata["truncated"],"capture_complete":r.metadata["capture_complete"]})).collect::<Vec<_>>(),"started_at_ms":started,"finished_at_ms":model::now_ms()?});
    let id = format!("check-{}", model::digest(work.operation_id.as_bytes()));
    let (record, bytes) = ArtifactFiles::document(
        "check_result",
        &id,
        &report,
        json!({"check_id":work.check_id,"candidate_ref":work.candidate.artifact_id,"state":state}),
    )?;
    files.publish(&record, &bytes)?;
    let completed = Completion {
        check_id: work.check_id,
        operation_id: work.operation_id,
        token: work.token,
        state: state.into(),
        exit_code: code,
        resource_released: executed.resource_released,
        coverage,
        result: record,
        outputs,
        cancellation: cancellation.evidence(),
    };
    write_once(&dir.join("terminal.json"), &json!(completed))?;
    write_once(&dir.join("completion.json"), &json!(completed))?;
    drop(lock);
    Ok(())
}
#[cfg(test)]
mod coverage_validation_tests {
    use super::*;

    fn package_id_for_root(root: &Path) -> String {
        let root = fs::canonicalize(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let root = if cfg!(windows) {
            let root = root.strip_prefix("//?/").unwrap_or(&root);
            format!("/{root}")
        } else {
            root
        };
        let mut encoded = String::new();
        for byte in root.bytes() {
            if byte.is_ascii_alphanumeric() || b"/-._~:".contains(&byte) {
                encoded.push(char::from(byte));
            } else {
                encoded.push_str(&format!("%{byte:02X}"));
            }
        }
        format!("file://{encoded}#1.0.0")
    }

    fn named_package_id_for_root(root: &Path) -> String {
        let root = fs::canonicalize(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let root = if cfg!(windows) {
            let root = root.strip_prefix("//?/").unwrap_or(&root);
            format!("/{root}")
        } else {
            root
        };
        let mut encoded = String::new();
        for byte in root.bytes() {
            if byte.is_ascii_alphanumeric() || b"/-._~:".contains(&byte) {
                encoded.push(char::from(byte));
            } else {
                encoded.push_str(&format!("%{byte:02X}"));
            }
        }
        format!("path+file://{encoded}#workspace_pkg@1.0.0")
    }

    #[test]
    fn passed_receipts_need_complete_parser_specific_coverage() {
        let targets = vec!["eliot_swarm_controller".to_string(), "swarm".to_string()];
        let identity = |name: &str, kind: &str, src_path: &str| CargoTargetIdentity {
            package_name: "eliot_swarm_controller".into(),
            package_version: "1.0.0".into(),
            manifest_path: "Cargo.toml".into(),
            name: name.into(),
            kinds: BTreeSet::from([kind.into()]),
            src_path: src_path.into(),
        };
        let identities = BTreeMap::from([
            (
                "eliot_swarm_controller".to_string(),
                vec![identity("eliot_swarm_controller", "lib", "src/lib.rs")],
            ),
            (
                "swarm".to_string(),
                vec![identity("swarm", "bin", "src/main.rs")],
            ),
        ]);
        let scope_plan = json!({"target_identities":identities});
        let checked_identities: Vec<_> = [
            identity("eliot_swarm_controller", "lib", "src/lib.rs"),
            identity("swarm", "bin", "src/main.rs"),
        ]
        .iter()
        .map(|identity| target_identity_key(identity).unwrap())
        .collect();
        let complete_cargo = json!({
            "requested":["eliot_swarm_controller","swarm"],
            "checked":["eliot_swarm_controller","swarm"],
            "checked_target_identities":checked_identities,
            "gaps":[],
            "build_finished":true,
            "errors":0
        });
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &complete_cargo)
                .is_ok()
        );

        let mut missing_target = complete_cargo.clone();
        missing_target["checked"] = json!(["eliot_swarm_controller"]);
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &missing_target)
                .is_err()
        );
        let mut missing_finished = complete_cargo.clone();
        missing_finished["build_finished"] = Value::Null;
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &missing_finished)
                .is_err()
        );
        let mut compile_error = complete_cargo.clone();
        compile_error["errors"] = json!(1);
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &compile_error)
                .is_err()
        );
        let mut wrong_identity = complete_cargo;
        wrong_identity["checked_target_identities"] = json!(["wrong"]);
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &wrong_identity)
                .is_err()
        );

        let empty_scope = Value::Null;
        let exit_code = json!({"requested":["process_exit"],"checked":["process_exit"],"gaps":[]});
        assert!(validate_passed_coverage(&Parser::ExitCode, &[], &empty_scope, &exit_code).is_ok());
        let forged_exit = json!({"requested":[],"checked":[],"gaps":[]});
        assert!(
            validate_passed_coverage(&Parser::ExitCode, &[], &empty_scope, &forged_exit).is_err()
        );
    }

    #[test]
    fn dependency_artifact_with_same_name_cannot_prove_workspace_bin() {
        let root = std::env::temp_dir().join(format!("swarm-target-identity-{}", model::new_id()));
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(root.join("external")).unwrap();
        fs::write(
            root.join("Cargo.toml"),
            "[package]\nname='workspace_pkg'\nversion='1.0.0'\n",
        )
        .unwrap();
        fs::write(
            root.join("external/Cargo.toml"),
            "[package]\nname='workspace_pkg'\nversion='1.0.0'\n",
        )
        .unwrap();
        fs::write(root.join("src/main.rs"), "fn main() {}\n").unwrap();
        let identity = CargoTargetIdentity {
            package_name: "workspace_pkg".into(),
            package_version: "1.0.0".into(),
            manifest_path: "Cargo.toml".into(),
            name: "foo".into(),
            kinds: BTreeSet::from(["bin".into()]),
            src_path: "src/main.rs".into(),
        };
        let scope_plan = json!({"target_identities":{"foo":[identity]}});
        let targets = vec!["foo".to_string()];
        let output = root.join("cargo-output.jsonl");
        let dependency_artifact = json!({
            "reason":"compiler-artifact",
            "package_id":"registry+https://example.invalid#index#dependency@9.0.0",
            "target":{"name":"foo","kind":["lib"],"src_path":"C:/registry/dependency/src/lib.rs"},
        });
        fs::write(
            &output,
            format!(
                "{}\n{}\n",
                dependency_artifact,
                json!({"reason":"build-finished","success":true})
            ),
        )
        .unwrap();
        let coverage = parse_cargo(&output, &targets, &scope_plan, &root).unwrap();
        assert!(coverage["checked"].as_array().unwrap().is_empty());
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &coverage).is_err()
        );

        let external_path_artifact = json!({
            "reason":"compiler-artifact",
            "package_id":package_id_for_root(&root.join("external")),
            "target":{"name":"foo","kind":["bin"],"src_path":root.join("src/main.rs")},
        });
        fs::write(
            &output,
            format!(
                "{}\n{}\n",
                external_path_artifact,
                json!({"reason":"build-finished","success":true})
            ),
        )
        .unwrap();
        let coverage = parse_cargo(&output, &targets, &scope_plan, &root).unwrap();
        assert!(coverage["checked"].as_array().unwrap().is_empty());
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &coverage).is_err()
        );

        let workspace_artifact = json!({
            "reason":"compiler-artifact",
            "package_id":package_id_for_root(&root),
            "target":{"name":"foo","kind":["bin"],"src_path":root.join("src/main.rs")},
        });
        fs::write(
            &output,
            format!(
                "{}\n{}\n",
                workspace_artifact,
                json!({"reason":"build-finished","success":true})
            ),
        )
        .unwrap();
        let coverage = parse_cargo(&output, &targets, &scope_plan, &root).unwrap();
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &coverage).is_ok()
        );

        let named_workspace_artifact = json!({
            "reason":"compiler-artifact",
            "package_id":named_package_id_for_root(&root),
            "target":{"name":"foo","kind":["bin"],"src_path":root.join("src/main.rs")},
        });
        fs::write(
            &output,
            format!(
                "{}\n{}\n",
                named_workspace_artifact,
                json!({"reason":"build-finished","success":true})
            ),
        )
        .unwrap();
        let coverage = parse_cargo(&output, &targets, &scope_plan, &root).unwrap();
        assert!(
            validate_passed_coverage(&Parser::CargoJson, &targets, &scope_plan, &coverage).is_ok()
        );
        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::checks::model::Parser;

    fn fixture(launch: Option<Value>) -> (PathBuf, Work) {
        let root = std::env::temp_dir().join(format!("swarm-check-{}", model::new_id()));
        fs::create_dir_all(&root).unwrap();
        let w = Work {
            check_id: model::new_id(),
            operation_id: model::new_id(),
            token: model::new_id(),
            data_dir: root.clone(),
            candidate: ArtifactRecord {
                kind: "source_snapshot".into(),
                artifact_id: "candidate-1".into(),
                relative_path: "artifacts/candidate-1".into(),
                byte_length: 0,
                content_digest: "digest".into(),
                metadata: json!({}),
            },
            profile: CheckProfile {
                profile_id: "profile".into(),
                profile_revision: "1".into(),
                executable: "/bin/true".into(),
                args: Vec::new(),
                parser: Parser::ExitCode,
                resource: "target".into(),
                environment: Default::default(),
                inherit_env: Vec::new(),
                expected_targets: Vec::new(),
                reproducible: false,
                fingerprint_env: Vec::new(),
                versioned_inputs: BTreeMap::new(),
            },
            resolved_inputs: None,
            scope_plan: None,
            input_fingerprint: None,
            preflight_error: None,
            cancel_request: None,
            expected_worker: None,
            launch,
        };
        fs::create_dir_all(directory(&root, &w.check_id).unwrap()).unwrap();
        (root, w)
    }

    #[test]
    fn pre_identity_departed_launch_is_fixed_incomplete() {
        let mut child = Command::new("/bin/true").spawn().unwrap();
        let pid = child.id();
        // The process may exit before capture; the partial record is by design.
        let process = spawned_identity(pid).unwrap_or_else(
            |_| json!({"pid":pid,"scope":"launcher_spawned_process","purpose":"check"}),
        );
        child.wait().unwrap();
        let (root, w) = fixture(Some(json!({"spawned_at_ms":0,"process":process})));
        let files = ArtifactFiles::new(&w.data_dir).unwrap();
        let c = recover_pre_identity(&w, &files)
            .unwrap()
            .expect("departed pre-identity launch must be fixed");
        assert_eq!(c.state, "incomplete");
        assert_eq!(c.exit_code, None);
        assert!(c.resource_released);
        // The written receipt passes the ordinary completion validation.
        let validated = completion(&w, &files)
            .unwrap()
            .expect("validated completion");
        assert_eq!(validated.state, "incomplete");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pre_identity_live_launch_is_not_fixed() {
        let mut child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let process = spawned_identity(child.id()).unwrap();
        let (root, w) = fixture(Some(json!({"spawned_at_ms":0,"process":process})));
        let files = ArtifactFiles::new(&w.data_dir).unwrap();
        assert!(recover_pre_identity(&w, &files).unwrap().is_none());
        child.kill().unwrap();
        child.wait().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pre_identity_without_launch_receipt_is_held() {
        let (root, w) = fixture(None);
        let files = ArtifactFiles::new(&w.data_dir).unwrap();
        assert!(recover_pre_identity(&w, &files).unwrap().is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pre_identity_with_worker_identity_is_not_fixed() {
        let mut child = Command::new("/bin/true").spawn().unwrap();
        let pid = child.id();
        let process = spawned_identity(pid).unwrap_or_else(
            |_| json!({"pid":pid,"scope":"launcher_spawned_process","purpose":"check"}),
        );
        child.wait().unwrap();
        let (root, w) = fixture(Some(json!({"spawned_at_ms":0,"process":process})));
        write_once(
            &directory(&w.data_dir, &w.check_id)
                .unwrap()
                .join("worker.json"),
            &json!({"token":w.token}),
        )
        .unwrap();
        let files = ArtifactFiles::new(&w.data_dir).unwrap();
        assert!(recover_pre_identity(&w, &files).unwrap().is_none());
        let _ = fs::remove_dir_all(root);
    }
}
