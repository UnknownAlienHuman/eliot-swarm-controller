//! One short-lived worker per check. It owns no DB or model session and executes
//! at most one configured command, after the host durably acknowledges its identity.
use super::{
    job::{Group, departed_empty},
    model::{CheckProfile, Parser},
    source,
};
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    error::{Error, Result},
    model,
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
    pub preflight_error: Option<Value>,
    #[serde(default)]
    pub cancel_request: Option<CancelRequest>,
    #[serde(default)]
    pub expected_worker: Option<Value>,
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
pub fn prepare_and_spawn(work: &Work) -> Result<()> {
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
    Ok(()) // Deliberately independent of a host/CLI disconnect.
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
    fn interrupt(&mut self, group: &Group) {
        if self.request.is_none() {
            return;
        }
        self.termination_attempted = true;
        match group.cancel_children() {
            Ok(sent) => self.signals_sent = self.signals_sent.saturating_add(sent),
            Err(e) => self.last_error = Some(e),
        }
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

pub fn completion(work: &Work, files: &ArtifactFiles) -> Result<Option<Completion>> {
    let p = directory(&work.data_dir, &work.check_id)?.join("completion.json");
    if !p.try_exists()? {
        return Ok(None);
    }
    let c: Completion = serde_json::from_value(read_value(&p)?)?;
    validate_completion(work, files, c).map(Some)
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
        || report["profile"] != json!(work.profile)
        || report["resource_released"] != true
        || report["cancellation"] != json!(c.cancellation)
    {
        return Err(Error::conflict(
            "check receipt differs from its published report",
        ));
    }
    if c.state == "passed"
        && (c.exit_code != Some(0)
            || report["source_checkout_verified"] != true
            || c.coverage["gaps"].as_array().is_none_or(|g| !g.is_empty()))
    {
        return Err(Error::conflict(
            "incomplete execution cannot be a passed CheckRun",
        ));
    }
    Ok(c)
}
pub fn failure(work: &Work, files: &ArtifactFiles, error: Value) -> Result<Completion> {
    let state = if error["code"] == "CHECK_CANCELLED" {
        "cancelled"
    } else {
        "error"
    };
    let coverage = json!({"requested":work.profile.expected_targets,"checked":[],"gaps":["command_not_started"]});
    let report = json!({"version":1,"check_id":work.check_id,"operation_id":work.operation_id,"candidate_ref":work.candidate.artifact_id,"profile":work.profile,"state":state,"exit_code":null,"source_checkout_verified":false,"resource_released":true,"coverage":coverage,"error":error,"process":null,"outputs":[]});
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
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("worker.lock"))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
        Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
    }
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
        "candidate_ref":work.candidate.artifact_id,"profile":work.profile,"state":"incomplete",
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

fn environment(profile: &CheckProfile) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for name in [
        "PATH",
        "SystemRoot",
        "WINDIR",
        "USERPROFILE",
        "HOME",
        "LOCALAPPDATA",
        "APPDATA",
        "TEMP",
        "TMP",
        "TMPDIR",
        "RUSTUP_HOME",
        "CARGO_HOME",
    ]
    .iter()
    .copied()
    .chain(profile.inherit_env.iter().map(String::as_str))
    {
        if let Some((key, value)) = std::env::vars().find(|(k, _)| {
            if cfg!(windows) {
                k.eq_ignore_ascii_case(name)
            } else {
                k == name
            }
        }) {
            values.insert(key, value);
        }
    }
    for (key, value) in &profile.environment {
        if cfg!(windows) {
            values.retain(|k, _| !k.eq_ignore_ascii_case(key));
        }
        values.insert(key.clone(), value.clone());
    }
    values
}
fn executable(program: &Path, env: &BTreeMap<String, String>) -> Result<PathBuf> {
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
fn parse_cargo(path: &Path, targets: &[String]) -> Result<Value> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut line = Vec::new();
    let mut oversized = false;
    let mut checked = BTreeSet::new();
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
                        if let Some(name) = v["target"]["name"].as_str() {
                            checked.insert(name.to_string());
                        } else {
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
    gaps.sort();
    gaps.dedup();
    Ok(
        json!({"requested":targets,"checked":checked,"gaps":gaps,"build_finished":finished,"errors":errors,"warnings":warnings,"diagnostic_preview":examples}),
    )
}
pub fn run(file: &Path) -> Result<()> {
    let work: Work = serde_json::from_value(read_value(file)?)?;
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
    let group = match Group::enter(&work.token) {
        Ok(g) => g,
        Err(e) => {
            failure(&work, &ArtifactFiles::new(&work.data_dir)?, json!(e))?;
            return Ok(());
        }
    };
    write_once(
        &dir.join("worker.json"),
        &super::job::waiting_identity(&group, &work.token),
    )?;
    let mut cancellation = Cancellation::default();
    loop {
        if cancellation.read(&work, &dir)? {
            cancellation.skipped_start = true;
            break;
        }
        let go = dir.join("go.json");
        if go.try_exists()? {
            if read_value(&go)?["token"] != work.token {
                return Err(Error::conflict("check start token mismatch"));
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let files = ArtifactFiles::new(&work.data_dir)?;
    let mut code = None;
    let mut outputs = Vec::new();
    let started = model::now_ms()?;
    let mut source_verified = false;
    let outcome = (|| -> Result<Value> {
        if cancellation.skipped_start {
            return Err(Error::new(
                "CHECK_CANCELLED",
                "cancelled before command execution",
            ));
        }
        let source_dir = dir.join("source");
        let manifest = source::materialize(&work.data_dir, &files, &work.candidate, &source_dir)?;
        let mut env = environment(&work.profile);
        let target = work
            .data_dir
            .join("targets")
            .join(work.profile.resource.to_lowercase());
        fs::create_dir_all(&target)?;
        env.insert(
            "CARGO_TARGET_DIR".into(),
            target.to_string_lossy().to_string(),
        );
        env.insert(
            "SWARM_CANDIDATE_FILE".into(),
            work.data_dir
                .join(&work.candidate.relative_path)
                .to_string_lossy()
                .to_string(),
        );
        let program = executable(&work.profile.executable, &env)?;
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
        let stdout = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("stdout"))?;
        let stderr = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("stderr"))?;
        let mut command = Command::new(&program);
        command
            .args(&work.profile.args)
            .current_dir(&source_dir)
            .env_clear()
            .envs(&env)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        if cancellation.read(&work, &dir)? {
            cancellation.skipped_start = true;
            return Err(Error::new(
                "CHECK_CANCELLED",
                "cancelled before command execution",
            ));
        }
        let mut child = command.spawn()?;
        write_once(
            &dir.join("started.json"),
            &json!({"pid":child.id(),"program":program,"started_at_ms":started,"token":work.token}),
        )?;
        loop {
            if let Some(status) = child.try_wait()? {
                code = status.code();
                break;
            }
            if cancellation.read(&work, &dir)? {
                cancellation.interrupt(&group);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        drop(command);
        while !group.children_empty()? {
            if cancellation.read(&work, &dir)? {
                cancellation.interrupt(&group);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let mut coverage = if work.profile.parser == Parser::CargoJson {
            parse_cargo(&dir.join("stdout"), &work.profile.expected_targets)?
        } else {
            json!({"requested":["process_exit"],"checked":["process_exit"],"gaps":[]})
        };
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
    // Do not publish terminal evidence until the entire owned group is done.
    while !group.children_empty()? {
        if cancellation.read(&work, &dir)? {
            cancellation.interrupt(&group);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // A late request cannot rewrite an already completed command's verdict.
    cancellation.read(&work, &dir)?;
    for stream in ["stdout", "stderr"] {
        let p = dir.join(stream);
        if p.try_exists()? {
            outputs.push(files.seal_file(
                &format!("{}:{stream}", work.operation_id),
                &p,
                json!({"check_id":work.check_id,"stream":stream}),
            )?);
        }
    }
    let (coverage, error) = match outcome {
        Ok(c) => (c, None),
        Err(e) => (
            json!({"requested":work.profile.expected_targets,"checked":[],"gaps":[e.code]}),
            Some(e),
        ),
    };
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
    let report = json!({"version":1,"check_id":work.check_id,"operation_id":work.operation_id,"candidate_ref":work.candidate.artifact_id,"candidate_sha256":work.candidate.content_digest,
        "profile":work.profile,"cancellation":cancellation.evidence(),"worker_version":env!("CARGO_PKG_VERSION"),"process":group.identity,"state":state,"exit_code":code,"resource_released":true,"source_checkout_verified":source_verified,"coverage":coverage,"error":error,
        "outputs":outputs.iter().map(|r|json!({"artifact_ref":r.artifact_id,"stream":r.metadata["stream"],"sha256":r.content_digest,"length":r.byte_length})).collect::<Vec<_>>(),"started_at_ms":started,"finished_at_ms":model::now_ms()?});
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
        resource_released: true,
        coverage,
        result: record,
        outputs,
        cancellation: cancellation.evidence(),
    };
    write_once(&dir.join("terminal.json"), &json!(completed))?;
    group.disarm()?;
    write_once(&dir.join("completion.json"), &json!(completed))?;
    drop(lock);
    Ok(())
}
