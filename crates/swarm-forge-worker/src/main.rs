use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    env,
    ffi::OsString,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio as ProcessStdio},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use swarm_contracts::error::{Error, Result};
use swarm_process::{
    Group, module_child_belongs_to_owner, process_image_identity, write_private_new,
};

const PLAN_LIMIT: u64 = 1_048_576;
const AUTH_LIMIT: u64 = 16_384;
const RESULT_LIMIT: usize = 262_144;
const AUTHORIZATION_WAIT: Duration = Duration::from_secs(30);
const COMMAND_WAIT_GRACE: Duration = Duration::from_secs(5);
const OUTPUT_DRAIN_GRACE: Duration = Duration::from_secs(2);
const MAX_PROJECT_ID: usize = 256;
const MAX_REF_BYTES: usize = 900;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkPlan {
    schema_version: u32,
    kind: String,
    job_id: String,
    operation_id: String,
    owner_token: String,
    phase: Phase,
    intent: PublicationIntent,
    git_executable: PathBuf,
    #[serde(default)]
    git_executable_sha256: Option<String>,
    timeout_seconds: u64,
    max_output_bytes: usize,
    project: Project,
    push_endpoint: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Phase {
    PushOnce,
    ReadbackOnly,
    PatchOnce,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GitHubDescriptionPlan {
    schema_version: u32,
    kind: String,
    job_id: String,
    operation_id: String,
    owner_token: String,
    phase: Phase,
    gh_executable: PathBuf,
    gh_executable_sha256: String,
    timeout_seconds: u64,
    max_output_bytes: usize,
    host: String,
    owner: String,
    repository: String,
    pull_request_number: i64,
    title: String,
    body: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationIntent {
    operation_id: String,
    project_id: String,
    canonical_repository: String,
    attempt_id: String,
    task_revision: i64,
    admitted_gm_epoch: i64,
    submission_ref: String,
    accepted_operation_id: String,
    candidate_ref: String,
    candidate_sha256: String,
    commit: String,
    tree: String,
    remote_name: String,
    target_ref: String,
    expected_old_ref: Option<String>,
    expected_create: bool,
    force: bool,
    policy_revision: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Project {
    canonical_repository: String,
    repository_path: PathBuf,
    remote_name: String,
    policy_revision: String,
    target_refs: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Authorization {
    schema_version: u32,
    kind: String,
    job_id: String,
    operation_id: String,
    owner_token: String,
    plan_sha256: String,
    phase: Phase,
    authorized: bool,
}

#[derive(Debug, Serialize)]
struct WorkerResult {
    schema_version: u32,
    kind: String,
    job_id: String,
    operation_id: String,
    owner_token: String,
    plan_sha256: String,
    phase: Phase,
    authorized: bool,
    outcome: &'static str,
    reason: &'static str,
    remote_ref: Option<Value>,
    push_exit_code: Option<i32>,
    timed_out: bool,
    stderr_sha256: Option<String>,
    stderr_bytes: Option<u64>,
    process_tree_empty: bool,
    process_tree_unconfirmed: bool,
}

#[derive(Debug, Clone)]
enum RemoteRef {
    Missing,
    At(String),
}

impl RemoteRef {
    fn value(&self) -> Value {
        match self {
            Self::Missing => json!({"present": false}),
            Self::At(commit) => json!({"present": true, "commit": commit}),
        }
    }

    fn is_candidate(&self, intent: &PublicationIntent) -> bool {
        matches!(self, Self::At(commit) if commit.eq_ignore_ascii_case(&intent.commit))
    }

    fn is_expected(&self, intent: &PublicationIntent) -> bool {
        match (&intent.expected_old_ref, intent.expected_create, self) {
            (Some(expected), false, Self::At(actual)) => expected.eq_ignore_ascii_case(actual),
            (None, true, Self::Missing) => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
struct Capture {
    prefix: Vec<u8>,
    byte_count: u64,
    truncated: bool,
    sha256: String,
}

#[derive(Debug)]
struct NativeResult {
    exit_code: Option<i32>,
    successful: bool,
    timed_out: bool,
    stdout: Option<Capture>,
    stderr: Option<Capture>,
    ownership_confirmed: bool,
    input_complete: bool,
    process_tree_empty: bool,
}

#[derive(Debug)]
struct RunResult {
    phase: Phase,
    outcome: &'static str,
    reason: &'static str,
    remote_ref: Option<RemoteRef>,
    push_exit_code: Option<i32>,
    timed_out: bool,
    stderr_sha256: Option<String>,
    stderr_bytes: Option<u64>,
    process_tree_empty: bool,
    process_tree_unconfirmed: bool,
}

fn main() {
    if run_from_args(env::args_os().skip(1).collect()).is_err() {
        std::process::exit(2);
    }
}

fn run_from_args(args: Vec<OsString>) -> Result<()> {
    let paths = parse_args(args)?;
    let plan_bytes = read_bounded(&paths.plan, PLAN_LIMIT)?;
    let envelope: Value = serde_json::from_slice(&plan_bytes)
        .map_err(|_| Error::invalid("Forge worker plan is invalid"))?;
    match envelope.get("kind").and_then(Value::as_str) {
        Some("forge_publish") => run_forge_plan(&paths, &plan_bytes),
        Some("github_pr_description") => run_github_plan(&paths, &plan_bytes),
        _ => Err(Error::invalid("Forge worker kind is unsupported")),
    }
}

fn run_forge_plan(paths: &JobPaths, plan_bytes: &[u8]) -> Result<()> {
    let plan_sha256 = digest(plan_bytes);
    let plan: WorkPlan = serde_json::from_slice(plan_bytes)
        .map_err(|_| Error::invalid("Forge work plan is invalid"))?;
    validate_job_paths(paths)?;
    validate_plan(&plan)?;

    let owner = Group::enter_module(&plan.owner_token)?;
    let owner_record = json!({
        "version": 1,
        "token": plan.owner_token,
        "process": owner.identity,
    });
    write_private_new(&paths.owner, &serde_json::to_vec(&owner_record)?)?;

    let authorization = match wait_for_authorization(&paths.authorization)? {
        Some(bytes) => Some(
            serde_json::from_slice::<Authorization>(&bytes)
                .map_err(|_| Error::invalid("Forge authorization receipt is invalid"))?,
        ),
        None => None,
    };
    if let Some(authorization) = authorization.as_ref() {
        validate_authorization(
            &plan.kind,
            &plan.job_id,
            &plan.operation_id,
            &plan.owner_token,
            &plan_sha256,
            plan.phase,
            authorization,
        )?;
    }
    let authorized = authorization
        .as_ref()
        .is_some_and(|receipt| receipt.authorized);

    let run = match authorization {
        None => RunResult {
            phase: plan.phase,
            outcome: "unknown",
            reason: "authorization_not_received",
            remote_ref: None,
            push_exit_code: None,
            timed_out: false,
            stderr_sha256: None,
            stderr_bytes: None,
            process_tree_empty: owner.children_empty().unwrap_or(false),
            process_tree_unconfirmed: false,
        },
        Some(_) if authorized => execute(&plan, &owner),
        Some(_) => RunResult {
            phase: plan.phase,
            outcome: "not_authorized",
            reason: "authorization_denied",
            remote_ref: None,
            push_exit_code: None,
            timed_out: false,
            stderr_sha256: None,
            stderr_bytes: None,
            process_tree_empty: owner.children_empty().unwrap_or(false),
            process_tree_unconfirmed: false,
        },
    };
    let result = WorkerResult {
        schema_version: 1,
        kind: plan.kind,
        job_id: plan.job_id,
        operation_id: plan.operation_id,
        owner_token: plan.owner_token,
        plan_sha256,
        phase: run.phase,
        authorized,
        outcome: run.outcome,
        reason: run.reason,
        remote_ref: run.remote_ref.as_ref().map(RemoteRef::value),
        push_exit_code: run.push_exit_code,
        timed_out: run.timed_out,
        stderr_sha256: run.stderr_sha256,
        stderr_bytes: run.stderr_bytes,
        process_tree_empty: run.process_tree_empty,
        process_tree_unconfirmed: run.process_tree_unconfirmed,
    };
    let result_bytes = serde_json::to_vec(&result)?;
    if result_bytes.len() > RESULT_LIMIT {
        return Err(Error::invalid("Forge worker result exceeds its envelope"));
    }
    write_private_new(&paths.result, &result_bytes)?;
    Ok(())
}

fn run_github_plan(paths: &JobPaths, plan_bytes: &[u8]) -> Result<()> {
    let plan_sha256 = digest(plan_bytes);
    let plan: GitHubDescriptionPlan = serde_json::from_slice(plan_bytes)
        .map_err(|_| Error::invalid("GitHub worker plan is invalid"))?;
    validate_job_paths(paths)?;
    validate_github_plan(&plan)?;
    let owner = Group::enter_module(&plan.owner_token)?;
    let owner_record = json!({
        "version":1,
        "token":plan.owner_token,
        "process":owner.identity,
    });
    write_private_new(&paths.owner, &serde_json::to_vec(&owner_record)?)?;
    let authorization = match wait_for_authorization(&paths.authorization)? {
        Some(bytes) => Some(
            serde_json::from_slice::<Authorization>(&bytes)
                .map_err(|_| Error::invalid("GitHub authorization receipt is invalid"))?,
        ),
        None => None,
    };
    if let Some(authorization) = authorization.as_ref() {
        validate_authorization(
            &plan.kind,
            &plan.job_id,
            &plan.operation_id,
            &plan.owner_token,
            &plan_sha256,
            plan.phase,
            authorization,
        )?;
    }
    let authorized = authorization
        .as_ref()
        .is_some_and(|receipt| receipt.authorized);
    let run = match authorization {
        None => RunResult {
            phase: plan.phase,
            outcome: "unknown",
            reason: "authorization_not_received",
            remote_ref: None,
            push_exit_code: None,
            timed_out: false,
            stderr_sha256: None,
            stderr_bytes: None,
            process_tree_empty: owner.children_empty().unwrap_or(false),
            process_tree_unconfirmed: false,
        },
        Some(_) if !authorized => RunResult {
            phase: plan.phase,
            outcome: "not_authorized",
            reason: "authorization_denied",
            remote_ref: None,
            push_exit_code: None,
            timed_out: false,
            stderr_sha256: None,
            stderr_bytes: None,
            process_tree_empty: owner.children_empty().unwrap_or(false),
            process_tree_unconfirmed: false,
        },
        Some(_) => execute_github_patch(&plan, &owner),
    };
    let result = WorkerResult {
        schema_version: 1,
        kind: plan.kind,
        job_id: plan.job_id,
        operation_id: plan.operation_id,
        owner_token: plan.owner_token,
        plan_sha256,
        phase: run.phase,
        authorized,
        outcome: run.outcome,
        reason: run.reason,
        remote_ref: None,
        push_exit_code: run.push_exit_code,
        timed_out: run.timed_out,
        stderr_sha256: run.stderr_sha256,
        stderr_bytes: run.stderr_bytes,
        process_tree_empty: run.process_tree_empty,
        process_tree_unconfirmed: run.process_tree_unconfirmed,
    };
    let result_bytes = serde_json::to_vec(&result)?;
    if result_bytes.len() > RESULT_LIMIT {
        return Err(Error::invalid("GitHub worker result exceeds its envelope"));
    }
    write_private_new(&paths.result, &result_bytes)?;
    Ok(())
}

fn validate_github_plan(plan: &GitHubDescriptionPlan) -> Result<()> {
    if plan.schema_version != 1
        || plan.kind != "github_pr_description"
        || !valid_uuid(&plan.job_id)
        || !valid_uuid(&plan.owner_token)
        || !valid_identifier(&plan.operation_id, 128)
        || plan.phase != Phase::PatchOnce
        || !plan.gh_executable.is_absolute()
        || !plan.gh_executable.metadata()?.is_file()
        || !valid_digest(&plan.gh_executable_sha256)
        || !(1..=45).contains(&plan.timeout_seconds)
        || !(1..=4 * 1024 * 1024).contains(&plan.max_output_bytes)
        || !valid_host(&plan.host)
        || !valid_path_component(&plan.owner)
        || !valid_path_component(&plan.repository)
        || plan.pull_request_number <= 0
        || plan.title.trim().is_empty()
        || plan.title.len() > 256 * 1024
        || plan.body.len() > 256 * 1024
    {
        return Err(Error::invalid(
            "GitHub pull-request description plan failed its bounded identity checks",
        ));
    }
    Ok(())
}

fn execute_github_patch(plan: &GitHubDescriptionPlan, owner: &Group) -> RunResult {
    let native = match run_github_patch_command(plan, owner) {
        Ok(native) => native,
        Err(error) => {
            return RunResult {
                phase: plan.phase,
                outcome: "unknown",
                reason: if error.code == "GITHUB_CLI_IMAGE_CHANGED" {
                    "github_cli_image_changed"
                } else {
                    "github_cli_unavailable"
                },
                remote_ref: None,
                push_exit_code: None,
                timed_out: false,
                stderr_sha256: None,
                stderr_bytes: None,
                process_tree_empty: group_empty(owner),
                process_tree_unconfirmed: false,
            };
        }
    };
    let process_tree_unconfirmed = !native.process_tree_empty || !native.ownership_confirmed;
    RunResult {
        phase: plan.phase,
        outcome: "unknown",
        reason: if process_tree_unconfirmed {
            "github_process_tree_unconfirmed"
        } else if native.timed_out {
            "github_cli_timed_out"
        } else if !native.ownership_confirmed {
            "github_process_tree_unconfirmed"
        } else if !native.input_complete {
            "github_request_body_delivery_failed"
        } else if native.successful {
            "github_write_completed_readback_required"
        } else {
            "github_cli_failed_readback_required"
        },
        remote_ref: None,
        push_exit_code: native.exit_code,
        timed_out: native.timed_out,
        stderr_sha256: native.stderr.as_ref().map(|capture| capture.sha256.clone()),
        stderr_bytes: native.stderr.as_ref().map(|capture| capture.byte_count),
        process_tree_empty: native.process_tree_empty,
        process_tree_unconfirmed,
    }
}

fn run_github_patch_command(plan: &GitHubDescriptionPlan, owner: &Group) -> Result<NativeResult> {
    let executable_bytes = read_bounded(&plan.gh_executable, 128 * 1024 * 1024)?;
    if digest(&executable_bytes) != plan.gh_executable_sha256 {
        return Err(Error::new(
            "GITHUB_CLI_IMAGE_CHANGED",
            "the configured GitHub CLI image changed after admission",
        ));
    }
    let endpoint = format!(
        "/repos/{}/{}/pulls/{}",
        plan.owner, plan.repository, plan.pull_request_number
    );
    let input = serde_json::to_vec(&json!({"title":plan.title,"body":plan.body}))?;
    let mut command = Command::new(&plan.gh_executable);
    command
        .args([
            "api",
            endpoint.as_str(),
            "--hostname",
            plan.host.as_str(),
            "--method",
            "PATCH",
            "--input",
            "-",
        ])
        .stdin(ProcessStdio::piped())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command.spawn().map_err(|_| {
        Error::new(
            "GITHUB_CLI_UNAVAILABLE",
            "configured GitHub CLI could not start",
        )
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("GITHUB_CLI_IO", "GitHub CLI stdout pipe was unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::new("GITHUB_CLI_IO", "GitHub CLI stderr pipe was unavailable"))?;
    let owner_record = json!({
        "version":1,
        "token":plan.owner_token,
        "process":owner.identity,
    });
    let expected_image = plan.gh_executable.canonicalize().ok();
    let ownership_confirmed = process_image_identity(child.id()).is_ok_and(|identity| {
        let image_path = identity
            .get("image_path")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .and_then(|path| path.canonicalize().ok());
        module_child_belongs_to_owner(&owner_record, &identity).unwrap_or(false)
            && image_path == expected_image
            && identity.get("image_sha256").and_then(Value::as_str)
                == Some(plan.gh_executable_sha256.as_str())
    });
    let cap = plan.max_output_bytes;
    let stdout_reader = thread::spawn(move || drain_bounded(stdout, cap));
    let stderr_reader = thread::spawn(move || drain_bounded(stderr, cap));
    let input_writer = child
        .stdin
        .take()
        .map(|mut stdin| thread::spawn(move || stdin.write_all(&input)));
    let deadline = Instant::now() + Duration::from_secs(plan.timeout_seconds);
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(exit)) => break Some(exit),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                timed_out = true;
                let _ = child.kill();
                break wait_for_child(&mut child, COMMAND_WAIT_GRACE);
            }
            Err(_) => {
                let _ = child.kill();
                break wait_for_child(&mut child, COMMAND_WAIT_GRACE);
            }
        }
    };
    let stdout_capture = join_capture(stdout_reader);
    let stderr_capture = join_capture(stderr_reader);
    let readers_complete = stdout_capture.is_some() && stderr_capture.is_some();
    let (input_writer_complete, input_complete) = match input_writer {
        None => (true, false),
        Some(writer) => join_input_writer(writer),
    };
    let process_tree_empty = readers_complete
        && input_writer_complete
        && ownership_confirmed
        && owner.children_empty().unwrap_or(false);
    Ok(NativeResult {
        exit_code: status.as_ref().and_then(ExitStatus::code),
        successful: status.as_ref().is_some_and(ExitStatus::success),
        timed_out,
        stdout: stdout_capture,
        stderr: stderr_capture,
        ownership_confirmed,
        input_complete,
        process_tree_empty,
    })
}

fn join_input_writer(writer: JoinHandle<std::io::Result<()>>) -> (bool, bool) {
    let deadline = Instant::now() + OUTPUT_DRAIN_GRACE;
    while !writer.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if !writer.is_finished() {
        return (false, false);
    }
    match writer.join() {
        Ok(Ok(())) => (true, true),
        Ok(Err(_)) | Err(_) => (true, false),
    }
}

struct JobPaths {
    plan: PathBuf,
    owner: PathBuf,
    authorization: PathBuf,
    result: PathBuf,
}

fn parse_args(args: Vec<OsString>) -> Result<JobPaths> {
    if args.len() != 8 {
        return Err(Error::invalid(
            "Forge worker requires four fixed file arguments",
        ));
    }
    let mut values = args.into_iter();
    let mut path = |expected: &str| -> Result<PathBuf> {
        if values.next().as_deref() != Some(OsString::from(expected).as_os_str()) {
            return Err(Error::invalid("Forge worker arguments are invalid"));
        }
        values
            .next()
            .map(PathBuf::from)
            .ok_or_else(|| Error::invalid("Forge worker argument path is absent"))
    };
    let paths = JobPaths {
        plan: path("--plan")?,
        owner: path("--owner")?,
        authorization: path("--authorization")?,
        result: path("--result")?,
    };
    if values.next().is_some() {
        return Err(Error::invalid("Forge worker arguments are invalid"));
    }
    Ok(paths)
}

fn validate_job_paths(paths: &JobPaths) -> Result<()> {
    for (path, expected) in [
        (&paths.plan, "plan.json"),
        (&paths.owner, "owner.json"),
        (&paths.authorization, "authorization.json"),
        (&paths.result, "result.json"),
    ] {
        if !path.is_absolute() || path.file_name().and_then(|name| name.to_str()) != Some(expected)
        {
            return Err(Error::invalid(
                "Forge worker paths are not canonical job paths",
            ));
        }
    }
    let directory = paths
        .plan
        .parent()
        .ok_or_else(|| Error::invalid("Forge job directory is absent"))?
        .canonicalize()?;
    if paths
        .owner
        .parent()
        .and_then(|path| path.canonicalize().ok())
        .as_deref()
        != Some(directory.as_path())
        || paths
            .authorization
            .parent()
            .and_then(|path| path.canonicalize().ok())
            .as_deref()
            != Some(directory.as_path())
        || paths
            .result
            .parent()
            .and_then(|path| path.canonicalize().ok())
            .as_deref()
            != Some(directory.as_path())
    {
        return Err(Error::invalid(
            "Forge worker paths must share one job directory",
        ));
    }
    Ok(())
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(Error::invalid("Forge worker file exceeds its envelope"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(Error::invalid("Forge worker file exceeds its envelope"));
    }
    Ok(bytes)
}

fn wait_for_authorization(path: &Path) -> Result<Option<Vec<u8>>> {
    let deadline = Instant::now() + AUTHORIZATION_WAIT;
    loop {
        match path.try_exists() {
            Ok(true) => return read_bounded(path, AUTH_LIMIT).map(Some),
            Ok(false) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(false) => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }
}

fn validate_authorization(
    kind: &str,
    job_id: &str,
    operation_id: &str,
    owner_token: &str,
    plan_sha256: &str,
    phase: Phase,
    auth: &Authorization,
) -> Result<()> {
    if auth.schema_version != 1
        || auth.kind != kind
        || auth.job_id != job_id
        || auth.operation_id != operation_id
        || auth.owner_token != owner_token
        || auth.plan_sha256 != plan_sha256
        || auth.phase != phase
    {
        return Err(Error::invalid(
            "Forge authorization does not match the exact work plan",
        ));
    }
    Ok(())
}

fn validate_plan(plan: &WorkPlan) -> Result<()> {
    if plan.schema_version != 1
        || plan.kind != "forge_publish"
        || !valid_uuid(&plan.job_id)
        || !valid_uuid(&plan.owner_token)
        || !valid_identifier(&plan.operation_id, 128)
        || plan.operation_id != plan.intent.operation_id
        || !plan.git_executable.is_absolute()
        || !plan.git_executable.metadata()?.is_file()
        || plan
            .git_executable_sha256
            .as_deref()
            .is_some_and(|digest| !valid_digest(digest))
        || !plan.project.repository_path.is_absolute()
        || !plan.project.repository_path.metadata()?.is_dir()
        || !(1..=900).contains(&plan.timeout_seconds)
        || !(1..=256 * 1024).contains(&plan.max_output_bytes)
    {
        return Err(Error::invalid(
            "Forge work plan failed bounded identity checks",
        ));
    }
    let canonical = canonical_repository(&plan.intent.canonical_repository)?;
    if canonical != plan.intent.canonical_repository
        || canonical_repository(&plan.project.canonical_repository)? != canonical
        || plan.intent.project_id.is_empty()
        || plan.intent.project_id.len() > MAX_PROJECT_ID
        || plan.intent.remote_name != plan.project.remote_name
        || plan.intent.policy_revision != plan.project.policy_revision
        || !plan
            .project
            .target_refs
            .iter()
            .any(|target| target == &plan.intent.target_ref)
        || !matches!(plan.phase, Phase::PushOnce | Phase::ReadbackOnly)
        || plan.phase == Phase::PushOnce && plan.push_endpoint.as_deref().is_none_or(str::is_empty)
        || plan.phase == Phase::ReadbackOnly && plan.push_endpoint.is_some()
    {
        return Err(Error::invalid(
            "Forge work plan differs from trusted project settings",
        ));
    }
    let intent = &plan.intent;
    if !valid_identifier(&intent.attempt_id, 256)
        || !valid_identifier(&intent.submission_ref, 512)
        || !valid_identifier(&intent.accepted_operation_id, 128)
        || !valid_identifier(&intent.candidate_ref, 512)
        || !valid_digest(&intent.candidate_sha256)
        || intent.task_revision < 1
        || intent.admitted_gm_epoch < 0
        || intent.force
        || !valid_object_id(&intent.commit)
        || !valid_object_id(&intent.tree)
        || !valid_branch_ref(&intent.target_ref)
        || !valid_remote_name(&intent.remote_name)
        || intent.expected_create == intent.expected_old_ref.is_some()
        || intent
            .expected_old_ref
            .as_deref()
            .is_some_and(|value| !valid_object_id(value))
    {
        return Err(Error::invalid("Forge publication intent is invalid"));
    }
    Ok(())
}

fn execute(plan: &WorkPlan, owner: &Group) -> RunResult {
    let mut evidence = Vec::new();
    let endpoint = match validate_remote(plan, owner, &mut evidence) {
        Ok(endpoint) => endpoint,
        Err(result) => return result.with_evidence(plan.phase, owner, &evidence),
    };
    if plan.phase == Phase::PushOnce && plan.push_endpoint.as_deref() != Some(endpoint.as_str()) {
        return unknown(
            plan.phase,
            "push_endpoint_changed_after_authorization",
            None,
            None,
            false,
            owner,
            &evidence,
        );
    }
    let before = match remote_ref_at(plan, owner, &endpoint, &mut evidence) {
        Ok(readback) => readback,
        Err(result) => return result.with_evidence(plan.phase, owner, &evidence),
    };
    if plan.phase == Phase::ReadbackOnly {
        return if before.is_candidate(&plan.intent) {
            applied(
                plan.phase,
                "exact_candidate_ref_observed",
                before,
                None,
                false,
                &evidence,
            )
        } else {
            unknown(
                plan.phase,
                "restart_readback_did_not_prove_publication",
                Some(before),
                None,
                false,
                owner,
                &evidence,
            )
        };
    }
    if !before.is_expected(&plan.intent) {
        return unknown(
            plan.phase,
            "expected_ref_changed_before_push",
            Some(before),
            None,
            false,
            owner,
            &evidence,
        );
    }

    let refspec = format!("{}:{}", plan.intent.commit, plan.intent.target_ref);
    let push = match run_git(
        plan,
        owner,
        &[
            "push".to_owned(),
            "--porcelain".to_owned(),
            "--no-verify".to_owned(),
            "--no-follow-tags".to_owned(),
            "--recurse-submodules=no".to_owned(),
            "--receive-pack=git-receive-pack".to_owned(),
            endpoint.clone(),
            refspec,
        ],
    ) {
        Ok(result) => result,
        Err(_) => {
            let after = match remote_ref_at(plan, owner, &endpoint, &mut evidence) {
                Ok(readback) => readback,
                Err(result) => return result.with_evidence(plan.phase, owner, &evidence),
            };
            if after.is_candidate(&plan.intent) {
                return applied(
                    plan.phase,
                    "exact_candidate_ref_observed",
                    after,
                    None,
                    false,
                    &evidence,
                );
            }
            return unknown(
                plan.phase,
                "push_process_unobservable",
                Some(after),
                None,
                false,
                owner,
                &evidence,
            );
        }
    };
    evidence.push(push.evidence());
    if !push.process_tree_empty {
        return unknown(
            plan.phase,
            "git_process_tree_unconfirmed",
            None,
            push.stderr.as_ref(),
            push.timed_out,
            owner,
            &evidence,
        );
    }
    let after = match remote_ref_at(plan, owner, &endpoint, &mut evidence) {
        Ok(readback) => readback,
        Err(result) => {
            return RunResult {
                phase: plan.phase,
                outcome: "unknown",
                reason: "readback_failed_after_push",
                remote_ref: None,
                push_exit_code: push.exit_code,
                timed_out: push.timed_out,
                stderr_sha256: push.stderr.as_ref().map(|capture| capture.sha256.clone()),
                stderr_bytes: push.stderr.as_ref().map(|capture| capture.byte_count),
                process_tree_empty: result.process_tree_empty,
                process_tree_unconfirmed: !result.process_tree_empty,
            };
        }
    };
    if after.is_candidate(&plan.intent) {
        return applied(
            plan.phase,
            "exact_candidate_ref_observed",
            after,
            push.exit_code,
            push.timed_out,
            &evidence,
        )
        .with_stderr(push.stderr.as_ref());
    }
    if !push.timed_out
        && push.exit_code.is_some_and(|code| code != 0)
        && after.is_expected(&plan.intent)
    {
        return RunResult {
            phase: plan.phase,
            outcome: "failed",
            reason: "git_rejected_and_expected_ref_remains",
            remote_ref: Some(after),
            push_exit_code: push.exit_code,
            timed_out: false,
            stderr_sha256: push.stderr.as_ref().map(|capture| capture.sha256.clone()),
            stderr_bytes: push.stderr.as_ref().map(|capture| capture.byte_count),
            process_tree_empty: group_empty(owner),
            process_tree_unconfirmed: false,
        };
    }
    RunResult {
        phase: plan.phase,
        outcome: "unknown",
        reason: "push_outcome_not_confirmed_by_exact_readback",
        remote_ref: Some(after),
        push_exit_code: push.exit_code,
        timed_out: push.timed_out,
        stderr_sha256: push.stderr.as_ref().map(|capture| capture.sha256.clone()),
        stderr_bytes: push.stderr.as_ref().map(|capture| capture.byte_count),
        process_tree_empty: group_empty(owner),
        process_tree_unconfirmed: false,
    }
}

fn validate_remote(
    plan: &WorkPlan,
    owner: &Group,
    evidence: &mut Vec<Value>,
) -> std::result::Result<String, RunResult> {
    let urls = run_git(
        plan,
        owner,
        &[
            "remote".into(),
            "get-url".into(),
            "--push".into(),
            "--all".into(),
            plan.intent.remote_name.clone(),
        ],
    );
    let output = match urls {
        Ok(output) if output.readable_success() => output,
        Ok(output) => {
            evidence.push(output.evidence());
            return Err(unknown(
                plan.phase,
                "trusted_remote_unavailable",
                None,
                None,
                output.timed_out,
                owner,
                evidence,
            ));
        }
        Err(_) => {
            return Err(unknown(
                plan.phase,
                "trusted_remote_unavailable",
                None,
                None,
                false,
                owner,
                evidence,
            ));
        }
    };
    evidence.push(output.evidence());
    let endpoint = parse_single_endpoint(
        output
            .stdout
            .as_ref()
            .map(|capture| capture.prefix.as_slice()),
    );
    let Some(endpoint) = endpoint.filter(|endpoint| {
        canonical_repository_from_remote(endpoint).ok().as_deref()
            == Some(plan.intent.canonical_repository.as_str())
    }) else {
        return Err(unknown(
            plan.phase,
            "trusted_remote_identity_mismatch",
            None,
            None,
            false,
            owner,
            evidence,
        ));
    };
    let mirror_key = format!("remote.{}.mirror", plan.intent.remote_name);
    match git_config_values(plan, owner, &mirror_key, evidence) {
        Ok(values) if !values.iter().any(|value| is_truthy(value)) => {}
        _ => {
            return Err(unknown(
                plan.phase,
                "trusted_remote_configuration_unsafe_or_unavailable",
                None,
                None,
                false,
                owner,
                evidence,
            ));
        }
    }
    for key in [
        "push.pushOption".to_owned(),
        "core.sshCommand".to_owned(),
        format!("remote.{}.vcs", plan.intent.remote_name),
        format!("remote.{}.uploadpack", plan.intent.remote_name),
        format!("remote.{}.receivepack", plan.intent.remote_name),
    ] {
        match git_config_values(plan, owner, &key, evidence) {
            Ok(values) if values.is_empty() => {}
            _ => {
                return Err(unknown(
                    plan.phase,
                    "trusted_remote_configuration_unsafe_or_unavailable",
                    None,
                    None,
                    false,
                    owner,
                    evidence,
                ));
            }
        }
    }
    Ok(endpoint)
}

trait ResultEvidence {
    fn with_evidence(self, phase: Phase, owner: &Group, evidence: &[Value]) -> Self;
}

impl ResultEvidence for RunResult {
    fn with_evidence(mut self, phase: Phase, owner: &Group, evidence: &[Value]) -> Self {
        self.phase = phase;
        self.process_tree_empty = group_empty(owner)
            && evidence
                .iter()
                .all(|item| item.get("process_tree_empty").and_then(Value::as_bool) == Some(true));
        self.process_tree_unconfirmed = !self.process_tree_empty;
        self
    }
}

fn git_config_values(
    plan: &WorkPlan,
    owner: &Group,
    key: &str,
    evidence: &mut Vec<Value>,
) -> Result<Vec<String>> {
    let result = run_git(
        plan,
        owner,
        &["config".into(), "--get-all".into(), key.to_owned()],
    )?;
    evidence.push(result.evidence());
    if result.timed_out || !result.process_tree_empty {
        return Err(Error::new(
            "FORGE_CONFIG_UNAVAILABLE",
            "Git config readback was incomplete",
        ));
    }
    let capture = result
        .stdout
        .as_ref()
        .filter(|capture| !capture.truncated)
        .ok_or_else(|| {
            Error::new(
                "FORGE_CONFIG_UNAVAILABLE",
                "Git config output was incomplete",
            )
        })?;
    let text = std::str::from_utf8(&capture.prefix)
        .map_err(|_| Error::new("FORGE_CONFIG_UNAVAILABLE", "Git config output was invalid"))?;
    if result.successful {
        Ok(text.lines().map(str::to_owned).collect())
    } else if result.exit_code == Some(1) && capture.prefix.is_empty() {
        Ok(Vec::new())
    } else {
        Err(Error::new(
            "FORGE_CONFIG_UNAVAILABLE",
            "Git config readback failed",
        ))
    }
}

fn remote_ref_at(
    plan: &WorkPlan,
    owner: &Group,
    endpoint: &str,
    evidence: &mut Vec<Value>,
) -> std::result::Result<RemoteRef, RunResult> {
    let output = match run_git(
        plan,
        owner,
        &[
            "ls-remote".into(),
            "--refs".into(),
            "--upload-pack=git-upload-pack".into(),
            endpoint.into(),
            plan.intent.target_ref.clone(),
        ],
    ) {
        Ok(output) if output.readable_success() => output,
        Ok(output) => {
            evidence.push(output.evidence());
            return Err(unknown(
                plan.phase,
                if output.timed_out {
                    "git_readback_timed_out"
                } else {
                    "git_readback_unavailable"
                },
                None,
                None,
                output.timed_out,
                owner,
                evidence,
            ));
        }
        Err(_) => {
            return Err(unknown(
                plan.phase,
                "git_readback_unavailable",
                None,
                None,
                false,
                owner,
                evidence,
            ));
        }
    };
    evidence.push(output.evidence());
    parse_exact_ref(
        output
            .stdout
            .as_ref()
            .map(|capture| capture.prefix.as_slice()),
        &plan.intent.target_ref,
    )
    .ok_or_else(|| {
        unknown(
            plan.phase,
            "remote_ref_readback_invalid",
            None,
            None,
            false,
            owner,
            evidence,
        )
    })
}

fn run_git(plan: &WorkPlan, owner: &Group, args: &[String]) -> Result<NativeResult> {
    if let Some(expected) = plan.git_executable_sha256.as_deref() {
        let actual = digest(&read_bounded(&plan.git_executable, 128 * 1024 * 1024)?);
        if actual != expected {
            return Err(Error::new(
                "FORGE_CONFIG_CHANGED",
                "selected Git executable changed after admission",
            ));
        }
    }
    let mut command = Command::new(&plan.git_executable);
    command
        .args(["--no-optional-locks", "--no-replace-objects"])
        .args(["-c", "core.fsmonitor=false", "-C"])
        .arg(&plan.project.repository_path)
        .args(args)
        .current_dir(&plan.project.repository_path)
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0");
    sanitize_git_environment(&mut command);
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("FORGE_GIT_START", "configured Git process could not start"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("FORGE_GIT_IO", "Git stdout pipe was unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::new("FORGE_GIT_IO", "Git stderr pipe was unavailable"))?;
    let owner_record = json!({
        "version":1,
        "token":plan.owner_token,
        "process":owner.identity,
    });
    let ownership_confirmed = process_image_identity(child.id())
        .and_then(|identity| module_child_belongs_to_owner(&owner_record, &identity))
        .unwrap_or(false);
    let cap = plan.max_output_bytes;
    let stdout_reader = thread::spawn(move || drain_bounded(stdout, cap));
    let stderr_reader = thread::spawn(move || drain_bounded(stderr, cap));
    let deadline = Instant::now() + Duration::from_secs(plan.timeout_seconds);
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(exit)) => break Some(exit),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                timed_out = true;
                let _ = child.kill();
                break wait_for_child(&mut child, COMMAND_WAIT_GRACE);
            }
            Err(_) => {
                let _ = child.kill();
                break wait_for_child(&mut child, COMMAND_WAIT_GRACE);
            }
        }
    };
    let stdout_capture = join_capture(stdout_reader);
    let stderr_capture = join_capture(stderr_reader);
    let readers_complete = stdout_capture.is_some() && stderr_capture.is_some();
    let process_tree_empty =
        readers_complete && ownership_confirmed && owner.children_empty().unwrap_or(false);
    Ok(NativeResult {
        exit_code: status.as_ref().and_then(ExitStatus::code),
        successful: status.as_ref().is_some_and(ExitStatus::success),
        timed_out,
        stdout: stdout_capture,
        stderr: stderr_capture,
        ownership_confirmed,
        input_complete: true,
        process_tree_empty,
    })
}

fn wait_for_child(child: &mut std::process::Child, grace: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + grace;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            _ => return None,
        }
    }
}

fn join_capture(reader: JoinHandle<std::io::Result<Capture>>) -> Option<Capture> {
    let deadline = Instant::now() + OUTPUT_DRAIN_GRACE;
    while !reader.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if !reader.is_finished() {
        return None;
    }
    reader.join().ok()?.ok()
}

fn drain_bounded<R: Read>(mut reader: R, cap: usize) -> std::io::Result<Capture> {
    let mut prefix = Vec::with_capacity(cap.min(8192));
    let mut byte_count = 0_u64;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        byte_count = byte_count.saturating_add(count as u64);
        let keep = cap.saturating_sub(prefix.len()).min(count);
        prefix.extend_from_slice(&buffer[..keep]);
        digest.update(&buffer[..keep]);
    }
    Ok(Capture {
        truncated: byte_count > prefix.len() as u64,
        prefix,
        byte_count,
        sha256: format!("{:x}", digest.finalize()),
    })
}

fn sanitize_git_environment(command: &mut Command) {
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_NAMESPACE",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "GIT_CONFIG",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
    ] {
        command.env_remove(key);
    }
    for (key, _) in env::vars_os() {
        let folded = key.to_string_lossy().to_ascii_uppercase();
        if folded.starts_with("GIT_CONFIG_KEY_")
            || folded.starts_with("GIT_CONFIG_VALUE_")
            || folded.starts_with("GIT_TRACE")
            || folded == "GIT_CURL_VERBOSE"
        {
            command.env_remove(key);
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
}

impl NativeResult {
    fn readable_success(&self) -> bool {
        self.successful
            && !self.timed_out
            && self.ownership_confirmed
            && self.process_tree_empty
            && self.stdout.as_ref().is_some_and(|output| !output.truncated)
    }

    fn evidence(&self) -> Value {
        json!({
            "exit_code": self.exit_code,
            "timed_out": self.timed_out,
            "process_tree_empty": self.process_tree_empty,
            "stdout_truncated": self.stdout.as_ref().is_none_or(|output| output.truncated),
            "stderr_bytes": self.stderr.as_ref().map(|output| output.byte_count),
            "stderr_sha256": self.stderr.as_ref().map(|output| output.sha256.as_str()),
            "stderr_truncated": self.stderr.as_ref().is_none_or(|output| output.truncated),
            "ownership_confirmed": self.ownership_confirmed,
        })
    }
}

impl RunResult {
    fn with_stderr(mut self, stderr: Option<&Capture>) -> Self {
        self.stderr_sha256 = stderr.map(|capture| capture.sha256.clone());
        self.stderr_bytes = stderr.map(|capture| capture.byte_count);
        self
    }
}

fn applied(
    phase: Phase,
    reason: &'static str,
    readback: RemoteRef,
    push_exit_code: Option<i32>,
    timed_out: bool,
    _evidence: &[Value],
) -> RunResult {
    RunResult {
        phase,
        outcome: "applied",
        reason,
        remote_ref: Some(readback),
        push_exit_code,
        timed_out,
        stderr_sha256: None,
        stderr_bytes: None,
        process_tree_empty: true,
        process_tree_unconfirmed: false,
    }
}

fn unknown(
    phase: Phase,
    reason: &'static str,
    readback: Option<RemoteRef>,
    stderr: Option<&Capture>,
    timed_out: bool,
    owner: &Group,
    evidence: &[Value],
) -> RunResult {
    let process_tree_empty = group_empty(owner)
        && evidence
            .iter()
            .all(|item| item.get("process_tree_empty").and_then(Value::as_bool) == Some(true));
    RunResult {
        phase,
        outcome: "unknown",
        reason,
        remote_ref: readback,
        push_exit_code: None,
        timed_out,
        stderr_sha256: stderr.map(|capture| capture.sha256.clone()),
        stderr_bytes: stderr.map(|capture| capture.byte_count),
        process_tree_empty,
        process_tree_unconfirmed: !process_tree_empty,
    }
}

fn group_empty(owner: &Group) -> bool {
    owner.children_empty().unwrap_or(false)
}

fn parse_single_endpoint(stdout: Option<&[u8]>) -> Option<String> {
    let text = std::str::from_utf8(stdout?).ok()?;
    let lines: Vec<_> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if lines.len() != 1 || lines[0].len() > 4096 || lines[0].chars().any(char::is_control) {
        return None;
    }
    Some(lines[0].to_owned())
}

fn parse_exact_ref(stdout: Option<&[u8]>, target_ref: &str) -> Option<RemoteRef> {
    let text = std::str::from_utf8(stdout?).ok()?;
    let lines: Vec<_> = text.lines().filter(|line| !line.is_empty()).collect();
    if lines.is_empty() {
        return Some(RemoteRef::Missing);
    }
    if lines.len() != 1 {
        return None;
    }
    let (oid, name) = lines[0].split_once('\t')?;
    if name != target_ref || !valid_object_id(oid) {
        return None;
    }
    Some(RemoteRef::At(oid.to_ascii_lowercase()))
}

fn valid_uuid(value: &str) -> bool {
    value.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| value.as_bytes()[index] == b'-')
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
}

fn valid_identifier(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

fn valid_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_remote_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && !value.contains("..")
        && !value.ends_with('.')
}

fn valid_branch_ref(value: &str) -> bool {
    let Some(tail) = value.strip_prefix("refs/heads/") else {
        return false;
    };
    if tail.is_empty()
        || tail.len() > MAX_REF_BYTES
        || tail.starts_with('/')
        || tail.ends_with('/')
        || tail.contains("//")
        || tail.contains("..")
        || tail.contains("@{")
        || tail.ends_with('.')
        || tail.chars().any(|ch| {
            ch.is_control() || matches!(ch, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
    {
        return false;
    }
    tail.split('/')
        .all(|part| !part.is_empty() && part != "." && part != ".." && !part.ends_with(".lock"))
}

fn canonical_repository(value: &str) -> Result<String> {
    let parts: Vec<_> = value.split('/').collect();
    if parts.len() < 3
        || !valid_host(parts[0])
        || parts[1..].iter().any(|part| !valid_path_component(part))
    {
        return Err(Error::invalid("canonical Forge repository is invalid"));
    }
    Ok(parts
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("/"))
}

fn valid_host(value: &str) -> bool {
    let host = match value.rsplit_once(':') {
        Some((host, port)) => {
            let Ok(port) = port.parse::<u16>() else {
                return false;
            };
            if port == 0 || host.contains(':') {
                return false;
            }
            host
        }
        None => value,
    };
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn valid_path_component(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && !value.ends_with('.')
        && !value.contains(".lock")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn canonical_repository_from_remote(value: &str) -> Result<String> {
    let (host, path) = if let Some(rest) = value.strip_prefix("https://") {
        let (authority, path) = rest
            .split_once('/')
            .ok_or_else(|| Error::invalid("remote URL is invalid"))?;
        if authority.is_empty() || authority.contains('@') {
            return Err(Error::invalid("remote URL is invalid"));
        }
        (strip_default_port(authority, 443), path)
    } else if let Some(rest) = value.strip_prefix("ssh://") {
        let (authority, path) = rest
            .split_once('/')
            .ok_or_else(|| Error::invalid("remote URL is invalid"))?;
        let (user, host) = authority
            .rsplit_once('@')
            .ok_or_else(|| Error::invalid("remote URL is invalid"))?;
        if user != "git" {
            return Err(Error::invalid("remote URL is invalid"));
        }
        (strip_default_port(host, 22), path)
    } else if !value.contains("://") {
        let (authority, path) = value
            .split_once(':')
            .ok_or_else(|| Error::invalid("remote URL is invalid"))?;
        let (user, host) = authority
            .rsplit_once('@')
            .ok_or_else(|| Error::invalid("remote URL is invalid"))?;
        if user != "git" || host.contains('/') {
            return Err(Error::invalid("remote URL is invalid"));
        }
        (host, path)
    } else {
        return Err(Error::invalid("remote URL is invalid"));
    };
    if !valid_host(host)
        || path
            .chars()
            .any(|ch| matches!(ch, '?' | '#' | '%' | '@' | '\\'))
    {
        return Err(Error::invalid("remote URL is invalid"));
    }
    let path = path.trim_start_matches('/').trim_end_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    canonical_repository(&format!("{host}/{path}"))
}

fn strip_default_port(host_port: &str, default_port: u16) -> &str {
    host_port
        .rsplit_once(':')
        .and_then(|(host, port)| (port == default_port.to_string()).then_some(host))
        .unwrap_or(host_port)
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "yes" | "on" | "1"
    )
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
