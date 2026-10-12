//! Narrow, configured Git ref publication support.
//!
//! This module only publishes an already accepted source-snapshot commit. It
//! does not create or merge pull requests, choose an arbitrary repository, or
//! own credentials. The local ForgeConfig is the authority for repository
//! paths, remote aliases, target refs and policy revisions.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::ExitStatus,
};

#[cfg(target_os = "linux")]
use std::{
    process::{Child, ChildStderr, ChildStdout, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(all(target_os = "linux", test))]
use std::io::Read;

#[cfg(target_os = "linux")]
use swarm_process::capture::{PipeRead, PollableRead};

#[cfg(windows)]
#[path = "forge/windows.rs"]
mod windows;

const DEFAULT_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 32 * 1024;
const MAX_OUTPUT_BYTES: usize = 256 * 1024;
const MAX_TIMEOUT_SECONDS: u64 = 15 * 60;

/// Local, operator-owned mapping from Task project IDs to a single canonical
/// repository and its configured local Git checkout. Disabled by default.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForgeConfig {
    pub enabled: bool,
    /// Must resolve to a concrete executable before publication can be enabled.
    pub git_executable: PathBuf,
    pub timeout_seconds: u64,
    pub max_output_bytes: usize,
    pub projects: BTreeMap<String, ForgeProject>,
}

impl Default for ForgeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            git_executable: PathBuf::from("git"),
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            projects: BTreeMap::new(),
        }
    }
}

/// Trusted project-to-repository binding. `canonical_repository` is a
/// credential-free `host/owner/repository` identity; nested owner paths are
/// supported for forge installations that use groups/subgroups.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForgeProject {
    pub canonical_repository: String,
    pub repository_path: PathBuf,
    pub remote_name: String,
    pub policy_revision: String,
    /// Exact full refs only. The API cannot choose an unconfigured destination.
    pub target_refs: Vec<String>,
}

impl ForgeConfig {
    /// Resolve configured paths against the directory containing the config
    /// file, then canonicalize them so execution never depends on the current
    /// working directory or a caller-supplied path.
    pub fn resolve_paths(&mut self, config_dir: &Path) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        self.git_executable = resolve_config_path(config_dir, &self.git_executable, false)?;
        for project in self.projects.values_mut() {
            project.repository_path =
                resolve_config_path(config_dir, &project.repository_path, true)?;
        }
        self.validate()
    }

    /// Validate the local publication boundary. This is also called at
    /// execution time so malformed or incomplete manually built configs fail
    /// closed even if the TOML loader was bypassed.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if !self.git_executable.is_absolute()
            || !self
                .git_executable
                .metadata()
                .is_ok_and(|metadata| metadata.is_file())
        {
            return Err(forge_config_error(
                "enabled forge requires an absolute, existing Git executable",
            ));
        }
        if self.timeout_seconds == 0
            || self.timeout_seconds > MAX_TIMEOUT_SECONDS
            || self.max_output_bytes == 0
            || self.max_output_bytes > MAX_OUTPUT_BYTES
        {
            return Err(forge_config_error(
                "forge timeout/output limits are outside their supported bounds",
            ));
        }
        if self.projects.is_empty() {
            return Err(forge_config_error(
                "enabled forge requires at least one project mapping",
            ));
        }
        for (project_id, project) in &self.projects {
            if project_id.trim().is_empty()
                || project_id.len() > 128
                || has_control(project_id)
                || !project.repository_path.is_absolute()
                || !project
                    .repository_path
                    .metadata()
                    .is_ok_and(|metadata| metadata.is_dir())
                || canonical_repository(&project.canonical_repository).is_err()
                || !valid_remote_name(&project.remote_name)
                || project.policy_revision.trim().is_empty()
                || project.policy_revision.len() > 128
                || has_control(&project.policy_revision)
                || project.target_refs.is_empty()
            {
                return Err(forge_config_error(
                    "forge project mapping is incomplete or invalid",
                ));
            }
            let mut refs = std::collections::BTreeSet::new();
            for target_ref in &project.target_refs {
                if !valid_branch_ref(target_ref) || !refs.insert(target_ref) {
                    return Err(forge_config_error(
                        "forge target refs must be unique full refs/heads names",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn project(&self, project_id: &str) -> Result<&ForgeProject> {
        if !self.enabled {
            return Err(Error::new(
                "FORGE_DISABLED",
                "forge publication is disabled by local configuration",
            ));
        }
        self.validate()?;
        self.projects.get(project_id).ok_or_else(|| {
            Error::new(
                "FORGE_PROJECT_UNCONFIGURED",
                "Task project has no trusted forge repository mapping",
            )
        })
    }
}

fn resolve_config_path(config_dir: &Path, path: &Path, directory: bool) -> Result<PathBuf> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    };
    let resolved = fs::canonicalize(candidate)
        .map_err(|_| forge_config_error("configured forge path could not be resolved"))?;
    let valid_kind = fs::metadata(&resolved)
        .map(|metadata| {
            if directory {
                metadata.is_dir()
            } else {
                metadata.is_file()
            }
        })
        .unwrap_or(false);
    if !valid_kind {
        return Err(forge_config_error(
            "configured forge path has the wrong kind",
        ));
    }
    Ok(resolved)
}

fn forge_config_error(message: &str) -> Error {
    Error::new("FORGE_CONFIG", message)
}

/// Caller-owned idempotent request. Repository paths, Git executable, remote
/// URLs and credentials intentionally have no request fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublishRefRequest {
    pub client_request_id: String,
    pub attempt_id: String,
    pub expected_revision: i64,
    pub submission_ref: String,
    pub accepted_operation_id: String,
    pub candidate_ref: String,
    pub expected_policy_revision: String,
    pub target_ref: String,
    /// Exact old object ID for an update; absent only for an explicitly
    /// requested create (`expected_create = true`).
    pub expected_old_ref: Option<String>,
    pub expected_create: bool,
}

impl PublishRefRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        let request: Self = serde_json::from_value(value.clone())
            .map_err(|_| Error::invalid("invalid forge.publish_ref request"))?;
        for (field, text) in [
            ("client_request_id", request.client_request_id.as_str()),
            ("attempt_id", request.attempt_id.as_str()),
            ("submission_ref", request.submission_ref.as_str()),
            (
                "accepted_operation_id",
                request.accepted_operation_id.as_str(),
            ),
            ("candidate_ref", request.candidate_ref.as_str()),
            (
                "expected_policy_revision",
                request.expected_policy_revision.as_str(),
            ),
            ("target_ref", request.target_ref.as_str()),
        ] {
            if text.trim().is_empty() || text.len() > 512 || has_control(text) {
                return Err(Error::invalid(format!("invalid {field}")));
            }
        }
        if request.expected_revision < 1 || !valid_branch_ref(&request.target_ref) {
            return Err(Error::invalid(
                "forge publication requires a positive Task revision and full branch ref",
            ));
        }
        if request.expected_create == request.expected_old_ref.is_some() {
            return Err(Error::invalid(
                "provide exactly one of expected_old_ref or expected_create=true",
            ));
        }
        if let Some(expected) = request.expected_old_ref.as_deref()
            && !valid_object_id(expected)
        {
            return Err(Error::invalid(
                "expected_old_ref must be an exact full Git object ID",
            ));
        }
        Ok(request)
    }
}

/// Immutable effective intent persisted on the Operation before any remote
/// write. It excludes local path, remote URL, credentials and process output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationIntent {
    pub operation_id: String,
    pub project_id: String,
    pub canonical_repository: String,
    pub attempt_id: String,
    pub task_revision: i64,
    /// GM designation generation frozen with admission. Zero represents the
    /// operator-only state before a GM has ever been designated.
    pub admitted_gm_epoch: i64,
    pub submission_ref: String,
    pub accepted_operation_id: String,
    pub candidate_ref: String,
    pub candidate_sha256: String,
    pub commit: String,
    pub tree: String,
    pub remote_name: String,
    pub target_ref: String,
    pub expected_old_ref: Option<String>,
    pub expected_create: bool,
    pub force: bool,
    pub policy_revision: String,
}

impl PublicationIntent {
    pub fn validate(&self) -> Result<()> {
        if self.force
            || self.task_revision < 1
            || self.admitted_gm_epoch < 0
            || !valid_branch_ref(&self.target_ref)
            || !valid_remote_name(&self.remote_name)
            || canonical_repository(&self.canonical_repository).is_err()
            || !valid_object_id(&self.commit)
            || !valid_object_id(&self.tree)
            || !valid_digest(&self.candidate_sha256)
            || self.expected_create == self.expected_old_ref.is_some()
            || self
                .expected_old_ref
                .as_deref()
                .is_some_and(|expected| !valid_object_id(expected))
        {
            return Err(Error::new(
                "FORGE_INTENT_INVALID",
                "saved publication intent failed its non-force identity checks",
            ));
        }
        Ok(())
    }

    pub(crate) fn from_request(
        operation_id: &str,
        project_id: &str,
        project: &ForgeProject,
        request: &PublishRefRequest,
        candidate_sha256: String,
        commit_tree: (String, String),
        admitted_gm_epoch: i64,
    ) -> Self {
        let (commit, tree) = commit_tree;
        Self {
            operation_id: operation_id.to_owned(),
            project_id: project_id.to_owned(),
            canonical_repository: canonical_repository(&project.canonical_repository)
                .unwrap_or_default(),
            attempt_id: request.attempt_id.clone(),
            task_revision: request.expected_revision,
            admitted_gm_epoch,
            submission_ref: request.submission_ref.clone(),
            accepted_operation_id: request.accepted_operation_id.clone(),
            candidate_ref: request.candidate_ref.clone(),
            candidate_sha256,
            commit,
            tree,
            remote_name: project.remote_name.clone(),
            target_ref: request.target_ref.clone(),
            expected_old_ref: request.expected_old_ref.clone(),
            expected_create: request.expected_create,
            force: false,
            policy_revision: project.policy_revision.clone(),
        }
    }
}

/// Classifies the one-ref exact readback. `expected_old` and `candidate` are
/// never conflated: a nonmatching ref remains unknown after a lost response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefReadback {
    Missing,
    At(String),
}

impl RefReadback {
    pub fn matches_intent(&self, intent: &PublicationIntent) -> bool {
        matches!(self, Self::At(value) if value.eq_ignore_ascii_case(&intent.commit))
    }

    pub fn matches_expected(&self, intent: &PublicationIntent) -> bool {
        match (&intent.expected_old_ref, intent.expected_create, self) {
            (Some(expected), false, Self::At(actual)) => expected.eq_ignore_ascii_case(actual),
            (None, true, Self::Missing) => true,
            _ => false,
        }
    }

    pub fn value(&self) -> Value {
        match self {
            Self::Missing => json!({"present":false}),
            Self::At(commit) => json!({"present":true,"commit":commit}),
        }
    }
}

/// Minimal metadata from a native Git command. Raw stderr is never retained;
/// stdout is held only up to the configured cap and is never persisted.
#[derive(Debug)]
pub(crate) struct GitOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stdout_truncated: bool,
    pub timed_out: bool,
}

#[cfg(all(target_os = "linux", test))]
struct Captured {
    bytes: Vec<u8>,
    truncated: bool,
}

#[cfg(all(target_os = "linux", test))]
fn drain_bounded<R: Read>(mut reader: R, cap: usize) -> std::io::Result<Captured> {
    let mut retained = Vec::with_capacity(cap.min(8192));
    let mut total = 0u64;
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total = total.saturating_add(count as u64);
        let remaining = cap.saturating_sub(retained.len());
        let keep = remaining.min(count);
        retained.extend_from_slice(&buffer[..keep]);
    }
    Ok(Captured {
        truncated: total > retained.len() as u64,
        bytes: retained,
    })
}

/// The caller must retain and poll any returned cleanup owner. A durable Store
/// hold is evidence of uncertainty; it does not own the Child or its pipes.
pub(crate) type GitRunResult = (Result<GitOutput>, Option<GitCleanupOwner>);

/// Run one configured Git command with separate argv/cwd, no inherited Git
/// repository overrides, bounded output and bounded foreground cleanup.
#[must_use = "retain and poll any returned Git cleanup owner before releasing its repository"]
pub(crate) fn run_git(
    config: &ForgeConfig,
    project: &ForgeProject,
    args: &[String],
) -> GitRunResult {
    if let Err(error) = config.validate() {
        return (Err(error), None);
    }
    #[cfg(windows)]
    {
        (windows::run_git(config, project, args), None)
    }
    #[cfg(not(windows))]
    run_git_nonwindows(config, project, args)
}

#[cfg(all(not(windows), target_os = "linux"))]
fn run_git_nonwindows(
    config: &ForgeConfig,
    project: &ForgeProject,
    args: &[String],
) -> GitRunResult {
    let mut command = Command::new(&config.git_executable);
    command
        .args(["--no-optional-locks", "--no-replace-objects"])
        .args(["-c", "core.fsmonitor=false", "-C"])
        .arg(&project.repository_path)
        .args(args)
        .current_dir(&project.repository_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0");
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
    for (key, _) in std::env::vars_os() {
        if key.to_str().is_some_and(|key| {
            key.starts_with("GIT_CONFIG_KEY_")
                || key.starts_with("GIT_CONFIG_VALUE_")
                || key.starts_with("GIT_TRACE")
                || key == "GIT_CURL_VERBOSE"
        }) {
            command.env_remove(key);
        }
    }
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }

    let child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            return (
                Err(Error::new(
                    "FORGE_GIT_START",
                    "configured Git process could not start",
                )),
                None,
            );
        }
    };
    // Establish physical custody before any fallible identity or pipe setup.
    run_git_foreground(
        GitCleanupOwner::new(child, config.max_output_bytes),
        Duration::from_secs(config.timeout_seconds),
        GIT_PROCESS_GROUP_CLEANUP_TIMEOUT,
    )
}

#[cfg(all(not(windows), not(target_os = "linux")))]
fn run_git_nonwindows(
    _config: &ForgeConfig,
    _project: &ForgeProject,
    _args: &[String],
) -> GitRunResult {
    (
        Err(Error::new(
            "FORGE_GIT_PLATFORM_UNSUPPORTED",
            "exact Git process-family cleanup is supported only on Windows and Linux",
        )),
        None,
    )
}

/// One directly owned pipe. Each poll reads at most one fixed-size chunk, so a
/// continuously writing or escaped descendant cannot monopolize process polls.
#[cfg(target_os = "linux")]
struct GitPipeCapture<R: PollableRead> {
    reader: R,
    bytes: Vec<u8>,
    cap: usize,
    observed: u64,
    configured: bool,
    eof: bool,
    failed: bool,
}

#[cfg(target_os = "linux")]
impl<R: PollableRead> GitPipeCapture<R> {
    fn new(reader: R, cap: usize) -> Self {
        Self {
            reader,
            bytes: Vec::with_capacity(cap.min(8192)),
            cap,
            observed: 0,
            configured: false,
            eof: false,
            failed: false,
        }
    }

    fn poll(&mut self) {
        if self.eof {
            return;
        }
        if !self.configured {
            if self.reader.make_nonblocking().is_err() {
                self.failed = true;
                return;
            }
            self.configured = true;
        }
        let mut buffer = [0u8; 8192];
        match self.reader.read_available(&mut buffer) {
            Ok(PipeRead::Eof | PipeRead::Data(0)) => self.eof = true,
            Ok(PipeRead::Pending) => {}
            Ok(PipeRead::Data(count)) if count <= buffer.len() => {
                self.observed = self.observed.saturating_add(count as u64);
                let keep = count.min(self.cap.saturating_sub(self.bytes.len()));
                self.bytes.extend_from_slice(&buffer[..keep]);
            }
            Ok(PipeRead::Data(_)) | Err(_) => self.failed = true,
        }
    }
}

/// Donor: hooks::git::GitCleanupOwner. This value, rather than a detached reader
/// or a repository hold bit, owns a started Git command through unresolved
/// cleanup. It is releasable only after group departure, direct reaping and EOF
/// on both captured pipes. The Store must retain it while is_pending() is true.
#[cfg(target_os = "linux")]
#[must_use = "retain this owner until direct exit, group departure and pipe EOF are confirmed"]
pub(crate) struct GitCleanupOwner {
    child: Child,
    identity: Option<Value>,
    stdout: Option<GitPipeCapture<ChildStdout>>,
    stderr: Option<GitPipeCapture<ChildStderr>>,
    direct_exited: bool,
    group_departed: bool,
    status: Option<ExitStatus>,
    cleanup_started: bool,
    timed_out: bool,
    identity_failed: bool,
    observation_failed: bool,
    capture_failed: bool,
    #[cfg(test)]
    fail_identity: bool,
    #[cfg(test)]
    fail_departure: bool,
}

#[cfg(not(target_os = "linux"))]
pub(crate) struct GitCleanupOwner;

#[cfg(not(target_os = "linux"))]
impl GitCleanupOwner {
    pub(crate) fn poll(&mut self) {}

    pub(crate) fn is_pending(&self) -> bool {
        false
    }
}

#[cfg(target_os = "linux")]
impl GitCleanupOwner {
    fn new(mut child: Child, cap: usize) -> Self {
        let stdout = child
            .stdout
            .take()
            .map(|pipe| GitPipeCapture::new(pipe, cap));
        let stderr = child.stderr.take().map(|pipe| GitPipeCapture::new(pipe, 0));
        let capture_failed = stdout.is_none() || stderr.is_none();
        Self {
            child,
            identity: None,
            stdout,
            stderr,
            direct_exited: false,
            group_departed: false,
            status: None,
            cleanup_started: false,
            timed_out: false,
            identity_failed: false,
            observation_failed: false,
            capture_failed,
            #[cfg(test)]
            fail_identity: false,
            #[cfg(test)]
            fail_departure: false,
        }
    }

    /// One nonblocking capture/status observation and one exact group query.
    /// An error leaves the original Child and pipe handles in this owner.
    pub(crate) fn poll(&mut self) {
        if !self.is_pending() {
            return;
        }
        if let Some(stdout) = self.stdout.as_mut() {
            stdout.poll();
            self.capture_failed |= stdout.failed;
        }
        if let Some(stderr) = self.stderr.as_mut() {
            stderr.poll();
            self.capture_failed |= stderr.failed;
        }
        if self.identity.is_none() {
            self.identity = self.capture_identity();
            self.identity_failed |= self.identity.is_none();
        }
        if self.status.is_none() && !self.direct_exited {
            match child_exited_without_reaping(self.child.id()) {
                Ok(exited) => self.direct_exited = exited,
                Err(_) => self.observation_failed = true,
            }
        }
        if self.identity_failed || self.observation_failed || self.capture_failed {
            self.cleanup_started = true;
        }
        if self.cleanup_started && !self.group_departed {
            // The direct Child is still unreaped and pins this dedicated PGID.
            terminate_remaining_process_group(self.child.id());
        }
        if !self.group_departed
            && (self.direct_exited || self.cleanup_started)
            && let Some(identity) = self.identity.as_ref()
        {
            match self.observe_departure(identity) {
                Ok(true) => self.group_departed = true,
                Ok(false) => self.cleanup_started = true,
                Err(_) => {
                    self.observation_failed = true;
                    self.cleanup_started = true;
                }
            }
        }
        if self.group_departed && self.status.is_none() {
            // Never call wait or reap while group signals/queries can still run.
            // try_wait is nonblocking even if the status observation failed.
            match self.child.try_wait() {
                Ok(status) => self.status = status,
                Err(_) => self.observation_failed = true,
            }
        }
        if self.direct_exited && self.is_pending() {
            // This includes an empty original group with an escaped pipe writer.
            self.cleanup_started = true;
        }
    }

    pub(crate) fn is_pending(&self) -> bool {
        !self.group_departed
            || self.status.is_none()
            || self.stdout.as_ref().is_some_and(|pipe| !pipe.eof)
            || self.stderr.as_ref().is_some_and(|pipe| !pipe.eof)
    }

    fn capture_identity(&self) -> Option<Value> {
        #[cfg(test)]
        if self.fail_identity {
            return None;
        }
        unix_process_group_identity(self.child.id())
    }

    fn observe_departure(&self, identity: &Value) -> Result<bool> {
        #[cfg(test)]
        if self.fail_departure {
            return Err(Error::new(
                "FORGE_FIXTURE_OBSERVATION",
                "injected departure error",
            ));
        }
        unix_process_group_is_empty(identity)
    }

    #[cfg(test)]
    pub(crate) fn fixture_child_id(&self) -> u32 {
        self.child.id()
    }

    #[cfg(test)]
    pub(crate) fn fixture_resume_observation(&mut self) {
        self.fail_identity = false;
        self.fail_departure = false;
    }

    fn into_output(self) -> Result<GitOutput> {
        // Only run_git_foreground calls this, after is_pending became false.
        let status = self
            .status
            .expect("completed Git custody retains its exit status");
        if self.identity_failed {
            return Err(Error::new(
                "FORGE_GIT_TREE_TERMINATION",
                "Git launch identity observation failed during cleanup",
            ));
        }
        if self.observation_failed {
            return Err(Error::new(
                "FORGE_GIT_WAIT",
                "Git process observation failed",
            ));
        }
        if self.capture_failed {
            return Err(Error::new("FORGE_GIT_OUTPUT", "Git pipe capture failed"));
        }
        let stdout = self.stdout.expect("successful Git capture retains stdout");
        Ok(GitOutput {
            status,
            stdout_truncated: stdout.observed > stdout.bytes.len() as u64,
            stdout: stdout.bytes,
            timed_out: self.timed_out,
        })
    }
}

#[cfg(target_os = "linux")]
fn run_git_foreground(
    mut owner: GitCleanupOwner,
    command_timeout: Duration,
    cleanup_timeout: Duration,
) -> GitRunResult {
    let command_deadline = Instant::now() + command_timeout;
    let mut cleanup_deadline = None;
    loop {
        let now = Instant::now();
        if cleanup_deadline.is_none() && now >= command_deadline {
            owner.timed_out = true;
            owner.cleanup_started = true;
            cleanup_deadline = Some(command_deadline + cleanup_timeout);
        }
        if owner.cleanup_started && cleanup_deadline.is_none() {
            cleanup_deadline = Some(now + cleanup_timeout);
        }
        if cleanup_deadline.is_some_and(|deadline| now >= deadline) {
            return pending_git_cleanup(owner);
        }
        owner.poll();
        let poll_finished = Instant::now();
        if owner.cleanup_started && cleanup_deadline.is_none() {
            // Include the observation that started cleanup in its fixed budget;
            // retrying an observation must never reset or extend this deadline.
            cleanup_deadline = Some(now + cleanup_timeout);
        }
        // Synchronous /proc work may overrun a poll. Do not turn an observation
        // obtained after the exact cleanup deadline into foreground success.
        if cleanup_deadline.is_some_and(|deadline| poll_finished >= deadline) {
            return pending_git_cleanup(owner);
        }
        if cleanup_deadline.is_none() && poll_finished >= command_deadline {
            owner.timed_out = true;
            owner.cleanup_started = true;
            cleanup_deadline = Some(command_deadline + cleanup_timeout);
            if poll_finished >= command_deadline + cleanup_timeout {
                return pending_git_cleanup(owner);
            }
        }
        if !owner.is_pending() {
            return (owner.into_output(), None);
        }
        let deadline = cleanup_deadline.unwrap_or(command_deadline);
        thread::sleep(
            Duration::from_millis(20).min(deadline.saturating_duration_since(poll_finished)),
        );
    }
}

#[cfg(target_os = "linux")]
fn pending_git_cleanup(owner: GitCleanupOwner) -> GitRunResult {
    (
        Err(Error::new(
            "FORGE_GIT_TREE_TERMINATION",
            "Git cleanup or natural pipe completion remains unconfirmed; retain the cleanup owner",
        )),
        Some(owner),
    )
}

#[cfg(all(target_os = "linux", test))]
pub(crate) fn fixture_git_observation_failure(child: Child) -> GitRunResult {
    let mut owner = GitCleanupOwner::new(child, 1024);
    owner.fail_identity = true;
    run_git_foreground(owner, Duration::from_millis(20), Duration::from_millis(40))
}

#[cfg(target_os = "linux")]
fn child_exited_without_reaping(pid: u32) -> std::io::Result<bool> {
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: `info` is writable storage for waitid, and WNOWAIT leaves
    // the child waitable so its PID pins the process-group identity.
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a successful waitid initialized the siginfo_t output and
    // si_pid reads the process ID from that initialized signal info.
    let info = unsafe { info.assume_init() };
    Ok(unsafe { info.si_pid() } != 0)
}

#[cfg(target_os = "linux")]
fn unix_process_group_identity(pid: u32) -> Option<Value> {
    let launch = crate::platform::process_group::spawned_identity(pid).ok();
    let (recorded_pid, process_group, start_ticks, boot_id) = if let Some(launch) = launch {
        (
            launch["pid"].as_u64()?,
            launch["spawned_pgid"].as_u64()?,
            launch["start_ticks"].as_str()?.to_owned(),
            launch["boot_id"].as_str()?.to_owned(),
        )
    } else {
        // WNOWAIT keeps a fast-exiting leader's proc entry available
        // until its process group is confirmed empty.
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let fields = stat
            .rsplit_once(')')?
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        let process_group = fields.get(2)?.parse::<u64>().ok()?;
        let start_ticks = fields.get(19)?.to_string();
        let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
        (
            u64::from(pid),
            process_group,
            start_ticks,
            boot_id.trim().to_owned(),
        )
    };
    if recorded_pid != u64::from(pid) || process_group != recorded_pid {
        return None;
    }
    if start_ticks
        .parse::<u64>()
        .ok()
        .is_none_or(|start_ticks| start_ticks == 0)
        || boot_id.trim().is_empty()
    {
        return None;
    }
    Some(json!({
        "pid": recorded_pid,
        "pgid": process_group,
        "start_ticks": start_ticks,
        "boot_id": boot_id,
        "scope": "linux_process_group",
        "purpose": "check",
        "disposition_source": "proc_group_members"
    }))
}

#[cfg(target_os = "linux")]
fn unix_process_group_is_empty(identity: &Value) -> Result<bool> {
    crate::platform::process_group::departed_empty(identity, "forge-git")
}

#[cfg(target_os = "linux")]
fn terminate_remaining_process_group(pid: u32) {
    let process_group = -(pid as libc::pid_t);
    // SAFETY: CommandExt::process_group(0) makes this child the leader of its
    // own group. On the normal-exit path its unreaped Child pins this PID.
    unsafe {
        libc::kill(process_group, libc::SIGKILL);
    }
}

#[cfg(target_os = "linux")]
const GIT_PROCESS_GROUP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

fn has_control(value: &str) -> bool {
    value.chars().any(char::is_control)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn valid_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn valid_remote_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && !value.contains("..")
        && !value.ends_with('.')
}

/// Deliberately conservative full branch-ref validation; Git performs its own
/// `check-ref-format` validation again immediately before any push.
pub(crate) fn valid_branch_ref(value: &str) -> bool {
    let Some(tail) = value.strip_prefix("refs/heads/") else {
        return false;
    };
    if tail.is_empty()
        || tail.len() > 900
        || tail.starts_with('/')
        || tail.ends_with('/')
        || tail.contains("//")
        || tail.contains("..")
        || tail.contains("@{")
        || tail.ends_with('.')
        || tail.chars().any(|character| {
            character.is_control()
                || matches!(character, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
    {
        return false;
    }
    tail.split('/').all(|component| {
        !component.is_empty()
            && component != "."
            && component != ".."
            && !component.ends_with(".lock")
    })
}

/// Canonicalize only a credential-free `host/owner/repository` identity.
pub fn canonical_repository(value: &str) -> Result<String> {
    let parts: Vec<_> = value.split('/').collect();
    if parts.len() < 3
        || !valid_host(parts[0])
        || parts[1..].iter().any(|part| !valid_path_component(part))
    {
        return Err(Error::invalid(
            "invalid canonical forge repository identity",
        ));
    }
    Ok(parts
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("/"))
}

fn valid_host(host: &str) -> bool {
    let name = match host.rsplit_once(':') {
        Some((name, port)) => {
            let Ok(port) = port.parse::<u16>() else {
                return false;
            };
            if name.contains(':') || port == 0 {
                return false;
            }
            name
        }
        None => host,
    };
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn valid_path_component(part: &str) -> bool {
    !part.is_empty()
        && part != "."
        && part != ".."
        && !part.ends_with('.')
        && !part.contains(".lock")
        && part
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Resolve a Git fetch/push URL to a canonical repository identity without
/// returning or persisting any URL authority. HTTPS userinfo and unsupported
/// transports are rejected so embedded credentials cannot enter diagnostics.
pub(crate) fn repository_from_remote_url(url: &str) -> Result<String> {
    let (host, path) = if let Some(rest) = url.strip_prefix("https://") {
        let (authority, path) = rest
            .split_once('/')
            .ok_or_else(|| Error::invalid("remote URL has no repository path"))?;
        if authority.contains('@') || authority.is_empty() {
            return Err(Error::invalid(
                "credential-bearing HTTPS remote URLs are not supported",
            ));
        }
        (strip_default_port(authority, 443), path)
    } else if let Some(rest) = url.strip_prefix("ssh://") {
        let (authority, path) = rest
            .split_once('/')
            .ok_or_else(|| Error::invalid("remote URL has no repository path"))?;
        let (user, host_port) = authority
            .rsplit_once('@')
            .ok_or_else(|| Error::invalid("SSH remote must use the canonical git account"))?;
        if user != "git" {
            return Err(Error::invalid(
                "SSH remote user is not the canonical git account",
            ));
        }
        (strip_default_port(host_port, 22), path)
    } else if url.contains("://") {
        return Err(Error::invalid("unsupported forge transport"));
    } else {
        let (authority, path) = url
            .split_once(':')
            .ok_or_else(|| Error::invalid("remote is not a supported forge URL"))?;
        let (user, host) = authority
            .rsplit_once('@')
            .ok_or_else(|| Error::invalid("SCP-style remote must identify the git account"))?;
        if user != "git" || host.contains('/') {
            return Err(Error::invalid("SCP-style remote identity is invalid"));
        }
        (host, path)
    };
    if !valid_host(host)
        || path
            .chars()
            .any(|character| matches!(character, '?' | '#' | '%' | '@' | '\\'))
    {
        return Err(Error::invalid("remote repository identity is invalid"));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn project(path: &str) -> ForgeProject {
        ForgeProject {
            canonical_repository: "github.com/Example/Project".into(),
            repository_path: PathBuf::from(path),
            remote_name: "origin".into(),
            policy_revision: "owner-policy-v1".into(),
            target_refs: vec!["refs/heads/main".into()],
        }
    }

    #[test]
    fn canonical_identity_resolves_supported_remotes_without_exposing_url() {
        assert_eq!(
            repository_from_remote_url("https://github.com/Example/Project.git").unwrap(),
            "github.com/example/project"
        );
        assert_eq!(
            repository_from_remote_url("git@github.com:Example/Project.git").unwrap(),
            "github.com/example/project"
        );
        assert_eq!(
            repository_from_remote_url("ssh://git@github.com/Example/Project.git").unwrap(),
            "github.com/example/project"
        );
        assert!(repository_from_remote_url("https://token@github.com/owner/repo.git").is_err());
        assert!(repository_from_remote_url("file:///tmp/repo").is_err());
    }

    #[test]
    fn publication_request_is_closed_and_requires_explicit_create_or_old_oid() {
        let base = json!({
            "client_request_id":"request-1",
            "attempt_id":"attempt-1",
            "expected_revision":3,
            "submission_ref":"submission-1",
            "accepted_operation_id":"accept-1",
            "candidate_ref":"candidate-1",
            "expected_policy_revision":"owner-policy-v1",
            "target_ref":"refs/heads/main",
            "expected_create":false,
            "expected_old_ref":"0123456789012345678901234567890123456789"
        });
        assert!(PublishRefRequest::parse(&base).is_ok());
        assert!(
            PublishRefRequest::parse(&json!({
                "client_request_id":"request-1",
                "attempt_id":"attempt-1",
                "expected_revision":3,
                "submission_ref":"submission-1",
                "accepted_operation_id":"accept-1",
                "candidate_ref":"candidate-1",
                "expected_policy_revision":"owner-policy-v1",
                "target_ref":"refs/heads/main",
                "expected_create":true,
                "expected_old_ref":"0123456789012345678901234567890123456789"
            }))
            .is_err()
        );
        let mut with_force = base;
        with_force["force"] = json!(true);
        assert!(PublishRefRequest::parse(&with_force).is_err());
    }

    #[test]
    fn ref_and_remote_names_reject_option_and_refspec_injection() {
        assert!(valid_branch_ref("refs/heads/release/v1"));
        assert!(!valid_branch_ref("+refs/heads/main"));
        assert!(!valid_branch_ref("refs/heads/a:refs/heads/b"));
        assert!(!valid_branch_ref("refs/tags/v1"));
        assert!(!valid_branch_ref("refs/heads/a..b"));
        assert!(valid_remote_name("origin"));
        assert!(!valid_remote_name("--upload-pack=evil"));
    }

    #[test]
    fn readback_only_can_distinguish_expected_old_from_applied_candidate() {
        let mut publication = PublicationIntent {
            operation_id: "op".into(),
            project_id: "project".into(),
            canonical_repository: "github.com/owner/repo".into(),
            attempt_id: "attempt".into(),
            task_revision: 1,
            admitted_gm_epoch: 0,
            submission_ref: "submission".into(),
            accepted_operation_id: "accept".into(),
            candidate_ref: "candidate".into(),
            candidate_sha256: "a".repeat(64),
            commit: "b".repeat(40),
            tree: "c".repeat(40),
            remote_name: "origin".into(),
            target_ref: "refs/heads/main".into(),
            expected_old_ref: Some("d".repeat(40)),
            expected_create: false,
            force: false,
            policy_revision: "owner-policy-v1".into(),
        };
        assert!(publication.validate().is_ok());
        assert!(RefReadback::At("b".repeat(40)).matches_intent(&publication));
        assert!(RefReadback::At("d".repeat(40)).matches_expected(&publication));
        assert!(!RefReadback::At("e".repeat(40)).matches_intent(&publication));
        assert!(!RefReadback::At("e".repeat(40)).matches_expected(&publication));
        publication.force = true;
        assert_eq!(
            publication.validate().unwrap_err().code,
            "FORGE_INTENT_INVALID"
        );
    }

    #[test]
    fn config_is_disabled_by_default_and_bounds_git_output() {
        let config = ForgeConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_output_bytes, DEFAULT_MAX_OUTPUT_BYTES);
        #[cfg(target_os = "linux")]
        {
            let output = drain_bounded(&b"abcdef"[..], 3).unwrap();
            assert_eq!(output.bytes, b"abc");
            assert!(output.truncated);
        }
    }

    #[test]
    fn helper_project_fixture_has_an_exact_allowlist() {
        assert_eq!(project("C:/work/repo").target_refs, ["refs/heads/main"]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unix_git_runner_terminates_descendant_that_closed_its_pipes() {
        struct TempDirectory(PathBuf);

        impl Drop for TempDirectory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        struct ProcessGroupCleanup(libc::pid_t);

        impl Drop for ProcessGroupCleanup {
            fn drop(&mut self) {
                // SAFETY: the test records the process-group leader while its
                // live descendant still pins that group ID.
                unsafe {
                    libc::kill(-self.0, libc::SIGKILL);
                }
            }
        }

        let unique = format!(
            "swarm-forge-git-family-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(unique);
        std::fs::create_dir(&root).unwrap();
        let _root = TempDirectory(root.clone());
        let repository_path = root.join("repository");
        std::fs::create_dir(&repository_path).unwrap();

        let script = root.join("git-family.sh");
        std::fs::write(
            &script,
            r##"#!/bin/sh
directory=${0%/*}
(
  : > "$directory/child.ready"
  exec /bin/sleep 30
) </dev/null >/dev/null 2>&1 &
child_pid=$!
printf '%s\n' "$child_pid" > "$directory/child.pid"
printf '%s\n' "$$" > "$directory/process-group.pid"
while [ ! -f "$directory/child.ready" ]; do /bin/sleep 0.01; done
exit 0
"##,
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();

        let project = ForgeProject {
            canonical_repository: "github.com/example/project".into(),
            repository_path,
            remote_name: "origin".into(),
            policy_revision: "owner-policy-v1".into(),
            target_refs: vec!["refs/heads/main".into()],
        };
        let config = ForgeConfig {
            enabled: true,
            git_executable: script,
            timeout_seconds: 5,
            max_output_bytes: 1024,
            projects: BTreeMap::from([("fixture".into(), project.clone())]),
        };

        let (output, cleanup_owner) = run_git(&config, &project, &[]);
        let child_pid = std::fs::read_to_string(root.join("child.pid"))
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        let descendant_is_live =
            crate::platform::process_group::spawned_identity(child_pid).is_ok();
        if descendant_is_live {
            let process_group = std::fs::read_to_string(root.join("process-group.pid"))
                .unwrap()
                .trim()
                .parse::<libc::pid_t>()
                .unwrap();
            let _cleanup = ProcessGroupCleanup(process_group);
            panic!("Git runner returned while a pipe-closed descendant was still live");
        }
        assert!(cleanup_owner.is_none());
        let output = output.unwrap();
        assert!(output.status.success());
        assert!(!output.timed_out);
        assert!(root.join("child.ready").is_file());
    }

    #[cfg(target_os = "linux")]
    struct GitCustodyFixture {
        root: PathBuf,
        config: ForgeConfig,
        project: ForgeProject,
    }

    #[cfg(target_os = "linux")]
    impl GitCustodyFixture {
        fn new(script: &str) -> Self {
            use std::os::unix::fs::PermissionsExt;

            let root = std::env::temp_dir().join(format!(
                "swarm-forge-custody-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&root).unwrap();
            let repository_path = root.join("repository");
            std::fs::create_dir(&repository_path).unwrap();
            let git_executable = root.join("git-custody.sh");
            std::fs::write(&git_executable, script).unwrap();
            let mut permissions = std::fs::metadata(&git_executable).unwrap().permissions();
            permissions.set_mode(0o700);
            std::fs::set_permissions(&git_executable, permissions).unwrap();
            let project = ForgeProject {
                canonical_repository: "github.com/example/project".into(),
                repository_path,
                remote_name: "origin".into(),
                policy_revision: "owner-policy-v1".into(),
                target_refs: vec!["refs/heads/main".into()],
            };
            let config = ForgeConfig {
                enabled: true,
                git_executable,
                timeout_seconds: 5,
                max_output_bytes: 1024,
                projects: BTreeMap::from([("fixture".into(), project.clone())]),
            };
            Self {
                root,
                config,
                project,
            }
        }

        fn spawn(&self) -> Child {
            use std::os::unix::process::CommandExt;

            Command::new(&self.config.git_executable)
                .current_dir(&self.project.repository_path)
                .process_group(0)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for GitCustodyFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[cfg(target_os = "linux")]
    fn finish_fixture_cleanup(owner: &mut GitCleanupOwner) {
        let deadline = Instant::now() + Duration::from_secs(5);
        owner.cleanup_started = true;
        while owner.is_pending() && Instant::now() < deadline {
            owner.poll();
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !owner.is_pending(),
            "fixture retains unresolved Git custody"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unix_git_identity_and_departure_errors_return_the_same_child_and_pipes() {
        let fixture = GitCustodyFixture::new("#!/bin/sh\nexec /bin/sleep 30\n");
        for fail_identity in [true, false] {
            let child = fixture.spawn();
            let pid = child.id();
            let mut owner = GitCleanupOwner::new(child, 1024);
            owner.fail_identity = fail_identity;
            owner.fail_departure = !fail_identity;
            let started = Instant::now();
            let (outcome, pending) = run_git_foreground(
                owner,
                Duration::from_millis(100),
                Duration::from_millis(150),
            );
            let elapsed = started.elapsed();
            let mut owner = pending.expect("observation failure returns exact physical custody");
            let same_pid = owner.child.id() == pid;
            let unreaped = owner.status.is_none();
            let retained_pipes = owner.stdout.is_some() && owner.stderr.is_some();
            let pending_before_retry = owner.is_pending();
            owner.fail_identity = false;
            owner.fail_departure = false;
            finish_fixture_cleanup(&mut owner);
            assert_eq!(outcome.unwrap_err().code, "FORGE_GIT_TREE_TERMINATION");
            assert!(elapsed < Duration::from_secs(3));
            assert!(same_pid && unreaped && retained_pipes && pending_before_retry);
            assert!(owner.group_departed && owner.status.is_some());
            assert!(owner.stdout.as_ref().unwrap().eof);
            assert!(owner.stderr.as_ref().unwrap().eof);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unix_git_escaped_pipe_writer_returns_pending_custody_within_cleanup_bound() {
        let fixture = GitCustodyFixture::new(
            r##"#!/bin/sh
directory=${0%/*}
/usr/bin/setsid /bin/sh -c '
  directory=$1
  printf "%s\n" "$$" > "$directory/escaped.pid"
  : > "$directory/escaped.ready"
  exec /bin/sleep 30
' escaped "$directory" &
while [ ! -f "$directory/escaped.ready" ]; do /bin/sleep 0.01; done
exit 0
"##,
        );
        let child = fixture.spawn();
        let started = Instant::now();
        let (outcome, pending) = run_git_foreground(
            GitCleanupOwner::new(child, 1024),
            Duration::from_secs(2),
            Duration::from_millis(200),
        );
        let elapsed = started.elapsed();
        let mut owner = pending.expect("escaped inherited pipes retain cleanup custody");
        let empty_group = owner.group_departed;
        let direct_reaped = owner.status.is_some();
        let stdout_pending = !owner.stdout.as_ref().unwrap().eof;
        let stderr_pending = !owner.stderr.as_ref().unwrap().eof;
        let escaped_pid = std::fs::read_to_string(fixture.root.join("escaped.pid"))
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        let escaped_identity = unix_process_group_identity(escaped_pid)
            .expect("fixture escaped process is its own live group leader");
        let escaped_live = !unix_process_group_is_empty(&escaped_identity).unwrap();
        // SAFETY: this fixture's still-live escaped leader pins its recorded
        // group; terminate it explicitly so natural EOF can release custody.
        unsafe {
            libc::kill(-(escaped_pid as libc::pid_t), libc::SIGKILL);
        }
        finish_fixture_cleanup(&mut owner);
        assert_eq!(outcome.unwrap_err().code, "FORGE_GIT_TREE_TERMINATION");
        assert!(elapsed < Duration::from_secs(3));
        assert!(empty_group && direct_reaped && escaped_live);
        assert!(stdout_pending && stderr_pending);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unix_git_capture_poll_reads_one_chunk_even_when_output_is_always_ready() {
        struct AlwaysReady {
            reads: usize,
        }

        impl Read for AlwaysReady {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                self.reads += 1;
                buffer.fill(b'x');
                Ok(buffer.len())
            }
        }

        impl PollableRead for AlwaysReady {
            fn make_nonblocking(&self) -> std::io::Result<()> {
                Ok(())
            }

            fn read_available(&mut self, buffer: &mut [u8]) -> std::io::Result<PipeRead> {
                self.read(buffer).map(PipeRead::Data)
            }
        }

        let mut capture = GitPipeCapture::new(AlwaysReady { reads: 0 }, 32);
        capture.poll();
        assert_eq!(capture.reader.reads, 1);
        assert_eq!(capture.bytes, vec![b'x'; 32]);
        assert_eq!(capture.observed, 8192);
        assert!(!capture.eof && !capture.failed);
    }
}
