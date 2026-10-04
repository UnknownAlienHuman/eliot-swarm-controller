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

#[cfg(not(windows))]
use std::{
    io::Read,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

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
pub(crate) struct GitOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_digest: String,
    pub stderr_bytes: u64,
    pub timed_out: bool,
}

#[cfg(not(windows))]
struct Captured {
    bytes: Vec<u8>,
    total: u64,
    truncated: bool,
}

#[cfg(not(windows))]
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
        total,
    })
}

/// Run one configured Git command with separate argv/cwd, no inherited Git
/// repository overrides, bounded output and a finite wall-clock limit.
pub(crate) fn run_git(
    config: &ForgeConfig,
    project: &ForgeProject,
    args: &[String],
) -> Result<GitOutput> {
    config.validate()?;
    #[cfg(windows)]
    {
        windows::run_git(config, project, args)
    }
    #[cfg(not(windows))]
    run_git_nonwindows(config, project, args)
}

#[cfg(not(windows))]
fn run_git_nonwindows(
    config: &ForgeConfig,
    project: &ForgeProject,
    args: &[String],
) -> Result<GitOutput> {
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
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }

    let mut child = command
        .spawn()
        .map_err(|_| Error::new("FORGE_GIT_START", "configured Git process could not start"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("FORGE_GIT_IO", "Git stdout pipe unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::new("FORGE_GIT_IO", "Git stderr pipe unavailable"))?;
    let out_cap = config.max_output_bytes;
    let stdout_reader = thread::spawn(move || drain_bounded(stdout, out_cap));
    let stderr_reader = thread::spawn(move || drain_bounded(stderr, out_cap));
    let deadline = Instant::now() + Duration::from_secs(config.timeout_seconds);
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                timed_out = true;
                terminate_process_tree(&mut child);
                break child.wait().map_err(|_| {
                    Error::new("FORGE_GIT_WAIT", "Git process did not report termination")
                })?;
            }
            Err(_) => {
                terminate_process_tree(&mut child);
                let _ = child.wait();
                return Err(Error::new(
                    "FORGE_GIT_WAIT",
                    "Git process status could not be observed",
                ));
            }
        }
    };
    let stdout = stdout_reader
        .join()
        .map_err(|_| Error::new("FORGE_GIT_OUTPUT", "Git stdout reader failed"))?
        .map_err(|_| Error::new("FORGE_GIT_OUTPUT", "Git stdout could not be read"))?;
    let stderr = stderr_reader
        .join()
        .map_err(|_| Error::new("FORGE_GIT_OUTPUT", "Git stderr reader failed"))?
        .map_err(|_| Error::new("FORGE_GIT_OUTPUT", "Git stderr could not be read"))?;
    Ok(GitOutput {
        status,
        stdout: stdout.bytes,
        stdout_truncated: stdout.truncated,
        stderr_digest: crate::model::digest(&stderr.bytes),
        stderr_bytes: stderr.total,
        timed_out,
    })
}

#[cfg(unix)]
fn terminate_process_tree(child: &mut std::process::Child) {
    let process_group = -(child.id() as i32);
    // SAFETY: the child was launched in its own process group by
    // CommandExt::process_group(0); negative pid addresses that group.
    unsafe {
        libc::kill(process_group, libc::SIGTERM);
    }
    thread::sleep(Duration::from_millis(100));
    // SAFETY: same owned process group; escalation bounds termination.
    unsafe {
        libc::kill(process_group, libc::SIGKILL);
    }
    let _ = child.kill();
}

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
        #[cfg(not(windows))]
        {
            let output = drain_bounded(&b"abcdef"[..], 3).unwrap();
            assert_eq!(output.bytes, b"abc");
            assert_eq!(output.total, 6);
            assert!(output.truncated);
        }
    }

    #[test]
    fn helper_project_fixture_has_an_exact_allowlist() {
        assert_eq!(project("C:/work/repo").target_refs, ["refs/heads/main"]);
    }
}
