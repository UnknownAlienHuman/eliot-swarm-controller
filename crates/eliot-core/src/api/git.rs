//! Typed request and result payloads for the connected native Git fetch action.
//!
//! These DTOs are pure data. Repository registration, transport construction,
//! process ownership, Store admission, and effect settlement belong to callers.

use super::agent_views::ProjectGitHubCliView;
use crate::{Error, FrozenSetupFile, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

/// Default for the optional rule-authored fetch timeout.
pub const DEFAULT_GIT_FETCH_TIMEOUT_MS: i64 = 10 * 60 * 1_000;
/// Default for one local publication, push, and optional PR creation.
pub const DEFAULT_GIT_PUBLISH_TIMEOUT_MS: i64 = 10 * 60 * 1_000;
/// Maximum first line retained as an automated commit subject.
pub const MAX_GIT_PUBLISH_SUBJECT_BYTES: usize = 256;

/// Literal rule input. The Router freezes registration and transport state before
/// admitting a command.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitFetchDefinitionArgs {
    pub repository: String,
    pub remote: String,
    pub source_ref: String,
    pub tracking_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

impl GitFetchDefinitionArgs {
    pub fn validate(&self) -> Result<()> {
        validate_literal(&self.repository, "repository")?;
        validate_literal(&self.remote, "remote")?;
        if self.remote.starts_with('-') || self.remote.chars().any(char::is_whitespace) {
            return Err(invalid("git.fetch remote is not a safe literal name"));
        }
        validate_full_ref(&self.source_ref, "source_ref")?;
        validate_tracking_ref(&self.tracking_ref, "tracking_ref")?;
        if let Some(timeout) = &self.timeout {
            validate_literal(timeout, "timeout")?;
        }
        Ok(())
    }

    /// Build the stable caller payload using the daemon's parsed effective timeout.
    pub fn binding_args(&self, timeout_ms: i64) -> Result<Value> {
        self.validate()?;
        canonical_git_fetch_binding_args(
            &self.repository,
            &self.remote,
            &self.source_ref,
            &self.tracking_ref,
            timeout_ms,
        )
    }
}

/// Frozen nonsecret intent persisted in the ordinary command args before dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenGitFetch {
    pub repository: String,
    pub registration_revision: i64,
    pub canonical_checkout: String,
    pub common_dir: String,
    pub remote: String,
    pub endpoint: String,
    pub source_ref: String,
    pub advertised_oid: String,
    pub tracking_ref: String,
    /// `None` means the tracking ref was absent when the intent was frozen.
    #[serde(deserialize_with = "required_expected_old_oid")]
    pub expected_old_oid: Option<String>,
    pub timeout_ms: i64,
    /// When present, this is the receipt for the explicitly approved manager helper.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_helper: Option<FrozenSetupFile>,
}

impl FrozenGitFetch {
    pub fn validate(&self) -> Result<()> {
        validate_literal(&self.repository, "repository")?;
        validate_literal(&self.remote, "remote")?;
        if self.remote.starts_with('-') || self.remote.chars().any(char::is_whitespace) {
            return Err(invalid("git.fetch remote is not a safe literal name"));
        }
        if self.registration_revision < 0 {
            return Err(invalid(
                "git.fetch registration_revision must be nonnegative",
            ));
        }
        validate_absolute_path(&self.canonical_checkout, "canonical_checkout")?;
        validate_absolute_path(&self.common_dir, "common_dir")?;
        validate_literal(&self.endpoint, "endpoint")?;
        if self.endpoint.starts_with('-') || endpoint_has_userinfo(&self.endpoint) {
            return Err(invalid(
                "git.fetch endpoint cannot be an option or contain URL credentials",
            ));
        }
        validate_full_ref(&self.source_ref, "source_ref")?;
        validate_oid(&self.advertised_oid, "advertised_oid")?;
        validate_tracking_ref(&self.tracking_ref, "tracking_ref")?;
        if let Some(oid) = &self.expected_old_oid {
            validate_oid(oid, "expected_old_oid")?;
        }
        if self.timeout_ms <= 0 {
            return Err(invalid("git.fetch timeout_ms must be positive"));
        }
        if let Some(receipt) = &self.credential_helper {
            validate_setup_file(receipt)?;
        }
        Ok(())
    }

    /// Project only caller-selected fields; frozen registration and remote
    /// observations remain retained first-admission intent, not the request digest.
    pub fn binding_args(&self) -> Result<Value> {
        self.validate()?;
        canonical_git_fetch_binding_args(
            &self.repository,
            &self.remote,
            &self.source_ref,
            &self.tracking_ref,
            self.timeout_ms,
        )
    }
}

/// Readback after a successful fetch and tracking-ref compare-and-swap.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitFetchOutcome {
    pub tracking_ref: String,
    pub tracking_oid: String,
}

/// Durable receipt emitted only after the exact final project check passed.
/// The event envelope binds this data to one Agent generation and workspace.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectCheckPassed {
    pub project: String,
    pub turn_id: String,
    pub fingerprint: String,
    /// The first line of the same retained final answer; used verbatim as the
    /// automatic commit subject.
    pub subject: String,
}

impl ProjectCheckPassed {
    pub fn validate(&self) -> Result<()> {
        validate_publish_literal(&self.project, "project", 128)?;
        validate_publish_literal(&self.turn_id, "turn_id", 256)?;
        validate_sha256(&self.fingerprint, "fingerprint")?;
        validate_commit_subject(&self.subject)
    }
}

/// Literal project-rule action. The command always commits locally; `remote`
/// and `create_pr` select only Owner-approved publication integrations.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitPublishDefinitionArgs {
    pub repository: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    #[serde(default)]
    pub create_pr: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

impl GitPublishDefinitionArgs {
    pub fn validate(&self) -> Result<()> {
        validate_publish_literal(&self.repository, "repository", 128)?;
        if let Some(remote) = &self.remote {
            validate_publish_remote_name(remote)?;
        }
        if self.create_pr && self.remote.is_none() {
            return Err(invalid("git.publish create_pr requires an approved remote"));
        }
        if let Some(timeout) = &self.timeout {
            validate_publish_literal(timeout, "timeout", 64)?;
        }
        Ok(())
    }
}

/// Event-bound input before Router freezes the Owner registration and remote.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitPublishTrigger {
    pub repository: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    pub create_pr: bool,
    pub timeout_ms: i64,
    pub source_event_seq: i64,
    pub check: ProjectCheckPassed,
    pub agent: String,
    pub generation: i64,
    pub workspace: String,
}

impl GitPublishTrigger {
    pub fn validate(&self) -> Result<()> {
        validate_publish_literal(&self.repository, "repository", 128)?;
        self.check.validate()?;
        if self.check.project != self.repository
            || self.source_event_seq <= 0
            || self.generation <= 0
            || self.timeout_ms <= 0
            || !valid_agent_id(&self.agent)
        {
            return Err(invalid("git.publish trigger binding is invalid"));
        }
        if let Some(remote) = &self.remote {
            validate_publish_remote_name(remote)?;
        }
        if self.create_pr && self.remote.is_none() {
            return Err(invalid("git.publish create_pr requires an approved remote"));
        }
        validate_absolute_path(&self.workspace, "workspace")
    }
}

/// Nonsecret intent retained in the ordinary `git.publish` command before
/// dispatch. It binds the check event, project registration, managed worktree,
/// passed Git fingerprint, and exact Owner-selected publication endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenGitPublish {
    pub repository: String,
    pub registration_revision: i64,
    pub canonical_checkout: String,
    pub common_dir: String,
    pub workspace: String,
    pub agent: String,
    pub generation: i64,
    pub source_event_seq: i64,
    pub check: ProjectCheckPassed,
    pub head_oid: String,
    pub base_oid: String,
    pub branch: String,
    pub remote: Option<String>,
    pub endpoint: Option<String>,
    pub create_pr: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_cli: Option<ProjectGitHubCliView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_helper: Option<FrozenSetupFile>,
    pub timeout_ms: i64,
}

impl FrozenGitPublish {
    pub fn validate(&self) -> Result<()> {
        validate_publish_literal(&self.repository, "repository", 128)?;
        self.check.validate()?;
        if self.check.project != self.repository
            || self.registration_revision < 0
            || self.source_event_seq <= 0
            || self.generation <= 0
            || self.timeout_ms <= 0
            || !valid_agent_id(&self.agent)
        {
            return Err(invalid("git.publish frozen binding is invalid"));
        }
        validate_absolute_path(&self.canonical_checkout, "canonical_checkout")?;
        validate_absolute_path(&self.common_dir, "common_dir")?;
        validate_absolute_path(&self.workspace, "workspace")?;
        validate_oid(&self.head_oid, "head_oid")?;
        validate_oid(&self.base_oid, "base_oid")?;
        validate_branch(&self.branch)?;
        match (&self.remote, &self.endpoint) {
            (Some(remote), Some(endpoint)) => {
                validate_publish_remote_name(remote)?;
                validate_publish_endpoint(endpoint)?;
            }
            (None, None) if !self.create_pr => {}
            _ => return Err(invalid("git.publish remote binding is incomplete")),
        }
        if self.create_pr {
            self.github_cli
                .as_ref()
                .ok_or_else(|| invalid("git.publish PR integration is not configured"))?
                .validate()?;
        } else if self.github_cli.is_some() {
            return Err(invalid("git.publish has an unused GitHub integration"));
        }
        if let Some(receipt) = &self.credential_helper {
            validate_setup_file(receipt)?;
        }
        Ok(())
    }
}

/// Readback from local commit, optional push, and optional PR creation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitPublishOutcome {
    pub commit_oid: String,
    pub commit_subject: String,
    pub committed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_request_url: Option<String>,
}

impl GitPublishOutcome {
    pub fn validate_for(&self, frozen: &FrozenGitPublish) -> Result<()> {
        frozen.validate()?;
        validate_oid(&self.commit_oid, "commit_oid")?;
        validate_commit_subject(&self.commit_subject)?;
        if (self.committed
            && (self.commit_oid == frozen.head_oid || self.commit_subject != frozen.check.subject))
            || (!self.committed && self.commit_oid != frozen.head_oid)
            || self.remote_ref.is_some() != frozen.remote.is_some()
            || self.pull_request_url.is_some() != frozen.create_pr
        {
            return Err(invalid(
                "git.publish outcome does not match its frozen intent",
            ));
        }
        if let Some(remote_ref) = &self.remote_ref {
            validate_full_ref(remote_ref, "remote_ref")?;
        }
        if let Some(url) = &self.pull_request_url {
            validate_publish_literal(url, "pull_request_url", 2_048)?;
            if !url.starts_with("https://") {
                return Err(invalid("git.publish PR URL must use HTTPS"));
            }
        }
        Ok(())
    }
}

impl GitFetchOutcome {
    pub fn validate(&self) -> Result<()> {
        validate_tracking_ref(&self.tracking_ref, "tracking_ref")?;
        validate_oid(&self.tracking_oid, "tracking_oid")
    }

    /// Prove that the applied readback is the exact frozen fetch target.
    pub fn validate_for(&self, frozen: &FrozenGitFetch) -> Result<()> {
        self.validate()?;
        frozen.validate()?;
        if self.tracking_ref != frozen.tracking_ref || self.tracking_oid != frozen.advertised_oid {
            return Err(invalid(
                "git.fetch readback differs from its frozen tracking target",
            ));
        }
        Ok(())
    }
}

fn validate_literal(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.contains("${") || value.chars().any(char::is_control) {
        return Err(invalid(format!(
            "git.fetch {field} must be nonempty literal text without control characters"
        )));
    }
    Ok(())
}

fn canonical_git_fetch_binding_args(
    repository: &str,
    remote: &str,
    source_ref: &str,
    tracking_ref: &str,
    timeout_ms: i64,
) -> Result<Value> {
    if timeout_ms <= 0 {
        return Err(invalid("git.fetch timeout_ms must be positive"));
    }
    let args = GitFetchDefinitionArgs {
        repository: repository.to_owned(),
        remote: remote.to_owned(),
        source_ref: source_ref.to_owned(),
        tracking_ref: tracking_ref.to_owned(),
        timeout: Some(format!("{timeout_ms}ms")),
    };
    args.validate()?;
    serde_json::to_value(args).map_err(|_| invalid("git.fetch binding args cannot be encoded"))
}

fn validate_absolute_path(value: &str, field: &str) -> Result<()> {
    validate_literal(value, field)?;
    if !Path::new(value).is_absolute() {
        return Err(invalid(format!("git.fetch {field} must be absolute")));
    }
    Ok(())
}

fn validate_full_ref(value: &str, field: &str) -> Result<()> {
    validate_literal(value, field)?;
    let invalid_character = value
        .chars()
        .any(|character| matches!(character, '~' | '^' | ':' | '?' | '*' | '[' | '\\'));
    if !value.starts_with("refs/")
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("//")
        || value.contains("..")
        || value.contains("@{")
        || invalid_character
        || value.bytes().any(|byte| byte <= b' ' || byte == 0x7f)
        || value.split('/').any(|component| {
            component.is_empty() || component.starts_with('.') || component.ends_with(".lock")
        })
    {
        return Err(invalid(format!(
            "git.fetch {field} must be a full valid Git ref"
        )));
    }
    Ok(())
}

fn validate_tracking_ref(value: &str, field: &str) -> Result<()> {
    if !value
        .strip_prefix("refs/remotes/")
        .is_some_and(|suffix| !suffix.is_empty())
    {
        return Err(invalid(format!(
            "git.fetch {field} must be under refs/remotes/"
        )));
    }
    validate_full_ref(value, field)
}

fn validate_oid(value: &str, field: &str) -> Result<()> {
    if !is_git_oid(value) {
        return Err(invalid(format!(
            "git.fetch {field} must be an exact Git object ID"
        )));
    }
    Ok(())
}

/// Whether `value` is an exact Git object ID: 40 hex characters (SHA-1) or
/// 64 (SHA-256 repositories).
pub fn is_git_oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_setup_file(file: &FrozenSetupFile) -> Result<()> {
    validate_absolute_path(&file.canonical_path, "credential helper receipt path")?;
    if file.sha256.len() != 64 || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("git.fetch credential helper receipt is malformed"));
    }
    Ok(())
}

fn endpoint_has_userinfo(endpoint: &str) -> bool {
    if let Some((_, rest)) = endpoint.split_once("://") {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        return authority.contains('@');
    }
    endpoint
        .split(['/', '\\'])
        .next()
        .is_some_and(|authority| authority.contains('@'))
}

fn validate_publish_literal(value: &str, field: &str, max_bytes: usize) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > max_bytes
        || value.contains("${")
        || value.chars().any(char::is_control)
    {
        return Err(invalid(format!(
            "git.publish {field} is not bounded literal text"
        )));
    }
    Ok(())
}

fn validate_commit_subject(value: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_GIT_PUBLISH_SUBJECT_BYTES
        || value
            .chars()
            .any(|character| matches!(character, '\r' | '\n'))
        || value.chars().any(char::is_control)
    {
        return Err(invalid(
            "git.publish subject is not a bounded first answer line",
        ));
    }
    Ok(())
}

pub fn validate_git_publish_subject(value: &str) -> Result<()> {
    validate_commit_subject(value)
}

fn validate_publish_remote_name(value: &str) -> Result<()> {
    validate_publish_literal(value, "remote", 128)?;
    if value.starts_with('-')
        || value.starts_with('.')
        || value.ends_with('.')
        || value.contains("..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(invalid("git.publish remote is not a safe literal name"));
    }
    Ok(())
}

fn validate_sha256(value: &str, field: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(format!(
            "git.publish {field} must be a SHA-256 digest"
        )));
    }
    Ok(())
}

fn validate_branch(value: &str) -> Result<()> {
    validate_publish_literal(value, "branch", 512)?;
    if value.starts_with('-') || value.starts_with("refs/") {
        return Err(invalid("git.publish branch is not a branch name"));
    }
    validate_full_ref(&format!("refs/heads/{value}"), "branch")
}

fn validate_publish_endpoint(endpoint: &str) -> Result<()> {
    validate_publish_literal(endpoint, "endpoint", 4_096)?;
    if endpoint.starts_with('-') || endpoint_has_userinfo(endpoint) {
        return Err(invalid(
            "git.publish endpoint cannot contain credentials or options",
        ));
    }
    Ok(())
}

fn valid_agent_id(value: &str) -> bool {
    value
        .strip_prefix('a')
        .and_then(|number| number.parse::<i64>().ok())
        .is_some_and(|number| number > 0 && format!("a{number}") == value)
}

fn required_expected_old_oid<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let oid = Option::<String>::deserialize(deserializer)?;
    if let Some(oid) = &oid {
        validate_oid(oid, "expected_old_oid").map_err(serde::de::Error::custom)?;
    }
    Ok(oid)
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new("invalid", message)
}
