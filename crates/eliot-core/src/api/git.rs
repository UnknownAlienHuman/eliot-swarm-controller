//! Typed request and result payloads for the connected native Git fetch action.
//!
//! These DTOs are pure data. Repository registration, transport construction,
//! process ownership, Store admission, and effect settlement belong to callers.

use crate::{Error, FrozenSetupFile, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

/// Default for the optional rule-authored fetch timeout.
pub const DEFAULT_GIT_FETCH_TIMEOUT_MS: i64 = 10 * 60 * 1_000;

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
