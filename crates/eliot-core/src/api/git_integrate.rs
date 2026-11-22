//! Typed local branch integration request and result payloads.
//!
//! Branch refs are resolved and pinned by the daemon before admission. This
//! module owns wire syntax only; Git, Store, Router and process custody remain
//! in their existing layers.

use crate::{AgentControl, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, path::Path};

pub const DEFAULT_GIT_INTEGRATE_TIMEOUT_MS: i64 = 10 * 60 * 1_000;
pub const MAX_GIT_INTEGRATE_BRANCHES: usize = 32;
pub const MAX_GIT_INTEGRATE_CONFLICT_PATHS: usize = 128;
pub const MAX_GIT_INTEGRATE_CHECK_TAIL_BYTES: usize = 6 * 1024;

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIntegrateDefinitionArgs {
    pub repository: String,
    pub base_branch: String,
    pub branches: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_forward_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

impl GitIntegrateDefinitionArgs {
    pub fn validate(&self) -> Result<()> {
        literal(&self.repository, "repository", 128)?;
        branch(&self.base_branch, "base_branch")?;
        if self.branches.is_empty() || self.branches.len() > MAX_GIT_INTEGRATE_BRANCHES {
            return Err(invalid("git.integrate branch count is outside its bound"));
        }
        let mut seen = HashSet::with_capacity(self.branches.len());
        for name in &self.branches {
            branch(name, "branches")?;
            if name == &self.base_branch || !seen.insert(name.as_str()) {
                return Err(invalid(
                    "git.integrate branches must be unique and differ from base",
                ));
            }
        }
        if let Some(name) = &self.fast_forward_to {
            branch(name, "fast_forward_to")?;
        }
        if let Some(timeout) = &self.timeout {
            literal(timeout, "timeout", 64)?;
        }
        Ok(())
    }

    pub fn binding_args(&self, timeout_ms: i64) -> Result<Value> {
        self.validate()?;
        validate_timeout(timeout_ms)?;
        serde_json::to_value(Self {
            repository: self.repository.clone(),
            base_branch: self.base_branch.clone(),
            branches: self.branches.clone(),
            fast_forward_to: self.fast_forward_to.clone(),
            timeout: Some(format!("{timeout_ms}ms")),
        })
        .map_err(|_| invalid("git.integrate binding args cannot be encoded"))
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIntegrateBranchPin {
    pub branch: String,
    pub oid: String,
}

impl GitIntegrateBranchPin {
    pub fn validate(&self) -> Result<()> {
        branch(&self.branch, "branch")?;
        oid(&self.oid, "oid")
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIntegrateCaller {
    pub agent: String,
    pub generation: i64,
    pub workspace: String,
    pub project_scope: String,
    pub control: AgentControl,
}

impl GitIntegrateCaller {
    pub fn validate(&self) -> Result<()> {
        if !valid_agent(&self.agent)
            || self.generation <= 0
            || !matches!(self.control, AgentControl::Tree | AgentControl::Project)
        {
            return Err(invalid("git.integrate caller binding is invalid"));
        }
        absolute_path(&self.workspace, "caller.workspace")?;
        literal(&self.project_scope, "caller.project_scope", 4_096)?;
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIntegrateTargetPin {
    pub branch: String,
    pub expected_oid: String,
}

impl GitIntegrateTargetPin {
    pub fn validate(&self) -> Result<()> {
        branch(&self.branch, "fast_forward_to")?;
        oid(&self.expected_oid, "expected_oid")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenGitIntegrate {
    pub repository: String,
    pub registration_revision: i64,
    pub canonical_checkout: String,
    pub common_dir: String,
    pub base_branch: String,
    pub base_oid: String,
    pub branches: Vec<GitIntegrateBranchPin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller: Option<GitIntegrateCaller>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_forward: Option<GitIntegrateTargetPin>,
    pub timeout_ms: i64,
}

impl FrozenGitIntegrate {
    pub fn validate(&self) -> Result<()> {
        literal(&self.repository, "repository", 128)?;
        if self.registration_revision < 0 {
            return Err(invalid("git.integrate registration revision is invalid"));
        }
        absolute_path(&self.canonical_checkout, "canonical_checkout")?;
        absolute_path(&self.common_dir, "common_dir")?;
        branch(&self.base_branch, "base_branch")?;
        oid(&self.base_oid, "base_oid")?;
        if self.branches.is_empty() || self.branches.len() > MAX_GIT_INTEGRATE_BRANCHES {
            return Err(invalid("git.integrate frozen branch count is invalid"));
        }
        let mut seen = HashSet::with_capacity(self.branches.len());
        for pin in &self.branches {
            pin.validate()?;
            if pin.branch == self.base_branch || !seen.insert(pin.branch.as_str()) {
                return Err(invalid("git.integrate frozen branches are duplicated"));
            }
        }
        if let Some(caller) = &self.caller {
            caller.validate()?;
        }
        if let Some(target) = &self.fast_forward {
            target.validate()?;
        }
        validate_timeout(self.timeout_ms)
    }

    pub fn binding_args(&self) -> Result<Value> {
        self.validate()?;
        GitIntegrateDefinitionArgs {
            repository: self.repository.clone(),
            base_branch: self.base_branch.clone(),
            branches: self.branches.iter().map(|pin| pin.branch.clone()).collect(),
            fast_forward_to: self
                .fast_forward
                .as_ref()
                .map(|target| target.branch.clone()),
            timeout: Some(format!("{}ms", self.timeout_ms)),
        }
        .binding_args(self.timeout_ms)
    }
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitIntegrateStatus {
    Merged,
    Conflict,
    CheckFailed,
    Unknown,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitIntegrateCheckClassification {
    BranchFailures,
    CombinationOnly,
    IsolationUnknown,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIntegrateCheckFailure {
    pub exit_code: i32,
    pub tail: String,
    pub classification: GitIntegrateCheckClassification,
    pub failed_branches: Vec<String>,
    pub unknown_branches: Vec<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIntegrateRefUpdate {
    pub branch: String,
    pub oid: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitIntegrateOutcome {
    pub status: GitIntegrateStatus,
    pub base_oid: String,
    pub branches: Vec<GitIntegrateBranchPin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_oid: Option<String>,
    #[serde(default)]
    pub conflict_paths: Vec<String>,
    #[serde(default)]
    pub conflict_paths_truncated: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cleanup_pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_failure: Option<GitIntegrateCheckFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_forward: Option<GitIntegrateRefUpdate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
}

impl GitIntegrateOutcome {
    pub fn validate_for(&self, frozen: &FrozenGitIntegrate) -> Result<()> {
        frozen.validate()?;
        oid(&self.base_oid, "base_oid")?;
        if self.base_oid != frozen.base_oid || self.branches != frozen.branches {
            return Err(invalid(
                "git.integrate result differs from frozen input pins",
            ));
        }
        if let Some(value) = &self.integration_oid {
            oid(value, "integration_oid")?;
        }
        if self.conflict_paths.len() > MAX_GIT_INTEGRATE_CONFLICT_PATHS
            || self
                .conflict_paths
                .iter()
                .any(|path| path.is_empty() || path.len() > 4_096)
        {
            return Err(invalid("git.integrate conflict paths exceed their bound"));
        }
        if let Some(check) = &self.check_failure
            && (check.tail.len() > MAX_GIT_INTEGRATE_CHECK_TAIL_BYTES
                || check.failed_branches.len() > MAX_GIT_INTEGRATE_BRANCHES
                || check.unknown_branches.len() > MAX_GIT_INTEGRATE_BRANCHES
                || check
                    .failed_branches
                    .iter()
                    .chain(&check.unknown_branches)
                    .any(|name| !frozen.branches.iter().any(|pin| &pin.branch == name)))
        {
            return Err(invalid("git.integrate check attribution exceeds its bound"));
        }
        if let Some(update) = &self.fast_forward {
            branch(&update.branch, "fast_forward.branch")?;
            oid(&update.oid, "fast_forward.oid")?;
            if frozen
                .fast_forward
                .as_ref()
                .map(|target| target.branch.as_str())
                != Some(update.branch.as_str())
                || self.integration_oid.as_deref() != Some(update.oid.as_str())
            {
                return Err(invalid("git.integrate ref update differs from the result"));
            }
        }
        if self.failure_code.as_ref().is_some_and(|code| {
            code.is_empty() || code.len() > 128 || code.chars().any(char::is_control)
        }) {
            return Err(invalid("git.integrate failure code is invalid"));
        }
        Ok(())
    }
}

fn validate_timeout(value: i64) -> Result<()> {
    if !(1_000..=DEFAULT_GIT_INTEGRATE_TIMEOUT_MS).contains(&value) {
        return Err(invalid(
            "git.integrate timeout must be between 1000 and 600000 ms",
        ));
    }
    Ok(())
}

fn literal(value: &str, field: &str, max_bytes: usize) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > max_bytes
        || value.as_bytes().windows(2).any(|pair| pair == [36, 123])
        || value.chars().any(char::is_control)
    {
        return Err(invalid(format!(
            "git.integrate {field} must be bounded literal text"
        )));
    }
    Ok(())
}

fn branch(value: &str, field: &str) -> Result<()> {
    literal(value, field, 512)?;
    let invalid_ref = value == "@"
        || value.starts_with('-')
        || value.starts_with("refs/")
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("//")
        || value.contains("..")
        || value.contains("@{")
        || value
            .chars()
            .any(|ch| matches!(ch, '~' | '^' | ':' | '?' | '*' | '[' | '\\'))
        || value.bytes().any(|byte| byte <= b' ' || byte == 0x7f)
        || value
            .split('/')
            .any(|part| part.is_empty() || part.starts_with('.') || part.ends_with(".lock"));
    if invalid_ref {
        return Err(invalid(format!(
            "git.integrate {field} must be a valid local branch name"
        )));
    }
    Ok(())
}

/// Validate a local branch name that can be passed to branch integration.
pub fn validate_integration_branch(value: &str) -> Result<()> {
    branch(value, "branch")
}

fn absolute_path(value: &str, field: &str) -> Result<()> {
    literal(value, field, 4_096)?;
    if !Path::new(value).is_absolute() {
        return Err(invalid(format!("git.integrate {field} must be absolute")));
    }
    Ok(())
}

fn oid(value: &str, field: &str) -> Result<()> {
    if !(value.len() == 40 || value.len() == 64)
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid(format!(
            "git.integrate {field} must be an exact Git OID"
        )));
    }
    Ok(())
}

fn valid_agent(value: &str) -> bool {
    value
        .strip_prefix('a')
        .and_then(|number| number.parse::<i64>().ok())
        .is_some_and(|number| number > 0 && format!("a{number}") == value)
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new("invalid", message)
}
