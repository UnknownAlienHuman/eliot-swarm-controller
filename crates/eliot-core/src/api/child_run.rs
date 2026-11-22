//! One-call child launch, bounded wait, and compact result DTOs.

use super::MAX_WAIT_FINAL_TEXT_BYTES;
use crate::{Error, Result, limits::MAX_EVENT_BYTES};
use serde::{Deserialize, Serialize};

/// Default child.run long-poll, leaving room under the native 30 s RPC deadline.
pub const DEFAULT_CHILD_RUN_WAIT_MS: u64 = 20_000;
/// Hard wait bound for one child.run call; continuation reuses the same limit.
pub const MAX_CHILD_RUN_WAIT_MS: u64 = 25_000;
/// Maximum edit paths carried in the shared bounded child-result facts.
pub const MAX_CHILD_RUN_CHANGED_FILES: usize = 8;
/// Maximum UTF-8 bytes in one project-relative edit path carried by the facts.
pub const MAX_CHILD_RUN_FILE_PATH_BYTES: usize = 128;
/// Maximum complete commit ids carried by the shared bounded child-result facts.
pub const MAX_CHILD_RUN_COMMITS: usize = 32;
/// Maximum serialized bytes in one shared child-result facts DTO.
pub const MAX_CHILD_RUN_FACTS_BYTES: usize = 10 * 1024;

fn valid_child_run_id(id: &str) -> bool {
    id.len() == 26
        && matches!(id.as_bytes().first(), Some(b'0'..=b'7'))
        && id
            .bytes()
            .all(|byte| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&byte))
}

/// Initial request or continuation for the same child operation.
///
/// MCP callers may omit id; the existing MCP mutation wrapper mints it and
/// returns it for continuation. A start has text; a continuation omits it
/// and supplies the returned id.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunRequest {
    #[schemars(
        description = "Mutation id. MCP callers may omit it on the initial call; the server returns the minted id for retry and continuation."
    )]
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-maxBytes" = MAX_EVENT_BYTES), description = "Required on the initial call; omit it to continue waiting on id.")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        range(min = 0, max = 25000),
        description = "Optional wait in milliseconds, 0..=25000. Defaults to 20000."
    )]
    pub wait_ms: Option<u64>,
}

impl ChildRunRequest {
    pub fn validate(&self) -> Result<()> {
        if !valid_child_run_id(&self.id) {
            return Err(Error::invalid_mutation_id());
        }
        if self
            .wait_ms
            .is_some_and(|wait| wait > MAX_CHILD_RUN_WAIT_MS)
        {
            return Err(Error::invalid_field(
                "params.wait_ms",
                "integer from 0 through 25000",
            ));
        }
        match self.text.as_deref() {
            Some(text) => {
                if text.trim().is_empty() || text.len() > MAX_EVENT_BYTES || text.contains('\0') {
                    return Err(Error::invalid_field(
                        "params.text",
                        "nonblank string up to 65536 UTF-8 bytes without NUL",
                    ));
                }
            }
            None if self.sources.is_some() || self.profile.is_some() => {
                return Err(Error::invalid_field(
                    "params.text",
                    "required for an initial call; continuation accepts only id and wait_ms",
                ));
            }
            None => {}
        }
        if self
            .profile
            .as_ref()
            .is_some_and(|profile| profile.trim().is_empty())
        {
            return Err(Error::invalid_field(
                "params.profile",
                "nonblank profile name",
            ));
        }
        Ok(())
    }

    pub fn effective_wait_ms(&self) -> u64 {
        self.wait_ms.unwrap_or(DEFAULT_CHILD_RUN_WAIT_MS)
    }

    pub fn is_continuation(&self) -> bool {
        self.text.is_none()
    }
}

/// Exact child input/turn outcome. still_running and unknown both keep id
/// usable for continuation; only the former is an ordinary pending wait.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildRunStatus {
    Succeeded,
    Failed,
    Interrupted,
    Unknown,
    StillRunning,
    Rejected,
    Cancelled,
    NotFound,
    NotVisible,
}

/// State of the one durable completion digest registered for this operation.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildRunWake {
    NotNeeded,
    Pending,
    Delivered,
}

/// Explicitly distinguish retained evidence from values that were not
/// available at the exact Agent generation and scope.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum ChildRunFact<T> {
    Observed(T),
    Unobserved,
}

impl<T> ChildRunFact<T> {
    pub fn unobserved() -> Self {
        Self::Unobserved
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunChangedFile {
    #[schemars(extend("x-maxBytes" = MAX_CHILD_RUN_FILE_PATH_BYTES))]
    pub path: String,
    pub observed_at: i64,
}

/// Observed Git paths or generation-attributed edits. `truncated` marks the compact
/// response cap or a path too long for this DTO; absence of edit evidence is
/// reported as unobserved.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunChangedFiles {
    #[schemars(extend("x-maxItems" = MAX_CHILD_RUN_CHANGED_FILES))]
    pub files: Vec<ChildRunChangedFile>,
    pub truncated: bool,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunLineCounts {
    pub files_changed: u64,
    pub insertions: u64,
    pub deletions: u64,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunGoalCheck {
    pub exit_code: i32,
    pub checked_at: i64,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunFacts {
    pub changed_files: ChildRunFact<ChildRunChangedFiles>,
    pub line_counts: ChildRunFact<ChildRunLineCounts>,
    #[schemars(extend("x-maxItems" = MAX_CHILD_RUN_COMMITS))]
    pub commits: ChildRunFact<Vec<String>>,
    pub last_goal_check: ChildRunFact<ChildRunGoalCheck>,
    pub outside_claims: ChildRunFact<bool>,
}

impl ChildRunFacts {
    pub fn unobserved() -> Self {
        Self {
            changed_files: ChildRunFact::unobserved(),
            line_counts: ChildRunFact::unobserved(),
            commits: ChildRunFact::unobserved(),
            last_goal_check: ChildRunFact::unobserved(),
            outside_claims: ChildRunFact::unobserved(),
        }
    }

    pub fn is_unobserved(&self) -> bool {
        matches!(&self.changed_files, ChildRunFact::Unobserved)
            && matches!(&self.line_counts, ChildRunFact::Unobserved)
            && matches!(&self.commits, ChildRunFact::Unobserved)
            && matches!(&self.last_goal_check, ChildRunFact::Unobserved)
            && matches!(&self.outside_claims, ChildRunFact::Unobserved)
    }

    pub fn validate(&self) -> Result<()> {
        if let ChildRunFact::Observed(files) = &self.changed_files
            && (files.files.len() > MAX_CHILD_RUN_CHANGED_FILES
                || files.files.iter().any(|file| {
                    file.path.trim().is_empty()
                        || file.path.len() > MAX_CHILD_RUN_FILE_PATH_BYTES
                        || file.path.contains('\0')
                        || file.path.chars().any(char::is_control)
                        || file.observed_at < 0
                }))
        {
            return Err(Error::new(
                "invalid",
                "child result changed-file facts are invalid or exceed their bound",
            ));
        }
        if let ChildRunFact::Observed(counts) = &self.line_counts
            && (counts.files_changed > i64::MAX as u64
                || counts.insertions > i64::MAX as u64
                || counts.deletions > i64::MAX as u64)
        {
            return Err(Error::new(
                "invalid",
                "child result line counts exceed their bound",
            ));
        }
        if let ChildRunFact::Observed(commits) = &self.commits
            && (commits.len() > MAX_CHILD_RUN_COMMITS
                || commits.iter().any(|oid| {
                    !matches!(oid.len(), 40 | 64)
                        || !oid.bytes().all(|byte| byte.is_ascii_hexdigit())
                }))
        {
            return Err(Error::new(
                "invalid",
                "child result commit facts are invalid or exceed their bound",
            ));
        }
        if let ChildRunFact::Observed(check) = &self.last_goal_check
            && check.checked_at < 0
        {
            return Err(Error::new(
                "invalid",
                "child result Goal check stamp is invalid",
            ));
        }
        if serde_json::to_vec(self)
            .map_err(|_| Error::new("invalid", "child result facts could not be encoded"))?
            .len()
            > MAX_CHILD_RUN_FACTS_BYTES
        {
            return Err(Error::new(
                "invalid",
                "child result facts exceed their serialized bound",
            ));
        }
        Ok(())
    }
}

impl Default for ChildRunFacts {
    fn default() -> Self {
        Self::unobserved()
    }
}

/// Compact response for both the initial call and wait continuation.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunResponse {
    /// Same id as the launch command; pass it to child.run to continue.
    pub id: String,
    pub status: ChildRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-maxBytes" = MAX_WAIT_FINAL_TEXT_BYTES))]
    pub final_text: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
    pub completion_wake: ChildRunWake,
    #[schemars(extend("x-maxBytes" = MAX_CHILD_RUN_FACTS_BYTES))]
    pub facts: ChildRunFacts,
}

impl ChildRunResponse {
    pub fn validate(&self) -> Result<()> {
        if !valid_child_run_id(&self.id)
            || self.generation.is_some_and(|generation| generation < 0)
            || self.agent.is_some() != self.generation.is_some()
            || ((self.final_text.is_some() || self.truncated) && self.turn_id.is_none())
            || self
                .final_text
                .as_ref()
                .is_some_and(|text| text.len() > MAX_WAIT_FINAL_TEXT_BYTES)
        {
            return Err(Error::new(
                "invalid",
                "child.run response binding is invalid",
            ));
        }
        if let Some(agent) = self.agent.as_deref() {
            crate::parse_agent_id(agent)?;
        }
        if let Some(turn_id) = self.turn_id.as_deref()
            && turn_id.trim().is_empty()
        {
            return Err(Error::new("invalid", "child.run turn id is empty"));
        }
        self.facts.validate()?;
        if (self.agent.is_none()
            || !matches!(
                self.status,
                ChildRunStatus::Succeeded | ChildRunStatus::Failed | ChildRunStatus::Interrupted
            ))
            && !self.facts.is_unobserved()
        {
            return Err(Error::new(
                "invalid",
                "child.run facts require a visible settled Agent result",
            ));
        }
        Ok(())
    }
}
