//! Typed candidate-bound projection payload validation.
//!
//! These DTOs enforce exact repository, candidate, Task revision and content
//! bounds. This module exposes no remote writer or write-authorization state.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

const MAX_SUMMARY_BYTES: usize = 64 * 1024;
const MAX_LABELS: usize = 32;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Queued,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckConclusion {
    Success,
    Failure,
    Neutral,
    Cancelled,
    TimedOut,
    ActionRequired,
    Stale,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckProjection {
    pub repository_id: i64,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub candidate_ref: String,
    pub head_sha: String,
    pub name: String,
    pub status: CheckStatus,
    #[serde(default)]
    pub conclusion: Option<CheckConclusion>,
    pub summary: String,
}

impl CheckProjection {
    pub fn validate(&self) -> Result<()> {
        validate_target(
            self.repository_id,
            &self.task_id,
            self.task_revision,
            &self.attempt_id,
            &self.candidate_ref,
            &self.head_sha,
        )?;
        if self.name.trim().is_empty()
            || self.name.len() > 255
            || self.summary.len() > MAX_SUMMARY_BYTES
        {
            return Err(Error::invalid("check name or summary is outside its bound"));
        }
        match (self.status, self.conclusion) {
            (CheckStatus::Completed, Some(_))
            | (CheckStatus::Queued | CheckStatus::InProgress, None) => Ok(()),
            _ => Err(Error::invalid(
                "completed checks require a conclusion; active checks cannot carry one",
            )),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryProjection {
    pub repository_id: i64,
    pub task_id: String,
    pub task_revision: i64,
    pub candidate_ref: String,
    pub head_sha: String,
    pub title: String,
    pub body: String,
}

impl SummaryProjection {
    pub fn validate(&self) -> Result<()> {
        validate_target(
            self.repository_id,
            &self.task_id,
            self.task_revision,
            "summary",
            &self.candidate_ref,
            &self.head_sha,
        )?;
        if self.title.trim().is_empty()
            || self.title.len() > 255
            || self.body.len() > MAX_SUMMARY_BYTES
        {
            return Err(Error::invalid("summary title or body is outside its bound"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LabelProjection {
    pub repository_id: i64,
    pub task_id: String,
    pub task_revision: i64,
    pub candidate_ref: String,
    pub head_sha: String,
    pub managed_labels: Vec<String>,
}

impl LabelProjection {
    pub fn validate(&self) -> Result<()> {
        validate_target(
            self.repository_id,
            &self.task_id,
            self.task_revision,
            "labels",
            &self.candidate_ref,
            &self.head_sha,
        )?;
        if self.managed_labels.len() > MAX_LABELS
            || self.managed_labels.iter().any(|label| {
                label.trim().is_empty() || label.len() > 50 || label.chars().any(char::is_control)
            })
        {
            return Err(Error::invalid(
                "managed labels exceed their count or text bound",
            ));
        }
        Ok(())
    }
}

fn validate_target(
    repository_id: i64,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    candidate_ref: &str,
    head_sha: &str,
) -> Result<()> {
    if repository_id <= 0
        || task_id.trim().is_empty()
        || task_revision <= 0
        || attempt_id.trim().is_empty()
        || candidate_ref.trim().is_empty()
        || !matches!(head_sha.len(), 40 | 64)
        || !head_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::invalid(
            "projection requires an exact repository, Task candidate, Attempt and full head SHA",
        ));
    }
    Ok(())
}
