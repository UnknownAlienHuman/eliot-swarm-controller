//! Bounded, generation-owned artifact cleanup. Effects and path policy live in the daemon.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Build,
    DataDir,
    Temp,
    Worktree,
    Branch,
    NativeSession,
    Other,
}

pub const MAX_CLEANUP_ITEMS: usize = 64;

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRegisterRequest {
    pub id: String,
    pub path: String,
    pub kind: ArtifactKind,
    pub purpose: String,
}
impl ArtifactRegisterRequest {
    pub fn validate(&self) -> Result<()> {
        cleanup_id(&self.id)?;
        if self.path.is_empty()
            || self.path.len() > 4096
            || self.path.contains('\0')
            || self.purpose.trim().is_empty()
            || self.purpose.len() > 512
            || matches!(
                self.kind,
                ArtifactKind::Branch | ArtifactKind::NativeSession | ArtifactKind::Worktree
            )
        {
            return Err(Error::new(
                "invalid",
                "invalid artifact path, kind or purpose; worktrees are registered by Swarm",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactView {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub project: String,
    pub tree: Vec<String>,
    pub path: String,
    pub kind: ArtifactKind,
    pub purpose: String,
    pub identity: String,
    pub created_at: i64,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<CleanupGitBinding>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupGitBinding {
    pub repo: String,
    pub common: String,
    pub branch: String,
    pub worktree: String,
    pub admin: String,
    pub admin_identity: String,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupScope {
    SelfTree,
    Project,
    All,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupPlanRequest {
    pub scope: CleanupScope,
    #[serde(default)]
    pub project: Option<String>,
    /// Unix epoch milliseconds, exclusive creation cutoff.
    #[serde(default)]
    pub older_than: Option<i64>,
}
impl CleanupPlanRequest {
    pub fn validate(&self) -> Result<()> {
        if self.older_than.is_some_and(|v| v < 0)
            || (self.scope == CleanupScope::Project
                && self
                    .project
                    .as_ref()
                    .is_none_or(|p| p.is_empty() || p.len() > 4096))
            || (self.scope != CleanupScope::Project && self.project.is_some())
        {
            return Err(Error::new("invalid", "cleanup scope or cutoff is invalid"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupCandidate {
    pub artifact: ArtifactView,
    pub bytes: Option<u64>,
    pub verdict: String,
    pub reason: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupPlan {
    pub plan_id: String,
    pub created_at: i64,
    pub items: Vec<CleanupCandidate>,
    pub truncated: bool,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupApplyRequest {
    pub id: String,
    pub plan_id: String,
}
impl CleanupApplyRequest {
    pub fn validate(&self) -> Result<()> {
        cleanup_id(&self.id)?;
        cleanup_id(&self.plan_id)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupItemOutcome {
    pub artifact_id: String,
    pub outcome: String,
    pub reason: Option<String>,
    pub bytes_reclaimed: u64,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupApplyResponse {
    pub plan_id: String,
    pub items: Vec<CleanupItemOutcome>,
    pub bytes_reclaimed: u64,
}

fn cleanup_id(id: &str) -> Result<()> {
    if id.len() != 26
        || !id
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err(Error::new("invalid", "cleanup id must be a ULID"));
    }
    Ok(())
}
