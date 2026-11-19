//! Owner readback of the worktrees Swarm manages (`workspace.list`): one
//! bounded page of retained managed-workspace rows, ordered by Agent ID.
use crate::{Error, Result, WorkspacePhase, parse_agent_id};
use serde::{Deserialize, Serialize};

/// `workspace.list {after?, limit?}`: managed worktrees not yet removed,
/// after the Agent ID cursor, at most `limit` (1–1,000, default 100).
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceListRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Agent ID cursor: the previous page's next.")]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl Default for WorkspaceListRequest {
    fn default() -> Self {
        Self {
            after: None,
            limit: super::default_limit(),
        }
    }
}

impl WorkspaceListRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(after) = &self.after {
            parse_agent_id(after)
                .map_err(|_| Error::invalid_field("params.after", "Agent ID cursor"))?;
        }
        Ok(())
    }

    /// The page must bind to this request: rows after the cursor, in
    /// order, within the limit, `next` at the last row.
    pub fn validate_response(&self, response: &WorkspaceListResponse) -> Result<()> {
        response.validate()?;
        let after = self.after.as_deref().map(parse_agent_id).transpose()?;
        if response.workspaces.len() > self.limit
            || response
                .workspaces
                .first()
                .zip(after)
                .is_some_and(|(row, after)| parse_agent_id(&row.agent).is_ok_and(|n| n <= after))
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

/// One managed worktree: the Agent it was prepared for, the source
/// repository and branch, where it lives and its preparation phase.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorkspaceView {
    pub agent: String,
    pub project: String,
    pub repo_path: String,
    pub branch: String,
    pub worktree_path: String,
    pub phase: WorkspacePhase,
    pub updated_at: i64,
}

/// One page: `total` counts every listed row ignoring the cursor,
/// `truncated` says more rows follow, `next` is the last row's Agent.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceListResponse {
    pub workspaces: Vec<ManagedWorkspaceView>,
    pub total: usize,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

impl WorkspaceListResponse {
    pub fn validate(&self) -> Result<()> {
        let mut previous = 0;
        for row in &self.workspaces {
            let number = parse_agent_id(&row.agent).map_err(|_| invalid_response())?;
            if number <= previous
                || row.worktree_path.trim().is_empty()
                || row.repo_path.trim().is_empty()
                || row.phase == WorkspacePhase::Removed
            {
                return Err(invalid_response());
            }
            previous = number;
        }
        if self.next.as_deref() != self.workspaces.last().map(|row| row.agent.as_str())
            || self.total < self.workspaces.len()
            || (self.truncated && self.workspaces.is_empty())
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

fn invalid_response() -> Error {
    Error::new("unknown", "workspace.list response is invalid")
}
