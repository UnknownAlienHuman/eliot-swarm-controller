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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(agent: &str) -> ManagedWorkspaceView {
        ManagedWorkspaceView {
            agent: agent.to_owned(),
            project: "app".to_owned(),
            repo_path: r"C:\src\app".to_owned(),
            branch: "fix/a".to_owned(),
            worktree_path: format!(r"C:\data\worktrees\{agent}"),
            phase: WorkspacePhase::Ready,
            updated_at: 1,
        }
    }

    #[test]
    fn requests_are_strict_and_pages_bind_cursor_order_and_next() {
        let request: WorkspaceListRequest = serde_json::from_value(json!({})).unwrap();
        assert_eq!(request, WorkspaceListRequest::default());
        assert_eq!(request.limit, 100);
        assert!(serde_json::from_value::<WorkspaceListRequest>(json!({"limit": 0})).is_err());
        assert!(serde_json::from_value::<WorkspaceListRequest>(json!({"x": 1})).is_err());
        let bad_cursor = WorkspaceListRequest {
            after: Some("project-1".to_owned()),
            limit: 10,
        };
        assert!(bad_cursor.validate().is_err());

        let request = WorkspaceListRequest {
            after: Some("a2".to_owned()),
            limit: 2,
        };
        request.validate().unwrap();
        let page = WorkspaceListResponse {
            workspaces: vec![row("a3"), row("a7")],
            total: 5,
            truncated: true,
            next: Some("a7".to_owned()),
        };
        request.validate_response(&page).unwrap();
        // Rows at or before the cursor, a wrong next, unordered rows or a
        // removed worktree never read back as a page.
        let mut stale = page.clone();
        stale.workspaces[0] = row("a2");
        assert!(request.validate_response(&stale).is_err());
        let mut wrong_next = page.clone();
        wrong_next.next = Some("a3".to_owned());
        assert!(wrong_next.validate().is_err());
        let mut unordered = page.clone();
        unordered.workspaces.reverse();
        unordered.next = Some("a3".to_owned());
        assert!(unordered.validate().is_err());
        let mut removed = page;
        removed.workspaces[1].phase = WorkspacePhase::Removed;
        assert!(removed.validate().is_err());
        let empty = WorkspaceListResponse {
            workspaces: Vec::new(),
            total: 0,
            truncated: false,
            next: None,
        };
        empty.validate().unwrap();
    }
}
