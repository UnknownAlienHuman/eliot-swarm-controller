use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Error, Result, parse_agent_id};

const MAX_CHECKPOINT_PAGE: usize = 100;

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointListRequest {
    pub agent: String,
    pub generation: i64,
    #[serde(default = "default_checkpoint_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<CheckpointListCursor>,
}

fn default_checkpoint_limit() -> usize {
    50
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointListCursor {
    pub agent: String,
    pub generation: i64,
    pub revision: String,
    pub after: String,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct CheckpointListEntry {
    pub checkpoint_ref: String,
    pub oid: String,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct CheckpointListResponse {
    pub agent: String,
    pub generation: i64,
    pub revision: String,
    pub entries: Vec<CheckpointListEntry>,
    pub next: Option<CheckpointListCursor>,
}

impl CheckpointListRequest {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0 || !(1..=MAX_CHECKPOINT_PAGE).contains(&self.limit) {
            return Err(Error::new("invalid", "checkpoint list bounds are invalid"));
        }
        if self.cursor.as_ref().is_some_and(|cursor| {
            cursor.agent != self.agent
                || cursor.generation != self.generation
                || cursor.revision.len() != 64
                || !cursor.revision.bytes().all(|b| b.is_ascii_hexdigit())
                || cursor.after.is_empty()
                || cursor.after.len() > 512
                || cursor.after.contains('\0')
        }) {
            return Err(Error::new(
                "invalid_cursor",
                "checkpoint list cursor is malformed",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &CheckpointListResponse) -> Result<()> {
        self.validate()?;
        let prefix = format!(
            "refs/eliot/checkpoints/{}/g{}/",
            self.agent, self.generation
        );
        if reply.agent != self.agent
            || reply.generation != self.generation
            || reply.revision.len() != 64
            || !reply.revision.bytes().all(|b| b.is_ascii_hexdigit())
            || reply.entries.len() > self.limit
            || reply
                .entries
                .iter()
                .any(|e| !e.checkpoint_ref.starts_with(&prefix) || !crate::api::is_git_oid(&e.oid))
            || reply
                .entries
                .windows(2)
                .any(|w| w[0].checkpoint_ref <= w[1].checkpoint_ref)
            || self
                .cursor
                .as_ref()
                .is_some_and(|c| c.revision != reply.revision)
            || self.cursor.as_ref().is_some_and(|c| {
                reply
                    .entries
                    .first()
                    .is_some_and(|e| e.checkpoint_ref.as_str() >= c.after.as_str())
            })
            || reply.next.as_ref().is_some_and(|c| {
                c.agent != self.agent
                    || c.generation != self.generation
                    || c.revision != reply.revision
                    || reply
                        .entries
                        .last()
                        .is_none_or(|e| e.checkpoint_ref != c.after)
            })
        {
            return Err(Error::new("unknown", "checkpoint.list response is invalid"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointGetRequest {
    pub agent: String,
    pub generation: i64,
    pub checkpoint_ref: String,
    pub oid: String,
    #[serde(default = "default_checkpoint_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<CheckpointGetCursor>,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointGetCursor {
    pub checkpoint_ref: String,
    pub oid: String,
    pub after: usize,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct CheckpointFileMetadata {
    pub path: String,
    pub mode: Option<String>,
    pub oid: Option<String>,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct CheckpointGetResponse {
    pub agent: String,
    pub generation: i64,
    pub checkpoint_ref: String,
    pub oid: String,
    pub head: String,
    pub staged: Vec<CheckpointFileMetadata>,
    pub worktree: Vec<CheckpointFileMetadata>,
    pub next: Option<CheckpointGetCursor>,
}

impl CheckpointGetRequest {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0
            || !crate::api::is_git_oid(&self.oid)
            || !self.checkpoint_ref.starts_with(&format!(
                "refs/eliot/checkpoints/{}/g{}/",
                self.agent, self.generation
            ))
            || self.checkpoint_ref.len() > 512
            || self.checkpoint_ref.contains('\0')
        {
            return Err(Error::new("invalid", "checkpoint identity is invalid"));
        }
        if !(1..=100).contains(&self.limit) {
            return Err(Error::new(
                "invalid",
                "checkpoint metadata page limit is invalid",
            ));
        }
        if self.cursor.as_ref().is_some_and(|cursor| {
            cursor.checkpoint_ref != self.checkpoint_ref || cursor.oid != self.oid
        }) {
            return Err(Error::new(
                "invalid_cursor",
                "checkpoint metadata cursor is malformed",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &CheckpointGetResponse) -> Result<()> {
        self.validate()?;
        let count = reply.staged.len() + reply.worktree.len();
        let after = self.cursor.as_ref().map_or(0, |c| c.after);
        let expected_next = after.checked_add(count);
        if reply.agent != self.agent
            || reply.generation != self.generation
            || reply.checkpoint_ref != self.checkpoint_ref
            || reply.oid != self.oid
            || !crate::api::is_git_oid(&reply.head)
            || count > self.limit
            || reply.staged.iter().chain(&reply.worktree).any(|f| {
                f.path.is_empty()
                    || f.path.starts_with('/')
                    || f.path.contains('\\')
                    || f.path.contains('\0')
                    || f.path
                        .split('/')
                        .any(|c| c.is_empty() || c == "." || c == "..")
                    || f.oid
                        .as_deref()
                        .is_some_and(|oid| !crate::api::is_git_oid(oid))
            })
            || reply.next.as_ref().is_some_and(|c| {
                c.checkpoint_ref != self.checkpoint_ref
                    || c.oid != self.oid
                    || Some(c.after) != expected_next
                    || count == 0
            })
            || serde_json::to_vec(reply).map_or(true, |bytes| bytes.len() > 64 * 1024)
        {
            return Err(Error::new("unknown", "checkpoint.get response is invalid"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRestoreRequest {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub checkpoint_ref: String,
    pub workspace: String,
}

impl CheckpointRestoreRequest {
    pub fn validate(&self) -> Result<()> {
        crate::api::inference::inference_id(&self.id)
            .map_err(|_| Error::new("invalid", "checkpoint restore id must be a ULID"))?;
        parse_agent_id(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "checkpoint generation is invalid"));
        }
        if self.checkpoint_ref.is_empty()
            || self.checkpoint_ref.len() > 512
            || self.checkpoint_ref.contains('\0')
        {
            return Err(Error::new("invalid", "checkpoint reference is invalid"));
        }
        if self.workspace.trim().is_empty()
            || self.workspace.len() > 32_768
            || self.workspace.contains('\0')
        {
            return Err(Error::new("invalid", "restore workspace is invalid"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRestoreResponse {
    pub head: String,
    pub paths: Vec<String>,
}
