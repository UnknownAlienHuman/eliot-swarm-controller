//! Read-only API for persisted Git task records.
use crate::{Error, Result, parse_agent_id, validate_command_id};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const TASK_RECORD_PAGE_SIZE: usize = 100;
pub const TASK_RECORD_TEXT_PAGE_BYTES: usize = 32 * 1024;
pub const MAX_TASK_SOURCES: usize = 8;

/// A source selector addresses a numbered entry in the pinned snapshot manifest.
pub fn task_source_index(section: &str) -> Option<usize> {
    let digit = section.strip_prefix("source:")?.as_bytes();
    if let [index @ b'0'..=b'9'] = digit {
        return Some(usize::from(index - b'0')).filter(|index| *index < MAX_TASK_SOURCES);
    }
    None
}

fn valid_section(section: &str) -> bool {
    matches!(
        section,
        "task.md" | "card.json" | "instructions.json" | "journal.md" | "manifest"
    ) || task_source_index(section).is_some()
}

/// The contents of the `manifest` section. Coverage describes this stored
/// revision only, independently of later commands or the original filesystem.
#[derive(JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct TaskSnapshotManifest {
    pub original: Option<TaskSnapshotOrigin>,
    pub task_saved: bool,
    pub instructions_saved: bool,
    pub corrections_retained: usize,
    pub corrections_omitted_at_least: usize,
    pub correction_history_incomplete: bool,
    pub correction_history_unresolved: bool,
    pub references_without_saved_content: Option<usize>,
    pub sources: Vec<TaskSnapshotSource>,
}

#[derive(JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct TaskSnapshotOrigin {
    pub command_id: String,
    pub agent: String,
    pub generation: i64,
    pub principal: String,
    pub verb: String,
    pub state: crate::CommandState,
}

#[derive(JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct TaskSnapshotSource {
    pub section: String,
    pub name: String,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskListRequest {
    pub agent: String,
    pub generation: i64,
    #[serde(default = "default_task_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<TaskListCursor>,
}

impl TaskListRequest {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0
            || !(1..=TASK_RECORD_PAGE_SIZE).contains(&self.limit)
            || self.cursor.as_ref().is_some_and(|c| {
                c.agent != self.agent
                    || c.generation != self.generation
                    || c.revision < 0
                    || validate_command_id(&c.after).is_err()
                    || c.after.contains('\0')
            })
        {
            return Err(Error::invalid_field("params", "task list request"));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &TaskListResponse) -> Result<()> {
        reply.validate()?;
        if reply.agent != self.agent
            || reply.generation != self.generation
            || reply.tasks.len() > self.limit
            || self.cursor.as_ref().is_some_and(|cursor| {
                cursor.revision != reply.revision
                    || reply
                        .tasks
                        .first()
                        .is_some_and(|item| item.task_command <= cursor.after)
            })
            || reply.next_cursor.as_ref().is_some_and(|cursor| {
                cursor.agent != self.agent
                    || cursor.generation != self.generation
                    || cursor.revision != reply.revision
                    || reply.tasks.len() != self.limit
                    || reply
                        .tasks
                        .last()
                        .is_none_or(|row| row.task_command != cursor.after)
            })
        {
            return Err(Error::new("unknown", "task.list response is invalid"));
        }
        Ok(())
    }
}

fn default_task_limit() -> usize {
    50
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskListCursor {
    pub agent: String,
    pub generation: i64,
    pub revision: i64,
    pub after: String,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskListItem {
    pub task_command: String,
    pub task_ref: String,
    pub principal: Option<String>,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskListResponse {
    pub agent: String,
    pub generation: i64,
    pub revision: i64,
    pub tasks: Vec<TaskListItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<TaskListCursor>,
}

impl TaskListResponse {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0
            || self.revision < 0
            || self.tasks.len() > TASK_RECORD_PAGE_SIZE
            || !self
                .tasks
                .windows(2)
                .all(|pair| pair[0].task_command < pair[1].task_command)
            || self.tasks.iter().any(|row| {
                validate_command_id(&row.task_command).is_err()
                    || row.task_command.contains('\0')
                    || row.task_ref.is_empty()
                    || row.task_ref.contains('\0')
            })
        {
            return Err(Error::new("unknown", "task.list response is invalid"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskGetRequest {
    pub agent: String,
    pub generation: i64,
    pub task_command: String,
    pub section: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
}

impl TaskGetRequest {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0
            || validate_command_id(&self.task_command).is_err()
            || self.task_command.contains('\0')
            || !valid_section(&self.section)
            || task_source_index(&self.section).is_some() && self.revision.is_none()
            || self
                .revision
                .as_ref()
                .is_some_and(|r| r.is_empty() || r.len() > 64)
            || self
                .content_digest
                .as_ref()
                .is_some_and(|d| d.len() != 64 || !d.bytes().all(|b| b.is_ascii_hexdigit()))
            || self.offset.is_some_and(|o| o > 16 * 1024 * 1024)
            || self.offset.is_some_and(|offset| offset > 0)
                && (self.revision.is_none() || self.content_digest.is_none())
        {
            return Err(Error::invalid_field("params", "task record request"));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &TaskGetResponse) -> Result<()> {
        reply.validate()?;
        if reply.agent != self.agent
            || reply.generation != self.generation
            || reply.task_command != self.task_command
            || reply.section != self.section
            || reply.offset != self.offset.unwrap_or(0)
            || self
                .revision
                .as_ref()
                .is_some_and(|revision| revision != &reply.revision)
            || self
                .content_digest
                .as_ref()
                .is_some_and(|digest| digest != &reply.content_digest)
        {
            return Err(Error::new("unknown", "task.get response is invalid"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskGetResponse {
    pub agent: String,
    pub generation: i64,
    pub task_command: String,
    pub section: String,
    /// Exact Git commit object ID used to read this section.
    pub revision: String,
    /// Digest of the full redacted section, binding UTF-8 continuation offsets.
    pub content_digest: String,
    pub offset: usize,
    pub text: String,
    pub next_offset: Option<usize>,
}

impl TaskGetResponse {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        let valid_hex = |value: &str| value.bytes().all(|byte| byte.is_ascii_hexdigit());
        let end = self.offset.checked_add(self.text.len());
        if self.generation < 0
            || validate_command_id(&self.task_command).is_err()
            || self.task_command.contains('\0')
            || !valid_section(&self.section)
            || !matches!(self.revision.len(), 40 | 64)
            || !valid_hex(&self.revision)
            || self.content_digest.len() != 64
            || !valid_hex(&self.content_digest)
            || self.text.len() > TASK_RECORD_TEXT_PAGE_BYTES
            || self
                .next_offset
                .is_some_and(|next| end != Some(next) || next <= self.offset)
        {
            return Err(Error::new("unknown", "task.get response is invalid"));
        }
        Ok(())
    }
}
