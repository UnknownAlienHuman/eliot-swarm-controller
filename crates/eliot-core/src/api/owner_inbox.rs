use crate::{Error, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(
    JsonSchema, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OwnerInboxKind {
    HarnessRequest,
    UnknownCommand,
    UnknownTurn,
    Hold,
    Agent,
    BlockedGoal,
    LaunchQueue,
    Stuck,
    HarnessCheck,
    HarnessAuth,
    /// A pending handoff or a successor continued after uncertain turn effects.
    Handoff,
    /// Work that is not fully recorded in Git.
    WorkNotInGit,
    ArtifactCapacity,
}

impl OwnerInboxKind {
    pub const ALL: [Self; 13] = [
        Self::HarnessRequest,
        Self::UnknownCommand,
        Self::UnknownTurn,
        Self::Hold,
        Self::Agent,
        Self::BlockedGoal,
        Self::LaunchQueue,
        Self::Stuck,
        Self::HarnessCheck,
        Self::HarnessAuth,
        Self::Handoff,
        Self::WorkNotInGit,
        Self::ArtifactCapacity,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HarnessRequest => "harness_request",
            Self::UnknownCommand => "unknown_command",
            Self::UnknownTurn => "unknown_turn",
            Self::Hold => "hold",
            Self::Agent => "agent",
            Self::BlockedGoal => "blocked_goal",
            Self::LaunchQueue => "launch_queue",
            Self::Stuck => "stuck",
            Self::HarnessCheck => "harness_check",
            Self::HarnessAuth => "harness_auth",
            Self::Handoff => "handoff",
            Self::WorkNotInGit => "work_not_in_git",
            Self::ArtifactCapacity => "artifact_capacity",
        }
    }
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerInboxItemRef {
    pub kind: OwnerInboxKind,
    pub subject: String,
    pub revision: String,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct OwnerAction {
    pub choice: String,
    pub method: String,
    pub args: Value,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct OwnerInboxItem {
    pub kind: OwnerInboxKind,
    pub subject: String,
    pub revision: String,
    pub age_ms: i64,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub recommended: Option<OwnerAction>,
    pub choices: Vec<OwnerAction>,
    /// Exact commands for the Owner when no single action does the work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

impl OwnerInboxItem {
    pub fn cursor(&self) -> String {
        format!("{}/{}", self.kind.as_str(), self.subject)
    }

    pub fn matches(&self, item: &OwnerInboxItemRef) -> bool {
        self.kind == item.kind && self.subject == item.subject && self.revision == item.revision
    }
}

#[derive(JsonSchema, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerInboxRequest {
    pub after: Option<String>,
    pub limit: Option<usize>,
}

impl OwnerInboxRequest {
    pub const MAX_ROWS: usize = 100;

    pub fn effective_limit(&self) -> Result<usize> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=Self::MAX_ROWS).contains(&limit)
            || self.after.as_ref().is_some_and(|after| !valid_key(after))
        {
            return Err(Error::new("invalid", "invalid owner inbox page"));
        }
        Ok(limit)
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct OwnerInboxResponse {
    pub items: Vec<OwnerInboxItem>,
    pub next: Option<String>,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerDecideRequest {
    pub id: String,
    pub item: OwnerInboxItemRef,
    pub choice: String,
    pub args: Option<Value>,
}

impl OwnerDecideRequest {
    pub fn validate(&self) -> Result<()> {
        super::launch_queue::validate_id(&self.id)?;
        if !valid_key(&self.item.subject)
            || !valid_key(&self.item.revision)
            || self.choice.is_empty()
            || self.choice.len() > 64
            || self.args.as_ref().is_some_and(|args| !args.is_object())
        {
            return Err(Error::new("invalid", "invalid owner inbox decision"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct OwnerDecideResponse {
    pub snoozed: bool,
    pub result: Option<Value>,
}

/// Maximum number of explicitly reviewed inbox snapshots in one bulk decision.
pub const MAX_BULK_OWNER_DECISIONS: usize = 100;
/// Keep a full bounded batch request and its aggregate response below one frame.
const MAX_BULK_OWNER_DECISION_REFS_BYTES: usize = 16 * 1024;

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerBulkDecideRequest {
    /// Stable batch identity. The daemon binds it to the exact ordered refs.
    pub id: String,
    pub items: Vec<OwnerInboxItemRef>,
}

impl OwnerBulkDecideRequest {
    pub fn validate(&self) -> Result<()> {
        super::launch_queue::validate_id(&self.id)?;
        if self.items.is_empty() || self.items.len() > MAX_BULK_OWNER_DECISIONS {
            return Err(Error::new(
                "invalid",
                "owner decision batch must contain 1 to 100 item refs",
            ));
        }
        let mut seen = BTreeSet::new();
        for item in &self.items {
            if !valid_key(&item.subject)
                || !valid_key(&item.revision)
                || !seen.insert((item.kind, item.subject.as_str(), item.revision.as_str()))
            {
                return Err(Error::new(
                    "invalid",
                    "owner decision batch contains an invalid or duplicate item ref",
                ));
            }
        }
        let refs_bytes = serde_json::to_vec(&self.items)
            .map_err(|_| Error::new("invalid", "owner decision refs could not be encoded"))?;
        if refs_bytes.len() > MAX_BULK_OWNER_DECISION_REFS_BYTES {
            return Err(Error::new(
                "invalid",
                "owner decision refs exceed the 16 KiB batch bound",
            ));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct OwnerBulkDecisionApplied {
    pub item: OwnerInboxItemRef,
    pub mutation_id: String,
    /// Preserves the admitted/completed state for actions which finish later.
    pub outcome: String,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct OwnerBulkDecisionSkipped {
    pub item: OwnerInboxItemRef,
    pub mutation_id: String,
    pub reason: String,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct OwnerBulkDecisionFailed {
    pub item: OwnerInboxItemRef,
    pub mutation_id: String,
    pub code: String,
    pub message: String,
    /// True when the action may have taken effect but has no settled readback.
    pub uncertain: bool,
}

#[derive(JsonSchema, Clone, Debug, Default, Deserialize, Serialize)]
pub struct OwnerBulkDecideResponse {
    pub applied: Vec<OwnerBulkDecisionApplied>,
    pub skipped: Vec<OwnerBulkDecisionSkipped>,
    pub failed: Vec<OwnerBulkDecisionFailed>,
}

#[derive(JsonSchema, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct SwarmLiveness {
    pub moving: usize,
    pub asked: usize,
    #[serde(default)]
    pub parked: usize,
    pub stuck: usize,
    pub long: usize,
    pub reasons: BTreeMap<String, usize>,
}

fn valid_key(value: &str) -> bool {
    !value.is_empty() && value.len() <= 2048 && !value.chars().any(char::is_control)
}
