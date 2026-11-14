//! Public Agent/daemon read grammar and pure reply binding checks.
//! Store authorization and the exact workspace target remain Router decisions.

use super::{GitView, ObservationView, ScriptInterpreter};
use crate::{
    Agent, AgentEdit, AgentState, CommandState, Error, NativeChildActivity, ProcessIdentity,
    ProcessRecord, Result, SessionUsage, TurnOutcome, TurnUsage,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const PROCESS_CURSOR_MAX_BYTES: usize = 2_048;
const EDIT_CURSOR_MAX_BYTES: usize = 64 * 1_024;

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentGetRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
}

impl AgentGetRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn validate(&self) -> Result<()> {
        text(&self.id)
    }

    pub fn validate_response(&self, reply: &AgentGetResponse) -> Result<()> {
        if self.validate().is_err()
            || reply.agent.id != self.id
            || !valid_agent(&reply.agent)
            || reply
                .session_usage
                .as_ref()
                .is_some_and(|usage| usage.validate().is_err())
            || reply
                .config
                .as_ref()
                .is_some_and(|config| config.validate().is_err())
            || reply.source_states.iter().any(|source| {
                source.agent_id != self.id
                    || source.generation < 0
                    || source
                        .owners
                        .iter()
                        .any(|owner| owner.source != source.source)
            })
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentListRequest {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
    #[serde(default)]
    pub observations: bool,
    /// Exact Agent state filter (`unknown`, `created`, `starting`, `idle`,
    /// `busy`, `waiting`, `stopped`, `failed`, `lost`). Wrong types reject as
    /// `invalid`; an unknown state name never matches silently.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<AgentState>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub project: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub harness: Option<String>,
    /// Direct-parent Agent ID filter (one subtree level).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub parent: Option<String>,
    /// Subtree-root Agent ID filter (the frozen hierarchy root).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub root: Option<String>,
}

impl Default for AgentListRequest {
    fn default() -> Self {
        Self {
            after: None,
            limit: super::DEFAULT_LIMIT,
            observations: false,
            state: None,
            project: None,
            harness: None,
            parent: None,
            root: None,
        }
    }
}

impl AgentListRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(after) = &self.after {
            text(after)?;
        }
        if let Some(project) = &self.project {
            text(project)?;
        }
        if let Some(harness) = &self.harness {
            text(harness)?;
        }
        if let Some(parent) = &self.parent {
            text(parent)?;
            crate::parse_agent_id(parent)?;
        }
        if let Some(root) = &self.root {
            text(root)?;
            crate::parse_agent_id(root)?;
        }
        limit(self.limit)
    }

    pub fn validate_response(&self, reply: &AgentListResponse) -> Result<()> {
        if self.validate().is_err() || reply.agents.len() > self.limit {
            return Err(invalid_response());
        }
        if reply.total < reply.agents.len() {
            return Err(invalid_response());
        }
        // The Store pages by Agent ID: every nonempty page carries `next` at
        // the last returned row, and an empty page carries no cursor. A quiet
        // nonempty page still requires continuing through `next` until empty.
        match reply.agents.last() {
            Some(last) => {
                if reply.next.as_deref() != Some(last.summary.id.as_str()) {
                    return Err(invalid_response());
                }
            }
            None => {
                if reply.next.is_some() {
                    return Err(invalid_response());
                }
            }
        }
        if reply
            .next
            .as_ref()
            .is_some_and(|next| self.after.as_ref() == Some(next))
        {
            return Err(invalid_response());
        }
        let mut ids = BTreeSet::new();
        if reply.agents.iter().any(|row| {
            row.validate().is_err()
                || !ids.insert(row.summary.id.clone())
                || row.observation.is_some() != self.observations
                || self.state.is_some_and(|state| row.summary.state != state)
                || self
                    .project
                    .as_ref()
                    .is_some_and(|project| &row.summary.project != project)
                || self
                    .harness
                    .as_ref()
                    .is_some_and(|harness| &row.summary.harness != harness)
                || self
                    .parent
                    .as_ref()
                    .is_some_and(|parent| row.summary.parent.as_ref() != Some(parent))
                || self
                    .root
                    .as_ref()
                    .is_some_and(|root| &row.summary.root != root)
        }) {
            return Err(invalid_response());
        }
        // Neither a non-null next nor a full page promises another record.
        Ok(())
    }
}

/// Fixed compact fleet projection for `agent.list`. Identity, state, current
/// turn and hierarchy position come from the live Agent row. `last_progress_at`
/// is the newest native progress timestamp where the Store retains one;
/// `progress_age_ms` is measured against the read clock and stays absent when
/// no progress timestamp exists. `last_outcome` is the newest terminal turn's
/// canonical outcome (legacy rows classify explicitly, never defaulting to
/// success) with its `last_turn_id` and retained `usage` where available.
/// `activity` is present only for native observed children. `updated_at` is
/// the last controller-observed write: for detached or restarted sources it
/// is freshness metadata, not proof of liveness. Full records stay available
/// through `agent.get`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentSummary {
    pub id: String,
    #[schemars(range(min = 0))]
    pub generation: i64,
    pub project: String,
    pub harness: String,
    pub state: AgentState,
    #[serde(deserialize_with = "nullable")]
    pub parent: Option<String>,
    pub root: String,
    #[serde(deserialize_with = "nullable")]
    pub active_turn: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_progress_at: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub progress_age_ms: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub last_turn_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_outcome: Option<TurnOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<NativeChildActivity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<TurnUsage>,
    pub updated_at: i64,
}

impl AgentSummary {
    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        crate::parse_agent_id(&self.id)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "Agent summary generation is invalid"));
        }
        text(&self.project)?;
        text(&self.harness)?;
        text(&self.root)?;
        crate::parse_agent_id(&self.root)?;
        if let Some(parent) = &self.parent {
            text(parent)?;
            crate::parse_agent_id(parent)?;
        }
        if let Some(turn) = &self.active_turn {
            text(turn)?;
        }
        if let Some(at) = self.last_progress_at
            && at < 0
        {
            return Err(Error::new(
                "invalid",
                "Agent summary progress time is invalid",
            ));
        }
        if let Some(age) = self.progress_age_ms
            && age < 0
        {
            return Err(Error::new(
                "invalid",
                "Agent summary progress age is invalid",
            ));
        }
        if self.last_outcome.is_none() && self.last_turn_id.is_some() {
            return Err(Error::new(
                "invalid",
                "Agent summary turn needs its outcome",
            ));
        }
        if let Some(turn) = &self.last_turn_id {
            text(turn)?;
        }
        if self.updated_at < 0 {
            return Err(Error::new(
                "invalid",
                "Agent summary observation time is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentProcessesRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
    #[schemars(range(min = 0))]
    pub generation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_scope"
    )]
    #[schemars(description = "Process scope: agent or workspace.")]
    pub scope: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl AgentProcessesRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            generation: None,
            scope: None,
            after: None,
            limit: super::DEFAULT_LIMIT,
        }
    }

    pub fn effective_scope(&self) -> &str {
        self.scope.as_deref().unwrap_or("agent")
    }

    pub fn cursor(&self) -> Result<Option<ProcessPageCursor>> {
        self.after
            .as_deref()
            .map(ProcessPageCursor::decode)
            .transpose()
    }

    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        generation(self.generation)?;
        scope(self.effective_scope())?;
        limit(self.limit)?;
        // Query binding follows Store authorization and workspace target resolution.
        self.cursor()?;
        Ok(())
    }

    pub fn validate_response(&self, reply: &AgentProcessesResponse) -> Result<()> {
        if self.validate().is_err()
            || !binding(&self.id, self.generation, &reply.agent, reply.generation)
            || reply.processes.len() > self.limit
            || match self.effective_scope() {
                "workspace" => reply.scope.as_deref() != Some("workspace"),
                _ => reply.scope.is_some(),
            }
        {
            return Err(invalid_response());
        }
        let cursor = self.cursor().map_err(|_| invalid_response())?;
        if let Some(cursor) = &cursor {
            cursor
                .validate_binding(&reply.agent, reply.generation, self.effective_scope())
                .map_err(|_| invalid_response())?;
        }
        let owner = format!(
            "{}:{}#{}",
            self.effective_scope(),
            reply.agent,
            reply.generation
        );
        let mut previous = cursor.as_ref().map(|cursor| &cursor.identity);
        for record in &reply.processes {
            if !valid_identity(&record.identity)
                || record.owner != owner
                || previous.is_some_and(|previous| previous >= &record.identity)
            {
                return Err(invalid_response());
            }
            previous = Some(&record.identity);
        }
        match (&reply.next, reply.processes.last()) {
            (None, None) => {}
            (Some(next), Some(last)) => {
                let next = ProcessPageCursor::decode(next).map_err(|_| invalid_response())?;
                next.validate_binding(&reply.agent, reply.generation, self.effective_scope())
                    .map_err(|_| invalid_response())?;
                if next.identity != last.identity {
                    return Err(invalid_response());
                }
            }
            _ => return Err(invalid_response()),
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStatsRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
    #[schemars(range(min = 0))]
    pub generation: Option<i64>,
}

impl AgentStatsRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            generation: None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        generation(self.generation)
    }
    pub fn validate_response(&self, reply: &AgentStatsResponse) -> Result<()> {
        if self.validate().is_err()
            || !binding(&self.id, self.generation, &reply.agent, reply.generation)
            || reply
                .session_usage
                .as_ref()
                .is_some_and(|usage| usage.validate().is_err())
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentGitRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
    #[schemars(range(min = 0))]
    pub generation: Option<i64>,
}

impl AgentGitRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            generation: None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        generation(self.generation)
    }
    pub fn validate_response(&self, reply: &AgentGitResponse) -> Result<()> {
        if self.validate().is_err()
            || !binding(&self.id, self.generation, &reply.agent, reply.generation)
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentEditsRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
    #[schemars(range(min = 0))]
    pub generation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_edit_cursor"
    )]
    #[schemars(description = "Nonblank retained edit cursor, at most 65536 bytes, no NUL.")]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl AgentEditsRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            generation: None,
            after: None,
            limit: super::DEFAULT_LIMIT,
        }
    }

    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        generation(self.generation)?;
        if let Some(after) = &self.after {
            edit_cursor(after)?;
        }
        limit(self.limit)
    }

    pub fn validate_response(&self, reply: &AgentEditsResponse) -> Result<()> {
        if self.validate().is_err()
            || !binding(&self.id, self.generation, &reply.agent, reply.generation)
            || reply.edits.len() > self.limit
        {
            return Err(invalid_response());
        }
        let mut previous = self.after.as_deref();
        for edit in &reply.edits {
            if edit.agent != reply.agent
                || edit.generation != reply.generation
                || edit_cursor(&edit.path).is_err()
                || previous.is_some_and(|path| path >= edit.path.as_str())
            {
                return Err(invalid_response());
            }
            previous = Some(&edit.path);
        }
        if reply.next.as_deref() != reply.edits.last().map(|edit| edit.path.as_str()) {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusRequest {}

impl DaemonStatusRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
    pub fn validate_response(&self, reply: &DaemonStatusResponse) -> Result<()> {
        let state = match reply.admission.as_str() {
            "open" => "running",
            "draining" => "draining",
            "forcing" => "forcing",
            _ => return Err(invalid_response()),
        };
        if reply.state != state
            || reply.unknown_commands_count < reply.unknown_commands.len()
            || !reply.unknown_commands_truncated
                && reply.unknown_commands_count != reply.unknown_commands.len()
            || reply.unsettled_commands_count != reply.unsettled_commands.len()
            || reply
                .drain
                .as_ref()
                .is_some_and(|drain| drain.validate().is_err())
            || reply.workspace_custody.scope != "current_host"
            || reply
                .cloud_mcp
                .as_ref()
                .is_some_and(|status| status.validate().is_err())
            || reply.automation_tasks.is_empty()
            || reply.automation_tasks.len() > 8
            || reply
                .automation_tasks
                .iter()
                .any(|task| task.validate().is_err())
            || reply.unknown_commands.iter().any(|id| text(id).is_err())
            || reply.unsettled_commands.iter().any(|command| {
                text(&command.id).is_err()
                    || text(&command.verb).is_err()
                    || command.generation.is_some_and(|generation| generation < 0)
            })
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentGetResponse {
    #[serde(flatten, deserialize_with = "object_agent")]
    pub agent: Agent,
    #[serde(deserialize_with = "object")]
    pub observation: ObservationView,
    #[serde(deserialize_with = "objects")]
    pub source_states: Vec<SourceState>,
    /// Latest retained `session.usage` snapshot for the Agent generation, in
    /// its own session-scope section; `None` when no session fact was ever
    /// committed. Never a turn total and never billing.
    #[serde(default, deserialize_with = "optional_object")]
    pub session_usage: Option<SessionUsage>,
    /// Current model/effort overlay for the Agent generation: the latest
    /// retained `configure` revision with the effective values the next
    /// native turn carries plus the original profile values. `None` when no
    /// `configure` command ever applied to this generation.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_object"
    )]
    pub config: Option<AgentConfigView>,
}

/// Retained model/effort overlay readback for one Agent generation. The
/// original profile snapshot is never rewritten: `original_model` and
/// `original_effort` repeat the frozen values while `model`/`effort` carry
/// the effective values the next native turn request uses. `profile` is the
/// retained requested profile name, `profile_inherited` reports whether the
/// Agent's snapshot was inherited from its parent launch, and `provenance`
/// is `configure` for a directly configured overlay or `inherited_from` for
/// a `resume` generation that explicitly inherited the previous generation.
///
/// The same view rides command and turn readbacks for their exact retained
/// generation and revision, so a retained turn still reads back the
/// configuration it ran under after later revisions apply.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentConfigView {
    /// Monotonically increasing revision within the Agent generation; 0 is
    /// never stored and means no overlay exists.
    pub revision: i64,
    /// Where the change applies: `next_turn`. A change needing a new
    /// native session is refused as `unsupported`, never stored.
    pub application: String,
    /// How this revision arose: `configure` or `inherited_from`.
    pub provenance: String,
    /// Retained requested profile name (the original launch request).
    pub profile: String,
    /// Whether the Agent snapshot was inherited from a parent launch.
    pub profile_inherited: bool,
    /// Exactly what the owner requested (each `None` when not requested).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_effort: Option<String>,
    /// Effective values the next native turn carries (requested value or
    /// the frozen original when not requested).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// Frozen profile-snapshot values the overlay leaves unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_effort: Option<String>,
}

impl AgentConfigView {
    /// Pure reply-shape check shared by the Agent, command and turn
    /// readbacks: a stored revision is always positive, the application
    /// point is `next_turn`, the provenance is one of the two retained
    /// origins, and the profile and requested values are nonblank.
    pub fn validate(&self) -> Result<()> {
        if self.revision < 1
            || self.application != "next_turn"
            || !matches!(self.provenance.as_str(), "configure" | "inherited_from")
            || self.profile.trim().is_empty()
            || self
                .requested_model
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || self
                .requested_effort
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentListRow {
    #[serde(flatten, deserialize_with = "object_summary")]
    pub summary: AgentSummary,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_object"
    )]
    pub observation: Option<ObservationView>,
}

impl AgentListRow {
    pub fn validate(&self) -> Result<()> {
        self.summary.validate()
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentListResponse {
    #[serde(deserialize_with = "objects")]
    pub agents: Vec<AgentListRow>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
    /// Matching records for the caller's scope and filters, ignoring the
    /// page cursor. `agents.len()` is the returned prefix of that set.
    /// Required on the wire: a missing count never reads back as zero.
    pub total: usize,
    /// True when the server knows records remain beyond this page: either a
    /// further row matched or the byte bound trimmed the page. A false value
    /// on a full page still requires continuing through `next` until the
    /// server returns an empty page. Required on the wire.
    pub truncated: bool,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentProcessesResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "process_records")]
    pub processes: Vec<ProcessRecord>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentStatsResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(flatten, deserialize_with = "object")]
    pub observation: ObservationView,
    /// Latest retained `session.usage` snapshot for the Agent generation, in
    /// its own session-scope section; `None` when no session fact was ever
    /// committed. Never a turn total and never billing.
    #[serde(default, deserialize_with = "optional_object")]
    pub session_usage: Option<SessionUsage>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentGitResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(flatten, deserialize_with = "object")]
    pub git: GitView,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEditsResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "objects")]
    pub edits: Vec<AgentEdit>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

/// The exact public Store source projection, separate from private plugin ACKs.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceState {
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub families: Vec<String>,
    #[serde(deserialize_with = "objects")]
    pub owners: Vec<SourceOwner>,
    pub state: String,
    pub health: String,
    #[serde(deserialize_with = "nullable")]
    pub cursor: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_store_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_first_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_last_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub gap_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_from: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_to: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_seq: Option<i64>,
    /// Bounded Muse view-health counters for this source. Always present on
    /// the wire going forward; older readers default them to zero.
    #[serde(default)]
    pub view_unavailable: u64,
    #[serde(default)]
    pub view_readback_recovered: u64,
    #[serde(default)]
    pub view_settled_unknown: u64,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOwner {
    pub family: String,
    pub source: String,
    pub mechanism: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCustodyView {
    pub scope: String,
    pub capability: String,
    pub exact: bool,
    pub orphan_possible: bool,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitCredentialHelper {
    Manager,
}

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectGitView {
    #[serde(default)]
    pub fetch_remotes: BTreeMap<String, String>,
    #[serde(default)]
    pub credential_helper: Option<GitCredentialHelper>,
}

impl ProjectGitView {
    pub fn validate(&self) -> Result<()> {
        if self.fetch_remotes.iter().any(|(remote, endpoint)| {
            remote.is_empty()
                || remote.len() > 128
                || remote.starts_with('-')
                || remote
                    .chars()
                    .any(|character| character.is_whitespace() || character.is_control())
                || endpoint.trim().is_empty()
                || endpoint.len() > 4_096
                || endpoint.chars().any(char::is_control)
        }) {
            return Err(Error::new(
                "invalid",
                "project Git fetch remotes must be bounded nonempty literals",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectRegistrationView {
    pub id: String,
    pub checkout: String,
    pub repo_common_dir: String,
    pub profiles: Vec<String>,
    pub actions: Vec<String>,
    pub interpreters: BTreeMap<String, ScriptInterpreter>,
    pub run_env: Vec<String>,
    #[serde(default)]
    pub git: Option<ProjectGitView>,
    pub revision: i64,
}

impl ProjectRegistrationView {
    /// Pure readback grammar. Store/daemon own canonical repository identity and
    /// the authority checks for the explicit profile/action allowsets.
    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        text(&self.checkout)?;
        text(&self.repo_common_dir)?;
        if self.revision < 0 {
            return Err(Error::new(
                "invalid",
                "project registration revision must be nonnegative",
            ));
        }
        validate_project_allowset(&self.profiles, "profiles")?;
        validate_project_allowset(&self.actions, "actions")?;
        validate_project_run_env(&self.run_env)?;
        if let Some(git) = &self.git {
            git.validate()?;
        }
        for (reference, interpreter) in &self.interpreters {
            if reference.is_empty()
                || reference.len() > 128
                || !reference
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "-_".contains(character))
            {
                return Err(Error::new(
                    "invalid",
                    "project interpreter reference is invalid",
                ));
            }
            interpreter.validate()?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsettledCommandSummary {
    pub id: String,
    #[serde(deserialize_with = "nullable")]
    pub agent: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub generation: Option<i64>,
    pub verb: String,
    pub state: CommandState,
}

/// One running turn that blocks drain: the Agent's current active turn.
/// `agent`/`generation` identify the exact live generation; `turn_id` is the
/// retained turn the native side is still working on. A missing entry never
/// proves the native effect did not happen; it only means the Store has no
/// recorded active turn for that generation.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunningTurnSummary {
    #[serde(deserialize_with = "required_text")]
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "required_text")]
    pub turn_id: String,
}

impl RunningTurnSummary {
    pub fn validate(&self) -> Result<()> {
        text(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "running turn generation is invalid"));
        }
        text(&self.turn_id)?;
        Ok(())
    }
}

/// Where a live owned process comes from. `plugin` is a controlled native
/// child for the named Agent generation; `workspace` is an owned workspace
/// setup/removal child for that generation. Both are custody the daemon
/// observes as not yet exited in the Store; neither proves task success. A
/// held workspace operation lock alone (no observed child) is never listed
/// here.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnedProcessKind {
    Plugin,
    Workspace,
}

/// One live owned process that blocks drain, bounded by the drain report cap.
/// An absent entry never proves no native effect occurred.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedProcessSummary {
    #[serde(deserialize_with = "required_text")]
    pub agent: String,
    pub generation: i64,
    pub kind: OwnedProcessKind,
}

impl OwnedProcessSummary {
    pub fn validate(&self) -> Result<()> {
        text(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "owned process generation is invalid"));
        }
        Ok(())
    }
}

/// Bounded drain report reusing the existing shutdown blockers. Every
/// category carries a count, a bounded list and a truncation flag: the count
/// is the number returned in this snapshot (or the exact in-memory total for
/// unknown commands); `truncated` is true whenever any qualifying record was
/// omitted for any reason (the list bound or the Store byte bound trimming
/// the page). Unknown stays unknown: an empty report never proves no native
/// effect occurred, and the daemon never auto-resolves unknown to finish
/// drain.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainReport {
    pub running_turns_count: usize,
    #[serde(deserialize_with = "objects")]
    pub running_turns: Vec<RunningTurnSummary>,
    pub running_turns_truncated: bool,
    pub unsettled_commands_count: usize,
    #[serde(deserialize_with = "objects")]
    pub unsettled_commands: Vec<UnsettledCommandSummary>,
    pub unsettled_commands_truncated: bool,
    pub unknown_commands_count: usize,
    pub unknown_commands: Vec<String>,
    pub unknown_commands_truncated: bool,
    pub owned_processes_count: usize,
    #[serde(deserialize_with = "objects")]
    pub owned_processes: Vec<OwnedProcessSummary>,
    pub owned_processes_truncated: bool,
}

impl DrainReport {
    pub fn empty() -> Self {
        Self {
            running_turns_count: 0,
            running_turns: Vec::new(),
            running_turns_truncated: false,
            unsettled_commands_count: 0,
            unsettled_commands: Vec::new(),
            unsettled_commands_truncated: false,
            unknown_commands_count: 0,
            unknown_commands: Vec::new(),
            unknown_commands_truncated: false,
            owned_processes_count: 0,
            owned_processes: Vec::new(),
            owned_processes_truncated: false,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.running_turns_count < self.running_turns.len()
            || (!self.running_turns_truncated
                && self.running_turns_count != self.running_turns.len())
        {
            return Err(invalid_response());
        }
        if self.unsettled_commands_count < self.unsettled_commands.len()
            || (!self.unsettled_commands_truncated
                && self.unsettled_commands_count != self.unsettled_commands.len())
        {
            return Err(invalid_response());
        }
        if self.unknown_commands_count < self.unknown_commands.len()
            || (!self.unknown_commands_truncated
                && self.unknown_commands_count != self.unknown_commands.len())
        {
            return Err(invalid_response());
        }
        if self.owned_processes_count < self.owned_processes.len()
            || (!self.owned_processes_truncated
                && self.owned_processes_count != self.owned_processes.len())
        {
            return Err(invalid_response());
        }
        if self
            .running_turns
            .iter()
            .any(|turn| turn.validate().is_err())
            || self
                .unsettled_commands
                .iter()
                .any(|command| text(&command.id).is_err() || text(&command.verb).is_err())
            || self.unknown_commands.iter().any(|id| text(id).is_err())
            || self
                .owned_processes
                .iter()
                .any(|process| process.validate().is_err())
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StatusBlocker {
    WorkspaceProcesses,
    DiscoveryReaders {
        count: usize,
    },
    AgentLive {
        agent: String,
        generation: i64,
        state: AgentState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        active_turn: Option<String>,
    },
    CommandUnsettled {
        command_id: String,
        state: CommandState,
    },
    StoreWriteFaulted,
    CommandUnknown {
        command_id: String,
    },
    CommandUnknownMore {
        count: usize,
    },
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudMcpHealth {
    StartingHttp,
    StartingTunnel,
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudMcpCustody {
    WindowsJobExact,
    UnixProcessGroupDegraded,
    Unknown,
}

/// Current managed cloud MCP child-pair state. `running` proves the two child
/// leaders are alive; it does not prove public tunnel connectivity or auth.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudMcpStatus {
    pub public_url: String,
    pub health: CloudMcpHealth,
    pub custody: CloudMcpCustody,
}

impl CloudMcpStatus {
    pub fn validate(&self) -> Result<()> {
        if text(&self.public_url).is_err()
            || !self.public_url.starts_with("https://")
            || !self.public_url.ends_with("/mcp")
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusResponse {
    pub state: String,
    pub admission: String,
    pub agents: usize,
    pub store_write_faulted: bool,
    pub owned_plugins: usize,
    pub observer_readers: usize,
    pub discovery_readers: usize,
    #[serde(deserialize_with = "object")]
    pub workspace_custody: WorkspaceCustodyView,
    #[serde(deserialize_with = "nullable")]
    pub workspace_recovery_error: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub cloud_mcp: Option<CloudMcpStatus>,
    #[serde(deserialize_with = "objects")]
    pub automation_tasks: Vec<WorkerTaskStatus>,
    pub unknown_commands_count: usize,
    pub unknown_commands: Vec<String>,
    pub unknown_commands_truncated: bool,
    pub unsettled_commands_count: usize,
    #[serde(deserialize_with = "objects")]
    pub unsettled_commands: Vec<UnsettledCommandSummary>,
    pub unsettled_commands_truncated: bool,
    #[serde(deserialize_with = "objects")]
    pub blockers: Vec<StatusBlocker>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain: Option<DrainReport>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerTaskHealth {
    Starting,
    Running,
    Stopped,
    Failed,
}

/// Bounded, redacted runtime status of one daemon-owned automation task.
/// `running` means the task is alive after its startup boundary; it does
/// not prove a rule/timer/goal delivery. `starting` means spawned but not
/// yet initialized. `failed` carries only a bounded code, never a raw error
/// message.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTaskStatus {
    pub name: String,
    pub health: WorkerTaskHealth,
    #[serde(deserialize_with = "nullable")]
    pub failure_code: Option<String>,
}

impl WorkerTaskStatus {
    pub fn validate(&self) -> Result<()> {
        if text(&self.name).is_err() {
            return Err(invalid_response());
        }
        if let Some(code) = self.failure_code.as_deref()
            && (code.is_empty() || code.len() > 128)
        {
            return Err(invalid_response());
        }
        if (self.health == WorkerTaskHealth::Failed) != self.failure_code.is_some() {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessPageCursor {
    pub agent: String,
    pub generation: i64,
    pub scope: String,
    #[serde(deserialize_with = "cursor_identity")]
    pub identity: ProcessIdentity,
}

impl ProcessPageCursor {
    pub fn decode(encoded: &str) -> Result<Self> {
        if encoded.len() > PROCESS_CURSOR_MAX_BYTES {
            return Err(Error::new("invalid", "process cursor exceeds its bound"));
        }
        let object: Map<String, Value> = serde_json::from_str(encoded)
            .map_err(|_| Error::new("invalid", "invalid process cursor"))?;
        let cursor: Self = serde_json::from_value(Value::Object(object))
            .map_err(|_| Error::new("invalid", "invalid process cursor"))?;
        cursor.validate()?;
        Ok(cursor)
    }

    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        let encoded = serde_json::to_string(self)
            .map_err(|_| Error::new("internal", "cannot encode process cursor"))?;
        if encoded.len() > PROCESS_CURSOR_MAX_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "encoded process cursor exceeds its bound",
            ));
        }
        Ok(encoded)
    }

    pub fn validate_binding(&self, agent: &str, generation: i64, scope: &str) -> Result<()> {
        self.validate()?;
        if self.agent != agent || self.generation != generation || self.scope != scope {
            return Err(Error::new(
                "invalid",
                "process cursor does not match the Agent, generation, and scope",
            ));
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        text(&self.agent)?;
        generation(Some(self.generation))?;
        scope(&self.scope)?;
        if !valid_identity(&self.identity) {
            return Err(Error::new("invalid", "invalid process cursor identity"));
        }
        Ok(())
    }
}

fn valid_identity(identity: &ProcessIdentity) -> bool {
    identity.pid != 0
        && identity.start_time != 0
        && !identity.host_boot_id.is_empty()
        && identity.host_boot_id.len() <= 512
        && !identity.host_boot_id.chars().any(char::is_control)
}

fn valid_agent(agent: &Agent) -> bool {
    text(&agent.id).is_ok() && agent.generation >= 0
}

fn binding(id: &str, expected: Option<i64>, agent: &str, actual: i64) -> bool {
    agent == id && actual >= 0 && expected.is_none_or(|expected| expected == actual)
}

fn text(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new("invalid", "text must be non-empty"));
    }
    Ok(())
}

fn validate_project_allowset(values: &[String], field: &str) -> Result<()> {
    let mut unique = BTreeSet::new();
    if values
        .iter()
        .any(|value| text(value).is_err() || !unique.insert(value.as_str()))
    {
        return Err(Error::new(
            "invalid",
            format!("project {field} must contain unique nonempty values"),
        ));
    }
    Ok(())
}

fn validate_project_run_env(values: &[String]) -> Result<()> {
    let mut unique = BTreeSet::new();
    if values.iter().any(|value| {
        let mut characters = value.chars();
        let valid = characters
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
            && characters.all(|character| character.is_ascii_alphanumeric() || character == '_');
        let key = if cfg!(windows) {
            value.to_ascii_uppercase()
        } else {
            value.clone()
        };
        !valid || !unique.insert(key)
    }) {
        return Err(Error::new(
            "invalid",
            "project run_env must contain unique environment names",
        ));
    }
    Ok(())
}

fn generation(value: Option<i64>) -> Result<()> {
    if value.is_some_and(|value| value < 0) {
        return Err(Error::new("invalid", "generation must be nonnegative"));
    }
    Ok(())
}

fn scope(value: &str) -> Result<()> {
    if !matches!(value, "agent" | "workspace") {
        return Err(Error::new(
            "invalid",
            "process scope must be agent or workspace",
        ));
    }
    Ok(())
}

fn limit(value: usize) -> Result<()> {
    if !(1..=super::MAX_LIMIT).contains(&value) {
        return Err(Error::new("invalid", "limit must be between 1 and 1000"));
    }
    Ok(())
}

fn edit_cursor(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > EDIT_CURSOR_MAX_BYTES || value.contains('\0') {
        return Err(Error::new("invalid", "invalid edit cursor"));
    }
    Ok(())
}

fn invalid_response() -> Error {
    Error::new("unknown", "Agent view response is invalid")
}

fn required_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    text(&value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn optional_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        text(value).map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}

fn optional_generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error> {
    let value = Option::<i64>::deserialize(deserializer)?;
    generation(value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn optional_scope<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let value = optional_text(deserializer)?;
    if let Some(value) = &value {
        scope(value).map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}

fn optional_edit_cursor<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        edit_cursor(value).map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}

// The same small object-map adapter used by the existing result-read DTOs.
// Reuse Core models instead of publishing a parallel response schema for them.
fn object<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> std::result::Result<T, D::Error> {
    let value = Map::<String, Value>::deserialize(deserializer)?;
    serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom)
}

fn objects<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error> {
    let values = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(|value| serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom))
        .collect()
}

fn optional_object<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    let value = Option::<Map<String, Value>>::deserialize(deserializer)?;
    value
        .map(|value| serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom))
        .transpose()
}

fn nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn object_summary<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<AgentSummary, D::Error> {
    let value = Map::<String, Value>::deserialize(deserializer)?;
    for field in ["activity", "usage"] {
        if value.get(field).is_some_and(|entry| !entry.is_object()) {
            return Err(serde::de::Error::custom(
                "Agent summary objects must be objects",
            ));
        }
    }
    serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom)
}

fn object_agent<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Agent, D::Error> {
    let value = Map::<String, Value>::deserialize(deserializer)?;
    if value.get("pending").is_some_and(|pending| {
        pending
            .as_array()
            .is_none_or(|entries| entries.iter().any(|entry| !entry.is_object()))
    }) {
        return Err(serde::de::Error::custom("pending entries must be objects"));
    }
    serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom)
}

fn process_records<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<ProcessRecord>, D::Error> {
    let records = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    records
        .into_iter()
        .map(|record| {
            if !record.get("identity").is_some_and(Value::is_object) {
                return Err(serde::de::Error::custom(
                    "process identity must be an object",
                ));
            }
            serde_json::from_value(Value::Object(record)).map_err(serde::de::Error::custom)
        })
        .collect()
}

fn cursor_identity<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<ProcessIdentity, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Identity {
        host_boot_id: String,
        pid: u32,
        start_time: u64,
    }
    let identity: Identity = object(deserializer)?;
    Ok(ProcessIdentity {
        host_boot_id: identity.host_boot_id,
        pid: identity.pid,
        start_time: identity.start_time,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProcessState;
    use serde_json::json;

    fn cursor() -> ProcessPageCursor {
        ProcessPageCursor {
            agent: "a1".to_owned(),
            generation: 1,
            scope: "workspace".to_owned(),
            identity: ProcessIdentity {
                host_boot_id: "boot".to_owned(),
                pid: 7,
                start_time: 9,
            },
        }
    }

    fn process() -> ProcessRecord {
        ProcessRecord {
            identity: cursor().identity,
            owner: "workspace:a1#1".to_owned(),
            role: "setup".to_owned(),
            state: ProcessState::Exited,
            exit_code: None,
            updated_at: 0,
        }
    }

    fn observation() -> ObservationView {
        serde_json::from_value(json!({"available":false,"error_code":"unavailable"})).unwrap()
    }

    fn unknown(result: Result<()>) {
        let error = result.unwrap_err();
        assert_eq!(error.code, "unknown");
        assert_eq!(error.message, "Agent view response is invalid");
        assert!(error.diagnostics.is_empty());
    }

    #[test]
    fn project_registration_readback_preserves_explicit_empty_allowsets() {
        let view = ProjectRegistrationView {
            id: "project-a".to_owned(),
            checkout: r"C:\work\project-a".to_owned(),
            repo_common_dir: r"C:\work\project-a\.git".to_owned(),
            profiles: Vec::new(),
            actions: Vec::new(),
            interpreters: BTreeMap::new(),
            run_env: Vec::new(),
            git: None,
            revision: 1,
        };
        view.validate().unwrap();
        let wire = serde_json::to_value(&view).unwrap();
        assert_eq!(wire["profiles"], json!([]));
        assert_eq!(wire["actions"], json!([]));
        assert_eq!(wire["interpreters"], json!({}));
        assert_eq!(wire["run_env"], json!([]));
        assert_eq!(wire["git"], Value::Null);
        assert_eq!(
            serde_json::from_value::<ProjectRegistrationView>(wire.clone()).unwrap(),
            view
        );
        let mut old_wire = wire.clone();
        old_wire.as_object_mut().unwrap().remove("git");
        assert_eq!(
            serde_json::from_value::<ProjectRegistrationView>(old_wire)
                .unwrap()
                .git,
            None
        );
        let mut unknown = wire;
        unknown["future"] = json!(true);
        assert!(serde_json::from_value::<ProjectRegistrationView>(unknown).is_err());

        let mut invalid = view.clone();
        invalid.profiles = vec!["fake".to_owned(), "fake".to_owned()];
        assert!(invalid.validate().is_err());
        invalid = view;
        invalid.actions = vec!["stop".to_owned(), " ".to_owned()];
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn project_git_view_roundtrips_only_approved_literal_endpoints_and_helper() {
        let view = ProjectGitView {
            fetch_remotes: BTreeMap::from([(
                "origin".to_owned(),
                "https://git.example.test/team/repo.git".to_owned(),
            )]),
            credential_helper: Some(GitCredentialHelper::Manager),
        };
        view.validate().unwrap();
        let wire = serde_json::to_value(&view).unwrap();
        assert_eq!(
            wire["fetch_remotes"]["origin"],
            "https://git.example.test/team/repo.git"
        );
        assert_eq!(wire["credential_helper"], "manager");
        assert_eq!(
            serde_json::from_value::<ProjectGitView>(wire).unwrap(),
            view
        );

        for (remote, endpoint) in [
            ("--upload-pack=evil", "https://git.example.test/repo.git"),
            ("origin", " "),
            ("origin", "https://git.example.test/\nrepo.git"),
        ] {
            let invalid = ProjectGitView {
                fetch_remotes: BTreeMap::from([(remote.to_owned(), endpoint.to_owned())]),
                credential_helper: None,
            };
            assert_eq!(invalid.validate().unwrap_err().code, "invalid");
        }
    }

    #[test]
    fn view_defaults_preserve_null_as_absent_and_omit_optional_fields() {
        let request: AgentProcessesRequest = serde_json::from_value(json!({
            "id":" a1 ","generation":null,"scope":null,"after":null
        }))
        .unwrap();
        assert_eq!(request, AgentProcessesRequest::new(" a1 "));
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            json!({"id":" a1 ","limit":100})
        );
        assert_eq!(request.effective_scope(), "agent");
        request.validate().unwrap();
        assert_eq!(
            serde_json::from_value::<AgentListRequest>(json!({"after":null})).unwrap(),
            AgentListRequest::default()
        );
        assert_eq!(
            serde_json::from_value::<AgentStatsRequest>(json!({"id":"a1","generation":null}))
                .unwrap(),
            AgentStatsRequest::new("a1")
        );
        assert_eq!(
            serde_json::from_value::<AgentGitRequest>(json!({"id":"a1","generation":null}))
                .unwrap(),
            AgentGitRequest::new("a1")
        );
        assert_eq!(
            serde_json::from_value::<AgentEditsRequest>(
                json!({"id":"a1","generation":null,"after":null})
            )
            .unwrap(),
            AgentEditsRequest::new("a1")
        );
        assert!(
            serde_json::from_value::<DaemonStatusRequest>(json!({}))
                .unwrap()
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn required_ids_and_unknown_request_fields_are_strict() {
        for request in [
            json!({}),
            json!({"id":null}),
            json!({"id":false}),
            json!({"id":1}),
            json!({"id":" \t "}),
            json!({"id":"a1","extra":0}),
        ] {
            assert!(serde_json::from_value::<AgentGetRequest>(request.clone()).is_err());
            assert!(serde_json::from_value::<AgentProcessesRequest>(request.clone()).is_err());
            assert!(serde_json::from_value::<AgentStatsRequest>(request.clone()).is_err());
            assert!(serde_json::from_value::<AgentGitRequest>(request.clone()).is_err());
            assert!(serde_json::from_value::<AgentEditsRequest>(request).is_err());
        }
        assert!(serde_json::from_value::<AgentListRequest>(json!({"extra":0})).is_err());
        assert!(serde_json::from_value::<DaemonStatusRequest>(json!({"extra":0})).is_err());
        assert_eq!(
            serde_json::from_value::<AgentGetRequest>(json!({"id":" raw id "}))
                .unwrap()
                .id,
            " raw id "
        );
    }

    #[test]
    fn supplied_limits_observations_and_optional_types_do_not_fall_back() {
        for bad in [
            Value::Null,
            json!(true),
            json!("1"),
            json!(1.0),
            json!(-1),
            json!(0),
            json!(1001),
            json!(u64::MAX),
        ] {
            assert!(serde_json::from_value::<AgentListRequest>(json!({"limit":bad})).is_err());
            assert!(
                serde_json::from_value::<AgentProcessesRequest>(json!({"id":"a1","limit":bad}))
                    .is_err()
            );
            assert!(
                serde_json::from_value::<AgentEditsRequest>(json!({"id":"a1","limit":bad}))
                    .is_err()
            );
        }
        for bad in [Value::Null, json!(0), json!("false"), json!([])] {
            assert!(
                serde_json::from_value::<AgentListRequest>(json!({"observations":bad})).is_err()
            );
        }
        for bad in [
            json!(-1),
            json!(true),
            json!("1"),
            json!(1.0),
            json!(i64::MAX as u64 + 1),
        ] {
            assert!(
                serde_json::from_value::<AgentProcessesRequest>(
                    json!({"id":"a1","generation":bad})
                )
                .is_err()
            );
            assert!(
                serde_json::from_value::<AgentStatsRequest>(json!({"id":"a1","generation":bad}))
                    .is_err()
            );
            assert!(
                serde_json::from_value::<AgentGitRequest>(json!({"id":"a1","generation":bad}))
                    .is_err()
            );
            assert!(
                serde_json::from_value::<AgentEditsRequest>(json!({"id":"a1","generation":bad}))
                    .is_err()
            );
        }
        for bad in [json!(false), json!(1), json!([]), json!(""), json!(" ")] {
            assert!(serde_json::from_value::<AgentListRequest>(json!({"after":bad})).is_err());
            assert!(
                serde_json::from_value::<AgentProcessesRequest>(json!({"id":"a1","after":bad}))
                    .is_err()
            );
        }
        for bad in [
            json!(false),
            json!(1),
            json!([]),
            json!("other"),
            json!(" workspace "),
        ] {
            assert!(
                serde_json::from_value::<AgentProcessesRequest>(json!({"id":"a1","scope":bad}))
                    .is_err()
            );
        }
        for limit in [1, 1000] {
            let request: AgentListRequest = serde_json::from_value(json!({"limit":limit})).unwrap();
            request.validate().unwrap();
        }
    }

    #[test]
    fn edit_cursor_is_opaque_without_trim_or_path_normalization() {
        for path in [" ", "\n", r"\\?\C:\work\note.txt", "é"] {
            let request: AgentEditsRequest =
                serde_json::from_value(json!({"id":"a1","after":path})).unwrap();
            request.validate().unwrap();
            assert_eq!(request.after.as_deref(), Some(path));
        }
        let mut request = AgentEditsRequest::new("a1");
        request.after = Some("é".repeat(EDIT_CURSOR_MAX_BYTES / 2));
        request.validate().unwrap();
        request.after.as_mut().unwrap().push('x');
        assert!(request.validate().is_err());
        for bad in [json!(""), json!("x\0y"), json!(false), json!(1), json!([])] {
            assert!(
                serde_json::from_value::<AgentEditsRequest>(json!({"id":"a1","after":bad}))
                    .is_err()
            );
        }
    }

    #[test]
    fn process_cursor_requires_objects_and_strict_fields_at_both_levels() {
        let valid = cursor();
        assert_eq!(
            ProcessPageCursor::decode(&valid.encode().unwrap()).unwrap(),
            valid
        );
        let encoded = serde_json::to_value(&valid).unwrap();
        for value in [
            json!(["a1",1,"workspace",{"host_boot_id":"boot","pid":7,"start_time":9}]),
            json!(null),
            json!(true),
        ] {
            assert!(ProcessPageCursor::decode(&value.to_string()).is_err());
        }
        for field in ["agent", "generation", "scope", "identity"] {
            let mut missing = encoded.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(ProcessPageCursor::decode(&missing.to_string()).is_err());
        }
        let mut extra = encoded.clone();
        extra["unknown"] = json!(1);
        assert!(ProcessPageCursor::decode(&extra.to_string()).is_err());
        let mut extra = encoded.clone();
        extra["identity"]["unknown"] = json!(1);
        assert!(ProcessPageCursor::decode(&extra.to_string()).is_err());
        let mut array = encoded;
        array["identity"] = json!(["boot", 7, 9]);
        assert!(ProcessPageCursor::decode(&array.to_string()).is_err());
    }

    #[test]
    fn process_cursor_identity_bounds_and_post_resolution_binding_are_separate() {
        for identity in [
            ProcessIdentity {
                pid: 0,
                ..cursor().identity
            },
            ProcessIdentity {
                start_time: 0,
                ..cursor().identity
            },
            ProcessIdentity {
                host_boot_id: String::new(),
                ..cursor().identity
            },
            ProcessIdentity {
                host_boot_id: "b".repeat(513),
                ..cursor().identity
            },
            ProcessIdentity {
                host_boot_id: "b\n".to_owned(),
                ..cursor().identity
            },
        ] {
            let invalid = ProcessPageCursor {
                identity,
                ..cursor()
            };
            assert!(ProcessPageCursor::decode(&serde_json::to_string(&invalid).unwrap()).is_err());
        }
        assert!(ProcessPageCursor::decode(&" ".repeat(PROCESS_CURSOR_MAX_BYTES + 1)).is_err());
        let mut huge = cursor();
        huge.agent = "a".repeat(PROCESS_CURSOR_MAX_BYTES);
        assert_eq!(huge.encode().unwrap_err().code, "frame_too_large");
        let mut request = AgentProcessesRequest::new("different");
        request.generation = Some(999);
        request.after = Some(cursor().encode().unwrap());
        request.validate().unwrap(); // Store/authorization error precedence is preserved.
        let cursor = request.cursor().unwrap().unwrap();
        assert!(cursor.validate_binding("different", 999, "agent").is_err());
        cursor.validate_binding("a1", 1, "workspace").unwrap();
        request.scope = Some("workspace".to_owned());
        request.generation = Some(i64::MAX);
        request.validate().unwrap(); // No Core rejection of a future workspace generation.
    }

    #[test]
    fn flattened_get_and_list_roundtrip_and_reject_nested_arrays() {
        let agent = Agent::new("a1", "project", "fake", "fake", 0);
        let reply = AgentGetResponse {
            agent: agent.clone(),
            observation: observation(),
            source_states: vec![],
            session_usage: None,
            config: None,
        };
        let wire = serde_json::to_value(&reply).unwrap();
        assert_eq!(wire["id"], "a1");
        assert!(wire.get("agent").is_none());
        assert_eq!(
            serde_json::from_value::<AgentGetResponse>(wire.clone()).unwrap(),
            reply
        );
        AgentGetRequest::new("a1")
            .validate_response(&reply)
            .unwrap();
        unknown(AgentGetRequest::new("a2").validate_response(&reply));
        for field in ["observation", "source_states"] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<AgentGetResponse>(missing).is_err());
        }
        let mut malformed = wire.clone();
        malformed["observation"] = json!([false, "unavailable"]);
        assert!(serde_json::from_value::<AgentGetResponse>(malformed).is_err());
        let mut malformed = wire.clone();
        malformed["source_states"] = json!([[]]);
        assert!(serde_json::from_value::<AgentGetResponse>(malformed).is_err());
        let mut malformed = wire;
        malformed["pending"] = json!([["request", "a1", 0, "turn", "approval", {}]]);
        assert!(serde_json::from_value::<AgentGetResponse>(malformed).is_err());
        let reply = AgentListResponse {
            agents: vec![AgentListRow {
                summary: summary(),
                observation: None,
            }],
            next: Some("a1".to_owned()),
            total: 1,
            truncated: false,
        };
        let wire = serde_json::to_value(&reply).unwrap();
        assert!(wire["agents"][0].get("observation").is_none());
        assert_eq!(
            serde_json::from_value::<AgentListResponse>(wire.clone()).unwrap(),
            reply
        );
        AgentListRequest::default()
            .validate_response(&reply)
            .unwrap();
        // Compact objects reject nested positional arrays; full Agent rows
        // stay on `agent.get`.
        let mut malformed = wire.clone();
        malformed["agents"][0]["activity"] = json!([["running"]]);
        assert!(serde_json::from_value::<AgentListResponse>(malformed).is_err());
        let mut malformed = wire;
        malformed["agents"][0]["usage"] = json!([["message", "m1"]]);
        assert!(serde_json::from_value::<AgentListResponse>(malformed).is_err());
    }

    fn summary() -> AgentSummary {
        AgentSummary {
            id: "a1".to_owned(),
            generation: 0,
            project: "p".to_owned(),
            harness: "fake".to_owned(),
            state: AgentState::Idle,
            parent: None,
            root: "a1".to_owned(),
            active_turn: None,
            last_progress_at: None,
            progress_age_ms: None,
            last_turn_id: None,
            last_outcome: None,
            activity: None,
            usage: None,
            updated_at: 7,
        }
    }

    #[test]
    fn list_context_allows_self_principal_and_checks_projection_and_cursor() {
        let mut request = AgentListRequest {
            after: Some("a9".to_owned()),
            ..Default::default()
        };
        let mut reply = AgentListResponse {
            agents: vec![AgentListRow {
                summary: summary(),
                observation: None,
            }],
            next: Some("a1".to_owned()),
            total: 1,
            truncated: false,
        };
        request.validate_response(&reply).unwrap(); // self-scope keeps one row with its cursor.
        reply.next = Some("wrong".to_owned());
        unknown(request.validate_response(&reply));
        reply.next = Some("a1".to_owned());
        request.after = Some("a1".to_owned());
        unknown(request.validate_response(&reply));
        request.after = None;
        request.observations = true;
        unknown(request.validate_response(&reply));
        reply.agents[0].observation = Some(observation());
        request.validate_response(&reply).unwrap();
        reply.agents.push(reply.agents[0].clone());
        unknown(request.validate_response(&reply));
        reply.agents[1].summary.id = "a2".to_owned();
        reply.agents[1].summary.root = "a2".to_owned();
        reply.next = Some("a2".to_owned());
        request.limit = 1;
        unknown(request.validate_response(&reply));
    }

    #[test]
    fn list_filters_match_projection_and_report_counts_honestly() {
        let request = AgentListRequest {
            project: Some("p".to_owned()),
            harness: Some("fake".to_owned()),
            state: Some(AgentState::Idle),
            root: Some("a1".to_owned()),
            ..Default::default()
        };
        request.validate().unwrap();
        let reply = AgentListResponse {
            agents: vec![AgentListRow {
                summary: summary(),
                observation: None,
            }],
            next: Some("a1".to_owned()),
            total: 1,
            truncated: false,
        };
        request.validate_response(&reply).unwrap();
        // A short page must not claim truncation; total covers the scope.
        let mut short = reply.clone();
        short.truncated = true;
        short.next = None;
        unknown(request.validate_response(&short));
        let mut undercounted = reply.clone();
        undercounted.total = 0;
        unknown(request.validate_response(&undercounted));
        // Filter mismatches reject instead of filtering silently.
        for mutate in [
            AgentListRequest {
                project: Some("other".to_owned()),
                ..request.clone()
            },
            AgentListRequest {
                harness: Some("other".to_owned()),
                ..request.clone()
            },
            AgentListRequest {
                state: Some(AgentState::Busy),
                ..request.clone()
            },
            AgentListRequest {
                parent: Some("a2".to_owned()),
                ..request.clone()
            },
            AgentListRequest {
                root: Some("a2".to_owned()),
                ..request.clone()
            },
        ] {
            unknown(mutate.validate_response(&reply));
        }
        // Unknown filter shapes are invalid, never empty matches.
        for invalid in [
            AgentListRequest {
                project: Some("  ".to_owned()),
                ..Default::default()
            },
            AgentListRequest {
                harness: Some(String::new()),
                ..Default::default()
            },
            AgentListRequest {
                parent: Some("a0".to_owned()),
                ..Default::default()
            },
            AgentListRequest {
                root: Some("nope".to_owned()),
                ..Default::default()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
        assert!(serde_json::from_value::<AgentListRequest>(json!({"state": "done"})).is_err());
    }

    #[test]
    fn list_pagination_requires_cursor_on_every_nonempty_page() {
        let request = AgentListRequest::default();
        let row = || AgentListRow {
            summary: summary(),
            observation: None,
        };
        // Every nonempty page carries `next` at the last returned id, even
        // when nothing is truncated; an empty page carries no cursor.
        let ok = AgentListResponse {
            agents: vec![row()],
            next: Some("a1".to_owned()),
            total: 1,
            truncated: false,
        };
        request.validate_response(&ok).unwrap();
        let mut missing_cursor = ok.clone();
        missing_cursor.next = None;
        unknown(request.validate_response(&missing_cursor));
        let mut wrong_cursor = ok.clone();
        wrong_cursor.next = Some("a2".to_owned());
        unknown(request.validate_response(&wrong_cursor));
        let empty_ok = AgentListResponse {
            agents: vec![],
            next: None,
            total: 1,
            truncated: false,
        };
        request.validate_response(&empty_ok).unwrap();
        let mut dangling = empty_ok.clone();
        dangling.next = Some("a1".to_owned());
        unknown(request.validate_response(&dangling));
        // `total` and `truncated` are required on the wire: a missing count
        // never reads back as zero, and a missing flag never reads as quiet.
        let wire = serde_json::to_value(&ok).unwrap();
        for field in ["total", "truncated"] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<AgentListResponse>(missing).is_err(),
                "{field} must be required"
            );
        }
    }

    #[test]
    fn process_response_keeps_owner_generation_order_and_cursor_binding() {
        let mut request = AgentProcessesRequest::new("a1");
        request.scope = Some("workspace".to_owned());
        let mut reply = AgentProcessesResponse {
            agent: "a1".to_owned(),
            generation: 1,
            scope: Some("workspace".to_owned()),
            processes: vec![process()],
            next: Some(cursor().encode().unwrap()),
        };
        request.validate_response(&reply).unwrap();
        let wire = serde_json::to_value(&reply).unwrap();
        assert!(wire["processes"][0]["exit_code"].is_null());
        assert_eq!(
            serde_json::from_value::<AgentProcessesResponse>(wire.clone()).unwrap(),
            reply
        );
        let mut malformed = wire.clone();
        malformed["processes"][0]["identity"] = json!(["boot", 7, 9]);
        assert!(serde_json::from_value::<AgentProcessesResponse>(malformed).is_err());
        let mut malformed = wire;
        malformed["processes"] = json!([[]]);
        assert!(serde_json::from_value::<AgentProcessesResponse>(malformed).is_err());
        request.generation = Some(2);
        unknown(request.validate_response(&reply));
        request.generation = None;
        reply.processes[0].owner = "agent:a1#1".to_owned();
        unknown(request.validate_response(&reply));
        reply.processes[0] = process();
        request.after = reply.next.clone();
        unknown(request.validate_response(&reply));
        request.after = None;
        reply.processes.push(process());
        unknown(request.validate_response(&reply));
        reply.processes.pop();
        reply.next = None;
        unknown(request.validate_response(&reply));
        reply.processes.clear();
        request.validate_response(&reply).unwrap();
        reply.next = Some(cursor().encode().unwrap());
        unknown(request.validate_response(&reply));
    }

    #[test]
    fn stats_git_edits_bind_explicit_generation_and_keep_flattened_failures() {
        let mut request = AgentStatsRequest::new("a1");
        let reply = AgentStatsResponse {
            agent: "a1".to_owned(),
            generation: 4,
            observation: observation(),
            session_usage: None,
        };
        let wire = serde_json::to_value(&reply).unwrap();
        assert_eq!(wire["available"], false);
        assert!(wire.get("observation").is_none());
        assert_eq!(
            serde_json::from_value::<AgentStatsResponse>(wire).unwrap(),
            reply
        );
        request.validate_response(&reply).unwrap();
        request.generation = Some(3);
        unknown(request.validate_response(&reply));
        let reply: AgentGitResponse = serde_json::from_value(
            json!({"agent":"a1","generation":4,"available":false,"error_code":"unavailable"}),
        )
        .unwrap();
        let mut request = AgentGitRequest::new("a1");
        request.validate_response(&reply).unwrap();
        assert_eq!(
            serde_json::to_value(&reply).unwrap()["error_code"],
            "unavailable"
        );
        request.id = "a2".to_owned();
        unknown(request.validate_response(&reply));
        let mut request = AgentEditsRequest::new("a1");
        request.generation = Some(1);
        request.after = Some(" ".to_owned());
        let mut reply = AgentEditsResponse {
            agent: "a1".to_owned(),
            generation: 1,
            edits: vec![AgentEdit {
                agent: "a1".to_owned(),
                generation: 1,
                workspace: "opaque".to_owned(),
                path: "note.txt".to_owned(),
                updated_at: 0,
            }],
            next: Some("note.txt".to_owned()),
        };
        request.validate_response(&reply).unwrap();
        reply.edits[0].generation = 2;
        unknown(request.validate_response(&reply));
        reply.edits[0].generation = 1;
        reply.next = None;
        unknown(request.validate_response(&reply));
        reply.edits.clear();
        request.validate_response(&reply).unwrap();
    }

    fn status() -> Value {
        json!({"state":"running","admission":"open","agents":0,"store_write_faulted":false,
            "owned_plugins":0,"observer_readers":0,"discovery_readers":0,
            "workspace_custody":{"scope":"current_host","capability":"unix_process_group_degraded","exact":false,"orphan_possible":true},
            "workspace_recovery_error":null,"cloud_mcp":null,
            "automation_tasks":[{"name":"rule_worker","health":"running","failure_code":null},{"name":"goal_driver","health":"running","failure_code":null}],
            "unknown_commands_count":1,"unknown_commands":["c1"],"unknown_commands_truncated":false,
            "unsettled_commands_count":1,"unsettled_commands":[{"id":"c1","agent":null,"generation":null,"verb":"launch","state":"unknown"}],
            "unsettled_commands_truncated":false,"blockers":[{"kind":"workspace_processes"},{"kind":"discovery_readers","count":1},
            {"kind":"agent_live","agent":"a1","generation":0,"state":"created"}, {"kind":"command_unsettled","command_id":"c1","state":"unknown"},
            {"kind":"store_write_faulted"},{"kind":"command_unknown","command_id":"c2"},{"kind":"command_unknown_more","count":1}]})
    }

    #[test]
    fn daemon_status_keeps_all_typed_blockers_nullable_fields_and_actual_modes() {
        let request = DaemonStatusRequest::default();
        let wire = status();
        let mut reply: DaemonStatusResponse = serde_json::from_value(wire.clone()).unwrap();
        request.validate_response(&reply).unwrap();
        assert_eq!(serde_json::to_value(&reply).unwrap(), wire);
        for mode in ["draining", "forcing"] {
            reply.admission = mode.to_owned();
            reply.state = mode.to_owned();
            request.validate_response(&reply).unwrap();
        }
        reply.state = "running".to_owned();
        unknown(request.validate_response(&reply));
        for field in ["workspace_custody", "unsettled_commands", "blockers"] {
            let mut malformed = wire.clone();
            malformed[field] = if field == "workspace_custody" {
                json!([])
            } else {
                json!([[]])
            };
            assert!(serde_json::from_value::<DaemonStatusResponse>(malformed).is_err());
        }
        for field in [
            "workspace_recovery_error",
            "observer_readers",
            "unsettled_commands",
            "blockers",
        ] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<DaemonStatusResponse>(missing).is_err());
        }
        // Drain report is optional for backward compatibility but validated
        // when present; AgentLive keeps its active turn when observed.
        let mut with_drain = wire.clone();
        with_drain["drain"] = json!({
            "running_turns_count":1,
            "running_turns":[{"agent":"a1","generation":0,"turn_id":"t1"}],
            "running_turns_truncated":false,
            "unsettled_commands_count":1,
            "unsettled_commands":[{"id":"c1","agent":null,"generation":null,"verb":"launch","state":"unknown"}],
            "unsettled_commands_truncated":false,
            "unknown_commands_count":1,"unknown_commands":["c1"],"unknown_commands_truncated":false,
            "owned_processes_count":1,
            "owned_processes":[{"agent":"a1","generation":0,"kind":"plugin"}],
            "owned_processes_truncated":false
        });
        let reply: DaemonStatusResponse = serde_json::from_value(with_drain.clone()).unwrap();
        request.validate_response(&reply).unwrap();
        assert_eq!(serde_json::to_value(&reply).unwrap(), with_drain);
        let mut live = with_drain.clone();
        live["blockers"][2] = json!({"kind":"agent_live","agent":"a1","generation":0,"state":"idle","active_turn":"t1"});
        let reply: DaemonStatusResponse = serde_json::from_value(live.clone()).unwrap();
        request.validate_response(&reply).unwrap();
        assert_eq!(serde_json::to_value(&reply).unwrap(), live);
    }

    #[test]
    fn drain_report_validates_counts_lists_and_truncation() {
        let mut report = DrainReport::empty();
        report.validate().unwrap();
        let wire = serde_json::to_value(&report).unwrap();
        let back: DrainReport = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(back, report);
        assert_eq!(serde_json::to_value(back).unwrap(), wire);
        // Counts must cover their lists; truncation permits more beyond bound.
        report.running_turns_count = 1;
        assert!(report.validate().is_err());
        report.running_turns.push(RunningTurnSummary {
            agent: "a1".to_owned(),
            generation: 0,
            turn_id: "t1".to_owned(),
        });
        report.validate().unwrap();
        report.running_turns_truncated = true;
        report.running_turns_count = 0;
        assert!(report.validate().is_err());
        report.running_turns_count = 2;
        report.validate().unwrap();
    }

    #[test]
    fn source_projection_retains_null_counters_and_pending_workspace_generation() {
        let wire = json!({"agent_id":"a1","generation":1,"source":"workspace_process","families":["workspace_process"],
            "owners":[{"family":"workspace_process","source":"workspace_process","mechanism":"os"}],"state":"open","health":"ok",
            "cursor":null,"last_store_seq":null,"last_batch_id":null,"last_batch_first_seq":null,"last_batch_last_seq":null,
            "gap_id":null,"gap_from":null,"gap_to":null,"gap_seq":null,
            "view_unavailable":0,"view_readback_recovered":0,"view_settled_unknown":0});
        let source: SourceState = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&source).unwrap(), wire);
        // Pre-counter wire without the view-health keys still reads back as zero.
        let mut legacy = wire.clone();
        for key in [
            "view_unavailable",
            "view_readback_recovered",
            "view_settled_unknown",
        ] {
            legacy.as_object_mut().unwrap().remove(key);
        }
        let legacy_source: SourceState = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy_source.view_unavailable, 0);
        assert_eq!(legacy_source.view_readback_recovered, 0);
        assert_eq!(legacy_source.view_settled_unknown, 0);
        let mut reply = AgentGetResponse {
            agent: Agent::new("a1", "p", "fake", "fake", 0),
            observation: observation(),
            source_states: vec![source],
            session_usage: None,
            config: None,
        };
        AgentGetRequest::new("a1")
            .validate_response(&reply)
            .unwrap();
        reply.source_states[0].agent_id = "a2".to_owned();
        unknown(AgentGetRequest::new("a1").validate_response(&reply));
        let mut malformed = wire.clone();
        malformed["owners"] = json!([["workspace_process", "workspace_process", "os"]]);
        assert!(serde_json::from_value::<SourceState>(malformed).is_err());
        let mut missing = wire;
        missing.as_object_mut().unwrap().remove("last_store_seq");
        assert!(serde_json::from_value::<SourceState>(missing).is_err());
    }
}
