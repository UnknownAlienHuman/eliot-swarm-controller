//! Public Agent/daemon read grammar and pure reply binding checks.
//! Store authorization and the exact workspace target remain Router decisions.

use super::{GitView, ObservationView};
use crate::{
    Agent, AgentEdit, AgentState, CommandState, Error, ProcessIdentity, ProcessRecord, Result,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

pub const PROCESS_CURSOR_MAX_BYTES: usize = 2_048;
const EDIT_CURSOR_MAX_BYTES: usize = 64 * 1_024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentGetRequest {
    #[serde(deserialize_with = "required_text")]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentListRequest {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    pub limit: usize,
    #[serde(default)]
    pub observations: bool,
}

impl Default for AgentListRequest {
    fn default() -> Self {
        Self {
            after: None,
            limit: super::DEFAULT_LIMIT,
            observations: false,
        }
    }
}

impl AgentListRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(after) = &self.after {
            text(after)?;
        }
        limit(self.limit)
    }

    pub fn validate_response(&self, reply: &AgentListResponse) -> Result<()> {
        if self.validate().is_err() || reply.agents.len() > self.limit {
            return Err(invalid_response());
        }
        let mut ids = BTreeSet::new();
        if reply.agents.iter().any(|row| {
            !valid_agent(&row.agent)
                || !ids.insert(&row.agent.id)
                || row.observation.is_some() != self.observations
        }) || reply.next.as_ref().is_some_and(|next| {
            reply.agents.last().is_none_or(|row| &row.agent.id != next)
                || self.after.as_ref() == Some(next)
        }) {
            return Err(invalid_response());
        }
        // A self-principal list ignores `after` and returns `next: null`.
        // Neither a non-null next nor a full page promises another record.
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentProcessesRequest {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
    pub generation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_scope"
    )]
    pub scope: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStatsRequest {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
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
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentGitRequest {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentEditsRequest {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
    pub generation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_edit_cursor"
    )]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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
            || reply.workspace_custody.scope != "current_host"
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentGetResponse {
    #[serde(flatten, deserialize_with = "object_agent")]
    pub agent: Agent,
    #[serde(deserialize_with = "object")]
    pub observation: ObservationView,
    #[serde(deserialize_with = "objects")]
    pub source_states: Vec<SourceState>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentListRow {
    #[serde(flatten, deserialize_with = "object_agent")]
    pub agent: Agent,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_object"
    )]
    pub observation: Option<ObservationView>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentListResponse {
    #[serde(deserialize_with = "objects")]
    pub agents: Vec<AgentListRow>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentStatsResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(flatten, deserialize_with = "object")]
    pub observation: ObservationView,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentGitResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(flatten, deserialize_with = "object")]
    pub git: GitView,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEditsResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "objects")]
    pub edits: Vec<AgentEdit>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

/// The exact public Store source projection, separate from private plugin ACKs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOwner {
    pub family: String,
    pub source: String,
    pub mechanism: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCustodyView {
    pub scope: String,
    pub capability: String,
    pub exact: bool,
    pub orphan_possible: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRegistrationView {
    pub id: String,
    pub checkout: String,
    pub repo_common_dir: String,
    pub profiles: Vec<String>,
    pub actions: Vec<String>,
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
        validate_project_allowset(&self.actions, "actions")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsettledCommandSummary {
    pub id: String,
    #[serde(deserialize_with = "nullable")]
    pub agent: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub generation: Option<i64>,
    pub verb: String,
    pub state: CommandState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    pub unknown_commands_count: usize,
    pub unknown_commands: Vec<String>,
    pub unknown_commands_truncated: bool,
    pub unsettled_commands_count: usize,
    #[serde(deserialize_with = "objects")]
    pub unsettled_commands: Vec<UnsettledCommandSummary>,
    pub unsettled_commands_truncated: bool,
    #[serde(deserialize_with = "objects")]
    pub blockers: Vec<StatusBlocker>,
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
            revision: 1,
        };
        view.validate().unwrap();
        let wire = serde_json::to_value(&view).unwrap();
        assert_eq!(wire["profiles"], json!([]));
        assert_eq!(wire["actions"], json!([]));
        assert_eq!(
            serde_json::from_value::<ProjectRegistrationView>(wire).unwrap(),
            view
        );

        let mut invalid = view.clone();
        invalid.profiles = vec!["fake".to_owned(), "fake".to_owned()];
        assert!(invalid.validate().is_err());
        invalid = view;
        invalid.actions = vec!["stop".to_owned(), " ".to_owned()];
        assert!(invalid.validate().is_err());
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
                agent,
                observation: None,
            }],
            next: Some("a1".to_owned()),
        };
        let wire = serde_json::to_value(&reply).unwrap();
        assert!(wire["agents"][0].get("observation").is_none());
        assert_eq!(
            serde_json::from_value::<AgentListResponse>(wire).unwrap(),
            reply
        );
        AgentListRequest::default()
            .validate_response(&reply)
            .unwrap();
    }

    #[test]
    fn list_context_allows_self_principal_and_checks_projection_and_cursor() {
        let mut request = AgentListRequest {
            after: Some("a9".to_owned()),
            ..Default::default()
        };
        let mut reply = AgentListResponse {
            agents: vec![AgentListRow {
                agent: Agent::new("a1", "p", "fake", "fake", 0),
                observation: None,
            }],
            next: None,
        };
        request.validate_response(&reply).unwrap(); // self-principal ignores after.
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
        reply.agents[1].agent.id = "a2".to_owned();
        reply.next = Some("a2".to_owned());
        request.limit = 1;
        unknown(request.validate_response(&reply));
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
            "workspace_recovery_error":null,"unknown_commands_count":1,"unknown_commands":["c1"],"unknown_commands_truncated":false,
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
    }

    #[test]
    fn source_projection_retains_null_counters_and_pending_workspace_generation() {
        let wire = json!({"agent_id":"a1","generation":1,"source":"workspace_process","families":["workspace_process"],
            "owners":[{"family":"workspace_process","source":"workspace_process","mechanism":"os"}],"state":"open","health":"ok",
            "cursor":null,"last_store_seq":null,"last_batch_id":null,"last_batch_first_seq":null,"last_batch_last_seq":null,
            "gap_id":null,"gap_from":null,"gap_to":null,"gap_seq":null});
        let source: SourceState = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&source).unwrap(), wire);
        let mut reply = AgentGetResponse {
            agent: Agent::new("a1", "p", "fake", "fake", 0),
            observation: observation(),
            source_states: vec![source],
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
