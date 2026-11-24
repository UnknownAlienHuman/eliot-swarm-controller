//! Public Agent/daemon read requests and pure reply binding checks.
//! Store authorization and the exact workspace target remain Router decisions.

use super::agent_views::{
    generation, invalid_response, invalid_response_detail, required_text, scope, text,
    valid_identity,
};
use super::{
    AgentEditsResponse, AgentGetResponse, AgentGitResponse, AgentListResponse,
    AgentPatchDigestResponse, AgentProcessesResponse, AgentStatsResponse, DaemonPanicTestResponse,
    DaemonStatusResponse, DiagnoseResponse, ProcessPageCursor, ProjectDiscoverResponse,
};
use crate::{Agent, AgentState, Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(super) const EDIT_CURSOR_MAX_BYTES: usize = 64 * 1_024;

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
                .instruction_delivery
                .as_ref()
                .is_some_and(|facts| facts.validate().is_err())
            || reply
                .owner_rules_delivery
                .as_ref()
                .is_some_and(|facts| facts.validate().is_err())
            || reply
                .pending_background_wake
                .as_ref()
                .is_some_and(|wake| wake.validate().is_err())
            || reply.last_control_failure.as_ref().is_some_and(|failure| {
                failure.validate().is_err() || failure.generation != reply.agent.generation
            })
            || reply
                .session_usage
                .as_ref()
                .is_some_and(|usage| usage.validate().is_err())
            || reply
                .config
                .as_ref()
                .is_some_and(|config| config.validate().is_err())
            || reply
                .heavy
                .as_ref()
                .is_some_and(|heavy| heavy.validate().is_err())
            || reply
                .session_growth
                .as_ref()
                .is_some_and(|growth| growth.validate().is_err())
            || reply
                .work
                .as_ref()
                .is_some_and(|work| work.validate().is_err())
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
    #[serde(default)]
    pub archived: super::ArchivedFilter,
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
    /// Nonempty distinct state set, mutually exclusive with `state`. `limit`
    /// remains a total row cap. Requests state totals for all nine states.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 9))]
    pub states: Option<Vec<AgentState>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub project: Option<String>,
    /// Exact authorization-scope match (`COALESCE(project_scope, project)`),
    /// the namespace `swarm.reparent` and `swarm.handoff` compare. A legacy
    /// row without a retained scope matches through its project label, so one
    /// scope read reaches legacy and registered rows together. Combined with
    /// `project` by conjunction when both are present.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub project_scope: Option<String>,
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
    /// Explicit list order: `id` (default, stable Agent-ID order) or
    /// `recent` (newest activity first: `updated_at` descending, then ID
    /// descending, with a stable `(updated_at, id)` cursor).
    #[serde(default)]
    pub order: super::AgentListOrder,
    /// Word search: every whitespace-separated word is an ASCII
    /// case-insensitive substring of the Agent's `aN` id, retained name,
    /// first-input preview, project label, execution directory or harness.
    /// Nonblank; combined with the other filters by conjunction. Rows,
    /// `total` and `state_totals` apply it alike, in either order.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub query: Option<String>,
}

impl Default for AgentListRequest {
    fn default() -> Self {
        Self {
            archived: super::ArchivedFilter::default(),
            after: None,
            limit: super::DEFAULT_LIMIT,
            observations: false,
            state: None,
            states: None,
            project: None,
            project_scope: None,
            harness: None,
            parent: None,
            root: None,
            order: super::AgentListOrder::default(),
            query: None,
        }
    }
}

impl AgentListRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(states) = &self.states
            && (self.state.is_some()
                || states.is_empty()
                || states.len() > 9
                || states
                    .iter()
                    .enumerate()
                    .any(|(i, state)| states[..i].contains(state)))
        {
            return Err(Error::new(
                "invalid",
                "states must contain 1 through 9 distinct states and cannot accompany state",
            ));
        }
        if let Some(after) = &self.after {
            text(after)?;
            if self.order == super::AgentListOrder::Recent {
                decode_recent_cursor(after)?;
            } else {
                crate::parse_agent_id(after)?;
            }
        }
        if let Some(project) = &self.project {
            text(project)?;
        }
        if let Some(scope) = &self.project_scope {
            text(scope)?;
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
        if let Some(query) = &self.query {
            text(query)?;
        }
        limit(self.limit)
    }

    /// Validate page bounds and continuation against keys from every wire row.
    /// The key tuples are `(Agent ID, updated_at_ms)` and remain complete when
    /// a human-facing reader rejects an individual row for display.
    pub fn validate_response_page(
        &self,
        reply: &AgentListResponse,
        row_keys: &[(String, i64)],
    ) -> Result<()> {
        self.validate().map_err(|_| {
            invalid_response_detail("params", None, "request must validate before readback")
        })?;
        if row_keys.len() > self.limit {
            return Err(invalid_response_detail(
                "agents",
                None,
                "page must not exceed the requested row limit",
            ));
        }
        if reply.total < row_keys.len() {
            return Err(invalid_response_detail(
                "total",
                None,
                "must be at least the number of rows in this page",
            ));
        }

        let after_recent = if self.order == super::AgentListOrder::Recent {
            self.after
                .as_deref()
                .map(decode_recent_cursor)
                .transpose()?
                .map(|(id, activity)| Ok::<_, Error>((activity, crate::parse_agent_id(&id)?)))
                .transpose()?
        } else {
            None
        };
        let after_id = if self.order == super::AgentListOrder::Id {
            self.after
                .as_deref()
                .map(crate::parse_agent_id)
                .transpose()?
        } else {
            None
        };
        let mut seen = BTreeSet::new();
        let mut previous_recent: Option<(i64, i64)> = None;
        let mut previous_id: Option<i64> = None;
        for (index, (id, activity)) in row_keys.iter().enumerate() {
            let number = crate::parse_agent_id(id).map_err(|_| {
                invalid_response_detail(
                    &format!("agents[{index}].id"),
                    None,
                    "must be a valid Agent ID",
                )
            })?;
            if *activity < 0 {
                return Err(invalid_response_detail(
                    &format!("agents[{index}].updated_at"),
                    Some(id),
                    "must be nonnegative milliseconds",
                ));
            }
            if !seen.insert(id) {
                return Err(invalid_response_detail(
                    &format!("agents[{index}].id"),
                    Some(id),
                    "must be unique within the page",
                ));
            }
            if self.order == super::AgentListOrder::Recent {
                let key = (*activity, number);
                if previous_recent.is_some_and(|previous| key >= previous) {
                    return Err(invalid_response_detail(
                        &format!("agents[{index}].updated_at"),
                        Some(id),
                        "rows must be strictly newest-first by updated_at then numeric Agent ID",
                    ));
                }
                if after_recent.is_some_and(|after| key >= after) {
                    return Err(invalid_response_detail(
                        &format!("agents[{index}].updated_at"),
                        Some(id),
                        "row must sort after the supplied recent cursor",
                    ));
                }
                previous_recent = Some(key);
            } else {
                if previous_id.is_some_and(|previous| number <= previous) {
                    return Err(invalid_response_detail(
                        &format!("agents[{index}].id"),
                        Some(id),
                        "rows must be in strictly increasing numeric Agent ID order",
                    ));
                }
                if after_id.is_some_and(|after| number <= after) {
                    return Err(invalid_response_detail(
                        &format!("agents[{index}].id"),
                        Some(id),
                        "row must follow the supplied Agent ID cursor",
                    ));
                }
                previous_id = Some(number);
            }
        }

        let expected_next = row_keys.last().map(|(id, activity)| {
            if self.order == super::AgentListOrder::Recent {
                encode_recent_cursor(id, *activity)
            } else {
                id.clone()
            }
        });
        if reply.next != expected_next {
            return Err(invalid_response_detail(
                "next",
                row_keys.last().map(|(id, _)| id.as_str()),
                "must identify the last row in this page using the requested order",
            ));
        }
        if reply
            .next
            .as_ref()
            .is_some_and(|next| self.after.as_ref() == Some(next))
        {
            return Err(invalid_response_detail(
                "next",
                row_keys.last().map(|(id, _)| id.as_str()),
                "must advance beyond the request cursor",
            ));
        }
        Ok(())
    }

    /// Validate one decoded row and its request filters, naming its page index
    /// and Agent ID in any response diagnostic.
    pub fn validate_row(&self, row: &super::AgentListRow, index: usize) -> Result<()> {
        row.validate()?;
        let id = row.summary.id.as_str();
        let fail = |field: &str, rule: &str| {
            invalid_response_detail(&format!("agents[{index}].{field}"), Some(id), rule)
        };
        if row.observation.is_some() != self.observations {
            return Err(fail(
                "observation",
                "must be present exactly when observations were requested",
            ));
        }
        // The default list keeps an archived ended Agent while its custody is
        // unresolved (core.md); custody is not on the row.
        match self.archived {
            super::ArchivedFilter::Exclude
                if row.summary.archived && !row.summary.state.is_terminal() =>
            {
                return Err(fail(
                    "archived",
                    "an archived nonterminal Agent must not appear in the default list",
                ));
            }
            super::ArchivedFilter::Only if !row.summary.archived => {
                return Err(fail("archived", "must be true for archived:only"));
            }
            _ => {}
        }
        if self.state.is_some_and(|state| row.summary.state != state) {
            return Err(fail("state", "must match the requested state filter"));
        }
        if self
            .states
            .as_ref()
            .is_some_and(|states| !states.contains(&row.summary.state))
        {
            return Err(fail("state", "must be in the requested states set"));
        }
        if self
            .project
            .as_ref()
            .is_some_and(|project| &row.summary.project != project)
        {
            return Err(fail("project", "must match the requested project filter"));
        }
        if self.project_scope.as_ref().is_some_and(|scope| {
            row.summary
                .project_scope
                .as_deref()
                .unwrap_or(&row.summary.project)
                != scope
        }) {
            return Err(fail(
                "project_scope",
                "must match the requested effective project scope",
            ));
        }
        if self
            .harness
            .as_ref()
            .is_some_and(|harness| &row.summary.harness != harness)
        {
            return Err(fail("harness", "must match the requested harness filter"));
        }
        if self
            .parent
            .as_ref()
            .is_some_and(|parent| row.summary.parent.as_ref() != Some(parent))
        {
            return Err(fail("parent", "must match the requested parent filter"));
        }
        if self
            .root
            .as_ref()
            .is_some_and(|root| &row.summary.root != root)
        {
            return Err(fail("root", "must match the requested root filter"));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &AgentListResponse) -> Result<()> {
        let row_keys = reply
            .agents
            .iter()
            .map(|row| (row.summary.id.clone(), row.summary.updated_at))
            .collect::<Vec<_>>();
        self.validate_response_page(reply, &row_keys)?;
        self.validate_response_state_totals(reply)?;
        for (index, row) in reply.agents.iter().enumerate() {
            self.validate_row(row, index)?;
        }
        Ok(())
    }

    /// Validate the state totals independently of the page and its continuation.
    pub fn validate_response_state_totals(&self, reply: &AgentListResponse) -> Result<()> {
        match (&self.states, &reply.state_totals) {
            (Some(states), Some(totals)) => {
                if totals.len() != super::AGENT_LIST_STATES.len()
                    || super::AGENT_LIST_STATES.iter().any(|state| {
                        totals.iter().filter(|entry| entry.state == *state).count() != 1
                    })
                    || totals
                        .iter()
                        .filter(|entry| states.contains(&entry.state))
                        .try_fold(0usize, |sum, entry| sum.checked_add(entry.total))
                        != Some(reply.total)
                    || totals.iter().any(|entry| {
                        reply
                            .agents
                            .iter()
                            .filter(|row| row.summary.state == entry.state)
                            .count()
                            > entry.total
                    })
                {
                    return Err(invalid_response_detail(
                        "state_totals",
                        None,
                        "must contain all nine states, sum selected totals to total, and cover returned rows",
                    ));
                }
            }
            (None, None) => {}
            _ => {
                return Err(invalid_response_detail(
                    "state_totals",
                    None,
                    "must be present exactly when states were requested",
                ));
            }
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
            || reply
                .child_counts
                .as_ref()
                .is_some_and(|counts| counts.exited < 0 || counts.exited > counts.observed)
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
            || reply
                .heavy
                .as_ref()
                .is_some_and(|heavy| heavy.validate().is_err())
            || reply
                .session_growth
                .as_ref()
                .is_some_and(|growth| growth.validate().is_err())
            || reply
                .work
                .as_ref()
                .is_some_and(|work| work.validate().is_err())
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
pub struct AgentPatchDigestRequest {
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

impl AgentPatchDigestRequest {
    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        generation(self.generation)
    }
    pub fn validate_response(&self, reply: &AgentPatchDigestResponse) -> Result<()> {
        if self.validate().is_err()
            || !binding(&self.id, self.generation, &reply.agent, reply.generation)
            || reply.outside_claims.len() > super::MAX_PATCH_DIGEST_OUTSIDE_CLAIMS
            || reply.outside_claims_declared == Some(false)
                && (reply.outside_claims_complete || !reply.outside_claims.is_empty())
            || reply.outside_claims.iter().any(|path| {
                path.trim().is_empty()
                    || path.len() > super::MAX_PATCH_DIGEST_PATH_BYTES
                    || path.contains('\0')
            })
            || reply
                .outside_claims
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
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
pub struct DaemonStatusRequest {
    /// Include the disk snapshot age and refresh state in the response.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disk_usage_freshness: bool,
}

/// Debug-only forced daemon panic for crash-capture tests. Compiled out of
/// release builds together with its router arm.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonPanicTestRequest {}

impl DaemonPanicTestRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
    pub fn validate_response(&self, reply: &DaemonPanicTestResponse) -> Result<()> {
        if reply.state != "panicking" {
            return Err(invalid_response());
        }
        Ok(())
    }
}

impl DaemonStatusRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
    pub fn validate_response(&self, reply: &DaemonStatusResponse) -> Result<()> {
        crate::api::ProfileCommandIssue::validate_list(&reply.profile_command_issues)?;
        let state = match reply.admission.as_str() {
            "open" => "running",
            "draining" => "draining",
            "forcing" => "forcing",
            "quiescing" => "quiescing",
            "restarting" => "restarting",
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
            || reply
                .retention
                .as_ref()
                .is_some_and(|retention| retention.validate().is_err())
            || reply
                .command_without_dispatcher
                .as_ref()
                .is_some_and(|status| {
                    status.validate().is_err()
                        || status.count
                            != reply
                                .liveness
                                .reasons
                                .get("command_without_dispatcher")
                                .copied()
                                .unwrap_or_default()
                })
            || reply
                .disk_usage
                .as_ref()
                .is_some_and(|usage| usage.validate().is_err())
            || reply.workspace_custody.scope != "current_host"
            || reply
                .cloud_mcp
                .as_ref()
                .is_some_and(|status| status.validate().is_err())
            || reply.automation_tasks.is_empty()
            || reply.automation_tasks.len() > DaemonStatusResponse::MAX_AUTOMATION_TASKS
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
            || reply.plugin_crashes.len() > DaemonStatusResponse::MAX_PLUGIN_CRASHES
            || reply
                .plugin_crashes
                .iter()
                .any(|crash| crash.code != "plugin.panicked" || crash.validate().is_err())
            || reply.provider_probe_attention.len() > 16
            || reply
                .provider_probe_attention
                .iter()
                .any(|hold| hold.validate().is_err())
            || reply
                .previous_crash
                .as_ref()
                .is_some_and(|crash| crash.validate().is_err())
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

/// Discover and register the current user's Git checkout through the daemon's
/// internal project-discovery settings author. A non-Git directory is a no-op.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectDiscoverRequest {
    #[serde(deserialize_with = "required_text")]
    pub workspace: String,
}

impl ProjectDiscoverRequest {
    pub fn validate(&self) -> Result<()> {
        text(&self.workspace)?;
        if self.workspace.len() > 4_096 || !std::path::Path::new(&self.workspace).is_absolute() {
            return Err(Error::new(
                "invalid",
                "project discovery workspace must be an absolute path of at most 4096 bytes",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &ProjectDiscoverResponse) -> Result<()> {
        self.validate()?;
        if let Some(project) = &reply.project {
            project.validate()?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnoseRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Command id, Agent id, or Agent id with turn id as agent:turn.")]
    pub target: String,
}

impl DiagnoseRequest {
    pub fn validate(&self) -> Result<()> {
        if self.target.chars().count() > 256 {
            return Err(Error::new("invalid", "diagnose target exceeds its bound"));
        }
        text(&self.target)
    }

    pub fn validate_response(&self, reply: &DiagnoseResponse) -> Result<()> {
        reply.validate()
    }
}

/// Stable recent cursor for the last row of a `recent` page:
/// `<id>@<updated_at_ms>`, e.g. `a587@1763440000000`. The activity is the
/// row's `updated_at` (bumped on every native event), so paging continues
/// after its exact sort position instead of re-seeking by ID.
pub fn encode_recent_cursor(id: &str, activity: i64) -> String {
    format!("{id}@{activity}")
}

/// Decode a [`encode_recent_cursor`] cursor into `(id, activity)`.
/// Rejects blank IDs, non-Agent IDs, negative activity and malformed
/// cursors as `invalid` so a stale `id`-order token never pages silently
/// in `recent` order.
pub fn decode_recent_cursor(cursor: &str) -> Result<(String, i64)> {
    let (id, activity) = cursor
        .rsplit_once('@')
        .ok_or_else(|| Error::new("invalid", "invalid recent cursor"))?;
    text(id)?;
    crate::parse_agent_id(id)?;
    let activity: i64 = activity
        .parse()
        .map_err(|_| Error::new("invalid", "invalid recent cursor"))?;
    if activity < 0 {
        return Err(Error::new("invalid", "invalid recent cursor"));
    }
    Ok((id.to_owned(), activity))
}

fn valid_agent(agent: &Agent) -> bool {
    text(&agent.id).is_ok() && agent.generation >= 0
}

fn binding(id: &str, expected: Option<i64>, agent: &str, actual: i64) -> bool {
    agent == id && actual >= 0 && expected.is_none_or(|expected| expected == actual)
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
