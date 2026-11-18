//! Committed fleet totals and paged project overview details.
use crate::{AgentState, CapacityScope, Error, Result, parse_agent_id};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `project.overview {project?}`: one project team, or every visible project
/// plus the whole-fleet totals when omitted.
#[derive(JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectOverviewRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Registered project id; omitted means every visible project.")]
    pub project: Option<String>,
    /// Opaque continuation from the previous page for the same project filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

impl ProjectOverviewRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(project) = &self.project {
            validate_project_name(project, "params.project")?;
        }
        if let Some(after) = &self.after
            && (after.is_empty() || after.len() > 4096 || after.contains(char::is_control))
        {
            return Err(Error::invalid_field(
                "params.after",
                "project overview continuation",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &ProjectOverviewResponse) -> Result<()> {
        reply.validate()?;
        if let Some(project) = &self.project
            && (reply.projects.iter().any(|entry| &entry.project != project)
                || self.after.is_none() && reply.projects.is_empty()
                || reply
                    .fleet
                    .native_children
                    .iter()
                    .any(|row| row.project.as_ref() != Some(project)))
        {
            return Err(Error::new(
                "unknown",
                "project.overview response is invalid",
            ));
        }
        Ok(())
    }
}

/// Current Owner limits for the 2c scopes, built by the daemon from its
/// config store. The Store fills usage beside them; a missing limit means
/// unlimited, as for every other configured capacity bound.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeLimits {
    pub global: Option<u64>,
    pub projects: BTreeMap<String, ProjectScopeLimits>,
}

/// Per-project Owner limits: the project cap and the manager-assignment cap.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProjectScopeLimits {
    pub project: Option<u64>,
    pub manager_assignment: Option<u64>,
    pub manager_min_live: Option<u64>,
    pub manager_priority_max: Option<u8>,
}

/// One scope's live usage beside its current Owner limit.
#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeUsage {
    #[schemars(description = "global:swarm, project:<id> or manager_assignment:<id>.")]
    pub scope: String,
    #[schemars(description = "Live reservations occupying this scope.")]
    pub usage: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Current Owner limit; absent means unlimited.")]
    pub limit: Option<u64>,
}

impl ScopeUsage {
    pub fn validate(&self) -> Result<()> {
        CapacityScope {
            scope: self.scope.clone(),
            max_live: self.limit,
        }
        .validate()
        .map_err(|_| Error::new("unknown", "project.overview response is invalid"))?;
        Ok(())
    }
}

/// The current manager of one project assignment, when one is assigned.
#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverviewManagerView {
    pub agent: String,
    pub generation: i64,
    pub state: AgentState,
    /// Monotonic assignment revision for this manager binding.
    #[serde(default)]
    pub assignment_revision: i64,
    /// Assignment workspace, including adopted checkouts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// The standard branch of the managed worktree at `workspace`, when the
    /// Store retains its preparation; absent for an adopted checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

impl OverviewManagerView {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)
            .map_err(|_| Error::new("unknown", "project.overview response is invalid"))?;
        if self.generation < 1
            || self.branch.as_ref().is_some_and(|branch| {
                branch.trim().is_empty() || branch.len() > 1024 || branch.contains(char::is_control)
            })
        {
            return Err(Error::new(
                "unknown",
                "project.overview response is invalid",
            ));
        }
        Ok(())
    }
}

/// One controlled (non-observe) Agent of a project team.
#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverviewWorkerView {
    pub agent: String,
    pub generation: i64,
    pub state: AgentState,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn: Option<String>,
}

impl OverviewWorkerView {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)
            .map_err(|_| Error::new("unknown", "project.overview response is invalid"))?;
        if self.generation < 0 || self.harness.trim().is_empty() || self.root.trim().is_empty() {
            return Err(Error::new(
                "unknown",
                "project.overview response is invalid",
            ));
        }
        if let Some(parent) = &self.parent {
            parse_agent_id(parent)
                .map_err(|_| Error::new("unknown", "project.overview response is invalid"))?;
        }
        parse_agent_id(&self.root)
            .map_err(|_| Error::new("unknown", "project.overview response is invalid"))?;
        Ok(())
    }
}

/// One observed native child registration, listed separately: registrations
/// of controlled team Agents stay with the team counts, so no Agent is
/// counted twice.
#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeChildView {
    pub child: String,
    pub producer: String,
    pub producer_generation: i64,
    /// `None` is an explicit no-project native chat. Such children are
    /// fleet observations only and are never projected into a team.
    pub project: Option<String>,
    pub state: AgentState,
}

impl NativeChildView {
    pub fn validate(&self) -> Result<()> {
        for id in [&self.child, &self.producer] {
            parse_agent_id(id)
                .map_err(|_| Error::new("unknown", "project.overview response is invalid"))?;
        }
        if self.producer_generation < 0
            || self
                .project
                .as_deref()
                .is_some_and(|project| project.trim().is_empty())
        {
            return Err(Error::new(
                "unknown",
                "project.overview response is invalid",
            ));
        }
        Ok(())
    }
}

/// Exact totals for one project team.
#[derive(JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectTotals {
    #[schemars(description = "Controlled Agents holding capacity in a live state.")]
    pub live: usize,
    #[schemars(description = "Turns in state running for this project.")]
    pub active_turns: usize,
    #[schemars(description = "Commands accepted or queued for this project.")]
    pub queued: usize,
    #[schemars(description = "Capacity holders retained in lost, stopped or failed.")]
    pub retained_unknown: usize,
    #[serde(default)]
    pub lifecycle: LifecycleTotals,
}

/// Controlled generation states, running turns, and commands awaiting dispatch.
#[derive(JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleTotals {
    pub live: usize,
    #[serde(default)]
    pub idle: usize,
    #[serde(default)]
    pub busy: usize,
    #[serde(default)]
    pub waiting: usize,
    pub starting: usize,
    #[serde(default)]
    pub stopping: usize,
    pub active: usize,
    pub queued: usize,
    pub retained_unknown: usize,
}

/// One project entry: the team in full (`detail: true`) or aggregates only.
#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectOverviewEntry {
    pub project: String,
    #[schemars(
        description = "True when workers and workspaces are listed; false carries aggregates only."
    )]
    pub detail: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manager: Option<OverviewManagerView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment: Option<crate::api::ProjectAssignment>,
    #[serde(default)]
    pub native_child_total: usize,
    #[serde(default)]
    pub workers: Vec<OverviewWorkerView>,
    pub worker_total: usize,
    pub workers_truncated: bool,
    #[serde(default)]
    pub workspaces: Vec<crate::api::ManagedWorkspaceView>,
    pub workspace_total: usize,
    pub workspaces_truncated: bool,
    #[schemars(description = "Queued work for this project.")]
    pub queue: usize,
    pub totals: ProjectTotals,
    #[schemars(
        description = "Exactly the project and manager_assignment usages for this project."
    )]
    pub capacity: Vec<ScopeUsage>,
}

impl ProjectOverviewEntry {
    pub fn validate(&self) -> Result<()> {
        validate_project_name(&self.project, "project.overview response")?;
        if let Some(manager) = &self.manager {
            manager.validate()?;
        }
        for worker in &self.workers {
            worker.validate()?;
        }
        for workspace in &self.workspaces {
            if workspace.agent.trim().is_empty() {
                return Err(Error::new(
                    "unknown",
                    "project.overview response is invalid",
                ));
            }
        }
        if self.worker_total < self.workers.len()
            || self.workspace_total < self.workspaces.len()
            || (!self.detail
                && (self.manager.is_some()
                    || self.assignment.is_some()
                    || !self.workers.is_empty()
                    || !self.workspaces.is_empty()
                    || self.workers_truncated
                    || self.workspaces_truncated))
            || self
                .assignment
                .as_ref()
                .is_some_and(|assignment| assignment.project != self.project)
            || self.capacity.len() != 2
            || self.capacity[0].scope != CapacityScope::project_scope(&self.project)
            || self.capacity[1].scope != CapacityScope::manager_assignment_scope(&self.project)
        {
            return Err(Error::new(
                "unknown",
                "project.overview response is invalid",
            ));
        }
        for scope in &self.capacity {
            scope.validate()?;
        }
        Ok(())
    }
}

/// One harness's share of the live fleet.
#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessCount {
    pub harness: String,
    pub total: usize,
}

/// Exact whole-fleet totals plus the separately listed native children.
#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetTotals {
    #[schemars(description = "Controlled Agents holding capacity in a live state.")]
    pub live_harnesses: usize,
    #[serde(default)]
    pub live_by_harness: Vec<HarnessCount>,
    #[schemars(description = "Turns in state running.")]
    pub active_turns: usize,
    #[schemars(description = "Commands accepted or queued.")]
    pub queued_work: usize,
    #[schemars(description = "Capacity holders retained in lost, stopped or failed.")]
    pub retained_unknown: usize,
    #[serde(default)]
    pub manager_total: usize,
    #[serde(default)]
    pub worker_total: usize,
    #[serde(default)]
    pub lifecycle: LifecycleTotals,
    #[serde(default)]
    pub native_children: Vec<NativeChildView>,
    pub native_child_total: usize,
    pub native_children_truncated: bool,
    #[schemars(description = "The global:swarm usage beside its Owner limit.")]
    pub capacity: ScopeUsage,
    #[schemars(
        description = "Exact project count for the fleet, independent of filtering and paging."
    )]
    pub projects: usize,
}

impl FleetTotals {
    pub fn validate(&self) -> Result<()> {
        let mut by_harness = 0;
        for entry in &self.live_by_harness {
            if entry.harness.trim().is_empty() {
                return Err(Error::new(
                    "unknown",
                    "project.overview response is invalid",
                ));
            }
            by_harness += entry.total;
        }
        if by_harness != self.live_harnesses || self.native_child_total < self.native_children.len()
        {
            return Err(Error::new(
                "unknown",
                "project.overview response is invalid",
            ));
        }
        for child in &self.native_children {
            child.validate()?;
        }
        self.capacity.validate()?;
        if self.capacity.scope != "global:swarm" {
            return Err(Error::new(
                "unknown",
                "project.overview response is invalid",
            ));
        }
        Ok(())
    }
}

/// Completeness of native-child observation independent of page completion.
#[derive(JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverviewCoverage {
    Complete,
    Partial,
}

/// The fleet readback: per-project teams plus whole-fleet totals.
#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectOverviewResponse {
    /// Store event sequence at the committed snapshot used for this reply.
    #[serde(default)]
    pub event_watermark: i64,
    pub as_of: i64,
    pub coverage: OverviewCoverage,
    /// `true` when all authorized detail rows have been returned.
    #[serde(default)]
    pub page_complete: bool,
    /// Cursor for the next response-wide bounded page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub fleet: FleetTotals,
    #[serde(default)]
    pub projects: Vec<ProjectOverviewEntry>,
}

impl ProjectOverviewResponse {
    pub fn validate(&self) -> Result<()> {
        let mut previous = String::new();
        for entry in &self.projects {
            entry.validate()?;
            if entry.project <= previous {
                return Err(Error::new(
                    "unknown",
                    "project.overview response is invalid",
                ));
            }
            previous = entry.project.clone();
        }
        self.fleet.validate()?;
        if self.fleet.projects < self.projects.len()
            || self.event_watermark < 0
            || self.page_complete == self.next_cursor.is_some()
        {
            return Err(Error::new(
                "unknown",
                "project.overview response is invalid",
            ));
        }
        Ok(())
    }
}

fn validate_project_name(value: &str, field: &str) -> Result<()> {
    if value.is_empty() || value.len() > 1024 || value.contains(char::is_control) {
        return Err(Error::invalid_field(
            field,
            "nonempty name of at most 1024 bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scope(scope: &str, usage: usize, limit: Option<u64>) -> ScopeUsage {
        ScopeUsage {
            scope: scope.to_owned(),
            usage,
            limit,
        }
    }

    fn entry(project: &str, detail: bool) -> ProjectOverviewEntry {
        ProjectOverviewEntry {
            project: project.to_owned(),
            detail,
            manager: None,
            assignment: None,
            native_child_total: 0,
            workers: Vec::new(),
            worker_total: 0,
            workers_truncated: false,
            workspaces: Vec::new(),
            workspace_total: 0,
            workspaces_truncated: false,
            queue: 0,
            totals: ProjectTotals::default(),
            capacity: vec![
                scope(&format!("project:{project}"), 0, None),
                scope(&format!("manager_assignment:{project}"), 0, None),
            ],
        }
    }

    fn response() -> ProjectOverviewResponse {
        ProjectOverviewResponse {
            event_watermark: 1,
            as_of: 1,
            coverage: OverviewCoverage::Complete,
            page_complete: true,
            next_cursor: None,
            fleet: FleetTotals {
                live_harnesses: 0,
                live_by_harness: Vec::new(),
                active_turns: 0,
                queued_work: 0,
                retained_unknown: 0,
                manager_total: 0,
                worker_total: 0,
                lifecycle: LifecycleTotals::default(),
                native_children: Vec::new(),
                native_child_total: 0,
                native_children_truncated: false,
                capacity: scope("global:swarm", 0, None),
                projects: 1,
            },
            projects: vec![entry("app", true)],
        }
    }

    #[test]
    fn overview_round_trips_and_rejects_bad_shapes() {
        let request = ProjectOverviewRequest {
            project: None,
            after: None,
        };
        request.validate().unwrap();
        let reply = response();
        request.validate_response(&reply).unwrap();
        assert!(reply.page_complete);
        assert!(reply.next_cursor.is_none());
        let paged = ProjectOverviewRequest {
            project: None,
            after: Some("a".to_owned()),
        };
        paged.validate().unwrap();
        let specific = ProjectOverviewRequest {
            project: Some("app".to_owned()),
            after: None,
        };
        specific.validate().unwrap();
        specific.validate_response(&reply).unwrap();
        let missing = ProjectOverviewRequest {
            project: Some("other".to_owned()),
            after: None,
        };
        assert!(missing.validate_response(&reply).is_err());

        let bad: ProjectOverviewRequest = serde_json::from_value(json!({"project": ""})).unwrap();
        assert!(bad.validate().is_err());
        assert!(serde_json::from_value::<ProjectOverviewRequest>(json!({"x": 1})).is_err());

        let mut bad_scope = response();
        bad_scope.projects[0].capacity[0].scope = "route:x".to_owned();
        assert!(bad_scope.validate().is_err());

        let mut unordered = response();
        unordered.projects.push(entry("aaa", false));
        assert!(unordered.validate().is_err());

        let mut aggregate_lists = response();
        aggregate_lists.projects[0].detail = false;
        aggregate_lists.projects[0]
            .workers
            .push(OverviewWorkerView {
                agent: "a1".to_owned(),
                generation: 1,
                state: AgentState::Idle,
                harness: "codex".to_owned(),
                parent: None,
                root: "a1".to_owned(),
                active_turn: None,
            });
        aggregate_lists.projects[0].worker_total = 1;
        assert!(aggregate_lists.validate().is_err());
    }
}
