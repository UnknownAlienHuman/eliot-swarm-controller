//! Server fleet readback for `project.overview`: exact counts plus bounded
//! team lists, read in one committed pass.
//!
//! Counts come from exact `COUNT(*)` reads, never from a UI page: the worker,
//! workspace and native-child lists are bounded by `MAX_PAGE_ENTRIES` and
//! carry truncation flags, while every total stays exact. Native
//! registrations of controlled team Agents stay with the team counts; the
//! native list holds observed (non-controlled) children only, so no Agent is
//! counted twice. No-project chats (`project = ""`) contribute to the fleet
//! totals only, never to a project team.
use super::*;
use eliot_core::api::{
    HarnessCount, NativeChildView, OverviewManagerView, OverviewWorkerView, ProjectOverviewEntry,
    ProjectTotals, ScopeLimits, ScopeUsage,
};

const LIVE_AGENT_STATES: &str = "('created', 'starting', 'idle', 'busy', 'waiting', 'unknown')";
const RETAINED_AGENT_STATES: &str = "('lost', 'stopped', 'failed')";
const QUEUED_COMMAND_STATES: &str = "('accepted', 'queued', 'dispatched')";

/// One committed readback: full-detail project teams plus whole-fleet
/// counters. The daemon shapes this per caller (a manager sees its own team
/// in full and aggregates for other projects) and stamps `as_of`.
pub struct FleetOverviewData {
    pub teams: Vec<ProjectOverviewEntry>,
    pub live_harnesses: usize,
    pub live_by_harness: Vec<HarnessCount>,
    pub active_turns: usize,
    pub queued_work: usize,
    pub retained_unknown: usize,
    pub native_children: Vec<NativeChildView>,
    pub native_child_total: usize,
    pub native_children_truncated: bool,
    pub global_usage: usize,
    pub truncated: bool,
}

impl Store {
    /// Exact fleet and team usage with bounded lists. `limits` carries the
    /// current Owner bounds from the one config store; usage is live
    /// physical reservations, so lowering a limit blocks new admissions
    /// without touching running work, and retained unknown reservations
    /// stay counted.
    pub fn fleet_overview_data(&self, limits: &ScopeLimits) -> Result<FleetOverviewData> {
        self.ensure_committed_reads()?;
        let connection = &self.connection;
        let mut teams = Vec::new();
        let mut truncated = false;

        for project in list_overview_projects(connection)? {
            let entry = project_team(connection, &project, limits)?;
            truncated = truncated || entry.workers_truncated || entry.workspaces_truncated;
            teams.push(entry);
        }

        let live_by_harness = list_live_by_harness(connection)?;
        let live_harnesses = live_by_harness.iter().map(|row| row.total).sum();
        let (native_children, native_children_truncated) = list_native_children(connection)?;
        truncated = truncated || native_children_truncated;

        Ok(FleetOverviewData {
            teams,
            live_harnesses,
            live_by_harness,
            active_turns: count_global_active_turns(connection)?,
            queued_work: count_global_queued(connection)?,
            retained_unknown: count_global_retained(connection)?,
            native_child_total: count_native_children(connection)?,
            native_children,
            native_children_truncated,
            global_usage: count_global_holders(connection)?,
            truncated,
        })
    }
}

fn project_team(
    connection: &Connection,
    project: &str,
    limits: &ScopeLimits,
) -> Result<ProjectOverviewEntry> {
    let manager = read_team_manager(connection, project)?;
    let manager_num = manager
        .as_ref()
        .and_then(|manager| parse_agent_id(&manager.agent).ok());
    let (workers, worker_total, workers_truncated) =
        list_team_workers(connection, project, manager_num)?;
    let (workspaces, workspace_total, workspaces_truncated) =
        list_team_workspaces(connection, project)?;
    let project_limits = limits.projects.get(project).cloned().unwrap_or_default();
    let usage = count_project_holders(connection, project)?;
    Ok(ProjectOverviewEntry {
        project: project.to_owned(),
        detail: true,
        manager,
        workers,
        worker_total,
        workers_truncated,
        workspaces,
        workspace_total,
        workspaces_truncated,
        queue: count_team_queued(connection, project)?,
        totals: ProjectTotals {
            live: count_team_holders(connection, project, true)?,
            active_turns: count_team_active_turns(connection, project)?,
            queued: count_team_queued(connection, project)?,
            retained_unknown: count_team_holders(connection, project, false)?,
        },
        capacity: vec![
            ScopeUsage {
                scope: eliot_core::CapacityScope::project_scope(project),
                usage,
                limit: project_limits.project,
            },
            ScopeUsage {
                scope: eliot_core::CapacityScope::manager_assignment_scope(project),
                usage,
                limit: project_limits.manager_assignment,
            },
        ],
    })
}

fn project_match_sql(column_prefix: &str) -> String {
    format!(
        "({column_prefix}.project = ?1 OR COALESCE({column_prefix}.project_scope, {column_prefix}.project) = ?1)"
    )
}

fn list_overview_projects(connection: &Connection) -> Result<Vec<String>> {
    let mut statement = connection
        .prepare(
            "SELECT project FROM project_assignments
             UNION
             SELECT DISTINCT project FROM agents WHERE project <> ''
             ORDER BY 1",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    let mut projects = Vec::new();
    for row in rows {
        projects.push(row.map_err(sql_error)?);
    }
    Ok(projects)
}

fn read_team_manager(
    connection: &Connection,
    project: &str,
) -> Result<Option<OverviewManagerView>> {
    let row: Option<(Option<i64>, Option<i64>)> = connection
        .query_row(
            "SELECT manager, manager_generation FROM project_assignments WHERE project = ?1",
            params![project],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let (Some(manager), Some(generation)) = row.unwrap_or((None, None)) else {
        return Ok(None);
    };
    let state: Option<String> = connection
        .query_row(
            "SELECT state FROM agents WHERE id = ?1",
            params![manager],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .flatten();
    Ok(Some(OverviewManagerView {
        agent: format_agent_id(manager),
        generation,
        state: parse_enum::<AgentState>(state.as_deref().unwrap_or("unknown"), "Agent state")?,
    }))
}

fn list_team_workers(
    connection: &Connection,
    project: &str,
    manager_num: Option<i64>,
) -> Result<(Vec<OverviewWorkerView>, usize, bool)> {
    let predicate = project_match_sql("a");
    let total: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM agents a
                 WHERE a.observe_only = 0 AND {predicate}
                   AND a.state NOT IN ('stopped', 'failed', 'lost')
                   AND a.id != COALESCE(?2, -1)"
            ),
            params![project, manager_num],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let bound = MAX_PAGE_ENTRIES;
    let mut statement = connection
        .prepare(&format!(
            "SELECT a.id, a.generation, a.state, a.harness, a.parent, a.root, a.active_turn
             FROM agents a
             WHERE a.observe_only = 0 AND {predicate}
               AND a.state NOT IN ('stopped', 'failed', 'lost')
               AND a.id != COALESCE(?2, -1)
             ORDER BY a.id LIMIT ?3"
        ))
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![project, manager_num, bound as i64 + 1], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })
        .map_err(sql_error)?;
    let mut workers = Vec::new();
    for row in rows {
        let (id, generation, state, harness, parent, root, active_turn) = row.map_err(sql_error)?;
        workers.push(OverviewWorkerView {
            agent: format_agent_id(id),
            generation,
            state: parse_enum::<AgentState>(&state, "Agent state")?,
            harness,
            parent: parent.map(format_agent_id),
            root: format_agent_id(root),
            active_turn,
        });
    }
    let truncated = workers.len() > bound;
    workers.truncate(bound);
    Ok((
        workers,
        usize::try_from(total).unwrap_or(usize::MAX),
        truncated,
    ))
}

fn list_team_workspaces(
    connection: &Connection,
    project: &str,
) -> Result<(Vec<eliot_core::api::ManagedWorkspaceView>, usize, bool)> {
    use eliot_core::api::ManagedWorkspaceView;
    let predicate = project_match_sql("a");
    let total: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM workspaces w JOIN agents a ON a.id = w.agent
                 WHERE {predicate} AND w.phase <> 'removed'"
            ),
            params![project],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let bound = MAX_PAGE_ENTRIES;
    let mut statement = connection
        .prepare(&format!(
            "SELECT w.agent, w.project, w.repo_path, w.requested_branch, w.worktree_path,
                    w.phase, w.updated_at
             FROM workspaces w JOIN agents a ON a.id = w.agent
             WHERE {predicate} AND w.phase <> 'removed'
             ORDER BY w.agent LIMIT ?2"
        ))
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![project, bound as i64 + 1], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .map_err(sql_error)?;
    let mut workspaces = Vec::new();
    for row in rows {
        let (agent, project, repo_path, branch, worktree_path, phase, updated_at) =
            row.map_err(sql_error)?;
        workspaces.push(ManagedWorkspaceView {
            agent: format_agent_id(agent),
            project,
            repo_path,
            branch,
            worktree_path,
            phase: parse_enum::<WorkspacePhase>(&phase, "workspace phase")?,
            updated_at,
        });
    }
    let truncated = workspaces.len() > bound;
    workspaces.truncate(bound);
    Ok((
        workspaces,
        usize::try_from(total).unwrap_or(usize::MAX),
        truncated,
    ))
}

fn count_project_holders(connection: &Connection, project: &str) -> Result<usize> {
    let predicate = project_match_sql("a");
    let count: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM agents a
                 WHERE a.capacity_generation IS NOT NULL AND {predicate}"
            ),
            params![project],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_team_holders(connection: &Connection, project: &str, live: bool) -> Result<usize> {
    let predicate = project_match_sql("a");
    let states = if live {
        LIVE_AGENT_STATES
    } else {
        RETAINED_AGENT_STATES
    };
    let count: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM agents a
                 WHERE a.capacity_generation IS NOT NULL AND a.state IN {states} AND {predicate}"
            ),
            params![project],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_team_active_turns(connection: &Connection, project: &str) -> Result<usize> {
    let predicate = project_match_sql("a");
    let count: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM turns t JOIN agents a ON a.id = t.agent
                 WHERE t.state = 'running' AND {predicate}"
            ),
            params![project],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_team_queued(connection: &Connection, project: &str) -> Result<usize> {
    let predicate = project_match_sql("a");
    let count: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM commands c JOIN agents a ON a.id = c.target
                 WHERE c.state IN {QUEUED_COMMAND_STATES} AND {predicate}"
            ),
            params![project],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_global_holders(connection: &Connection) -> Result<usize> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE capacity_generation IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_global_retained(connection: &Connection) -> Result<usize> {
    let count: i64 = connection
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM agents
                 WHERE capacity_generation IS NOT NULL AND state IN {RETAINED_AGENT_STATES}"
            ),
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_global_active_turns(connection: &Connection) -> Result<usize> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE state = 'running'",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_global_queued(connection: &Connection) -> Result<usize> {
    let count: i64 = connection
        .query_row(
            &format!("SELECT COUNT(*) FROM commands WHERE state IN {QUEUED_COMMAND_STATES}"),
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn list_live_by_harness(connection: &Connection) -> Result<Vec<HarnessCount>> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT harness, COUNT(*) FROM agents
             WHERE capacity_generation IS NOT NULL AND state IN {LIVE_AGENT_STATES}
             GROUP BY harness ORDER BY harness"
        ))
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(sql_error)?;
    let mut counts = Vec::new();
    for row in rows {
        let (harness, total) = row.map_err(sql_error)?;
        counts.push(HarnessCount {
            harness,
            total: usize::try_from(total).unwrap_or(usize::MAX),
        });
    }
    Ok(counts)
}

fn count_native_children(connection: &Connection) -> Result<usize> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM native_child_registrations r
             JOIN agents c ON c.id = r.child
             WHERE c.observe_only = 1",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn list_native_children(connection: &Connection) -> Result<(Vec<NativeChildView>, bool)> {
    let bound = MAX_PAGE_ENTRIES;
    let mut statement = connection
        .prepare(
            "SELECT r.child, r.producer, r.producer_generation,
                    COALESCE(c.project_scope, c.project), c.state
             FROM native_child_registrations r
             JOIN agents c ON c.id = r.child
             WHERE c.observe_only = 1
             ORDER BY r.child LIMIT ?1",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![bound as i64 + 1], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(sql_error)?;
    let mut children = Vec::new();
    for row in rows {
        let (child, producer, producer_generation, project, state) = row.map_err(sql_error)?;
        children.push(NativeChildView {
            child: format_agent_id(child),
            producer: format_agent_id(producer),
            producer_generation,
            project,
            state: parse_enum::<AgentState>(&state, "Agent state")?,
        });
    }
    let truncated = children.len() > bound;
    children.truncate(bound);
    Ok((children, truncated))
}
