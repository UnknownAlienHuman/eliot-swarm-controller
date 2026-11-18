//! Committed fleet, assignment and observation queries for project overview.
mod page;
#[cfg(test)]
mod tests;

use super::*;
use eliot_core::api::{
    FleetTotals, HarnessCount, LifecycleTotals, NativeChildView, OverviewCoverage,
    OverviewManagerView, OverviewWorkerView, ProjectAssignment, ProjectAssignmentState,
    ProjectManagerIdentity, ProjectOverviewEntry, ProjectTotals, ScopeLimits, ScopeUsage,
};

const PROJECTS: &str = "SELECT id AS project FROM project_registrations
     UNION SELECT project FROM project_assignments
     UNION SELECT project FROM agents WHERE project <> ''";

const WORKER: &str = "a.observe_only = 0 AND (a.capacity_generation IS NOT NULL
                              OR a.state NOT IN ('stopped','failed','lost'))";
const ASSIGNED_MANAGER: &str = "a.id = p.manager AND a.project = p.project AND
    (a.generation = p.manager_generation
        OR (a.generation = 0 AND p.manager_generation = 1 AND p.start_command IS NOT NULL))";
const LIVE: &str = "a.capacity_generation = a.generation AND a.state IN ('idle','busy','waiting')";
const UNKNOWN: &str = "a.capacity_generation IS NOT NULL AND (
    (a.capacity_generation = a.generation AND a.state IN ('unknown','lost','stopped','failed'))
    OR EXISTS (SELECT 1 FROM commands c
               WHERE c.target = a.id AND c.verb IN ('launch','resume') AND c.state = 'unknown'
                 AND (c.generation = a.capacity_generation
                      OR (c.generation = a.capacity_generation - 1
                          AND json_extract(c.outcome, '$.native_dispatched') = 0))))";

enum DetailAccess {
    Owner,
    Manager {
        agent: i64,
        project: String,
        control: core::AgentControl,
        project_scope: String,
    },
    Aggregate,
}

impl DetailAccess {
    fn resolve(conn: &Connection, principal: &str) -> Result<Self> {
        let hierarchy::Caller::Agent {
            num,
            id,
            control,
            project: project_scope,
        } = hierarchy::parse_caller(conn, principal)?
        else {
            return Ok(Self::Owner);
        };
        let caller = load_agent_by_num(conn, num)?
            .ok_or_else(|| Error::new("stale_generation", "overview caller disappeared"))?;
        if caller.project.is_empty()
            || caller.root != id
            || projects::project_registration_tx(conn, &caller.project)?.is_none()
        {
            return Ok(Self::Aggregate);
        }
        let assignment = project_assignment::read_assignment(conn, &caller.project)?;
        if assignment.state != ProjectAssignmentState::Active
            || assignment.manager
                != Some(ProjectManagerIdentity::Local {
                    agent: id,
                    generation: caller.generation,
                })
        {
            return Ok(Self::Aggregate);
        }
        Ok(Self::Manager {
            agent: num,
            project: caller.project,
            control,
            project_scope,
        })
    }

    fn can_detail(&self, project: &str) -> bool {
        matches!(self, Self::Owner)
            || matches!(self, Self::Manager { project: own, .. } if own == project)
    }

    // A manager's team is rooted at that manager. Apply the frozen Agent read
    // scope within that team, including to any native child's producer.
    fn members(&self) -> (Option<i64>, Option<&str>, bool) {
        match self {
            Self::Manager {
                agent,
                control,
                project_scope,
                ..
            } => (
                Some(*agent),
                (*control == core::AgentControl::Project).then_some(project_scope.as_str()),
                *control == core::AgentControl::Slf,
            ),
            _ => (None, None, false),
        }
    }
}

pub(super) fn queue_detail_access(conn: &Connection, principal: &str) -> Result<Option<String>> {
    Ok(match DetailAccess::resolve(conn, principal)? {
        DetailAccess::Manager { project, .. } => Some(project),
        _ => None,
    })
}

fn member_sql(alias: &str, root: u8, scope: u8, slf: u8) -> String {
    format!(
        "(?{root} IS NULL OR ({alias}.root = ?{root}
        AND (?{scope} IS NULL OR COALESCE({alias}.project_scope,{alias}.project) = ?{scope})
        AND (NOT ?{slf} OR {alias}.id = ?{root})
        AND (?{scope} IS NOT NULL OR {alias}.depth < {})))",
        hierarchy::ANCESTOR_WALK_CAP
    )
}

fn count(conn: &Connection, sql: &str, values: impl rusqlite::Params) -> Result<usize> {
    conn.query_row(sql, values, |row| row.get::<_, i64>(0))
        .map(|count| count as usize)
        .map_err(sql_error)
}

fn read_fleet(conn: &Connection, limits: &ScopeLimits) -> Result<(FleetTotals, OverviewCoverage)> {
    let lifecycle = lifecycle(conn, None)?;
    let global = core::CapacityScope::global_scope();
    let usage = capacity::capacity_scope_usage(conn, &global)?;
    // The daemon knows its retained registrations exactly. A controlled
    // generation with outstanding custody can have native work outside the
    // registered observations; it has no complete enumeration proof.
    let outstanding_observation = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM agents
         WHERE observe_only = 0 AND generation > 0 AND capacity_generation = generation)",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sql_error)?;
    let coverage = if outstanding_observation {
        OverviewCoverage::Partial
    } else {
        OverviewCoverage::Complete
    };
    let fleet = FleetTotals {
        live_harnesses: lifecycle.live,
        live_by_harness: list_live_by_harness(conn)?,
        active_turns: lifecycle.active,
        queued_work: lifecycle.queued,
        retained_unknown: lifecycle.retained_unknown,
        manager_total: count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM project_assignments p JOIN agents a ON {ASSIGNED_MANAGER}"
            ),
            [],
        )?,
        worker_total: count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM agents a WHERE {WORKER}
             AND NOT EXISTS (SELECT 1 FROM project_assignments p WHERE {ASSIGNED_MANAGER})"
            ),
            [],
        )?,
        lifecycle,
        native_children: Vec::new(),
        native_child_total: count_native_children(conn, None)?,
        native_children_truncated: false,
        capacity: ScopeUsage {
            scope: global,
            usage,
            limit: limits.global,
        },
        projects: count(conn, &format!("SELECT COUNT(*) FROM ({PROJECTS})"), [])?,
    };
    Ok((fleet, coverage))
}

pub(super) fn list_overview_projects(
    conn: &Connection,
    filter: Option<&str>,
    after: Option<&str>,
) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT project FROM ({PROJECTS})
         WHERE (?1 IS NULL OR project = ?1) AND (?2 IS NULL OR project > ?2)
         ORDER BY project LIMIT ?3"
        ))
        .map_err(sql_error)?;
    let rows = stmt
        .query_map(params![filter, after, MAX_PAGE_ENTRIES as i64 + 1], |row| {
            row.get(0)
        })
        .map_err(sql_error)?;
    rows.collect::<rusqlite::Result<_>>().map_err(sql_error)
}

/// The retained manager view for one project assignment, shared by the
/// project overview and the organization projection: a local manager in a
/// matching (or starting) generation reads its live state, anything else
/// reads unknown without changing the assignment.
pub(super) fn overview_manager_view(
    conn: &Connection,
    assignment: Option<&ProjectAssignment>,
) -> Result<Option<OverviewManagerView>> {
    assignment
        .and_then(|assignment| match assignment.manager.as_ref() {
            Some(ProjectManagerIdentity::Local { agent, generation }) => {
                Some((assignment, agent, *generation))
            }
            _ => None,
        })
        .map(|(assignment, agent, generation)| {
            let state = load_agent_by_num(conn, parse_agent_id(agent)?)?
                .filter(|row| {
                    row.generation == generation
                        || (row.generation == 0
                            && generation == 1
                            && assignment.state == ProjectAssignmentState::Starting)
                })
                .map_or(AgentState::Unknown, |row| row.state);
            // The managed worktree's standard branch, by its unique path.
            let branch = match assignment.workspace.as_deref() {
                Some(path) => conn
                    .query_row(
                        "SELECT requested_branch FROM workspaces
                         WHERE worktree_path = ?1 AND phase <> 'removed'",
                        [path],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(sql_error)?,
                None => None,
            };
            Ok::<_, Error>(OverviewManagerView {
                agent: agent.clone(),
                generation,
                state,
                assignment_revision: assignment.revision,
                workspace: assignment.workspace.clone(),
                branch,
            })
        })
        .transpose()
}

fn project_team(
    conn: &Connection,
    project: &str,
    limits: &ScopeLimits,
    access: &DetailAccess,
    worker_after: Option<i64>,
    workspace_after: Option<i64>,
) -> Result<ProjectOverviewEntry> {
    let detail = access.can_detail(project);
    let assignment = detail
        .then(|| project_assignment::read_assignment(conn, project))
        .transpose()?;
    let manager = overview_manager_view(conn, assignment.as_ref())?;

    let worker_total = count(
        conn,
        &format!(
            "SELECT COUNT(*) FROM agents a WHERE a.project = ?1 AND {WORKER}
         AND NOT EXISTS (SELECT 1 FROM project_assignments p WHERE {ASSIGNED_MANAGER})"
        ),
        [project],
    )?;
    let workspace_total = count(
        conn,
        "SELECT COUNT(*) FROM workspaces w JOIN agents a ON a.id = w.agent
         WHERE a.project = ?1 AND w.phase <> 'removed'",
        [project],
    )?;
    let (workers, workers_truncated) = match worker_after.filter(|_| detail) {
        Some(after) => list_team_workers(conn, project, access, after)?,
        None => (Vec::new(), false),
    };
    let (workspaces, workspaces_truncated) = match workspace_after.filter(|_| detail) {
        Some(after) => list_team_workspaces(conn, project, access, after)?,
        None => (Vec::new(), false),
    };
    let project_limits = limits.projects.get(project).cloned().unwrap_or_default();
    let manager_scope = core::CapacityScope::manager_assignment_scope(project);
    let manager_limit = launch_queue::effective_manager_limit(conn, project, &project_limits)?;
    let (totals, project_usage) = project_header_totals(conn, project, limits)?;
    Ok(ProjectOverviewEntry {
        project: project.to_owned(),
        detail,
        manager,
        assignment,
        native_child_total: count_native_children(conn, Some(project))?,
        workers,
        worker_total,
        workers_truncated,
        workspaces,
        workspace_total,
        workspaces_truncated,
        queue: totals.queued,
        totals,
        capacity: vec![
            project_usage,
            ScopeUsage {
                usage: capacity::capacity_scope_usage(conn, &manager_scope)?,
                scope: manager_scope,
                limit: manager_limit,
            },
        ],
    })
}

/// One project's exact team totals and its project slot use beside the
/// Owner limit: the header numbers `project.overview` and the organization
/// projection both report, without the fleet totals or team detail.
pub(super) fn project_header_totals(
    conn: &Connection,
    project: &str,
    limits: &ScopeLimits,
) -> Result<(ProjectTotals, ScopeUsage)> {
    let lifecycle = lifecycle(conn, Some(project))?;
    let scope = core::CapacityScope::project_scope(project);
    let usage = ScopeUsage {
        usage: capacity::capacity_scope_usage(conn, &scope)?,
        scope,
        limit: limits
            .projects
            .get(project)
            .and_then(|policy| policy.project),
    };
    Ok((
        ProjectTotals {
            live: lifecycle.live,
            active_turns: lifecycle.active,
            queued: lifecycle.queued,
            retained_unknown: lifecycle.retained_unknown,
            lifecycle,
        },
        usage,
    ))
}

fn list_team_workers(
    conn: &Connection,
    project: &str,
    access: &DetailAccess,
    after: i64,
) -> Result<(Vec<OverviewWorkerView>, bool)> {
    let (root, scope, slf) = access.members();
    let member = member_sql("a", 3, 4, 5);
    let mut stmt = conn.prepare(&format!(
        "SELECT a.id, a.generation, a.state, a.harness, a.parent, COALESCE(a.root,a.id), a.active_turn
         FROM agents a WHERE a.project = ?1 AND a.id > ?2 AND {WORKER} AND {member}
           AND NOT EXISTS (SELECT 1 FROM project_assignments p WHERE {ASSIGNED_MANAGER})
         ORDER BY a.id LIMIT ?6"
    )).map_err(sql_error)?;
    let rows = stmt
        .query_map(
            params![
                project,
                after,
                root,
                scope,
                slf,
                MAX_PAGE_ENTRIES as i64 + 1
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get(1)?,
                    row.get::<_, String>(2)?,
                    row.get(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let mut workers = Vec::new();
    for row in rows {
        let (id, generation, state, harness, parent, root, active_turn) = row.map_err(sql_error)?;
        workers.push(OverviewWorkerView {
            agent: format_agent_id(id),
            generation,
            state: parse_enum(&state, "Agent state")?,
            harness,
            parent: parent.map(format_agent_id),
            root: format_agent_id(root),
            active_turn,
        });
    }
    Ok(bounded_detail(workers))
}

fn list_team_workspaces(
    conn: &Connection,
    project: &str,
    access: &DetailAccess,
    after: i64,
) -> Result<(Vec<core::api::ManagedWorkspaceView>, bool)> {
    let (root, scope, slf) = access.members();
    let member = member_sql("a", 3, 4, 5);
    let mut stmt = conn.prepare(&format!(
        "SELECT w.agent, w.project, w.repo_path, w.requested_branch, w.worktree_path, w.phase, w.updated_at
         FROM workspaces w JOIN agents a ON a.id = w.agent
         WHERE a.project = ?1 AND w.agent > ?2 AND w.phase <> 'removed' AND {member}
         ORDER BY w.agent LIMIT ?6"
    )).map_err(sql_error)?;
    let rows = stmt
        .query_map(
            params![
                project,
                after,
                root,
                scope,
                slf,
                MAX_PAGE_ENTRIES as i64 + 1
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get::<_, String>(5)?,
                    row.get(6)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let mut workspaces = Vec::new();
    for row in rows {
        let (id, project, repo_path, branch, worktree_path, phase, updated_at) =
            row.map_err(sql_error)?;
        workspaces.push(core::api::ManagedWorkspaceView {
            agent: format_agent_id(id),
            project,
            repo_path,
            branch,
            worktree_path,
            phase: parse_enum(&phase, "workspace phase")?,
            updated_at,
        });
    }
    Ok(bounded_detail(workspaces))
}

fn bounded_detail<T>(mut rows: Vec<T>) -> (Vec<T>, bool) {
    let more = rows.len() > MAX_PAGE_ENTRIES;
    rows.truncate(MAX_PAGE_ENTRIES);
    (rows, more)
}

fn lifecycle(conn: &Connection, project: Option<&str>) -> Result<LifecycleTotals> {
    let (live, idle, busy, waiting, starting, stopping, retained_unknown) = conn.query_row(&format!(
        "SELECT
            COUNT(*) FILTER (WHERE {LIVE}),
            COUNT(*) FILTER (WHERE {LIVE} AND a.state = 'idle'),
            COUNT(*) FILTER (WHERE {LIVE} AND a.state = 'busy'),
            COUNT(*) FILTER (WHERE {LIVE} AND a.state = 'waiting'),
            COUNT(*) FILTER (WHERE a.capacity_generation = a.generation AND a.state = 'starting'
                AND EXISTS (SELECT 1 FROM commands c WHERE c.target = a.id
                    AND c.generation = a.generation AND c.verb IN ('launch','resume')
                    AND c.state = 'dispatched')),
            COUNT(*) FILTER (WHERE a.capacity_generation = a.generation
                AND EXISTS (SELECT 1 FROM commands c WHERE c.target = a.id
                    AND c.generation = a.generation AND c.verb = 'stop' AND c.state = 'dispatched')),
            COUNT(*) FILTER (WHERE {UNKNOWN})
         FROM agents a WHERE a.observe_only = 0 AND (?1 IS NULL OR a.project = ?1)"
    ), [project], |row| Ok((
        row.get::<_, i64>(0)? as usize, row.get::<_, i64>(1)? as usize,
        row.get::<_, i64>(2)? as usize, row.get::<_, i64>(3)? as usize,
        row.get::<_, i64>(4)? as usize, row.get::<_, i64>(5)? as usize,
        row.get::<_, i64>(6)? as usize,
    ))).map_err(sql_error)?;
    let active = count(conn,
        "SELECT COUNT(*) FROM turns t JOIN agents a ON a.id = t.agent AND a.generation = t.generation
         WHERE t.state = 'running' AND a.observe_only = 0 AND (?1 IS NULL OR a.project = ?1)", [project])?;
    let queued = count(
        conn,
        "SELECT COUNT(*) FROM commands c LEFT JOIN agents a ON a.id = c.target
         WHERE c.state IN ('accepted','queued') AND (?1 IS NULL OR a.project = ?1)",
        [project],
    )?;
    Ok(LifecycleTotals {
        live,
        idle,
        busy,
        waiting,
        starting,
        stopping,
        active,
        queued: queued + launch_queue::queue_usage(conn, project)?,
        retained_unknown,
    })
}

fn list_live_by_harness(conn: &Connection) -> Result<Vec<HarnessCount>> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT harness, COUNT(*) FROM agents a WHERE a.observe_only = 0 AND {LIVE}
         GROUP BY harness ORDER BY harness"
        ))
        .map_err(sql_error)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(HarnessCount {
                harness: row.get(0)?,
                total: row.get::<_, i64>(1)? as usize,
            })
        })
        .map_err(sql_error)?;
    rows.collect::<rusqlite::Result<_>>().map_err(sql_error)
}

fn count_native_children(conn: &Connection, project: Option<&str>) -> Result<usize> {
    count(conn,
        "SELECT COUNT(DISTINCT r.child) FROM native_child_registrations r JOIN agents c ON c.id = r.child
         WHERE c.observe_only = 1 AND (?1 IS NULL OR c.project = ?1)", [project])
}

fn list_native_children(
    conn: &Connection,
    filter: Option<&str>,
    access: &DetailAccess,
    after: i64,
) -> Result<(Vec<NativeChildView>, bool)> {
    let project = match access {
        DetailAccess::Owner => filter,
        DetailAccess::Manager {
            project, control, ..
        } if *control != core::AgentControl::Slf
            && filter.is_none_or(|filter| filter == project) =>
        {
            Some(project.as_str())
        }
        _ => return Ok((Vec::new(), false)),
    };
    let (root, scope, slf) = access.members();
    let child = member_sql("c", 3, 4, 5);
    let producer = member_sql("p", 3, 4, 5);
    let mut stmt = conn
        .prepare(&format!(
            "WITH matched AS (
             SELECT r.child, r.producer, r.producer_generation, NULLIF(c.project, '') AS project,
                    c.state, ROW_NUMBER() OVER (
                        PARTITION BY r.child ORDER BY r.producer_generation DESC, r.producer
                    ) AS occurrence
             FROM native_child_registrations r JOIN agents c ON c.id = r.child
             JOIN agents p ON p.id = r.producer
             WHERE c.observe_only = 1 AND r.child > ?2
               AND (?1 IS NULL OR (c.project = ?1 AND p.project = ?1))
               AND {child} AND {producer}
         )
         SELECT child, producer, producer_generation, project, state FROM matched
         WHERE occurrence = 1 ORDER BY child LIMIT ?6"
        ))
        .map_err(sql_error)?;
    let rows = stmt
        .query_map(
            params![
                project,
                after,
                root,
                scope,
                slf,
                MAX_PAGE_ENTRIES as i64 + 1
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let mut views = Vec::new();
    for row in rows {
        let (child, producer, producer_generation, project, state) = row.map_err(sql_error)?;
        views.push(NativeChildView {
            child: format_agent_id(child),
            producer: format_agent_id(producer),
            producer_generation,
            project,
            state: parse_enum(&state, "Agent state")?,
        });
    }
    Ok(bounded_detail(views))
}
