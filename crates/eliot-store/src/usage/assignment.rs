//! Turn usage by the assignment that owned a command at dispatch.

use super::*;
use eliot_core::api::{
    MAX_ASSIGNMENT_USAGE_PAGE, ProjectAssignmentUsageRequest, ProjectAssignmentUsageResponse,
    ProjectManagerIdentity,
};

pub(crate) fn send_assignment_usage(conn: &Connection, agent: &Agent) -> Result<Option<String>> {
    if agent.project.is_empty() || agent.chat || agent.observe_only {
        return Ok(None);
    }
    // The target's retained root is the team subject even for an Owner SEND.
    // Match the manager incarnation recorded by the current assignment.
    conn.query_row(
        "SELECT json_object('project', p.project, 'epoch', p.epoch, 'manager',
                            json_object('kind', 'local', 'agent', 'a' || p.manager,
                                        'generation', p.manager_generation))
         FROM project_assignments p
         JOIN project_registrations r ON r.id = p.project
         JOIN agents m ON m.id = p.manager AND m.generation = p.manager_generation
                       AND m.project = p.project
         WHERE p.project = ?1 AND p.manager = ?2",
        params![agent.project, parse_agent_id(&agent.root)?],
        |row| row.get(0),
    )
    .optional()
    .map_err(sql_error)
}

impl Store {
    pub fn read_project_assignment_usage(
        &self,
        request: &ProjectAssignmentUsageRequest,
    ) -> Result<ProjectAssignmentUsageResponse> {
        request.validate()?;
        self.ensure_committed_reads()?;
        let tx = self.connection.unchecked_transaction().map_err(sql_error)?;
        let (turns, manager, next_after) = assignment_usage_page(&tx, request)?;
        let pruned_through = retention::coverage_pruned_through(&tx)?;
        tx.commit().map_err(sql_error)?;
        Ok(ProjectAssignmentUsageResponse {
            project: request.project.clone(),
            epoch: request.epoch,
            manager,
            turns,
            pruned_through,
            next_after,
        })
    }
}

/// One usage page row: insertion_seq, state, usage, harness, manager identity.
type UsagePageRow = (i64, String, Option<String>, String, Option<String>);

fn assignment_usage_page(
    conn: &Connection,
    request: &ProjectAssignmentUsageRequest,
) -> Result<(TurnExpenditure, Option<ProjectManagerIdentity>, Option<i64>)> {
    const EPOCH: &str = "SELECT t.insertion_seq, t.state, t.usage, a.harness,
                               json_extract(t.assignment_usage, '$.manager')
        FROM turns t INDEXED BY turns_assignment_usage
        CROSS JOIN agents a ON a.id = t.agent
        WHERE t.assignment_usage IS NOT NULL
          AND json_extract(t.assignment_usage, '$.project') = ?1
          AND json_extract(t.assignment_usage, '$.epoch') = ?2 AND t.insertion_seq > ?3
        ORDER BY t.insertion_seq LIMIT ?4";
    let after = request.after.unwrap_or_default();
    let limit = MAX_ASSIGNMENT_USAGE_PAGE as i64 + 1;
    let page: Vec<UsagePageRow> = match request.epoch {
        Some(epoch) => {
            let mut statement = conn.prepare_cached(EPOCH).map_err(sql_error)?;
            statement
                .query_map(params![request.project, epoch, after, limit], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })
                .map_err(sql_error)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_error)?
        }
        None => unattributed_usage_page(conn, &request.project, after, limit)?,
    };
    let mut turns = TurnExpenditureAccum::default();
    let mut manager = None;
    let mut last = None;
    for (seq, state, usage, harness, identity) in page.iter().take(MAX_ASSIGNMENT_USAGE_PAGE) {
        last = Some(*seq);
        if let Some(identity) = identity {
            let identity: ProjectManagerIdentity = serde_json::from_str(identity)
                .map_err(|_| Error::new("source_integrity", "invalid retained usage assignment"))?;
            if manager.as_ref().is_some_and(|stored| stored != &identity) {
                return Err(Error::new(
                    "source_integrity",
                    "assignment epoch has conflicting managers",
                ));
            }
            manager = Some(identity);
        }
        turns.observe(state, usage.as_deref(), harness)?;
    }
    let next_after = (page.len() > MAX_ASSIGNMENT_USAGE_PAGE)
        .then_some(last)
        .flatten();
    Ok((turns.finish(), manager, next_after))
}

/// Unattributed turns in global insertion order without sorting the whole
/// project per page. Each project agent probes its own
/// `turns_unattributed_usage(agent, insertion_seq)` range past the cursor, so
/// every probe is an index-ordered seek; the page-sized runs merge here.
fn unattributed_usage_page(
    conn: &Connection,
    project: &str,
    after: i64,
    limit: i64,
) -> Result<Vec<UsagePageRow>> {
    const AGENTS: &str = "SELECT id, harness FROM agents
        WHERE project = ?1 AND observe_only = 0 ORDER BY id";
    const TURNS: &str = "SELECT insertion_seq, state, usage FROM turns
        INDEXED BY turns_unattributed_usage
        WHERE agent = ?1 AND assignment_usage IS NULL AND insertion_seq > ?2
        ORDER BY insertion_seq LIMIT ?3";
    const TURNS_BOUNDED: &str = "SELECT insertion_seq, state, usage FROM turns
        INDEXED BY turns_unattributed_usage
        WHERE agent = ?1 AND assignment_usage IS NULL
          AND insertion_seq > ?2 AND insertion_seq < ?3
        ORDER BY insertion_seq LIMIT ?4";
    let mut agents = conn
        .prepare_cached(AGENTS)
        .map_err(sql_error)?
        .query_map(params![project], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    agents.sort();
    agents.dedup();
    // (insertion_seq, agent) keeps the merge order deterministic; the
    // sequence itself is globally unique. Once a full page is held, later
    // probes only need rows below the page ceiling, so each stays a small
    // index range seek instead of re-reading the agent's whole tail.
    let page_len = limit.max(0) as usize;
    let mut merged: Vec<(i64, i64, String, Option<String>, String)> = Vec::new();
    for (agent, harness) in agents {
        let decode = |row: &rusqlite::Row<'_>| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        };
        let rows = if merged.len() < page_len {
            conn.prepare_cached(TURNS)
                .map_err(sql_error)?
                .query_map(params![agent, after, limit], decode)
                .map_err(sql_error)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_error)?
        } else {
            let ceiling = merged.iter().map(|row| row.0).max().unwrap_or(after);
            conn.prepare_cached(TURNS_BOUNDED)
                .map_err(sql_error)?
                .query_map(params![agent, after, ceiling, limit], decode)
                .map_err(sql_error)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_error)?
        };
        merged.extend(
            rows.into_iter()
                .map(|(seq, state, usage)| (seq, agent, state, usage, harness.clone())),
        );
        if merged.len() > page_len {
            merged.sort_by_key(|a| (a.0, a.1));
            merged.truncate(page_len);
        }
    }
    merged.sort_by_key(|a| (a.0, a.1));
    merged.truncate(page_len);
    Ok(merged
        .into_iter()
        .map(|(seq, _, state, usage, harness)| (seq, state, usage, harness, None))
        .collect())
}
