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
    const UNATTRIBUTED: &str = "SELECT t.insertion_seq, t.state, t.usage, a.harness, NULL
        FROM agents a CROSS JOIN turns t INDEXED BY turns_unattributed_usage ON t.agent = a.id
        WHERE a.project = ?1 AND a.observe_only = 0 AND t.assignment_usage IS NULL
          AND t.insertion_seq > ?2
        ORDER BY t.insertion_seq LIMIT ?3";
    let after = request.after.unwrap_or_default();
    let limit = MAX_ASSIGNMENT_USAGE_PAGE as i64 + 1;
    let sql = if request.epoch.is_some() {
        EPOCH
    } else {
        UNATTRIBUTED
    };
    let mut statement = conn.prepare_cached(sql).map_err(sql_error)?;
    let mut rows = match request.epoch {
        Some(epoch) => statement.query(params![request.project, epoch, after, limit]),
        None => statement.query(params![request.project, after, limit]),
    }
    .map_err(sql_error)?;
    let mut turns = TurnExpenditureAccum::default();
    let mut manager = None;
    let mut last = None;
    let mut next_after = None;
    for _ in 0..MAX_ASSIGNMENT_USAGE_PAGE {
        let Some(row) = rows.next().map_err(sql_error)? else {
            return Ok((turns.finish(), manager, None));
        };
        last = Some(row.get(0).map_err(sql_error)?);
        let state: String = row.get(1).map_err(sql_error)?;
        let usage: Option<String> = row.get(2).map_err(sql_error)?;
        let harness: String = row.get(3).map_err(sql_error)?;
        let identity: Option<String> = row.get(4).map_err(sql_error)?;
        if let Some(identity) = identity {
            let identity = serde_json::from_str(&identity)
                .map_err(|_| Error::new("source_integrity", "invalid retained usage assignment"))?;
            if manager.as_ref().is_some_and(|stored| stored != &identity) {
                return Err(Error::new(
                    "source_integrity",
                    "assignment epoch has conflicting managers",
                ));
            }
            manager = Some(identity);
        }
        turns.observe(&state, usage.as_deref(), &harness)?;
    }
    if rows.next().map_err(sql_error)?.is_some() {
        next_after = last;
    }
    Ok((turns.finish(), manager, next_after))
}
