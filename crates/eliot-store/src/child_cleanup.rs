use super::*;
use eliot_core::api::{MailLimits, ReparentedAgent};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildCleanupState {
    Pending,
    Kept,
    Removed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildCleanupOutcome {
    Kept,
    Removed,
}

impl ChildCleanupOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Kept => "kept",
            Self::Removed => "removed",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildCleanupJob {
    pub child: String,
    pub child_generation: i64,
    pub parent: String,
    pub parent_generation: i64,
    pub not_before: i64,
    pub state: ChildCleanupState,
    pub reason: String,
    pub detail: Option<String>,
    pub plan_id: Option<String>,
    pub apply_id: Option<String>,
    pub notice_id: Option<String>,
    pub notice_delivered: bool,
}

macro_rules! child_cleanup_columns {
    () => {
        "SELECT 'a' || child, child_generation, 'a' || parent,
    parent_generation, not_before, state, reason, detail,
    plan_id, apply_id, notice_id, notice_delivered"
    };
}

const CHILD_CLEANUP_COLUMNS: &str = child_cleanup_columns!();
const CHILD_CLEANUP_SELECT: &str = concat!(child_cleanup_columns!(), " FROM child_cleanup");

/// Admit retirement from the applied manager Stop that targeted a live child.
/// The native stream can record session.ended before the command settles.
pub(super) fn record_manager_stop_tx(
    tx: &Transaction<'_>,
    child: i64,
    stop: &CommandRecord,
    now: i64,
) -> Result<()> {
    if !stop.command.principal.starts_with("agent:")
        || stop.expected_turn_id.is_some()
        || stop.command.args["force"].as_bool() == Some(true)
    {
        return Ok(());
    }
    db!(tx.execute(
        "INSERT OR IGNORE INTO child_cleanup
            (child, child_generation, parent, parent_generation, stop_id, not_before, state, reason)
         SELECT ch.id, ch.generation, p.id, p.generation, ?3, ?5, 'pending', 'manager_stop'
         FROM agents ch JOIN agents p ON p.id = ch.parent
         JOIN workspaces w ON w.agent = ch.id AND w.target_generation = ch.generation
         WHERE ch.id = ?1 AND ch.generation = ?2 AND ch.state = 'stopped'
           AND ch.origin = 'eliot' AND w.phase <> 'removed'
           AND EXISTS (SELECT 1 FROM turns t WHERE t.agent = ch.id
                       AND t.generation = ch.generation AND t.state = 'completed')
           AND EXISTS (SELECT 1 FROM events e WHERE e.agent = ch.id
                       AND e.generation = ch.generation AND e.kind = 'command.dispatched'
                       AND json_extract(e.data, '$.command_id') = ?3
                       AND json_extract(e.data, '$.live_at_dispatch') = 1)
           AND ?4 = ('agent:a' || p.id || ':' || p.generation)",
        params![
            child,
            stop.command.generation,
            stop.command.id,
            stop.command.principal,
            now.saturating_add(workspace::WORKSPACE_REMOVAL_QUIET_MS),
        ],
    ));
    Ok(())
}

/// Recheck the stopped child, the manager binding and the live artifact in the
/// same transaction that reserves the automatic Git removal.
pub(super) fn require_automatic_removal_tx(
    tx: &Transaction<'_>,
    child: &Agent,
    workspace: &WorkspacePreparation,
    command_id: &str,
    now: i64,
) -> Result<()> {
    let child_num = parse_agent_id(&child.id)?;
    let job: Option<(i64, i64, Option<String>, String)> = tx
        .query_row(
            "SELECT parent, parent_generation, apply_id, state FROM child_cleanup
             WHERE child = ?1 AND child_generation = ?2",
            params![child_num, child.generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(sql_error)?;
    if job.is_none() && child.parent.is_none() {
        return Ok(());
    }
    let Some((parent, parent_generation, Some(apply_id), state)) = job else {
        return Err(Error::new(
            "manager_stop_required",
            "automatic removal requires a direct manager's settled completion Stop",
        ));
    };
    let current_parent = load_agent_by_num(tx, parent)?;
    if state != "pending"
        || child.state != AgentState::Stopped
        || current_parent.as_ref().is_none_or(|manager| {
            child.parent.as_deref() != Some(manager.id.as_str())
                || manager.generation != parent_generation
        })
    {
        return Err(Error::new(
            "stale_generation",
            "the stopped child or its manager binding changed before automatic removal",
        ));
    }
    if child.archived || current_parent.is_some_and(|manager| manager.archived) {
        return Err(Error::new(
            "owner_hold",
            "the child or its manager is archived",
        ));
    }
    let another_stopper: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM commands
             WHERE target = ?1 AND generation = ?2 AND verb = 'stop'
               AND principal <> ?3 AND state IN ('dispatched', 'applied', 'unknown'))",
            params![
                child_num,
                child.generation,
                format!("agent:a{parent}:{parent_generation}")
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if another_stopper {
        return Err(Error::new(
            "owner_hold",
            "another principal's Stop is recorded for the child generation",
        ));
    }
    let artifact: Option<(String, String, Option<i64>)> = tx
        .query_row(
            "SELECT id, json_extract(body, '$.state'), json_extract(body, '$.expires_at')
             FROM artifacts WHERE path = ?1 AND agent = ?2 AND generation = ?3
               AND json_extract(body, '$.kind') = 'worktree'",
            params![workspace.worktree_path, child_num, child.generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((artifact_id, state, expires_at)) = artifact else {
        return Err(Error::new(
            "identity_changed",
            "the owned worktree artifact changed",
        ));
    };
    if state != "registered" {
        return Err(Error::new(
            state,
            "the worktree artifact is no longer registered",
        ));
    }
    if expires_at.is_some_and(|expires| expires > now) {
        return Err(Error::new(
            "not_expired",
            "the worktree is held until its recorded expiry",
        ));
    }
    if cleanup_worktree_command_id(&apply_id, &artifact_id)? != command_id {
        return Err(Error::new(
            "manager_stop_required",
            "automatic removal is outside the manager completion cleanup",
        ));
    }
    Ok(())
}

fn cleanup_job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChildCleanupJob> {
    let state: String = row.get(5)?;
    Ok(ChildCleanupJob {
        child: row.get(0)?,
        child_generation: row.get(1)?,
        parent: row.get(2)?,
        parent_generation: row.get(3)?,
        not_before: row.get(4)?,
        state: match state.as_str() {
            "pending" => ChildCleanupState::Pending,
            "kept" => ChildCleanupState::Kept,
            _ => ChildCleanupState::Removed,
        },
        reason: row.get(6)?,
        detail: row.get(7)?,
        plan_id: row.get(8)?,
        apply_id: row.get(9)?,
        notice_id: row.get(10)?,
        notice_delivered: row.get::<_, i64>(11)? != 0,
    })
}

/// Rebind pending child cleanup work for the exact direct workers moved
/// between project managers. The original stop_id remains its provenance.
pub(super) fn follow_project_transfer(
    tx: &Transaction<'_>,
    from: &Agent,
    to: &Agent,
    moved_agents: &[ReparentedAgent],
    limits: MailLimits,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if from.id == to.id {
        return Err(Error::new(
            "invalid",
            "project transfer managers must differ",
        ));
    }
    let source = parse_agent_id(&from.id)?;
    let destination = parse_agent_id(&to.id)?;
    for moved in moved_agents.iter().filter(|moved| {
        moved.previous_parent.as_deref() == Some(from.id.as_str())
            && moved.parent.as_deref() == Some(to.id.as_str())
    }) {
        let child = parse_agent_id(&moved.agent)?;
        let binding: Option<(ChildCleanupJob, String)> = tx
            .query_row(
                &format!("{CHILD_CLEANUP_COLUMNS}, stop_id FROM child_cleanup WHERE child=?1 AND child_generation=?2"),
                params![child, moved.generation],
                |row| Ok((cleanup_job_from_row(row)?, row.get(12)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((job, stop_id)) = binding else {
            continue;
        };
        if job.state == ChildCleanupState::Removed {
            continue;
        }
        if job.parent != from.id {
            return Err(cleanup_transfer_blocked(
                &job,
                "is bound to another manager",
            ));
        }
        let stop_is_original_applied_manager_stop: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM commands c
                     WHERE c.id = ?1 AND c.target = ?2 AND c.generation = ?3 AND c.verb = 'stop'
                       AND principal LIKE 'agent:%' AND state = 'applied'
                       AND EXISTS (SELECT 1 FROM events e WHERE e.agent = c.target
                         AND e.generation = c.generation AND e.kind = 'command.dispatched'
                         AND json_extract(e.data, '$.command_id') = c.id
                         AND json_extract(e.data, '$.live_at_dispatch') = 1))",
                params![stop_id, child, moved.generation],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if !stop_is_original_applied_manager_stop {
            return Err(cleanup_transfer_blocked(
                &job,
                "has no applied manager Stop",
            ));
        }
        if job.state == ChildCleanupState::Pending
            && let Some(apply_id) = &job.apply_id
            && write_binding(tx, apply_id)?.is_some()
        {
            return Err(cleanup_transfer_blocked(
                &job,
                "has an admitted cleanup apply",
            ));
        }
        let mut notice_id = job.notice_id.clone();
        let mut notice_delivered = job.notice_delivered;
        if let Some(id) = &notice_id {
            if let Some((principal, method, _, _)) = write_binding(tx, id)? {
                if principal != core::api::SWARM_MAIL_SENDER || method != "mail.send" {
                    return Err(cleanup_transfer_blocked(
                        &job,
                        "has a conflicting notice receipt",
                    ));
                }
                notice_delivered = true;
            }
            if notice_delivered {
                let old_delivery = format!("{id}/{}", from.id);
                if let Some(mail) = Store::transfer_unread_service_mail(
                    tx,
                    &old_delivery,
                    from,
                    to,
                    limits,
                    now,
                    events,
                )? {
                    notice_id = mail
                        .delivery_id
                        .split_once('/')
                        .map(|(id, _)| id.to_owned());
                } else {
                    // Keep read notice receipt and original provenance intact.
                    db!(tx.execute(
                        "UPDATE child_cleanup SET notice_delivered=1 WHERE child=?1 AND child_generation=?2",
                        params![child, moved.generation],
                    ));
                    continue;
                }
            }
        }
        db!(tx.execute(
            "UPDATE child_cleanup SET parent=?1, parent_generation=?2, notice_id=?7, notice_delivered=?8
             WHERE child=?3 AND child_generation=?4 AND parent=?5 AND parent_generation=?6",
            params![
                destination,
                to.generation,
                child,
                moved.generation,
                source,
                job.parent_generation,
                notice_id,
                notice_delivered,
            ],
        ));
    }
    Ok(())
}

fn cleanup_transfer_blocked(job: &ChildCleanupJob, reason: &str) -> Error {
    Error::new(
        "project_transfer_child_cleanup_blocked",
        format!("child {} cleanup {reason}", job.child),
    )
}

impl Store {
    pub fn verify_child_cleanup_manager(&self, expected: &ChildCleanupJob) -> Result<()> {
        let current: (i64, i64) = self.connection.query_row(
            "SELECT parent,parent_generation FROM child_cleanup WHERE child=?1 AND child_generation=?2",
            params![parse_agent_id(&expected.child)?, expected.child_generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(sql_error)?;
        if current
            != (
                parse_agent_id(&expected.parent)?,
                expected.parent_generation,
            )
        {
            return Err(Error::new(
                "child_cleanup_rebound",
                "child cleanup moved to the successor manager",
            ));
        }
        Ok(())
    }

    /// Return due cleanup rows and kept rows whose notice still needs delivery.
    pub fn child_cleanup_due(&self, now: i64, limit: usize) -> Result<Vec<ChildCleanupJob>> {
        let limit = i64::try_from(limit)
            .map_err(|_| Error::new("invalid", "child cleanup limit is too large"))?;
        let sql = format!(
            "{CHILD_CLEANUP_SELECT} WHERE not_before <= ?1
            AND (state = 'pending' OR (state = 'kept' AND notice_delivered = 0))
            ORDER BY not_before, child, child_generation LIMIT ?2"
        );
        let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
        let rows = statement
            .query_map(params![now, limit], cleanup_job_from_row)
            .map_err(sql_error)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    /// Lazily reserve the stable K69 plan, apply and notice IDs.
    pub fn reserve_child_cleanup_ids(
        &mut self,
        child: &str,
        generation: i64,
    ) -> Result<ChildCleanupJob> {
        self.ensure_writer_healthy()?;
        let child_num = parse_agent_id(child)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let existing: Option<ChildCleanupJob> = tx
            .query_row(
                &format!("{CHILD_CLEANUP_SELECT} WHERE child = ?1 AND child_generation = ?2"),
                params![child_num, generation],
                cleanup_job_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        let Some(existing) = existing else {
            return Err(Error::new("not_found", "child cleanup job was not found"));
        };
        if existing.plan_id.is_none() {
            db!(tx.execute(
                "UPDATE child_cleanup SET plan_id = ?1, apply_id = ?2, notice_id = ?3
                 WHERE child = ?4 AND child_generation = ?5 AND plan_id IS NULL",
                params![
                    ulid::Ulid::new().to_string(),
                    ulid::Ulid::new().to_string(),
                    ulid::Ulid::new().to_string(),
                    child_num,
                    generation
                ],
            ));
        }
        let job = tx
            .query_row(
                &format!("{CHILD_CLEANUP_SELECT} WHERE child = ?1 AND child_generation = ?2"),
                params![child_num, generation],
                cleanup_job_from_row,
            )
            .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(job)
    }

    pub fn retry_child_cleanup_after(
        &mut self,
        child: &str,
        generation: i64,
        retry_after: i64,
        detail: &str,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let changed = self
            .connection
            .execute(
                "UPDATE child_cleanup SET not_before = ?1,
                    detail = CASE WHEN state = 'pending' THEN ?2 ELSE detail END
                 WHERE child = ?3 AND child_generation = ?4
                   AND (state = 'pending' OR (state = 'kept' AND notice_delivered = 0))",
                params![
                    retry_after.max(0),
                    detail,
                    parse_agent_id(child)?,
                    generation
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "bad_state",
                "child cleanup has no pending action",
            ));
        }
        Ok(())
    }

    pub fn finish_child_cleanup(
        &mut self,
        child: &str,
        generation: i64,
        outcome: ChildCleanupOutcome,
        reason: &str,
        detail: Option<&str>,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let changed = self
            .connection
            .execute(
                "UPDATE child_cleanup SET state = ?1, reason = ?2, detail = ?3
             WHERE child = ?4 AND child_generation = ?5 AND state = 'pending'",
                params![
                    outcome.as_str(),
                    reason,
                    detail,
                    parse_agent_id(child)?,
                    generation
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new("bad_state", "child cleanup job is not pending"));
        }
        Ok(())
    }

    pub fn mark_child_cleanup_notice_delivered(
        &mut self,
        child: &str,
        generation: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let changed = self.connection.execute(
            "UPDATE child_cleanup SET notice_delivered = 1
             WHERE child = ?1 AND child_generation = ?2 AND state = 'kept' AND notice_id IS NOT NULL",
            params![parse_agent_id(child)?, generation],
        ).map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "bad_state",
                "kept child cleanup notice is not reserved",
            ));
        }
        Ok(())
    }
}
