use super::*;

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
    pub latest_read_turn: Option<String>,
    pub not_before: i64,
    pub state: ChildCleanupState,
    pub reason: String,
    pub detail: Option<String>,
    pub stop_id: Option<String>,
    pub plan_id: Option<String>,
    pub apply_id: Option<String>,
    pub notice_id: Option<String>,
    pub notice_delivered: bool,
}

const CHILD_CLEANUP_SELECT: &str = "SELECT 'a' || child, child_generation, 'a' || parent,
    parent_generation, latest_read_turn, not_before, state, reason, detail,
    stop_id, plan_id, apply_id, notice_id, notice_delivered FROM child_cleanup";

/// An inline completion that the manager's exact wake turn acknowledged.
/// The digest's frozen target must still identify the child's newest result.
pub(super) fn record_acknowledged_child_cleanup_tx(
    tx: &Transaction<'_>,
    parent: i64,
    generation: i64,
    notice: &str,
    now: i64,
) -> Result<()> {
    db!(tx.execute(
        "INSERT OR IGNORE INTO child_cleanup
            (child, child_generation, parent, parent_generation, latest_read_turn,
             not_before, state, reason)
         SELECT ch.id, ch.generation, p.id, p.generation, t.turn_id, ?4, 'pending', 'result_read'
         FROM agents p
         JOIN messages m ON m.recipient = p.id AND m.recipient_generation = p.generation
         JOIN completion_digests d ON d.id = substr(m.delivery_id, 1, instr(m.delivery_id, '/') - 1)
                  AND d.delivered_mail_id = m.delivery_id AND d.parent = p.id
         JOIN agents ch ON ch.parent = p.id
         JOIN workspaces w ON w.agent = ch.id AND w.target_generation = ch.generation
         JOIN turns t ON t.agent = ch.id AND t.generation = ch.generation
         WHERE p.id = ?1 AND p.generation = ?2 AND m.presentation_command = ?3
           AND m.read_at IS NOT NULL AND m.presented_at IS NOT NULL AND m.inline_ack_pending = 0
           AND m.sender = 'swarm' AND d.requester = ('agent:a' || p.id || ':' || p.generation)
           AND ch.origin = 'eliot' AND ch.state = 'idle' AND ch.active_turn IS NULL
           AND w.phase <> 'removed' AND t.state = 'completed'
           AND NOT EXISTS (SELECT 1 FROM turns later WHERE later.agent = ch.id
                  AND later.generation = ch.generation AND later.insertion_seq > t.insertion_seq)
           AND (EXISTS (
                 SELECT 1 FROM json_each(d.commands) item JOIN commands c ON c.id = item.value
                 WHERE c.verb = 'send' AND c.state = 'applied'
                   AND c.target = ch.id AND c.generation = ch.generation AND c.turn_id = t.turn_id
               ) OR EXISTS (
                 SELECT 1 FROM json_each(d.turns) item
                 WHERE json_extract(item.value, '$.agent') = 'a' || ch.id
                   AND json_extract(item.value, '$.generation') = ch.generation
                   AND json_extract(item.value, '$.turn_id') = t.turn_id
               ))",
        params![parent, generation, notice, now],
    ));
    Ok(())
}

fn cleanup_job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChildCleanupJob> {
    let state: String = row.get(6)?;
    Ok(ChildCleanupJob {
        child: row.get(0)?,
        child_generation: row.get(1)?,
        parent: row.get(2)?,
        parent_generation: row.get(3)?,
        latest_read_turn: row.get(4)?,
        not_before: row.get(5)?,
        state: match state.as_str() {
            "pending" => ChildCleanupState::Pending,
            "kept" => ChildCleanupState::Kept,
            _ => ChildCleanupState::Removed,
        },
        reason: row.get(7)?,
        detail: row.get(8)?,
        stop_id: row.get(9)?,
        plan_id: row.get(10)?,
        apply_id: row.get(11)?,
        notice_id: row.get(12)?,
        notice_delivered: row.get::<_, i64>(13)? != 0,
    })
}

impl Store {
    /// Record a result that the Router has authenticated as an exact direct-parent read.
    pub fn record_child_cleanup_result(
        &mut self,
        parent: &str,
        parent_generation: i64,
        child: &str,
        child_generation: i64,
        turn: &str,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let parent_num = parse_agent_id(parent)?;
        let child_num = parse_agent_id(child)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let valid: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM agents p JOIN agents ch ON ch.parent = p.id
                 JOIN workspaces w ON w.agent = ch.id AND w.target_generation = ch.generation
                 JOIN turns t ON t.agent = ch.id AND t.generation = ch.generation
                 WHERE p.id = ?1 AND p.generation = ?2 AND ch.id = ?3
                   AND ch.generation = ?4 AND ch.origin = 'eliot' AND w.phase <> 'removed'
                   AND ch.state = 'idle' AND ch.active_turn IS NULL AND ch.pending = '[]'
                   AND t.turn_id = ?5 AND t.state = 'completed'
                   AND NOT EXISTS (SELECT 1 FROM turns later WHERE later.agent = ch.id
                       AND later.generation = ch.generation AND later.state = 'completed'
                       AND later.insertion_seq > t.insertion_seq)
                   AND NOT EXISTS (SELECT 1 FROM commands cmd WHERE cmd.target = ch.id
                       AND cmd.generation = ch.generation AND cmd.state IN ('accepted','queued','dispatched','unknown'))
                   AND NOT EXISTS (SELECT 1 FROM turns open_turn WHERE open_turn.agent = ch.id
                       AND open_turn.generation = ch.generation AND open_turn.state IN ('running','unknown'))
             )",
            params![parent_num, parent_generation, child_num, child_generation, turn],
            |row| row.get(0),
        ).map_err(sql_error)?;
        if !valid {
            return Err(Error::new(
                "bad_state",
                "direct-parent result does not identify the current live idle child and its exact latest completed turn",
            ));
        }
        db!(tx.execute(
            "INSERT INTO child_cleanup
                 (child, child_generation, parent, parent_generation, latest_read_turn,
                  not_before, state, reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 'result_read')
             ON CONFLICT(child, child_generation) DO UPDATE SET
                 parent = excluded.parent, parent_generation = excluded.parent_generation,
                 latest_read_turn = excluded.latest_read_turn,
                 not_before = excluded.not_before,
                 state = 'pending', reason = 'result_read', detail = NULL
             WHERE child_cleanup.state = 'pending'
               AND child_cleanup.latest_read_turn IS NOT excluded.latest_read_turn
               AND NOT EXISTS (SELECT 1 FROM commands stop WHERE stop.id = child_cleanup.stop_id)",
            params![
                child_num,
                child_generation,
                parent_num,
                parent_generation,
                turn,
                now
            ],
        ));
        if tx.changes() != 1 {
            return Err(Error::new(
                "bad_state",
                "child cleanup generation is already terminal",
            ));
        }
        commit_transaction(tx, &self.write_faulted)
    }

    /// Return due cleanup rows and kept rows whose notice still needs delivery.
    pub fn child_cleanup_due(&self, now: i64, limit: usize) -> Result<Vec<ChildCleanupJob>> {
        let limit = i64::try_from(limit)
            .map_err(|_| Error::new("invalid", "child cleanup limit is too large"))?;
        let sql = format!(
            "{CHILD_CLEANUP_SELECT} WHERE (state = 'pending' AND not_before <= ?1)
            OR (state = 'kept' AND notice_delivered = 0 AND not_before <= ?1)
            ORDER BY not_before, child, child_generation LIMIT ?2"
        );
        let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
        let rows = statement
            .query_map(params![now, limit], cleanup_job_from_row)
            .map_err(sql_error)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    /// Lazily reserve the stable stop, K69 plan, apply and notice IDs.
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
        if existing.stop_id.is_none() {
            db!(tx.execute(
                "UPDATE child_cleanup SET stop_id = ?1, plan_id = ?2, apply_id = ?3, notice_id = ?4
                 WHERE child = ?5 AND child_generation = ?6 AND stop_id IS NULL",
                params![
                    ulid::Ulid::new().to_string(),
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

    /// Require an exact parent-generation, read-turn and idle proof before an automatic stop.
    pub fn child_cleanup_stop_guard(&self, child: &str, generation: i64) -> Result<()> {
        require_idle_cleanup_child(&self.connection, parse_agent_id(child)?, generation, None)
    }
}

pub(super) fn child_cleanup_stop_guard_tx(
    tx: &Transaction<'_>,
    stop_id: &str,
    child_num: i64,
    generation: i64,
) -> Result<()> {
    let reserved: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM child_cleanup WHERE child = ?1 AND child_generation = ?2
                      AND stop_id = ?3)",
            params![child_num, generation, stop_id],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if reserved {
        require_idle_cleanup_child(tx, child_num, generation, Some(stop_id))
    } else {
        Ok(())
    }
}

fn require_idle_cleanup_child(
    connection: &Connection,
    child: i64,
    generation: i64,
    excluded_stop_id: Option<&str>,
) -> Result<()> {
    let valid: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM child_cleanup c JOIN agents ch ON ch.id = c.child
             JOIN agents p ON p.id = c.parent
             WHERE c.child = ?1 AND c.child_generation = ?2 AND c.state = 'pending'
               AND c.latest_read_turn IS NOT NULL
               AND ch.generation = c.child_generation AND ch.parent = p.id
               AND p.generation = c.parent_generation AND ch.state = 'idle'
               AND ch.archived = 0 AND p.archived = 0
               AND ch.active_turn IS NULL AND ch.pending = '[]'
               AND EXISTS (SELECT 1 FROM turns t WHERE t.agent = ch.id AND t.generation = ch.generation
                           AND t.turn_id = c.latest_read_turn AND t.state = 'completed')
               AND NOT EXISTS (SELECT 1 FROM turns later JOIN turns current
                           ON current.agent = later.agent AND current.generation = later.generation
                           WHERE current.agent = ch.id AND current.generation = ch.generation
                             AND current.turn_id = c.latest_read_turn AND later.state = 'completed'
                             AND later.insertion_seq > current.insertion_seq)
               AND NOT EXISTS (SELECT 1 FROM commands cmd WHERE cmd.target = ch.id
                           AND cmd.id <> COALESCE(?3, '')
                           AND (cmd.state = 'unknown' OR (
                             cmd.generation = ch.generation AND cmd.state IN ('accepted','queued','dispatched'))))
               AND NOT EXISTS (SELECT 1 FROM turns t WHERE t.agent = ch.id AND t.generation = ch.generation
                           AND t.state IN ('running','unknown'))
         )",
        params![child, generation, excluded_stop_id], |row| row.get(0),
    ).map_err(sql_error)?;
    if !valid {
        return Err(Error::new(
            "bad_state",
            "child cleanup stop requires the current parent generation, exact latest completed read turn, and an idle child",
        ));
    }
    Ok(())
}
