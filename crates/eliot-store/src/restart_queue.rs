//! Proven-undispatched direct SENDs parked by a planned restart.

use super::*;

// Explicit generation/turn bindings and linked/selector inputs keep their
// own lifecycle. Only an admitted, never-dispatched ordinary SEND may move.
pub(super) const CAN_PARK: &str = "commands.verb = 'send'
    AND commands.state IN ('accepted','queued')
    AND commands.parent_command IS NULL AND commands.selector IS NULL
    AND commands.expected_generation IS NULL AND commands.expected_turn_id IS NULL
    AND commands.turn_id IS NULL AND commands.principal NOT GLOB 'swarm_note:*'
    AND EXISTS (SELECT 1 FROM agents a WHERE a.id=commands.target
        AND a.generation=commands.generation AND a.observe_only=0 AND a.archived=0
        AND a.state IN ('idle','busy','waiting')
        AND trim(COALESCE(a.native_scope,''))<>'' AND trim(COALESCE(a.native_ref,''))<>'')";

impl Store {
    pub fn restart_send_parked(&self, id: &str) -> Result<bool> {
        is_parked(&self.connection, id)
    }
}

fn parking_refusal() -> Error {
    Error::new(
        "daemon_update_in_progress",
        "update can only park direct unbound SENDs to a live controlled native session",
    )
}

pub(super) fn validate_new_send(request: &SubmitCommand) -> Result<()> {
    if request.verb != "send"
        || request.target.is_none()
        || request.selector.is_some()
        || request.expected_generation.is_some()
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
        || request.args.get("notice_kind").is_some()
    {
        return Err(parking_refusal());
    }
    Ok(())
}

fn park_matching(tx: &Transaction<'_>, restart_id: &str, id: Option<&str>) -> Result<usize> {
    let filter = if id.is_some() {
        "AND commands.id = ?2"
    } else {
        ""
    };
    let sql = format!(
        "INSERT INTO restart_deferred_sends(command_id, restart_id, native_scope, native_ref)
        SELECT commands.id, ?1, agent.native_scope, agent.native_ref FROM commands
        JOIN agents agent ON agent.id=commands.target AND agent.generation=commands.generation
        WHERE {CAN_PARK} {filter}
        ON CONFLICT(command_id) DO NOTHING"
    );
    let mut statement = tx.prepare_cached(&sql).map_err(sql_error)?;
    match id {
        Some(id) => statement.execute(params![restart_id, id]),
        None => statement.execute([restart_id]),
    }
    .map_err(sql_error)
}

pub(super) fn park(tx: &Transaction<'_>, restart_id: &str) -> Result<()> {
    park_matching(tx, restart_id, None)?;
    Ok(())
}

pub(super) fn park_new(tx: &Transaction<'_>, restart_id: &str, id: &str) -> Result<()> {
    if park_matching(tx, restart_id, Some(id))? != 1 {
        // Roll back admission with the custody check, including any implicit
        // Resume expansion that would otherwise target a terminal session.
        return Err(parking_refusal());
    }
    Ok(())
}

/// Mark the exact preparation in the transaction that seals its restart.
pub(super) fn seal(tx: &Transaction<'_>, restart_id: &str) -> Result<()> {
    tx.execute(
        "UPDATE restart_deferred_sends SET sealed=1 WHERE restart_id=?1",
        [restart_id],
    )
    .map_err(sql_error)?;
    Ok(())
}

impl Store {
    pub fn expire_parked_restart(
        &mut self,
        restart_id: &str,
        now: i64,
    ) -> Result<Change<Option<Error>>> {
        self.ensure_writer_healthy()?;
        let result = self.expire_parked_restart_inner(restart_id, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn expire_parked_restart_inner(
        &mut self,
        restart_id: &str,
        now: i64,
    ) -> Result<Change<Option<Error>>> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let error = unpark(&tx, restart_id).err();
        let mut events = Vec::new();
        if error.is_some() {
            // A DELETE refusal can leave the writer usable. Give these inputs
            // a durable terminal outcome even if the marker cannot be removed.
            settle_unsealed(&tx, Some(restart_id), now, &mut events)?;
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: error,
            events,
        })
    }
}

pub(super) fn recover_unsealed(
    tx: &Transaction<'_>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    settle_unsealed(tx, None, now, events)?;
    tx.execute(
        "DELETE FROM restart_deferred_sends
         WHERE sealed=0 OR command_id IN (
           SELECT id FROM commands WHERE id=restart_deferred_sends.command_id
             AND state NOT IN ('accepted','queued')
         )",
        [],
    )
    .map_err(sql_error)?;
    Ok(())
}

fn settle_unsealed(
    tx: &Transaction<'_>,
    restart_id: Option<&str>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let mut statement = tx
        .prepare_cached(
            "SELECT c.id, c.target, c.generation
             FROM restart_deferred_sends d JOIN commands c ON c.id=d.command_id
             WHERE d.sealed=0 AND (?1 IS NULL OR d.restart_id=?1)
               AND c.verb='send' AND c.state IN ('accepted','queued') AND c.turn_id IS NULL",
        )
        .map_err(sql_error)?;
    let sends = statement
        .query_map([restart_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    for (id, target, generation) in sends {
        events.push(reject_parked(
            tx,
            &id,
            target,
            generation,
            "restart_preparation_abandoned",
            "The update preparation ended before sealing. This queued input was never dispatched.",
            now,
        )?);
    }
    Ok(())
}

fn reject_parked(
    tx: &Transaction<'_>,
    id: &str,
    target: Option<i64>,
    generation: Option<i64>,
    code: &str,
    message: &str,
    now: i64,
) -> Result<Event> {
    let outcome = json!({"code":code,"message":message,"native_dispatched":false});
    update_command_state(tx, id, CommandState::Rejected, Some(&outcome), None, now)?;
    insert_daemon_command_event(
        tx,
        now,
        "command.rejected",
        target,
        generation,
        id,
        json!({"command_id":id,"state":"rejected","outcome":outcome}),
    )
}

pub(super) fn unpark(connection: &Connection, restart_id: &str) -> Result<()> {
    connection
        .execute(
            "DELETE FROM restart_deferred_sends WHERE restart_id=?1",
            [restart_id],
        )
        .map_err(sql_error)?;
    Ok(())
}

pub(super) fn is_parked(connection: &Connection, id: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM restart_deferred_sends d
             JOIN commands c ON c.id=d.command_id
             WHERE c.id=?1 AND c.state IN ('accepted','queued'))",
            [id],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

pub(super) fn pending_before_resume(
    connection: &Connection,
    agent_num: i64,
    agent: &Agent,
) -> Result<bool> {
    connection
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM commands c
               JOIN restart_deferred_sends d ON d.command_id=c.id
              WHERE c.target=?1 AND c.generation<?2 AND c.state IN ('accepted','queued')
                AND d.native_scope=?3 AND d.native_ref=?4)",
        )
        .map_err(sql_error)?
        .query_row(
            params![
                agent_num,
                agent.generation,
                agent.native_scope,
                agent.native_ref
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

pub(super) fn consume_on_dispatch(tx: &Transaction<'_>, id: &str) -> Result<()> {
    clear_parked(tx, id)
}

fn clear_parked(tx: &Transaction<'_>, id: &str) -> Result<()> {
    tx.prepare_cached("DELETE FROM restart_deferred_sends WHERE command_id=?1")
        .map_err(sql_error)?
        .execute([id])
        .map_err(sql_error)?;
    Ok(())
}

/// Settle never-dispatched sealed input when this generation loses its
/// continuation. A Resume may keep only the exact captured native subject.
pub(super) fn retire_sealed(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    retained_subject: Option<(&str, &str)>,
    code: &str,
    now: i64,
) -> Result<Vec<Event>> {
    let mut statement = tx
        .prepare_cached(
            "SELECT c.id, c.generation
         FROM commands c JOIN restart_deferred_sends d ON d.command_id=c.id
         WHERE c.target=?1 AND c.generation<=?2 AND c.verb='send'
           AND c.state IN ('accepted','queued') AND c.turn_id IS NULL AND d.sealed=1
           AND (?3 IS NULL OR d.native_scope IS NOT ?3 OR d.native_ref IS NOT ?4)",
        )
        .map_err(sql_error)?;
    let (scope, reference) = retained_subject.map_or((None, None), |(scope, reference)| {
        (Some(scope), Some(reference))
    });
    let candidates = statement
        .query_map(params![agent, generation, scope, reference], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    let mut events = Vec::with_capacity(candidates.len());
    for (id, original_generation) in candidates {
        events.push(reject_parked(
            tx, &id, Some(agent), Some(original_generation), code,
            "The Agent no longer has a matching continuation. This parked input was never dispatched.",
            now,
        )?);
    }
    tx.execute(
        "DELETE FROM restart_deferred_sends WHERE sealed=1 AND command_id IN (
           SELECT id FROM commands WHERE target=?1 AND generation<=?2 AND state NOT IN ('accepted','queued')
         )",
        params![agent, generation],
    ).map_err(sql_error)?;
    Ok(events)
}

/// Rebind the original receipts only after this Resume settled applied on
/// the same native subject. A failed or unknown Resume leaves the rows parked.
pub(super) fn release_after_resume(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let mut statement = tx
        .prepare_cached(
            "UPDATE commands SET generation=?2, updated_at=?3
         WHERE target=?1 AND generation<?2 AND verb='send' AND state IN ('accepted','queued')
           AND EXISTS (SELECT 1 FROM restart_deferred_sends d
               JOIN agents a ON a.id=?1 AND a.generation=?2
                 AND a.state IN ('idle','busy','waiting')
                 AND a.native_scope=d.native_scope AND a.native_ref=d.native_ref
               WHERE d.command_id=commands.id)
         RETURNING id",
        )
        .map_err(sql_error)?;
    let ids = statement
        .query_map(params![agent, generation, now], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    for id in ids {
        // Keep the marker through recovery and work scheduling. Dispatch
        // consumes it atomically with the no-replay boundary.
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.restart_released",
            Some(agent),
            Some(generation),
            &id,
            json!({"command_id":id}),
        )?);
    }

    // The matching, live bindings above now carry the new generation.
    // Any older pending receipt after an applied Resume cannot use it.
    let mut statement = tx
        .prepare_cached(
            "SELECT c.id, c.generation,
                    a.native_scope IS NOT d.native_scope OR a.native_ref IS NOT d.native_ref
               FROM commands c
               JOIN restart_deferred_sends d ON d.command_id=c.id
               JOIN agents a ON a.id=c.target AND a.generation=?2
              WHERE c.target=?1 AND c.generation<?2 AND c.verb='send'
                AND c.state IN ('accepted','queued')",
        )
        .map_err(sql_error)?;
    let remaining = statement
        .query_map(params![agent, generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    for (id, original_generation, changed_subject) in remaining {
        let (code, message) = if changed_subject {
            (
                "restart_native_subject_changed",
                "The resumed session has a different native identity. The queued input was never dispatched.",
            )
        } else {
            (
                "restart_resumed_session_unavailable",
                "The resumed session cannot accept queued input. The input was never dispatched.",
            )
        };
        events.push(reject_parked(
            tx,
            &id,
            Some(agent),
            Some(original_generation),
            code,
            message,
            now,
        )?);
        clear_parked(tx, &id)?;
    }
    tx.execute(
        "DELETE FROM restart_deferred_sends WHERE command_id IN (
           SELECT id FROM commands WHERE target=?1 AND generation<?2
             AND state NOT IN ('accepted','queued')
         )",
        params![agent, generation],
    )
    .map_err(sql_error)?;
    Ok(())
}
