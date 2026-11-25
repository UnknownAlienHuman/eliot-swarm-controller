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

pub(super) fn park(tx: &Transaction<'_>, restart_id: &str) -> Result<()> {
    let sql = format!(
        "INSERT INTO restart_deferred_sends(command_id, restart_id, native_scope, native_ref)
        SELECT commands.id, ?1, agent.native_scope, agent.native_ref FROM commands
        JOIN agents agent ON agent.id=commands.target AND agent.generation=commands.generation
        WHERE {CAN_PARK}
        ON CONFLICT(command_id) DO NOTHING"
    );
    tx.execute(&sql, [restart_id]).map_err(sql_error)?;
    Ok(())
}

impl Store {
    pub fn cancel_parked_restart(&mut self, restart_id: &str) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = unpark(&self.connection, restart_id);
        self.fault_on_persistence_error(&result);
        result
    }
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
            "SELECT EXISTS(SELECT 1 FROM restart_deferred_sends WHERE command_id=?1)",
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
    tx.prepare_cached("DELETE FROM restart_deferred_sends WHERE command_id=?1")
        .map_err(sql_error)?
        .execute([id])
        .map_err(sql_error)?;
    Ok(())
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
               JOIN agents a ON a.id=?1 AND a.generation=?2 AND a.state='idle'
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
    Ok(())
}
