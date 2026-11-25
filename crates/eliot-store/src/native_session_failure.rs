use super::*;

pub(super) fn is_broken(connection: &Connection, agent: i64, generation: i64) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM broken_native_sessions WHERE agent=?1 AND generation=?2)",
            params![agent, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

/// Retain the exact dispatched command's adapter evidence in its settlement.
pub(super) fn record_broken_session(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    state: CommandState,
    outcome: Option<&Value>,
    now: i64,
) -> Result<Option<Event>> {
    let Some(outcome) = outcome else {
        return Ok(None);
    };
    let reason = core::limits::NATIVE_HISTORY_MISSING_TOOL_OUTPUT;
    if outcome["native_session_failure"].as_str() != Some(reason)
        || record.state != CommandState::Dispatched
        || !matches!(
            state,
            CommandState::Unknown | CommandState::Failed | CommandState::Rejected
        )
        || !matches!(record.command.verb.as_str(), "send" | "compact")
        || (record.command.verb == "compact" && outcome["compaction"]["completed"] != false)
    {
        return Ok(None);
    }
    let (Some(agent), Some(generation)) = (record.target, record.command.generation) else {
        return Ok(None);
    };
    if load_agent_by_num(tx, agent)?.is_none_or(|current| {
        current.generation != generation || current.observe_only || current.native_ref.is_none()
    }) {
        return Ok(None);
    }
    let id = &record.command.id;
    let inserted = tx
        .execute(
            "INSERT OR IGNORE INTO broken_native_sessions(agent,generation,command_id,recorded_at)
         VALUES (?1,?2,?3,?4)",
            params![agent, generation, id, now],
        )
        .map_err(sql_error)?;
    if inserted == 0 {
        return Ok(None);
    }
    insert_daemon_command_event(
        tx,
        now,
        "session.broken",
        Some(agent),
        Some(generation),
        id,
        json!({"command_id": id, "reason": reason}),
    )
    .map(Some)
}

pub(super) fn require_usable(connection: &Connection, agent: &Agent) -> Result<()> {
    if is_broken(connection, parse_agent_id(&agent.id)?, agent.generation)? {
        return Err(Error::new(
            "native_session_broken",
            "the native session is broken; switch to a fresh session in this worktree",
        ));
    }
    Ok(())
}
