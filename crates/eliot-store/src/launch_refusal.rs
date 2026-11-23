use super::*;

impl Store {
    /// An exact linked launch SEND, with the native model resolver's refusal,
    /// and a fresh managed checkout that has never run setup or another turn.
    pub fn refused_initial_model_workspace(
        &self,
        agent: &Agent,
        turn_id: &str,
        allowed_stop: Option<&str>,
    ) -> Result<Option<WorkspacePreparation>> {
        refused_initial_model_workspace(&self.connection, agent, turn_id, allowed_stop)
    }

    /// A terminal refusal lifecycle command with proved custody and an unused
    /// managed checkout. The retained command binds the cleanup to one Agent.
    pub fn refused_launch_cleanup_workspace(
        &self,
        agent: &Agent,
        command_id: &str,
    ) -> Result<Option<WorkspacePreparation>> {
        refused_launch_cleanup_workspace(&self.connection, agent, command_id)
    }
}

fn unused_launch_workspace(
    connection: &Connection,
    agent: &Agent,
) -> Result<Option<WorkspacePreparation>> {
    if agent.generation != 1 || agent.observe_only || agent.active_turn.is_some() {
        return Ok(None);
    }
    Ok(
        workspace::load_workspace(connection, parse_agent_id(&agent.id)?)?.filter(|workspace| {
            workspace.phase == WorkspacePhase::Ready
                && workspace.target_generation == agent.generation
                && workspace.frozen_setup.is_empty()
        }),
    )
}

fn refused_initial_model_workspace(
    connection: &Connection,
    agent: &Agent,
    turn_id: &str,
    allowed_stop: Option<&str>,
) -> Result<Option<WorkspacePreparation>> {
    if agent.harness != "opencode"
        || !matches!(agent.state, AgentState::Idle | AgentState::Stopped)
        || (allowed_stop.is_none() && agent.state != AgentState::Idle)
    {
        return Ok(None);
    }
    let agent_num = parse_agent_id(&agent.id)?;
    let Some(workspace) = unused_launch_workspace(connection, agent)? else {
        return Ok(None);
    };
    let exact: bool = connection
        .query_row(
            "SELECT EXISTS (
            SELECT 1 FROM turns t
            JOIN commands send ON send.id = t.preceded_by
            JOIN commands launch ON launch.id = send.parent_command
            WHERE t.agent = ?1 AND t.generation = 1 AND t.turn_id = ?2
              AND t.state = 'completed' AND t.outcome = 'failure'
              AND t.tool_calls = 0 AND t.final_text IS NULL AND t.usage IS NULL
              AND send.target = ?1 AND send.generation = 1 AND send.turn_id = ?2
              AND send.verb = 'send' AND send.state = 'applied'
              AND send.link_kind = 'send' AND send.ordinal = 0
              AND launch.id = ?3 AND launch.target = ?1 AND launch.generation = 1
              AND launch.verb = 'launch' AND launch.state = 'applied'
              AND EXISTS (
                SELECT 1 FROM events e WHERE e.agent = ?1 AND e.generation = 1
                  AND e.kind = 'turn.completed' AND e.source = 'stream' AND e.native_id IS NOT NULL
                  AND json_extract(e.data, '$.turn_id') = ?2
                  AND json_extract(e.data, '$.command_id') = send.id
                  AND json_extract(e.data, '$.native_status') = 'session.execution.failed'
                  AND json_extract(e.data, '$.model_unavailable') = 1
              )
              AND NOT EXISTS (SELECT 1 FROM turns other
                WHERE other.agent = ?1 AND other.generation = 1 AND other.turn_id <> ?2)
              AND NOT EXISTS (SELECT 1 FROM commands other
                WHERE other.target = ?1 AND other.generation = 1
                  AND other.id NOT IN (send.id, launch.id, COALESCE(?4, '')))
              AND EXISTS (SELECT 1 FROM source_state s
                WHERE s.agent = ?1 AND s.generation = 1 AND s.source = 'stream'
                  AND s.gap_id IS NULL
                  AND (?5 OR (s.state = 'open' AND s.health = 'ok')))
        )",
            params![
                agent_num,
                turn_id,
                workspace.start_command_id,
                allowed_stop,
                agent.state == AgentState::Stopped,
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(exact.then_some(workspace))
}

pub(super) fn require_refused_launch_stop(
    tx: &Transaction<'_>,
    command: &Command,
    agent: &Agent,
) -> Result<()> {
    let Some(turn) = command.args["refused_launch_turn"].as_str() else {
        return Ok(());
    };
    // The command itself has already been admitted, so exclude only this
    // reservation when rechecking that no other command got in first.
    let eligible = agent.state == AgentState::Idle
        && refused_initial_model_workspace(tx, agent, turn, Some(&command.id))?.is_some();
    if !eligible {
        return Err(Error::new(
            "bad_state",
            "the refused launch is no longer eligible for automatic worktree rollback",
        ));
    }
    Ok(())
}

pub(super) fn require_refused_launch_removal(
    tx: &Transaction<'_>,
    agent: &Agent,
    command_id: &str,
    expected_tip: Option<&str>,
) -> Result<()> {
    if let Some(workspace) = refused_launch_cleanup_workspace(tx, agent, command_id)?
        && expected_tip == Some(workspace.base_oid.as_str())
    {
        return Ok(());
    }
    Err(Error::new(
        "bad_state",
        "the refused launch no longer has a proved stopped worktree",
    ))
}

fn refused_launch_cleanup_workspace(
    connection: &Connection,
    agent: &Agent,
    command_id: &str,
) -> Result<Option<WorkspacePreparation>> {
    let Some(command) = load_command_record(connection, command_id)?.map(|row| row.command) else {
        return Ok(None);
    };
    if command.target.as_deref() != Some(&agent.id) || command.generation != Some(agent.generation)
    {
        return Ok(None);
    }
    let Some(outcome) = command.outcome.as_ref() else {
        return Ok(None);
    };
    if command.verb == "stop" {
        if command.state == CommandState::Applied
            && agent.state == AgentState::Stopped
            && outcome["process_custody_exit_proven"] == true
            && outcome["native_source_drain_proven"] == true
            && let Some(turn) = command.args["refused_launch_turn"].as_str()
        {
            return refused_initial_model_workspace(connection, agent, turn, Some(command_id));
        }
        return Ok(None);
    }
    if command.verb != "launch"
        || command.state != CommandState::Failed
        || agent.state != AgentState::Failed
    {
        return Ok(None);
    }
    let Some(workspace) = unused_launch_workspace(connection, agent)?
        .filter(|workspace| workspace.start_command_id == command_id)
    else {
        return Ok(None);
    };
    let agent_num = parse_agent_id(&agent.id)?;
    if outcome["unspawned_start"] != true
        && (outcome["native_start_refused"] != true
            || !capacity::has_owned_exit_and_reader_drain_tx(
                connection,
                agent_num,
                agent.generation,
            )?)
    {
        return Ok(None);
    }
    let unused: bool = connection
        .query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM turns WHERE agent = ?1 AND generation = ?2)
            AND NOT EXISTS(SELECT 1 FROM commands other
                WHERE other.target = ?1 AND other.generation = ?2 AND other.id <> ?3
                  AND NOT (other.parent_command IS ?3 AND other.link_kind IS 'send'
                           AND other.verb = 'send' AND other.state = 'cancelled'))",
            params![agent_num, agent.generation, command_id],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(unused.then_some(workspace))
}

pub(super) fn require_no_refused_launch_stop_in_flight(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
) -> Result<()> {
    let stopping: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE target = ?1 AND generation = ?2
         AND verb = 'stop' AND state IN ('dispatched', 'unknown')
         AND json_type(args, '$.refused_launch_turn') = 'text')",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if stopping {
        return Err(Error::new("busy", "the refused launch is stopping"));
    }
    Ok(())
}
