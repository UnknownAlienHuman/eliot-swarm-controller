//! Durable Store reservations for configured live Agent capacity.
use super::*;
use eliot_core::CapacityScope;

const MIGRATION_BATCH_SIZE: i64 = 128;

pub(super) fn scopes(agent: &Agent) -> Result<Vec<CapacityScope>> {
    let mut scopes: Vec<CapacityScope> = match agent.profile_snapshot.get("capacity_scopes") {
        None => Vec::new(),
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|_| Error::new("invalid", "frozen capacity scopes are malformed"))?,
    };
    let profile_scope = format!("profile:{}", agent.profile);
    if !scopes.iter().any(|scope| scope.scope == profile_scope) {
        scopes.push(CapacityScope {
            scope: profile_scope.clone(),
            max_live: None,
        });
    }

    let mut kinds = HashSet::new();
    for scope in &scopes {
        scope.validate()?;
        let kind = scope
            .scope
            .split_once(':')
            .map(|(kind, _)| kind)
            .unwrap_or("");
        if !kinds.insert(kind) || kind == "profile" && scope.scope != profile_scope {
            return Err(Error::new(
                "invalid",
                "capacity scopes do not match the frozen profile",
            ));
        }
    }
    Ok(scopes)
}

pub(super) fn reserve_tx(tx: &Transaction<'_>, agent: &Agent, generation: i64) -> Result<()> {
    if agent.observe_only {
        return Ok(());
    }
    if generation <= 0 {
        return Err(Error::new(
            "invalid",
            "capacity target generation must be positive",
        ));
    }

    let id = parse_agent_id(&agent.id)?;
    match reservation_generation_tx(tx, id)? {
        Some(reserved) if reserved == generation => return Ok(()),
        Some(_) => {
            return Err(Error::new(
                "capacity_unavailable",
                "a prior generation retains its capacity reservation",
            ));
        }
        None => {}
    }

    for scope in scopes(agent)? {
        let Some(max_live) = scope.max_live else {
            continue;
        };
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM agents a
                 WHERE a.capacity_generation IS NOT NULL AND a.id <> ?1
                   AND (?2 = 'profile:' || a.profile OR EXISTS (
                       SELECT 1 FROM json_each(a.profile_snapshot, '$.capacity_scopes') s
                       WHERE CASE WHEN s.type = 'object'
                                  THEN json_extract(s.value, '$.scope') END = ?2
                   ))",
                params![id, scope.scope],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if u64::try_from(count).map_or(true, |count| count >= max_live) {
            return Err(Error::new(
                "capacity_unavailable",
                "configured live capacity is reserved",
            ));
        }
    }

    set_reservation_generation_tx(tx, id, None, generation)
}

pub(super) fn require_reservation_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<()> {
    if reservation_generation_tx(tx, agent)? != Some(generation) {
        return Err(Error::new(
            "bad_state",
            "native start has no matching capacity reservation",
        ));
    }
    Ok(())
}

pub(super) fn release_unstarted_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    command_id: &str,
) -> Result<()> {
    if has_other_start_claim_tx(tx, agent, generation, command_id)? {
        return Ok(());
    }
    release_tx(tx, agent, generation)
}

/// Transfer an old generation's slot to an already-admitted resume, or release
/// it after the caller has recorded a terminal session/owned-exit proof.
pub(super) fn release_proven_generation_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<bool> {
    if reservation_generation_tx(tx, agent)? != Some(generation) {
        return Ok(false);
    }
    if pending_resume_tx(tx, agent, generation)? {
        let Some(next_generation) = generation.checked_add(1) else {
            release_tx(tx, agent, generation)?;
            return Ok(true);
        };
        set_reservation_generation_tx(tx, agent, Some(generation), next_generation)?;
        return Ok(false);
    }
    release_tx(tx, agent, generation)?;
    Ok(true)
}

pub(super) fn release_owned_exit_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<bool> {
    if !has_owned_exit_and_reader_drain_tx(tx, agent, generation)? {
        return Ok(false);
    }
    release_proven_generation_tx(tx, agent, generation)
}

pub(super) fn migrate_reservations_tx(tx: &Transaction<'_>) -> Result<()> {
    let mut after = 0_i64;
    loop {
        let mut statement = tx
            .prepare(
                "SELECT id, generation, observe_only FROM agents
                 WHERE id > ?1 ORDER BY id LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after, MIGRATION_BATCH_SIZE], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let mut agents = Vec::with_capacity(MIGRATION_BATCH_SIZE as usize);
        for row in rows {
            agents.push(row.map_err(sql_error)?);
        }
        drop(statement);
        let Some((last_agent, _, _)) = agents.last().copied() else {
            break;
        };
        after = last_agent;

        for (agent, generation, observe_only) in agents {
            if observe_only {
                continue;
            }
            let reserved = if generation == 0 {
                pending_launch_tx(tx, agent)?.then_some(1)
            } else if generation > 0 {
                if has_terminal_session_tx(tx, agent, generation)?
                    || has_owned_exit_and_reader_drain_tx(tx, agent, generation)?
                    || has_rejected_dispatched_start_tx(tx, agent, generation)?
                {
                    if pending_resume_tx(tx, agent, generation)? {
                        generation.checked_add(1)
                    } else {
                        None
                    }
                } else {
                    Some(generation)
                }
            } else {
                None
            };
            if let Some(reserved) = reserved {
                set_reservation_generation_tx(tx, agent, None, reserved)?;
            }
        }
    }
    Ok(())
}

fn pending_launch_tx(tx: &Transaction<'_>, agent: i64) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM commands
             WHERE target = ?1 AND verb = 'launch' AND generation = 0
               AND state IN ('accepted', 'queued', 'unknown')
         )",
        [agent],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn pending_resume_tx(tx: &Transaction<'_>, agent: i64, generation: i64) -> Result<bool> {
    let Some(target_generation) = generation.checked_add(1) else {
        return Ok(false);
    };
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM commands c
             WHERE c.target = ?1 AND c.verb = 'resume' AND c.generation = ?2
               AND (c.state IN ('accepted', 'queued') OR (
                   c.state = 'unknown' AND EXISTS(
                       SELECT 1 FROM workspaces w
                       WHERE w.start_command_id = c.id AND w.target_generation = ?3
                   )
               ))
         )",
        params![agent, generation, target_generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn has_other_start_claim_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    command_id: &str,
) -> Result<bool> {
    let previous = generation.saturating_sub(1);
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM commands c
             WHERE c.target = ?1 AND c.id <> ?2 AND (
                 (c.verb = 'launch' AND ?3 = 1 AND c.generation = 0
                   AND c.state IN ('accepted', 'queued', 'unknown'))
                 OR (c.verb = 'resume' AND c.generation = ?4
                   AND c.state IN ('accepted', 'queued'))
                 OR (c.verb = 'resume' AND c.generation = ?4 AND c.state = 'unknown'
                   AND EXISTS (
                       SELECT 1 FROM workspaces w
                       WHERE w.start_command_id = c.id AND w.target_generation = ?3
                   ))
                 OR (c.verb IN ('launch', 'resume') AND c.generation = ?3
                   AND c.state IN ('dispatched', 'unknown', 'applied'))
             )
         )",
        params![agent, command_id, generation, previous],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn has_terminal_session_tx(tx: &Transaction<'_>, agent: i64, generation: i64) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM events
             WHERE agent = ?1 AND generation = ?2 AND family = 'session'
               AND kind = 'session.ended'
               AND source NOT IN ('daemon', 'os', 'git', 'workspace_process')
         )",
        params![agent, generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn has_rejected_dispatched_start_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM commands c
             WHERE c.target = ?1 AND c.generation = ?2
               AND c.verb IN ('launch', 'resume') AND c.state = 'rejected'
               AND EXISTS(
                   SELECT 1 FROM events e
                   WHERE e.agent = ?1 AND e.generation = ?2
                     AND e.kind = 'agent.starting'
                     AND json_extract(e.data, '$.command_id') = c.id
               )
         )",
        params![agent, generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

// Shared by migration and live release so neither treats source closure alone
// as custody proof.
fn has_owned_exit_and_reader_drain_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<bool> {
    let os_closed: bool = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM source_state
                 WHERE agent = ?1 AND generation = ?2 AND source = 'os' AND state = 'closed'
             )",
            params![agent, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let (native_sources, open_native_sources): (i64, i64) = tx
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(state = 'open'), 0)
             FROM source_state
             WHERE agent = ?1 AND generation = ?2
               AND source NOT IN ('daemon', 'os', 'git', 'workspace_process')",
            params![agent, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)?;
    let owner = format!("agent:{}#{}", format_agent_id(agent), generation);
    let (processes, exited_processes, exited_plugins): (i64, i64, i64) = tx
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(state = 'exited'), 0),
                    COALESCE(SUM(role IN ('plugin', 'harness') AND state = 'exited'), 0)
             FROM processes WHERE owner = ?1",
            [&owner],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(sql_error)?;
    Ok(os_closed
        && native_sources > 0
        && open_native_sources == 0
        && processes > 0
        && processes == exited_processes
        && exited_plugins > 0)
}

fn reservation_generation_tx(tx: &Transaction<'_>, agent: i64) -> Result<Option<i64>> {
    tx.query_row(
        "SELECT capacity_generation FROM agents WHERE id = ?1",
        [agent],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn set_reservation_generation_tx(
    tx: &Transaction<'_>,
    agent: i64,
    expected: Option<i64>,
    generation: i64,
) -> Result<()> {
    let changed = tx
        .execute(
            "UPDATE agents SET capacity_generation = ?1
             WHERE id = ?2 AND capacity_generation IS ?3",
            params![generation, agent, expected],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "capacity_unavailable",
            "capacity reservation changed during admission",
        ));
    }
    Ok(())
}

fn release_tx(tx: &Transaction<'_>, agent: i64, generation: i64) -> Result<()> {
    tx.execute(
        "UPDATE agents SET capacity_generation = NULL
         WHERE id = ?1 AND capacity_generation = ?2",
        params![agent, generation],
    )
    .map_err(sql_error)?;
    Ok(())
}
