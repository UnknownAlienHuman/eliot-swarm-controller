//! Durable Store reservations for configured live Agent capacity.
use super::*;
use eliot_core::CapacityScope;

const MIGRATION_BATCH_SIZE: i64 = 128;
const RETAINED_PAGE_SIZE: i64 = 128;

/// A controlled generation left `lost` or `stopped` by a daemon replacement
/// while it still reserves capacity, with the process identities recorded in
/// its custody.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedGeneration {
    pub agent: String,
    pub generation: i64,
    /// The generation started after this Store had a custody namespace, so
    /// its custody was created under the name derived from it.
    pub named_custody: bool,
    pub processes: Vec<RecordedProcess>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedProcess {
    pub identity: ProcessIdentity,
    pub role: String,
    pub exited: bool,
}

impl Store {
    /// Random per-database namespace for OS custody names. A later daemon finds
    /// a previous generation's custody from it without storing another name.
    pub fn custody_namespace(&self) -> Result<String> {
        self.connection
            .query_row(
                "SELECT id FROM store_identity WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    /// One page, ordered by Agent, of lost or stopped generations that still
    /// hold their reservation (a forced stop records no `session.ended`). Call it
    /// at startup before any new generation can start.
    pub fn restart_retained_generations(
        &self,
        after: Option<&str>,
    ) -> Result<Vec<RetainedGeneration>> {
        let after = after.map(parse_agent_id).transpose()?.unwrap_or(0);
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, generation FROM agents
                 WHERE id > ?1 AND observe_only = 0 AND state IN ('lost', 'stopped')
                   AND capacity_generation = generation
                 ORDER BY id LIMIT ?2",
            )
            .map_err(sql_error)?;
        let agents = statement
            .query_map(params![after, RETAINED_PAGE_SIZE], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        let mut named = self
            .connection
            .prepare(
                "SELECT EXISTS(
                     SELECT 1 FROM events
                     WHERE agent = ?1 AND generation = ?2 AND kind = 'agent.starting'
                       AND seq > (SELECT since_seq FROM store_identity WHERE singleton = 1)
                 )",
            )
            .map_err(sql_error)?;
        let mut processes = self
            .connection
            .prepare(
                "SELECT host_boot_id, pid, start_time, role, state = 'exited' FROM processes
                 WHERE owner = ?1 ORDER BY host_boot_id, pid, start_time LIMIT ?2",
            )
            .map_err(sql_error)?;
        let mut retained = Vec::with_capacity(agents.len());
        for (agent_num, generation) in agents {
            let named_custody = named
                .query_row(params![agent_num, generation], |row| row.get(0))
                .map_err(sql_error)?;
            let agent = format_agent_id(agent_num);
            let owner = format!("agent:{agent}#{generation}");
            let rows = processes
                .query_map(params![owner, MAX_PAGE_ENTRIES as i64 + 1], |row| {
                    Ok(RecordedProcess {
                        identity: ProcessIdentity {
                            host_boot_id: row.get(0)?,
                            pid: row.get(1)?,
                            start_time: row.get::<_, i64>(2)? as u64,
                        },
                        role: row.get(3)?,
                        exited: row.get(4)?,
                    })
                })
                .map_err(sql_error)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_error)?;
            if rows.len() > MAX_PAGE_ENTRIES {
                return Err(Error::new(
                    "frame_too_large",
                    "retained generation process readback exceeds its bound",
                ));
            }
            retained.push(RetainedGeneration {
                agent,
                generation,
                named_custody,
                processes: rows,
            });
        }
        Ok(retained)
    }

    /// Commit custody closure that the daemon verified outside the Store lock,
    /// then release or transfer that generation's reservation in the same
    /// transaction. A newer reservation or an Agent that is neither lost nor
    /// stopped is left unchanged.
    pub fn release_restart_closed_generation(
        &mut self,
        agent: &str,
        generation: i64,
        evidence: &str,
        orphan_possible: bool,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let agent_num = parse_agent_id(agent)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "restart capacity Agent was not found"))?;
            observer::require_controlled(&current)?;
            if !matches!(current.state, AgentState::Lost | AgentState::Stopped)
                || current.generation != generation
                || reservation_generation_tx(&tx, agent_num)? != Some(generation)
            {
                return Ok(Vec::new());
            }
            let open_native: bool = tx
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM source_state
                         WHERE agent = ?1 AND generation = ?2 AND state = 'open'
                           AND source NOT IN ('daemon', 'os', 'git', 'workspace_process')
                     )",
                    params![agent_num, generation],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if open_native {
                return Err(Error::new(
                    "bad_state",
                    "restart capacity release requires reconciled native sources",
                ));
            }
            let owner = format!("agent:{agent}#{generation}");
            let processes = tx
                .execute(
                    "UPDATE processes SET state = 'exited', exit_code = NULL, updated_at = ?1
                 WHERE owner = ?2 AND state IN ('running', 'unknown')",
                    params![now, owner],
                )
                .map_err(sql_error)?;
            tx.execute(
                "UPDATE source_state SET state = 'closed', updated_at = ?1
                 WHERE agent = ?2 AND generation = ?3 AND source = 'os' AND state = 'open'",
                params![now, agent_num, generation],
            )
            .map_err(sql_error)?;
            let mut events = Vec::new();
            if release_proven_generation_tx(&tx, agent_num, generation)? {
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "capacity.released",
                    "source",
                    Some(agent_num),
                    Some(generation),
                    json!({
                        "reason": "restart_custody_closed",
                        "evidence": evidence,
                        "orphan_possible": orphan_possible,
                        "processes": processes,
                    }),
                )?);
            }
            commit_transaction(tx, &self.write_faulted).map(|()| events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Make a reservation that restart reconciliation cannot release visible
    /// in event readback, naming the custody evidence that is missing.
    pub fn record_restart_capacity_retained(
        &mut self,
        agent: &str,
        generation: i64,
        missing: &str,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let agent_num = parse_agent_id(agent)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if reservation_generation_tx(&tx, agent_num)? != Some(generation) {
                return Ok(Vec::new());
            }
            let event = insert_daemon_event(
                &tx,
                now,
                "capacity.retained",
                "source",
                Some(agent_num),
                Some(generation),
                json!({"reason": "restart_custody_unproven", "missing": missing}),
            )?;
            commit_transaction(tx, &self.write_faulted).map(|()| vec![event])
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

/// `since_seq` marks where named custody begins: generations started before
/// this migration ran under unnamed custody that a later daemon cannot find.
pub(super) fn create_store_identity_tx(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE store_identity (
             singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
             id TEXT NOT NULL,
             since_seq INTEGER NOT NULL
         );",
    )
    .map_err(sql_error)?;
    tx.execute(
        "INSERT INTO store_identity(singleton, id, since_seq)
         VALUES (1, ?1, COALESCE(
             (SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0))",
        [Ulid::new().to_string()],
    )
    .map_err(sql_error)?;
    Ok(())
}

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
        Some(reserved) => {
            return Err(Error::new(
                "capacity_unavailable",
                format!(
                    "generation {reserved} retains its capacity reservation until its custody is proven closed"
                ),
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

/// Record the plugin host's proof that it saw the generation's plugin exit and
/// drained its reader. Stop and lost transitions close native sources without
/// a drain, so only this record proves one.
pub(super) fn record_owned_exit_proof_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    now: i64,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO owned_exits(agent, generation, proven_at) VALUES (?1, ?2, ?3)",
        params![agent, generation, now],
    )
    .map_err(sql_error)?;
    Ok(())
}

/// Release the slot as soon as the exit proof is complete. The host's drain
/// record and the monitor's OS-source closure arrive in either order; each of
/// them calls this, and whichever lands last releases.
pub(super) fn release_owned_exit_event_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    reason: &str,
    now: i64,
) -> Result<Option<Event>> {
    if !release_owned_exit_tx(tx, agent, generation)? {
        return Ok(None);
    }
    insert_daemon_event(
        tx,
        now,
        "capacity.released",
        "source",
        Some(agent),
        Some(generation),
        json!({ "reason": reason }),
    )
    .map(Some)
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
                    || has_physical_exit_tx(tx, agent, generation)?
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

/// Live release proof: physical exit plus the plugin host's drain record.
fn has_owned_exit_and_reader_drain_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<bool> {
    if !has_physical_exit_tx(tx, agent, generation)? {
        return Ok(false);
    }
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM owned_exits WHERE agent = ?1 AND generation = ?2)",
        params![agent, generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

// Physical exit facts, shared by the reservation migration (which predates the
// drain record) and live release, so neither treats source closure alone as
// custody proof.
fn has_physical_exit_tx(tx: &Transaction<'_>, agent: i64, generation: i64) -> Result<bool> {
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
