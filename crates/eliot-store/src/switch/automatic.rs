//! Bounded selection and reservation of the daemon's own handoffs. Selection
//! reads committed facts; the reservation rechecks them in one transaction.
use super::*;

pub const HANDOFF_QUEUED: &str = "handoff.queued";
pub const HANDOFF_SETTLED: &str = "handoff.settled";
pub const HANDOFF_WAITING: &str = "handoff.waiting";

/// A refused admission has no effect, so it may be tried again later.
const REJECTED_RETRY_MS: i64 = 5 * 60 * 1_000;
/// A source refusal normally needs the Owner; check it again only rarely.
const REFUSED_RETRY_MS: i64 = 60 * 60 * 1_000;
/// Native process-loss recovery gets this long to reopen the session first.
const LOSS_GRACE_MS: i64 = 20_000;
/// A candidate with no eligible successor is evaluated at most this often.
const WAITING_RETRY_MS: i64 = 60_000;
const HELD_SCAN_LIMIT: i64 = 256;
const LOST_SCAN_LIMIT: i64 = 64;
const MAX_AUTOMATIC_HANDOFFS_PER_TASK: i64 = 3;
type HandoffBudgetState = (i64, Option<i64>, Option<i64>, Option<String>, Option<i64>);

#[derive(Clone, Debug)]
pub struct AutomaticHandoff {
    pub agent: Agent,
    pub reason: HandoffReason,
    pub kind: Option<core::ProviderRestrictionKind>,
    pub held_since: Option<i64>,
}

fn lineage_for(
    connection: &Connection,
    agent: i64,
    generation: i64,
) -> Result<Option<(i64, Option<i64>)>> {
    connection
        .query_row(
            "SELECT lineage_agent, successor_agent FROM handoff_task_lineages
             WHERE agent = ?1 AND generation = ?2",
            params![agent, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)
}

fn handoff_budget_available(connection: &Connection, agent: &Agent, now: i64) -> Result<bool> {
    let agent_num = parse_agent_id(&agent.id)?;
    if lineage_for(connection, agent_num, agent.generation)?
        .is_some_and(|(_, successor)| successor.is_some())
    {
        return Ok(false);
    }
    // Older successful same-worktree switches predate the durable lineage row.
    let already_succeeded: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_successors
             WHERE agent = ?1 AND generation = ?2)",
            params![agent_num, agent.generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if already_succeeded {
        return Ok(false);
    }
    let lineage_agent = lineage_for(connection, agent_num, agent.generation)?
        .map_or(agent_num, |(lineage_agent, _)| lineage_agent);
    let budget: Option<HandoffBudgetState> = connection
        .query_row(
            "SELECT automatic_attempts, last_agent, last_generation, last_state, last_updated_at
             FROM automatic_handoff_budgets WHERE lineage_agent = ?1",
            [lineage_agent],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some((attempts, last_agent, last_generation, state, updated_at)) = budget else {
        return Ok(true);
    };
    if attempts >= MAX_AUTOMATIC_HANDOFFS_PER_TASK {
        return Ok(false);
    }
    if last_agent != Some(agent_num) || last_generation != Some(agent.generation) {
        return Ok(true);
    }
    let updated_at = updated_at.unwrap_or(now);
    Ok(match state.as_deref() {
        Some("rejected") => updated_at + REJECTED_RETRY_MS <= now,
        Some("refused") => updated_at + REFUSED_RETRY_MS <= now,
        Some("failed") => updated_at + REFUSED_RETRY_MS <= now,
        Some("reserved" | "unknown" | "applied") | None => false,
        _ => false,
    })
}

fn ensure_lineage_tx(tx: &Transaction<'_>, agent: i64, generation: i64) -> Result<i64> {
    if let Some((lineage_agent, _)) = lineage_for(tx, agent, generation)? {
        tx.execute(
            "INSERT OR IGNORE INTO automatic_handoff_budgets(lineage_agent)
             VALUES (?1)",
            [lineage_agent],
        )
        .map_err(sql_error)?;
        return Ok(lineage_agent);
    }
    tx.execute(
        "INSERT INTO handoff_task_lineages(agent, generation, lineage_agent)
         VALUES (?1, ?2, ?1)",
        params![agent, generation],
    )
    .map_err(sql_error)?;
    tx.execute(
        "INSERT OR IGNORE INTO automatic_handoff_budgets(lineage_agent) VALUES (?1)",
        [agent],
    )
    .map_err(sql_error)?;
    Ok(agent)
}

fn insert_lineage_generation_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    lineage_agent: i64,
) -> Result<()> {
    let inserted = tx
        .execute(
            "INSERT INTO handoff_task_lineages(agent, generation, lineage_agent)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(agent, generation) DO NOTHING",
            params![agent, generation, lineage_agent],
        )
        .map_err(sql_error)?;
    if inserted == 0
        && lineage_for(tx, agent, generation)?.map(|(current, _)| current) != Some(lineage_agent)
    {
        return Err(Error::new(
            "source_integrity",
            "Agent generation is already bound to another task continuation",
        ));
    }
    tx.execute(
        "INSERT OR IGNORE INTO automatic_handoff_budgets(lineage_agent) VALUES (?1)",
        [lineage_agent],
    )
    .map_err(sql_error)?;
    Ok(())
}

impl Store {
    /// Carry one task's continuation lineage across a committed switch.
    pub(crate) fn transfer_handoff_lineage_tx(
        tx: &Transaction<'_>,
        source: &Agent,
        successor: &Agent,
    ) -> Result<()> {
        let source_num = parse_agent_id(&source.id)?;
        let successor_num = parse_agent_id(&successor.id)?;
        let lineage_agent = ensure_lineage_tx(tx, source_num, source.generation)?;
        let current =
            lineage_for(tx, source_num, source.generation)?.and_then(|(_, successor)| successor);
        if current.is_some_and(|current| current != successor_num) {
            return Err(Error::new(
                "bad_state",
                "task continuation already has another successor",
            ));
        }
        if current.is_none() {
            let changed = tx
                .execute(
                    "UPDATE handoff_task_lineages SET successor_agent = ?1
                     WHERE agent = ?2 AND generation = ?3 AND successor_agent IS NULL",
                    params![successor_num, source_num, source.generation],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(Error::new(
                    "stale_generation",
                    "task continuation changed before switch completion",
                ));
            }
        }
        insert_lineage_generation_tx(tx, successor_num, successor.generation, lineage_agent)
    }

    /// Resume advances the generation but keeps the same durable task lineage.
    pub(crate) fn inherit_handoff_lineage_tx(
        tx: &Transaction<'_>,
        agent: i64,
        previous_generation: i64,
        next_generation: i64,
    ) -> Result<()> {
        let lineage_agent = ensure_lineage_tx(tx, agent, previous_generation)?;
        if lineage_for(tx, agent, previous_generation)?
            .is_some_and(|(_, successor)| successor.is_some())
        {
            return Err(Error::new(
                "bad_state",
                "a handed-off task cannot resume from its previous Agent",
            ));
        }
        insert_lineage_generation_tx(tx, agent, next_generation, lineage_agent)
    }

    /// Charge only after the automatic switch reservation is ready to commit.
    fn charge_automatic_handoff_tx(
        tx: &Transaction<'_>,
        agent: &Agent,
        switch_command: &str,
        now: i64,
    ) -> Result<bool> {
        if !handoff_budget_available(tx, agent, now)? {
            return Ok(false);
        }
        let agent_num = parse_agent_id(&agent.id)?;
        let lineage_agent = ensure_lineage_tx(tx, agent_num, agent.generation)?;
        let changed = tx
            .execute(
                "UPDATE automatic_handoff_budgets
                 SET automatic_attempts = automatic_attempts + 1,
                     last_agent = ?1, last_generation = ?2,
                     last_switch_command = ?3, last_state = 'reserved', last_updated_at = ?4
                 WHERE lineage_agent = ?5 AND automatic_attempts < ?6",
                params![
                    agent_num,
                    agent.generation,
                    switch_command,
                    now,
                    lineage_agent,
                    MAX_AUTOMATIC_HANDOFFS_PER_TASK
                ],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
    }

    pub(crate) fn settle_automatic_handoff_attempt_tx(
        tx: &Transaction<'_>,
        switch_command: &str,
        state: CommandState,
        now: i64,
    ) -> Result<()> {
        let state = match state {
            CommandState::Applied => "applied",
            CommandState::Rejected => "rejected",
            CommandState::Failed => "failed",
            CommandState::Unknown => "unknown",
            _ => return Ok(()),
        };
        tx.execute(
            "UPDATE automatic_handoff_budgets
             SET last_state = ?1, last_updated_at = ?2
             WHERE last_switch_command = ?3 AND last_state = 'reserved'",
            params![state, now, switch_command],
        )
        .map_err(sql_error)?;
        Ok(())
    }

    pub(crate) fn settle_automatic_handoff_attempt(
        &mut self,
        switch_command: &str,
        state: CommandState,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            Self::settle_automatic_handoff_attempt_tx(&tx, switch_command, state, now)?;
            commit_transaction(tx, &self.write_faulted)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_automatic_handoff_refusal_tx(
        tx: &Transaction<'_>,
        agent: &Agent,
        now: i64,
    ) -> Result<()> {
        let agent_num = parse_agent_id(&agent.id)?;
        let lineage_agent = ensure_lineage_tx(tx, agent_num, agent.generation)?;
        tx.execute(
            "UPDATE automatic_handoff_budgets
             SET last_agent = ?1, last_generation = ?2, last_switch_command = NULL,
                 last_state = 'refused', last_updated_at = ?3
             WHERE lineage_agent = ?4",
            params![agent_num, agent.generation, now, lineage_agent],
        )
        .map_err(sql_error)?;
        Ok(())
    }
}

struct OwedWork {
    task: String,
    commands: Vec<String>,
    /// More input is waiting than one transfer can carry.
    overflow: bool,
}

pub(super) fn hold_is_current(tx: &Connection, agent: &Agent, held_since: i64) -> Result<bool> {
    let scope = core::frozen_provider_scope(agent)?;
    let current: Option<i64> = tx
        .query_row(
            "SELECT held_since FROM provider_holds
             WHERE scope = ?1 AND kind IN ('quota','rate_limit')",
            [scope],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(current == Some(held_since))
}

/// Whether another automatic attempt is allowed for this generation.
fn attempt_allowed(connection: &Connection, agent: &Agent, now: i64) -> Result<bool> {
    handoff_budget_available(connection, agent, now)
}

/// The input a successor must carry, when this generation still owes work.
fn owed_work(
    connection: &Connection,
    agent: &Agent,
    reason: HandoffReason,
    limits: core::api::HierarchyLimits,
    now: i64,
) -> Result<Option<OwedWork>> {
    if !attempt_allowed(connection, agent, now)? {
        return Ok(None);
    }
    let num = parse_agent_id(&agent.id)?;
    if agent.is_terminal() {
        let resuming: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM commands INDEXED BY commands_target
                 WHERE target = ?1 AND verb = 'resume'
                   AND state IN ('accepted','queued','dispatched','unknown'))",
                [num],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if resuming {
            return Ok(None);
        }
    }
    let mut statement = connection
        .prepare(
            "SELECT id FROM commands INDEXED BY commands_target
             WHERE target = ?1 AND generation = ?2 AND verb = 'send'
               AND state IN ('accepted','queued','cancelled','rejected')
             ORDER BY created_at, rowid LIMIT 256",
        )
        .map_err(sql_error)?;
    let ids = statement
        .query_map(params![num, agent.generation], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    let bound = usize::try_from(limits.max_selector_targets)
        .unwrap_or(0)
        .min(core::api::MAX_HANDOFF_SENDS);
    let mut commands = Vec::new();
    for id in ids {
        if commands.len() > bound {
            break;
        }
        if handoff::is_brief_tx(connection, &id)? {
            continue;
        }
        if let Some(record) = load_command_record(connection, &id)?
            && handoff::transferable_send(&record, agent.is_terminal())
        {
            commands.push(id);
        }
    }
    let overflow = commands.len() > bound;
    commands.truncate(bound);
    let goal_active: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM goals WHERE agent = ?1 AND mode = 'eliot'
             AND generation = ?2 AND json_extract(state, '$.status') = 'active')",
            params![num, agent.generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let unfinished = handoff::last_unfinished_turn(connection, num, agent.generation)?;
    let turn_owed = match (reason, unfinished) {
        (HandoffReason::ProviderHold, Some(turn)) => connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM events INDEXED BY events_kind
                 WHERE kind = 'provider.hold_updated' AND agent = ?1 AND generation = ?2
                   AND json_extract(data, '$.turn_id') = ?3)",
                params![num, agent.generation, turn],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sql_error)?,
        (HandoffReason::SessionLost, Some(_)) => true,
        _ => false,
    };
    if commands.is_empty() && !goal_active && !turn_owed {
        return Ok(None);
    }
    // The first real instruction is the task. A successor that never received
    // one carries its predecessor's brief, which quotes the original task.
    let task: Option<String> = connection
        .query_row(
            "SELECT id FROM commands INDEXED BY commands_target
             WHERE target = ?1 AND generation = ?2 AND verb = 'send' AND state <> 'rejected'
             ORDER BY (link_kind IS 'handoff' AND ordinal = 0), created_at, rowid LIMIT 1",
            params![num, agent.generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    // An empty task lets the switch find it in earlier generations or Git.
    Ok(Some(OwedWork {
        task: task.unwrap_or_default(),
        commands,
        overflow,
    }))
}

impl Store {
    /// Agents that owe work and whose session cannot continue it: a quota or
    /// rate-limit hold on an idle Agent, or a loss since `losses_since`.
    pub fn automatic_handoff_candidates(
        &self,
        losses_since: i64,
        now: i64,
        limit: usize,
    ) -> Result<Vec<AutomaticHandoff>> {
        self.ensure_committed_reads()?;
        let mut held = self
            .connection
            .prepare_cached(
                "SELECT a.id, h.kind, h.held_since
                 FROM provider_holds h CROSS JOIN agents a
                 WHERE h.kind IN ('quota','rate_limit')
                   AND a.state = 'idle' AND a.chat = 0 AND a.observe_only = 0
                   AND a.archived = 0 AND a.project <> ''
                   AND h.scope = CASE WHEN json_type(a.profile_snapshot, '$.account') = 'text'
                       THEN 'account:' || json_extract(a.profile_snapshot, '$.account')
                       ELSE 'profile:' || a.profile END
                 ORDER BY a.id LIMIT ?1",
            )
            .map_err(sql_error)?;
        let held = held
            .query_map([HELD_SCAN_LIMIT], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    Some(row.get::<_, String>(1)?),
                    Some(row.get::<_, i64>(2)?),
                ))
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        let mut lost = self
            .connection
            .prepare_cached(
                "SELECT id FROM agents
                 WHERE state IN ('lost','failed') AND chat = 0 AND observe_only = 0
                   AND archived = 0 AND project <> '' AND updated_at BETWEEN ?1 AND ?2
                 ORDER BY updated_at DESC LIMIT ?3",
            )
            .map_err(sql_error)?;
        let lost = lost
            .query_map(
                params![losses_since, now - LOSS_GRACE_MS, LOST_SCAN_LIMIT],
                |row| Ok((row.get::<_, i64>(0)?, None, None)),
            )
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        let mut candidates = Vec::new();
        for (id, kind, held_since) in held.into_iter().chain(lost) {
            if candidates.len() == limit {
                break;
            }
            let Some(agent) = load_agent_by_num(&self.connection, id)? else {
                continue;
            };
            let kind = match kind.as_deref() {
                Some("quota") => Some(core::ProviderRestrictionKind::Quota),
                Some("rate_limit") => Some(core::ProviderRestrictionKind::RateLimit),
                _ => None,
            };
            let reason = if held_since.is_some() {
                HandoffReason::ProviderHold
            } else {
                HandoffReason::SessionLost
            };
            if agent.is_terminal()
                && handoff::session_exit_blocker(&self.connection, &agent)?.is_some()
            {
                continue;
            }
            // A lost worker of an ended manager moves with that manager's successor.
            if let Some(parent) = agent.parent.as_deref()
                && load_agent_by_num(&self.connection, parse_agent_id(parent)?)?
                    .is_none_or(|parent| parent.is_terminal())
            {
                continue;
            }
            let waited: Option<i64> = self
                .connection
                .query_row(
                    "SELECT MAX(ts) FROM events INDEXED BY events_kind
                     WHERE kind = ?1 AND agent = ?2 AND generation = ?3",
                    params![HANDOFF_WAITING, id, agent.generation],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if waited.is_some_and(|ts| ts + WAITING_RETRY_MS > now) {
                continue;
            }
            if owed_work(&self.connection, &agent, reason, self.hierarchy_limits, now)?.is_some() {
                candidates.push(AutomaticHandoff {
                    agent,
                    reason,
                    kind,
                    held_since,
                });
            }
        }
        Ok(candidates)
    }

    /// Reserve the ordinary switch after rechecking the source in one
    /// transaction. `None` means nothing is owed or the source is busy.
    pub fn reserve_automatic_handoff(
        &mut self,
        candidate: &AutomaticHandoff,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Change<Option<SwitchResponse>>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let none = || Change {
                value: None,
                events: Vec::new(),
            };
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let num = parse_agent_id(&candidate.agent.id)?;
            let Some(agent) = load_agent_by_num(&tx, num)?
                .filter(|agent| agent.generation == candidate.agent.generation)
            else {
                return Ok(none());
            };
            let source_matches = match candidate.reason {
                HandoffReason::ProviderHold => {
                    agent.state == AgentState::Idle
                        && candidate
                            .held_since
                            .map_or(Ok(false), |since| hold_is_current(&tx, &agent, since))?
                }
                _ => matches!(agent.state, AgentState::Lost | AgentState::Failed),
            };
            if !source_matches {
                return Ok(none());
            }
            let Some(owed) = owed_work(&tx, &agent, candidate.reason, self.hierarchy_limits, now)?
            else {
                return Ok(none());
            };
            if agent.is_terminal() {
                // Another process's unfinished round can never complete.
                tx.execute(
                    "UPDATE goal_markers SET state = 'unknown', updated_at = ?1
                     WHERE agent = ?2 AND state = 'running' AND holder <> ?3",
                    params![now, num, self.goal_holder],
                )
                .map_err(sql_error)?;
            }
            let request = SwitchRequest {
                id: Ulid::new().to_string(),
                from: agent.id.clone(),
                from_generation: agent.generation,
                task_command: owed.task,
                commands: owed.commands,
                launch: SwitchLaunch::default(),
            };
            let checkout_key = agent.workspace.as_deref().map(core::workspace_path_key);
            if switch_blocker(&tx, &request, checkout_key.as_deref())?.is_some() {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(none());
            }
            let checked = if owed.overflow {
                Err(Error::new(
                    "fanout_limit",
                    "more input is queued than one handoff can carry; move it by hand",
                ))
            } else {
                source_tx(&tx, &request, self.hierarchy_limits, policy).map(|_| ())
            };
            if let Err(error) = checked {
                if error.code == "busy" {
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(none());
                }
                let event = insert_daemon_event(
                    &tx,
                    now,
                    HANDOFF_SETTLED,
                    "handoff",
                    Some(num),
                    Some(agent.generation),
                    json!({
                        "reason": candidate.reason,
                        "kind": candidate.kind,
                        "state": "refused",
                        "error": core::bound_chars(&error.code, 128),
                        "message": core::bound_chars(&policy.redact_text(&error.message), 512),
                    }),
                )?;
                Self::record_automatic_handoff_refusal_tx(&tx, &agent, now)?;
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: None,
                    events: vec![event],
                });
            }
            let mut args = serde_json::to_value(&request)
                .map_err(|_| Error::new("internal", "cannot encode handoff"))?;
            args[CONTINUATION] = serde_json::to_value(SwitchContinuation {
                reason: candidate.reason,
                kind: candidate.kind,
                held_since: candidate.held_since,
            })
            .map_err(|_| Error::new("internal", "cannot encode handoff reason"))?;
            args[OWNER_AUTOMATION] = json!(true);
            let digest = canonical::digest(&args)?;
            if !Self::charge_automatic_handoff_tx(&tx, &agent, &request.id, now)? {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(none());
            }
            let mut change = reserve_switch_tx(&tx, &request, args, &digest, now, None)?;
            change.events.push(insert_daemon_event(
                &tx,
                now,
                HANDOFF_QUEUED,
                "handoff",
                Some(num),
                Some(agent.generation),
                json!({
                    "switch_command": request.id,
                    "task_command": request.task_command,
                    "reason": candidate.reason,
                    "kind": candidate.kind,
                }),
            )?);
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: Some(change.value),
                events: change.events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Record once why an owed handoff has no eligible successor right now.
    pub fn record_handoff_waiting(
        &mut self,
        candidate: &AutomaticHandoff,
        message: &str,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let event = insert_daemon_event(
                &tx,
                now,
                HANDOFF_WAITING,
                "handoff",
                Some(parse_agent_id(&candidate.agent.id)?),
                Some(candidate.agent.generation),
                json!({
                    "reason": candidate.reason,
                    "kind": candidate.kind,
                    "message": core::bound_chars(message, 512),
                }),
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(vec![event])
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// The newest handoff fact for this exact generation.
    pub fn latest_handoff_event(&self, agent: &str, generation: i64) -> Result<Option<Event>> {
        self.ensure_committed_reads()?;
        let row = self
            .connection
            .query_row(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
                 FROM events INDEXED BY events_agent
                 WHERE agent = ?1 AND generation = ?2 AND kind IN (?3, ?4, ?5)
                 ORDER BY seq DESC LIMIT 1",
                params![
                    parse_agent_id(agent)?,
                    generation,
                    HANDOFF_QUEUED,
                    HANDOFF_SETTLED,
                    HANDOFF_WAITING
                ],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        row.map(event_from_record).transpose()
    }
}
