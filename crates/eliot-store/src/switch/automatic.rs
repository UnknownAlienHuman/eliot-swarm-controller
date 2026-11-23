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

#[derive(Clone, Debug)]
pub struct AutomaticHandoff {
    pub agent: Agent,
    pub reason: HandoffReason,
    pub kind: Option<core::ProviderRestrictionKind>,
    pub held_since: Option<i64>,
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

fn latest_attempt(connection: &Connection, agent: &Agent) -> Result<Option<(String, Value, i64)>> {
    let row: Option<(String, String, i64)> = connection
        .query_row(
            "SELECT kind, data, ts FROM events INDEXED BY events_agent
             WHERE agent = ?1 AND generation = ?2 AND kind IN (?3, ?4)
             ORDER BY seq DESC LIMIT 1",
            params![
                parse_agent_id(&agent.id)?,
                agent.generation,
                HANDOFF_QUEUED,
                HANDOFF_SETTLED
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    row.map(|(kind, data, ts)| Ok((kind, decode_json(&data)?, ts)))
        .transpose()
}

/// Whether another automatic attempt is allowed for this generation.
fn attempt_allowed(connection: &Connection, agent: &Agent, now: i64) -> Result<bool> {
    let moved: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM events INDEXED BY events_kind
             WHERE kind = ?1 AND agent = ?2 AND generation = ?3)",
            params![
                handoff::AGENT_HANDED_OFF,
                parse_agent_id(&agent.id)?,
                agent.generation
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if moved {
        return Ok(false);
    }
    let Some((kind, data, ts)) = latest_attempt(connection, agent)? else {
        return Ok(true);
    };
    if kind == HANDOFF_SETTLED {
        // Applied work has moved, and an unknown outcome is never retried.
        return Ok(matches!(data["state"].as_str(), Some("refused" | "failed"))
            && ts + REFUSED_RETRY_MS <= now);
    }
    let Some(id) = data["switch_command"].as_str() else {
        return Ok(false);
    };
    Ok(load_command(connection, id)?.is_none_or(|command| {
        command.state == CommandState::Rejected && command.updated_at + REJECTED_RETRY_MS <= now
    }))
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
                   AND NOT EXISTS(SELECT 1 FROM events x INDEXED BY events_kind
                       WHERE x.kind = 'agent.handed_off' AND x.agent = a.id
                         AND x.generation = a.generation)
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
            if switch_blocker(&tx, &request)?.is_some() {
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
            let mut change = reserve_switch_tx(&tx, &request, args, &digest, now)?;
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
