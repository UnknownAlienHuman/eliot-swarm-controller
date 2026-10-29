//! Durable provider restriction holds and exact SEND probe claims.

use super::{
    Change, Error, Result, Store, canonical, command_binding, commit_transaction, decode_json,
    encode_json, load_agent_by_num, require_nonempty, sql_error, validate_id_age, write_binding,
};
mod admin;
use admin::{clear_hold_tx, get, list_page};

use eliot_core::{
    self as core, Agent, Command, CommandState, ProviderHold, ProviderHoldState,
    ProviderRestriction, ProviderRestrictionKind, TurnState,
    api::{ProviderClearHoldRequest, ProviderClearHoldResponse, ProviderHoldsResponse},
    parse_agent_id,
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

const CLEAR_HOLD_METHOD: &str = "provider.clear_hold";
const INTERVENING_RESTRICTION_BATCH: i64 = 64;

impl Store {
    /// Return the Core-owned bounded page, ordered by the frozen provider scope.
    pub fn list_provider_holds(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProviderHoldsResponse> {
        list_page(&self.connection, after, limit)
    }

    /// Clear a hold using the Core request's closed corrective evidence and
    /// compare-and-swap probe binding. The mutation result and delete commit
    /// together so retries return the original outcome.
    pub fn clear_provider_hold(
        &mut self,
        principal: &str,
        request: &ProviderClearHoldRequest,
        now: i64,
    ) -> Result<Change<ProviderClearHoldResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.clear_provider_hold_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn clear_provider_hold_inner(
        &mut self,
        principal: &str,
        request: &ProviderClearHoldRequest,
        now: i64,
    ) -> Result<Change<ProviderClearHoldResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "provider hold correction requires the owner principal",
            ));
        }
        require_nonempty(principal, "principal")?;
        let semantic_payload = json!({
            "scope": request.scope,
            "corrective_evidence": request.corrective_evidence.as_str(),
            "expected_probe_command": request.expected_probe_command,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;

        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "provider hold write id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != CLEAR_HOLD_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "provider hold write id is bound to another principal, method, or payload",
                ));
            }
            let value =
                serde_json::from_value::<ProviderClearHoldResponse>(decode_json(&old_result)?)
                    .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        let cleared = match clear_hold_tx(
            &tx,
            &request.scope,
            request.expected_probe_command.as_deref(),
        ) {
            Ok(cleared) => cleared,
            Err(error)
                if error.code == "bad_state"
                    && request.expected_probe_command.is_some()
                    && get(&tx, &request.scope)?.is_none() =>
            {
                false
            }
            Err(error) => return Err(error),
        };
        let value = ProviderClearHoldResponse {
            id: request.id.clone(),
            scope: request.scope.clone(),
            cleared,
        };
        request.validate_response(&value)?;
        let events = if cleared {
            vec![super::insert_daemon_event(
                &tx,
                now,
                "provider.hold_cleared",
                "provider",
                None,
                None,
                json!({
                    "scope": request.scope,
                    "write_id": request.id,
                    "corrective_evidence": request.corrective_evidence.as_str(),
                }),
            )?]
        } else {
            Vec::new()
        };
        let result_json = encode_json(&value)?;
        tx.execute(
            "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                request.id,
                principal,
                CLEAR_HOLD_METHOD,
                payload_digest,
                result_json,
                now,
            ],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value, events })
    }
}

pub(super) fn require_unheld_tx(tx: &Transaction<'_>, agent: &Agent) -> Result<()> {
    let scope = core::frozen_provider_scope(agent)?;
    if get(tx, &scope)?.is_some() {
        return Err(provider_held());
    }
    Ok(())
}

/// Bind a probe claim only after its accepted command row exists in this same
/// admission transaction. Ordinary sends remain pre-binding rejections.
pub(super) fn admit_send_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    command_id: &str,
    probe: bool,
    now: i64,
) -> Result<Option<core::Event>> {
    let scope = core::frozen_provider_scope(agent)?;
    let Some(hold) = get(tx, &scope)? else {
        return if probe {
            Err(Error::new(
                "bad_state",
                "a provider probe requires an eligible rate-limit or quota hold",
            ))
        } else {
            Ok(None)
        };
    };

    if !probe {
        return Err(provider_held());
    }
    if !matches!(
        hold.kind,
        ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
    ) {
        return Err(provider_held());
    }
    if hold.state != ProviderHoldState::Held || hold.probe_command.is_some() {
        return Err(provider_held());
    }
    if hold
        .retry_after
        .is_some_and(|retry_after| retry_after > now)
    {
        return Err(Error::new(
            "bad_state",
            "provider probe is not eligible before retry_after",
        ));
    }

    let agent_num = parse_agent_id(&agent.id)?;
    let command_is_probe: bool = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands
                 WHERE id = ?1 AND target = ?2 AND generation = ?3 AND verb = 'send'
                   AND probe = 1 AND state IN ('accepted','queued')
             )",
            params![command_id, agent_num, agent.generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if !command_is_probe {
        return Err(Error::new(
            "source_integrity",
            "provider probe claim is not bound to its admitted SEND",
        ));
    }
    let changed = tx
        .execute(
            "UPDATE provider_holds
             SET state = 'probing', probe_command = ?1, updated_at = ?2
             WHERE scope = ?3 AND kind IN ('rate_limit','quota')
               AND state = 'held' AND probe_command IS NULL
               AND (retry_after IS NULL OR retry_after <= ?2)",
            params![command_id, now, scope],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(provider_held());
    }
    Ok(Some(super::insert_daemon_event(
        tx,
        now,
        "provider.hold_updated",
        "provider",
        Some(agent_num),
        Some(agent.generation),
        json!({
            "scope": scope,
            "kind": hold.kind.as_str(),
            "state": ProviderHoldState::Probing.as_str(),
            "probe_command": command_id,
        }),
    )?))
}

/// Recheck current hold state at the effect boundary. A probe must match its
/// already-persisted claim exactly; this function never reacquires a claim.
pub(super) fn require_dispatch_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    command: &Command,
) -> Result<()> {
    let scope = core::frozen_provider_scope(agent)?;
    let Some(hold) = get(tx, &scope)? else {
        if command.probe {
            return Err(Error::new(
                "bad_state",
                "provider probe lost its durable hold claim",
            ));
        }
        return Ok(());
    };

    if command.probe
        && hold.state == ProviderHoldState::Probing
        && hold.probe_command.as_deref() == Some(command.id.as_str())
        && matches!(
            hold.kind,
            ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
        )
    {
        return Ok(());
    }
    Err(provider_held())
}

/// Persist structured restrictions only from the exact controlled terminal
/// effect; resolve a probe claim only from its exact terminal command binding.
pub(super) fn terminal_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    effect: &core::Effect,
    native_event: &core::NativeEvent,
    now: i64,
) -> Result<Vec<core::Event>> {
    let core::Effect::TurnTerminal {
        command_id: Some(command_id),
        turn_id,
        state,
        status,
        ..
    } = effect
    else {
        return Ok(Vec::new());
    };
    let agent_num = parse_agent_id(&agent.id)?;
    let generation = agent.generation;
    if native_event.kind != "turn.completed"
        || native_event.data.get("command_id").and_then(Value::as_str) != Some(command_id.as_str())
        || native_event.data.get("turn_id").and_then(Value::as_str) != Some(turn_id.as_str())
        || agent.observe_only
    {
        return Err(Error::new(
            "source_integrity",
            "provider hold fact is not bound to the controlled terminal turn",
        ));
    }
    let command = super::load_command_record(tx, command_id)?.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "provider hold terminal references an unknown command",
        )
    })?;
    if command.target != Some(agent_num)
        || command.command.generation != Some(generation)
        || command.command.verb != "send"
        || command.command.turn_id.as_deref() != Some(turn_id.as_str())
        || command.state != CommandState::Applied
    {
        return Err(Error::new(
            "source_integrity",
            "provider hold terminal is not bound to the exact applied SEND",
        ));
    }

    let restriction_present = native_event.data.get("provider_restriction").is_some();
    let mut events = Vec::new();
    if let Some(value) = native_event.data.get("provider_restriction")
        && let Some(restriction) = parse_restriction(value)
        && let Some(event) = record_restriction_tx(tx, agent, &restriction, now)?
    {
        events.push(event);
    }

    if command.command.probe {
        let scope = core::frozen_provider_scope(agent)?;
        let Some(hold) = get(tx, &scope)? else {
            return Ok(events);
        };
        if hold.state != ProviderHoldState::Probing
            || hold.probe_command.as_deref() != Some(command_id)
        {
            return Ok(events);
        }

        let successful = *state == TurnState::Completed
            && status.as_deref() == Some("completed")
            && !restriction_present
            && matches!(
                hold.kind,
                ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
            )
            && !has_intervening_same_scope_restriction_tx(
                tx,
                &scope,
                command_id,
                agent_num,
                generation,
                native_event,
            )?;
        if successful {
            let changed = tx
                .execute(
                    "DELETE FROM provider_holds
                     WHERE scope = ?1 AND state = 'probing' AND probe_command = ?2
                       AND kind IN ('rate_limit','quota')",
                    params![scope, command_id],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(Error::new(
                    "source_integrity",
                    "provider probe hold changed before successful terminal commit",
                ));
            }
            events.push(super::insert_daemon_event(
                tx,
                now,
                "provider.hold_cleared",
                "provider",
                Some(agent_num),
                Some(generation),
                json!({
                    "scope": scope,
                    "probe_command": command_id,
                    "reason": "probe_succeeded",
                }),
            )?);
        } else {
            if release_probe_claim_tx(tx, command_id, now)? {
                events.push(super::insert_daemon_event(
                    tx,
                    now,
                    "provider.hold_updated",
                    "provider",
                    Some(agent_num),
                    Some(generation),
                    json!({
                        "scope": scope,
                        "probe_command": command_id,
                        "state": ProviderHoldState::Held.as_str(),
                        "reason": "probe_terminal",
                    }),
                )?);
            }
        }
    }
    Ok(events)
}

/// A known terminal command result releases only that exact probe claim. An
/// unknown command keeps the claim, and applied sends are resolved by turn fact.
pub(super) fn command_terminal_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    previous: CommandState,
    state: CommandState,
    now: i64,
) -> Result<()> {
    if state == CommandState::Rejected
        || (state == CommandState::Cancelled
            && matches!(previous, CommandState::Accepted | CommandState::Queued))
    {
        release_probe_claim_tx(tx, command_id, now)?;
    }
    Ok(())
}

fn record_restriction_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    restriction: &ProviderRestriction,
    now: i64,
) -> Result<Option<core::Event>> {
    if restriction
        .retry_after
        .is_some_and(|retry_after| retry_after < 0)
    {
        return Err(Error::new(
            "source_integrity",
            "provider restriction retry_after must be non-negative",
        ));
    }
    let scope = core::frozen_provider_scope(agent)?;
    let current = get(tx, &scope)?;
    if let Some(current) = &current
        && is_permanent(current.kind)
        && !is_permanent(restriction.kind)
    {
        // A delayed rate/quota result cannot downgrade a permanent hold.
        return Ok(None);
    }
    let (probe_command, state) = current
        .map(|hold| (hold.probe_command, hold.state))
        .unwrap_or((None, ProviderHoldState::Held));
    let hold = ProviderHold {
        scope: scope.clone(),
        kind: restriction.kind,
        retry_after: restriction.retry_after,
        probe_command: probe_command.clone(),
        state,
        updated_at: now,
    };
    hold.validate()?;
    tx.execute(
        "INSERT INTO provider_holds(scope, kind, retry_after, probe_command, state, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(scope) DO UPDATE SET kind = excluded.kind,
             retry_after = excluded.retry_after, probe_command = excluded.probe_command,
             state = excluded.state, updated_at = excluded.updated_at",
        params![
            hold.scope,
            hold.kind.as_str(),
            hold.retry_after,
            hold.probe_command,
            hold.state.as_str(),
            hold.updated_at,
        ],
    )
    .map_err(sql_error)?;
    Ok(Some(super::insert_daemon_event(
        tx,
        now,
        "provider.hold_updated",
        "provider",
        Some(parse_agent_id(&agent.id)?),
        Some(agent.generation),
        json!({
            "scope": hold.scope,
            "kind": hold.kind.as_str(),
            "retry_after": hold.retry_after,
            "probe_command": hold.probe_command,
            "state": hold.state.as_str(),
        }),
    )?))
}

fn release_probe_claim_tx(tx: &Transaction<'_>, command_id: &str, now: i64) -> Result<bool> {
    let changed = tx
        .execute(
            "UPDATE provider_holds
         SET probe_command = NULL, state = 'held', updated_at = ?1
         WHERE probe_command = ?2 AND state = 'probing'",
            params![now, command_id],
        )
        .map_err(sql_error)?;
    Ok(changed == 1)
}

fn has_intervening_same_scope_restriction_tx(
    tx: &Transaction<'_>,
    scope: &str,
    probe_command: &str,
    probe_agent: i64,
    generation: i64,
    terminal: &core::NativeEvent,
) -> Result<bool> {
    let accepted_seq: Option<i64> = tx
        .query_row(
            "SELECT MAX(seq) FROM events
             WHERE agent = ?1 AND generation = ?2 AND kind = 'command.accepted'
               AND json_extract(data, '$.command_id') = ?3",
            params![probe_agent, generation, probe_command],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let accepted_seq = accepted_seq.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "provider probe has no durable accepted event",
        )
    })?;
    let terminal_seq: Option<i64> = tx
        .query_row(
            "SELECT seq FROM events
             WHERE agent = ?1 AND generation = ?2 AND kind = 'turn.completed'
               AND native_id = ?3 AND json_extract(data, '$.command_id') = ?4
               AND json_extract(data, '$.turn_id') = ?5
             ORDER BY seq DESC LIMIT 1",
            params![
                probe_agent,
                generation,
                terminal.native_id,
                probe_command,
                terminal.data.get("turn_id").and_then(Value::as_str),
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let terminal_seq = terminal_seq.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "provider probe terminal event was not stored before settlement",
        )
    })?;
    let mut after = accepted_seq;
    loop {
        let candidates = {
            let mut statement = tx
                .prepare(
                    "SELECT e.seq, e.agent, json_extract(e.data, '$.command_id'),
                            json_extract(e.data, '$.provider_restriction')
                     FROM events e JOIN agents a ON a.id = e.agent
                     WHERE e.seq > ?1 AND e.seq < ?2 AND e.kind = 'turn.completed'
                       AND e.native_id IS NOT NULL AND a.observe_only = 0
                       AND e.source NOT IN ('daemon','os','git','workspace_process')
                       AND json_type(e.data, '$.provider_restriction') = 'object'
                     ORDER BY e.seq LIMIT ?3",
                )
                .map_err(sql_error)?;
            let rows = statement
                .query_map(
                    params![after, terminal_seq, INTERVENING_RESTRICTION_BATCH],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .map_err(sql_error)?;
            let mut candidates = Vec::new();
            for row in rows {
                candidates.push(row.map_err(sql_error)?);
            }
            candidates
        };
        if candidates.is_empty() {
            return Ok(false);
        }
        for (seq, candidate_agent_num, candidate_command, raw_restriction) in candidates {
            after = seq;
            if candidate_command.as_deref() == Some(probe_command) {
                continue;
            }
            let Some(raw_restriction) = raw_restriction else {
                continue;
            };
            let Ok(restriction) = serde_json::from_str::<ProviderRestriction>(&raw_restriction)
            else {
                continue;
            };
            if restriction
                .retry_after
                .is_some_and(|retry_after| retry_after < 0)
            {
                continue;
            }
            let Some(candidate_agent) = load_agent_by_num(tx, candidate_agent_num)? else {
                continue;
            };
            if super::observer::require_controlled(&candidate_agent).is_ok()
                && core::frozen_provider_scope(&candidate_agent)
                    .ok()
                    .as_deref()
                    == Some(scope)
            {
                return Ok(true);
            }
        }
    }
}

fn parse_restriction(value: &Value) -> Option<ProviderRestriction> {
    let restriction = serde_json::from_value::<ProviderRestriction>(value.clone()).ok()?;
    if restriction
        .retry_after
        .is_some_and(|retry_after| retry_after < 0)
    {
        return None;
    }
    Some(restriction)
}

fn is_permanent(kind: ProviderRestrictionKind) -> bool {
    matches!(
        kind,
        ProviderRestrictionKind::Auth | ProviderRestrictionKind::ModelRemoved
    )
}

fn provider_held() -> Error {
    Error::new(
        "provider_held",
        "provider scope is held pending owner correction or an eligible probe",
    )
}
