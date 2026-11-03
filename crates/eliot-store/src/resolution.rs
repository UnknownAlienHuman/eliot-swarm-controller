//! Constrained owner resolution of unknown commands and turns.
//!
//! A resolution records an owner assertion about an already-uncertain outcome:
//! it moves exactly one unknown command or turn to its asserted terminal state
//! and releases only that command/turn gate. It never invents a native receipt
//! or turn association, never replays an effect, and never touches capacity
//! reservations, Agent rows, or any sibling command/turn.

use super::{
    Change, CommandRecord, Error, MAX_EVENT_BYTES, Result, Store, canonical, command_binding,
    command_state_name, commit_transaction, decode_json, encode_json, insert_daemon_command_event,
    insert_daemon_event, load_agent_by_num, load_command_record, load_turn, require_nonempty,
    sql_error, turn_state_name, update_command_state, validate_id_age, write_binding,
};
use eliot_core::{
    CommandState, TurnState,
    api::{CommandResolveRequest, CommandResolveResponse, TurnResolveRequest, TurnResolveResponse},
    parse_agent_id,
};
use rusqlite::{Transaction, TransactionBehavior, params};
use serde_json::json;

const RESOLVE_COMMAND_METHOD: &str = "command.resolve";
const RESOLVE_TURN_METHOD: &str = "turn.resolve";
const OWNER_ASSERTION: &str = "owner";

impl Store {
    /// Resolve one unknown command to its owner-asserted terminal state. The
    /// mutation id binds the assertion for idempotent retries; the command
    /// keeps any committed turn association and gains no invented one.
    pub fn resolve_command(
        &mut self,
        principal: &str,
        request: &CommandResolveRequest,
        now: i64,
    ) -> Result<Change<CommandResolveResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.resolve_command_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn resolve_command_inner(
        &mut self,
        principal: &str,
        request: &CommandResolveRequest,
        now: i64,
    ) -> Result<Change<CommandResolveResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "command resolution requires the owner principal",
            ));
        }
        require_nonempty(principal, "principal")?;
        let semantic_payload = json!({
            "command_id": request.command_id,
            "as": request.resolution.as_str(),
            "reason": request.reason,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;

        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "resolution write id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != RESOLVE_COMMAND_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "resolution write id is bound to another principal, method, or payload",
                ));
            }
            let value = serde_json::from_value::<CommandResolveResponse>(decode_json(&old_result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        let record = load_command_record(&tx, &request.command_id)?.ok_or_else(|| {
            Error::new(
                "not_found",
                format!("command {} was not found", request.command_id),
            )
        })?;
        if record.state != CommandState::Unknown {
            return Err(Error::new(
                "bad_state",
                if record.state.is_terminal() {
                    format!(
                        "command {} is already {}",
                        request.command_id,
                        command_state_name(record.state)
                    )
                } else {
                    format!(
                        "command {} is {}; only unknown commands can be owner-resolved",
                        request.command_id,
                        command_state_name(record.state)
                    )
                },
            ));
        }
        let event = resolve_command_tx(&tx, &record, request, request.resolution.state(), now)?;
        let value = CommandResolveResponse {
            id: request.id.clone(),
            command_id: request.command_id.clone(),
            state: request.resolution.state(),
        };
        insert_resolution_write(
            &tx,
            &request.id,
            principal,
            RESOLVE_COMMAND_METHOD,
            &payload_digest,
            &value,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value,
            events: vec![event],
        })
    }

    /// Resolve one unknown turn to its owner-asserted terminal status. The
    /// turn must already be stored unknown, and the Agent row must prove it
    /// inactive or its generation terminal. No command row is touched.
    pub fn resolve_turn(
        &mut self,
        principal: &str,
        request: &TurnResolveRequest,
        now: i64,
    ) -> Result<Change<TurnResolveResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.resolve_turn_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn resolve_turn_inner(
        &mut self,
        principal: &str,
        request: &TurnResolveRequest,
        now: i64,
    ) -> Result<Change<TurnResolveResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "turn resolution requires the owner principal",
            ));
        }
        require_nonempty(principal, "principal")?;
        let semantic_payload = json!({
            "agent": request.agent,
            "generation": request.generation,
            "turn_id": request.turn_id,
            "status": request.status.as_str(),
            "reason": request.reason,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;

        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "resolution write id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != RESOLVE_TURN_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "resolution write id is bound to another principal, method, or payload",
                ));
            }
            let value = serde_json::from_value::<TurnResolveResponse>(decode_json(&old_result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        let agent_num = parse_agent_id(&request.agent)?;
        let agent = load_agent_by_num(&tx, agent_num)?.ok_or_else(|| {
            Error::new(
                "not_found",
                format!("Agent {} was not found", request.agent),
            )
        })?;
        if agent.generation != request.generation {
            return Err(Error::new(
                "stale_generation",
                "turn resolution requires the Agent's current generation",
            ));
        }
        let turn =
            load_turn(&tx, agent_num, request.generation, &request.turn_id)?.ok_or_else(|| {
                Error::new(
                    "not_found",
                    format!("turn {} was not found", request.turn_id),
                )
            })?;
        if turn.state != TurnState::Unknown {
            return Err(Error::new(
                "bad_state",
                if turn.state == TurnState::Running {
                    format!("turn {} is still active", request.turn_id)
                } else {
                    format!("turn {} is already terminal", request.turn_id)
                },
            ));
        }
        if agent.active_turn.as_deref() == Some(request.turn_id.as_str())
            && !agent.state.is_terminal()
        {
            return Err(Error::new(
                "bad_state",
                format!("turn {} is still active", request.turn_id),
            ));
        }
        let event = resolve_turn_tx(&tx, agent_num, request, now)?;
        let value = TurnResolveResponse {
            id: request.id.clone(),
            agent: request.agent.clone(),
            generation: request.generation,
            turn_id: request.turn_id.clone(),
            status: request.status,
        };
        insert_resolution_write(
            &tx,
            &request.id,
            principal,
            RESOLVE_TURN_METHOD,
            &payload_digest,
            &value,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value,
            events: vec![event],
        })
    }
}

/// Move one unknown command to its asserted terminal state and record the
/// owner assertion on the row, outcome, and event. The shared transition
/// check keeps this to the Unknown -> Applied/Failed edges; no Agent,
/// capacity, or sibling state is touched.
fn resolve_command_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    request: &CommandResolveRequest,
    state: CommandState,
    now: i64,
) -> Result<eliot_core::Event> {
    let command_id = record.command.id.as_str();
    let outcome = json!({
        "resolution_source": OWNER_ASSERTION,
        "reason": request.reason,
    });
    if encode_json(&outcome)?.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "resolution outcome exceeds 64 KiB",
        ));
    }
    update_command_state(tx, command_id, state, Some(&outcome), None, now)?;
    let changed = tx
        .execute(
            "UPDATE commands SET resolution_source = ?1 WHERE id = ?2",
            params![OWNER_ASSERTION, command_id],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "resolved command changed before its assertion was stored",
        ));
    }
    insert_daemon_command_event(
        tx,
        now,
        "command.resolved",
        record.target,
        record.command.generation,
        command_id,
        json!({
            "command_id": command_id,
            "state": command_state_name(state),
            "write_id": request.id,
            "resolution_source": OWNER_ASSERTION,
            "reason": request.reason,
        }),
    )
}

/// Complete one proven-inactive unknown turn with its asserted status. The
/// stored status and completion time describe the owner assertion, not a
/// native receipt; final text and every command row are left untouched.
fn resolve_turn_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    request: &TurnResolveRequest,
    now: i64,
) -> Result<eliot_core::Event> {
    let changed = tx
        .execute(
            "UPDATE turns SET state = ?1, status = ?2, completed_at = ?3,
                              resolution_source = ?4
             WHERE agent = ?5 AND generation = ?6 AND turn_id = ?7 AND state = 'unknown'",
            params![
                turn_state_name(TurnState::Completed),
                request.status.as_str(),
                now,
                OWNER_ASSERTION,
                agent_num,
                request.generation,
                request.turn_id,
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "resolved turn changed before its assertion was stored",
        ));
    }
    insert_daemon_event(
        tx,
        now,
        "turn.resolved",
        "turn",
        Some(agent_num),
        Some(request.generation),
        json!({
            "turn_id": request.turn_id,
            "status": request.status.as_str(),
            "write_id": request.id,
            "resolution_source": OWNER_ASSERTION,
            "reason": request.reason,
        }),
    )
}

fn insert_resolution_write(
    tx: &Transaction<'_>,
    id: &str,
    principal: &str,
    method: &str,
    payload_digest: &[u8],
    value: &impl serde::Serialize,
    now: i64,
) -> Result<()> {
    let result_json = encode_json(value)?;
    if result_json.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "resolution result exceeds 64 KiB",
        ));
    }
    tx.execute(
        "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, principal, method, payload_digest, result_json, now],
    )
    .map_err(sql_error)?;
    Ok(())
}
