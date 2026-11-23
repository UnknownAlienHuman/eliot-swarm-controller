//! Constrained owner resolution of unknown commands and turns.
//!
//! A resolution records an owner assertion about an already-uncertain outcome:
//! it moves exactly one unknown command or turn to its asserted terminal state
//! and releases only that command/turn gate. It never invents a native receipt
//! or turn association and never replays an effect. An exact workspace launch
//! failure before native dispatch also settles its created Agent and linked input.
//!
//! Provenance is retained where every readback can see it: a resolved command carries
//! `resolution_source=owner` in its outcome, and each resolution emits a
//! `command.resolved`/`turn.resolved` event with the same marker, and a resolved turn
//! carries it in its row. An owner-resolved turn never reads back as native success.
//!
//! A turn resolves only when its generation is stored terminal (Eliot no
//! longer controls it), when its generation is superseded by a later one
//! (resume proves the old generation ended terminal with no active turn),
//! or when the caller passes a native-readback proof that the exact turn is
//! inactive. The stored active turn alone is never proof.
//! Observe-only Agents have no resolvable commands or turns.

use super::{
    Change, CommandRecord, Error, MAX_EVENT_BYTES, Result, Store, canonical, command_binding,
    command_state_name, commit_transaction, decode_json, encode_json, insert_daemon_command_event,
    insert_daemon_event, load_agent_by_num, load_command_record, load_turn, sql_error,
    turn_state_name, update_command_state, validate_id_age, write_binding,
};
use eliot_core::is_start_verb;
use eliot_core::{
    CommandState, TurnOutcome, TurnState, WorkspacePhase, WorkspacePreparation,
    api::{
        CommandResolution, CommandResolveRequest, CommandResolveResponse, SettleStoppedRequest,
        SettleStoppedResponse, StoppedCommandSettlement, TurnResolveRequest, TurnResolveResponse,
        TurnResolveStatus, inference::sha256_hex,
    },
    parse_agent_id,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

const RESOLVE_COMMAND_METHOD: &str = "command.resolve";
const RESOLVE_TURN_METHOD: &str = "turn.resolve";
const OWNER_ASSERTION: &str = "owner";
const SETTLE_STOPPED_METHOD: &str = "command.settle_stopped";
const STOP_SETTLEMENT_PAGE: usize = 64;

/// Native readback proving one turn inactive, obtained by the Router through
/// the plugin's `agent.snapshot` turn readback before resolving a turn whose
/// generation is still live. The Store checks only the binding: the proof
/// must name the resolved turn. A stored terminal generation needs no proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnInactiveProof {
    pub turn_id: String,
}

/// Exact current Muse session model, read through the owned generation's plugin.
pub struct ConfigureReadback {
    pub agent: String,
    pub generation: i64,
    pub native_ref: String,
    pub model: String,
}

enum CommandReadback<'a> {
    Configure(&'a ConfigureReadback),
    WorkspaceAbsent(&'a WorkspacePreparation),
    RemovalAbsent(
        &'a WorkspacePreparation,
        &'a eliot_core::WorkspaceRemovalOutcome,
    ),
}

impl Store {
    pub fn settle_stopped(
        &mut self,
        request: &SettleStoppedRequest,
        now: i64,
    ) -> Result<Change<SettleStoppedResponse>> {
        request.validate()?;
        self.ensure_committed_reads()?;
        let Some(review) = &request.review else {
            return Ok(Change {
                value: stopped_settlement_page(&self.connection, request)?,
                events: Vec::new(),
            });
        };

        self.ensure_writer_healthy()?;
        let result = self.apply_stopped_settlement(request, review, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn apply_stopped_settlement(
        &mut self,
        request: &SettleStoppedRequest,
        review: &str,
        now: i64,
    ) -> Result<Change<SettleStoppedResponse>> {
        let payload = canonical::digest(&json!({"after": request.after, "review": review}))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        ensure_resolution_write_id(&tx, &request.id)?;
        if let Some((principal, method, digest, result)) = write_binding(&tx, &request.id)? {
            if principal != OWNER_ASSERTION || method != SETTLE_STOPPED_METHOD || digest != payload
            {
                return Err(Error::new(
                    "id_conflict",
                    "stop settlement id has another binding",
                ));
            }
            let value = serde_json::from_value(decode_json(&result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;
        let mut value = stopped_settlement_page(&tx, request)?;
        if value.review != *review {
            return Err(Error::new(
                "conflict",
                "stop settlement evidence changed; preview the page again",
            ));
        }

        let mut events = Vec::new();
        for stop in &value.stops {
            let agent_num = parse_agent_id(&stop.agent)?;
            let record = load_command_record(&tx, &stop.command_id)?.ok_or_else(|| {
                Error::new("source_integrity", "reviewed stop command disappeared")
            })?;
            let command = CommandResolveRequest {
                id: request.id.clone(),
                command_id: stop.command_id.clone(),
                resolution: CommandResolution::Applied,
                reason: "Owned processes exited and the native reader drained. A native stop receipt was unavailable.".into(),
            };
            events.push(resolve_command_tx(
                &tx,
                &record,
                &command,
                CommandState::Applied,
                None,
                now,
            )?);
            if let Some(turn_id) = &stop.interrupted_turn {
                let turn = TurnResolveRequest {
                    id: request.id.clone(),
                    agent: stop.agent.clone(),
                    generation: stop.generation,
                    turn_id: turn_id.clone(),
                    status: TurnResolveStatus::Interrupted,
                    reason: "stopped_by_owner".into(),
                };
                events.push(resolve_turn_tx(&tx, agent_num, &turn, now)?);
            }
            if stop.mark_stopped {
                super::lifecycle::settle_lost_stop_after_owned_exit_tx(
                    &tx,
                    &stop.command_id,
                    now,
                    &mut events,
                )?;
            }
        }
        value.applied = true;
        insert_resolution_write(
            &tx,
            &request.id,
            OWNER_ASSERTION,
            SETTLE_STOPPED_METHOD,
            &payload,
            &value,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value, events })
    }

    /// Resolve one unknown command to its owner-asserted terminal state. The
    /// mutation id binds the assertion for idempotent retries; the command
    /// keeps any committed turn association and gains no invented one.
    pub fn resolve_command(
        &mut self,
        principal: &str,
        request: &CommandResolveRequest,
        now: i64,
    ) -> Result<Change<CommandResolveResponse>> {
        self.resolve_command_with_readback(principal, request, None, now)
    }

    pub fn resolve_command_with_readback(
        &mut self,
        principal: &str,
        request: &CommandResolveRequest,
        readback: Option<&ConfigureReadback>,
        now: i64,
    ) -> Result<Change<CommandResolveResponse>> {
        self.resolve_command_with_proof(
            principal,
            request,
            readback.map(CommandReadback::Configure),
            now,
        )
    }

    /// `preparation` binds the daemon's Git absence readback to the exact
    /// workspace snapshot. The manager holds its effect gates through commit.
    pub fn resolve_command_with_absent_workspace(
        &mut self,
        principal: &str,
        request: &CommandResolveRequest,
        preparation: &WorkspacePreparation,
        now: i64,
    ) -> Result<Change<CommandResolveResponse>> {
        self.resolve_command_with_proof(
            principal,
            request,
            Some(CommandReadback::WorkspaceAbsent(preparation)),
            now,
        )
    }

    pub fn resolve_command_with_absent_removal(
        &mut self,
        request: &CommandResolveRequest,
        preparation: &WorkspacePreparation,
        outcome: &eliot_core::WorkspaceRemovalOutcome,
        now: i64,
    ) -> Result<Change<CommandResolveResponse>> {
        self.resolve_command_with_proof(
            "owner",
            request,
            Some(CommandReadback::RemovalAbsent(preparation, outcome)),
            now,
        )
    }

    fn resolve_command_with_proof(
        &mut self,
        principal: &str,
        request: &CommandResolveRequest,
        readback: Option<CommandReadback<'_>>,
        now: i64,
    ) -> Result<Change<CommandResolveResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.resolve_command_inner(principal, request, readback, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn stored_command_resolution(
        &self,
        principal: &str,
        request: &CommandResolveRequest,
    ) -> Result<Option<CommandResolveResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "command resolution requires the owner principal",
            ));
        }
        ensure_resolution_write_id(&self.connection, &request.id)?;
        stored_command_resolution(
            &self.connection,
            principal,
            request,
            &command_resolution_digest(request)?,
        )
    }

    fn resolve_command_inner(
        &mut self,
        principal: &str,
        request: &CommandResolveRequest,
        readback: Option<CommandReadback<'_>>,
        now: i64,
    ) -> Result<Change<CommandResolveResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "command resolution requires the owner principal",
            ));
        }
        let payload_digest = command_resolution_digest(request)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;

        ensure_resolution_write_id(&tx, &request.id)?;
        if let Some(value) = stored_command_resolution(&tx, principal, request, &payload_digest)? {
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
        if let Some(target) = record.target
            && load_agent_by_num(&tx, target)?.is_some_and(|agent| agent.observe_only)
        {
            return Err(Error::new(
                "not_allowed",
                "observe-only Agents have no resolvable commands",
            ));
        }
        let failed_brief = record.state == CommandState::Failed
            && request.resolution == CommandResolution::Applied
            && super::handoff::is_brief_tx(&tx, &request.command_id)?;
        if record.state != CommandState::Unknown && !failed_brief {
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
        if is_start_verb(record.command.verb.as_str())
            && request.resolution == CommandResolution::Applied
        {
            return Err(Error::new(
                "bad_state",
                "launch/resume resolution must be failed; an applied start needs its Agent transition",
            ));
        }
        if record.command.verb == "configure" && request.resolution == CommandResolution::Applied {
            let configure = match readback {
                Some(CommandReadback::Configure(proof)) => Some(proof),
                _ => None,
            };
            resolve_configure_overlay(&tx, &record, configure, now)?;
        }
        let state = request.resolution.state();
        let workspace_launch = state == CommandState::Failed
            && record.command.verb == "launch"
            && record.command.generation == Some(0)
            && record
                .command
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome["stage"] == "workspace");
        if workspace_launch {
            let Some(CommandReadback::WorkspaceAbsent(proof)) = readback else {
                return Err(Error::new(
                    "bad_state",
                    "managed launch resolution requires Git readback proving the workspace path, registration, and branch are absent",
                ));
            };
            if record.target != Some(parse_agent_id(&proof.agent)?)
                || proof.start_command_id != request.command_id
                || proof.target_generation != 1
                || proof.phase != WorkspacePhase::Unknown
                || super::workspace::load_workspace(&tx, parse_agent_id(&proof.agent)?)?.as_ref()
                    != Some(proof)
            {
                return Err(Error::new(
                    "bad_state",
                    "workspace changed after Git absence readback",
                ));
            }
            if super::workspace::workspace_cancel_requested(proof) {
                let stop_id = proof
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.get("stop_command_id"))
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        Error::new("source_integrity", "workspace cancel has no stop")
                    })?;
                let stop = load_command_record(&tx, stop_id)?.ok_or_else(|| {
                    Error::new("source_integrity", "workspace cancel stop was not found")
                })?;
                if stop.command.verb != "stop"
                    || stop.target != record.target
                    || stop.command.generation != record.command.generation
                {
                    return Err(Error::new(
                        "source_integrity",
                        "workspace cancel is bound to a different stop",
                    ));
                }
                if !stop.state.is_terminal() && stop.state != CommandState::Unknown {
                    return Err(Error::new(
                        "bad_state",
                        "workspace stop is still in flight; read back the stop before resolving launch",
                    ));
                }
            }
        }
        let mut events = Vec::new();
        let removal_outcome =
            if state == CommandState::Applied && record.command.verb == "worktree.remove" {
                let Some(CommandReadback::RemovalAbsent(preparation, outcome)) = readback else {
                    return Err(Error::new(
                        "bad_state",
                        "removal completion requires verified legacy no-write absence",
                    ));
                };
                Some(super::workspace::resolve_legacy_removal_tx(
                    &tx,
                    &record,
                    preparation,
                    outcome,
                    now,
                    &mut events,
                )?)
            } else {
                None
            };
        events.push(resolve_command_tx(
            &tx,
            &record,
            request,
            state,
            removal_outcome,
            now,
        )?);
        if workspace_launch && let Some(agent_num) = record.target {
            super::capacity::release_unstarted_tx(&tx, agent_num, 1, &request.command_id)?;
            super::lifecycle::apply_settle_agent_transition(
                &tx,
                &record,
                state,
                &request.command_id,
                true,
                now,
                &mut events,
            )?;
            // The same daemon proof holds the workspace's quiescence gate.
            // Reconcile records whose last observation preceded a restart.
            tx.execute(
                "UPDATE processes SET state = 'exited', exit_code = NULL, updated_at = ?1
                 WHERE owner = ?2 AND state IN ('running','unknown')",
                params![
                    now,
                    format!("workspace:{}#1", super::format_agent_id(agent_num))
                ],
            )
            .map_err(sql_error)?;
            super::close_drained_workspace_process_source(&tx, agent_num, 1, now, &mut events)?;
        }
        let value = CommandResolveResponse {
            id: request.id.clone(),
            command_id: request.command_id.clone(),
            state,
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
        Ok(Change { value, events })
    }

    /// Resolve one unknown turn to its owner-asserted terminal status. The
    /// turn must already be stored unknown, and either its generation must be
    /// stored terminal or the caller must pass a native-readback proof that
    /// the exact turn is inactive. No command row is touched.
    pub fn resolve_turn(
        &mut self,
        principal: &str,
        request: &TurnResolveRequest,
        proof: Option<&TurnInactiveProof>,
        now: i64,
    ) -> Result<Change<TurnResolveResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.resolve_turn_inner(principal, request, proof, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn resolve_turn_inner(
        &mut self,
        principal: &str,
        request: &TurnResolveRequest,
        proof: Option<&TurnInactiveProof>,
        now: i64,
    ) -> Result<Change<TurnResolveResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "turn resolution requires the owner principal",
            ));
        }
        let payload_digest = turn_resolution_digest(request)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;

        ensure_resolution_write_id(&tx, &request.id)?;
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
        if agent.observe_only {
            return Err(Error::new(
                "not_allowed",
                "observe-only Agents have no resolvable turns",
            ));
        }
        if request.generation > agent.generation {
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
        if request.generation == agent.generation
            && agent.active_turn.as_deref() == Some(request.turn_id.as_str())
        {
            return Err(Error::new(
                "bad_state",
                "turn retains its live native binding",
            ));
        }
        // The stored active turn alone is never proof: a live generation
        // resolves only with a native readback proving this exact turn
        // inactive. A stored terminal generation needs no proof, and neither
        // does a superseded one: resume commits only from a stored terminal
        // generation with no active turn, so a generation the Agent has moved
        // past provably ended. Future generations stay stale.
        let proven_inactive = proof.is_some_and(|proof| proof.turn_id == request.turn_id);
        let superseded = request.generation < agent.generation;
        if !agent.state.is_terminal() && !proven_inactive && !superseded {
            return Err(Error::new(
                "bad_state",
                "turn resolution needs a stored terminal generation or a native readback proving the turn inactive",
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

    /// Stored response for a turn-resolution write id already bound to this
    /// exact assertion, without resolving anything. The Router calls this
    /// before any plugin readback so a replay of a stored resolution does no
    /// native I/O; the resolving transaction still re-checks the binding
    /// before committing. `None` means the id is free; a binding to another
    /// principal, method, or payload — or to a command — is `id_conflict`,
    /// exactly as the resolving transaction would report it.
    pub fn stored_turn_resolution(
        &self,
        principal: &str,
        request: &TurnResolveRequest,
    ) -> Result<Option<TurnResolveResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "turn resolution requires the owner principal",
            ));
        }
        ensure_resolution_write_id(&self.connection, &request.id)?;
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&self.connection, &request.id)?
        {
            if old_principal != principal
                || old_method != RESOLVE_TURN_METHOD
                || old_digest != turn_resolution_digest(request)?
            {
                return Err(Error::new(
                    "id_conflict",
                    "resolution write id is bound to another principal, method, or payload",
                ));
            }
            let value = serde_json::from_value::<TurnResolveResponse>(decode_json(&old_result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    /// Latest send bound to one turn, for native turn readback. The Router
    /// passes this command id to the plugin's `agent.snapshot` so the plugin
    /// can prove the exact turn inactive. `None` means no send carries this
    /// turn association and no readback binding exists.
    pub fn turn_readback_command(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<Option<String>> {
        let agent_num = parse_agent_id(agent)?;
        self.connection
            .query_row(
                "SELECT id FROM commands
                 WHERE target = ?1 AND generation = ?2 AND turn_id = ?3 AND verb = 'send'
                 ORDER BY rowid DESC LIMIT 1",
                params![agent_num, generation, turn_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }
}

fn stopped_settlement_page(
    connection: &Connection,
    request: &SettleStoppedRequest,
) -> Result<SettleStoppedResponse> {
    // Loss clears the projection pointer. An older stop can still recover the
    // turn captured by the same loss, with journal order proving it was in flight.
    let mut statement = connection
        .prepare(
            "SELECT c.id, c.target, c.generation, t.turn_id,
                    a.generation = c.generation AND a.state = 'lost'
             FROM commands c JOIN agents a ON a.id = c.target
         LEFT JOIN turns t ON c.principal = 'owner' AND t.agent = c.target
            AND t.generation = c.generation AND t.turn_id = COALESCE(
                c.expected_turn_id,
                CASE WHEN a.state='lost' AND a.generation=c.generation THEN
                    COALESCE(a.active_turn, (
                        SELECT json_extract(turn_loss.data,'$.turn_id')
                        FROM events AS loss JOIN events AS turn_loss
                          ON turn_loss.agent=loss.agent AND turn_loss.generation=loss.generation
                          AND turn_loss.ts=loss.ts AND turn_loss.seq < loss.seq
                          AND turn_loss.kind='turn.unknown'
                          AND json_extract(turn_loss.data,'$.reason')=json_extract(loss.data,'$.reason')
                        WHERE loss.agent=c.target AND loss.generation=c.generation AND loss.kind='agent.lost'
                          AND EXISTS (
                            SELECT 1 FROM events AS dispatch
                            WHERE dispatch.agent=c.target AND dispatch.generation=c.generation
                              AND dispatch.kind='command.dispatched' AND dispatch.seq < turn_loss.seq
                              AND json_extract(dispatch.data,'$.command_id')=c.id
                          )
                        ORDER BY loss.seq DESC, turn_loss.seq DESC LIMIT 1
                    )) END
            )
            AND t.state = 'unknown'
         WHERE c.state = 'unknown' AND c.verb = 'stop' AND a.observe_only = 0
           AND c.id > COALESCE(?1, '')
           AND (a.generation > c.generation OR
                (a.generation = c.generation AND a.state IN ('stopped', 'failed', 'lost')))
         ORDER BY c.id LIMIT ?2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![request.after, STOP_SETTLEMENT_PAGE as i64 + 1],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            },
        )
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    let next =
        (rows.len() > STOP_SETTLEMENT_PAGE).then(|| rows[STOP_SETTLEMENT_PAGE - 1].0.clone());
    let mut stops = Vec::new();
    let mut awaiting_proof = Vec::new();
    let mut turns = std::collections::HashSet::new();
    let mut lost = std::collections::HashSet::new();
    for (command_id, agent, generation, turn, is_lost) in
        rows.into_iter().take(STOP_SETTLEMENT_PAGE)
    {
        if super::capacity::has_owned_exit_and_reader_drain_tx(connection, agent, generation)? {
            let interrupted_turn = turn.filter(|id| turns.insert((agent, generation, id.clone())));
            stops.push(StoppedCommandSettlement {
                agent: super::format_agent_id(agent),
                generation,
                command_id,
                mark_stopped: is_lost && lost.insert(agent),
                interrupted_turn,
            });
        } else {
            awaiting_proof.push(command_id);
        }
    }
    let review = sha256_hex(
        encode_json(&json!({
            "stops": stops,
            "awaiting_proof": awaiting_proof,
            "next": next,
        }))?
        .as_bytes(),
    );
    Ok(SettleStoppedResponse {
        id: request.id.clone(),
        review,
        applied: false,
        stops,
        awaiting_proof,
        next,
    })
}

fn command_resolution_digest(request: &CommandResolveRequest) -> Result<Vec<u8>> {
    canonical::digest(&json!({
        "command_id": request.command_id,
        "as": request.resolution.as_str(),
        "reason": request.reason,
    }))
}

fn ensure_resolution_write_id(connection: &Connection, id: &str) -> Result<()> {
    if command_binding(connection, id)?.is_some() {
        return Err(Error::new(
            "id_conflict",
            "resolution write id is already used by a command",
        ));
    }
    crate::ensure_no_digest_id(
        connection,
        id,
        "resolution write id is already used by a completion digest",
    )
}

fn stored_command_resolution(
    connection: &Connection,
    principal: &str,
    request: &CommandResolveRequest,
    payload_digest: &[u8],
) -> Result<Option<CommandResolveResponse>> {
    let Some((old_principal, old_method, old_digest, old_result)) =
        write_binding(connection, &request.id)?
    else {
        return Ok(None);
    };
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
    Ok(Some(value))
}

fn resolve_configure_overlay(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    readback: Option<&ConfigureReadback>,
    now: i64,
) -> Result<()> {
    let agent = record
        .target
        .map(|target| load_agent_by_num(tx, target))
        .transpose()?
        .flatten()
        .ok_or_else(|| Error::new("bad_state", "configure has no retained Agent"))?;
    let native = if eliot_core::api::configure_uses_native_session(
        &agent.harness,
        &record.command.args,
    ) {
        let proof = readback.filter(|proof| {
            agent.harness == "muse"
                && agent.state == eliot_core::AgentState::Idle
                && proof.agent == agent.id
                && proof.generation == agent.generation
                && Some(proof.generation) == record.command.generation
                && agent.native_ref.as_deref() == Some(proof.native_ref.as_str())
                && record.command.args.get("model").and_then(serde_json::Value::as_str) == Some(proof.model.as_str())
        }).ok_or_else(|| Error::new("bad_state", "applying an unknown native configure requires exact live configuration readback"))?;
        Some((Some(proof.model.clone()), None))
    } else {
        None
    };
    super::config_overlay::apply_agent_config_tx(tx, record, native, now)?;
    Ok(())
}

/// Digest of one turn-resolution assertion payload, shared by the
/// idempotency pre-check and the resolving transaction so both bind the
/// same bytes.
fn turn_resolution_digest(request: &TurnResolveRequest) -> Result<Vec<u8>> {
    canonical::digest(&json!({
        "agent": request.agent,
        "generation": request.generation,
        "turn_id": request.turn_id,
        "status": request.status.as_str(),
        "reason": request.reason,
    }))
}

/// Move one unknown command to its asserted terminal state and record the
/// owner assertion in the outcome and the event. The shared transition
/// check keeps ordinary resolutions on the Unknown -> Applied/Failed edges.
fn resolve_command_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    request: &CommandResolveRequest,
    state: CommandState,
    outcome: Option<Value>,
    now: i64,
) -> Result<eliot_core::Event> {
    let command_id = record.command.id.as_str();
    let mut outcome = outcome.unwrap_or_else(|| json!({}));
    outcome["resolution_source"] = json!(OWNER_ASSERTION);
    outcome["reason"] = json!(request.reason);
    if encode_json(&outcome)?.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "resolution outcome exceeds 64 KiB",
        ));
    }
    if record.state == CommandState::Failed {
        if !super::handoff::is_brief_tx(tx, command_id)?
            || !eliot_core::handoff_brief_resolution_allowed(record.state, state)
        {
            return Err(Error::new(
                "bad_state",
                "only a failed handoff brief may be owner-applied",
            ));
        }
        super::write_command_state(tx, command_id, state, Some(&outcome), None, now)?;
    } else {
        update_command_state(tx, command_id, state, Some(&outcome), None, now)?;
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
/// native receipt; the row keeps resolution_source owner so no readback mistakes it for native success. Final text and every command row are left untouched.
fn resolve_turn_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    request: &TurnResolveRequest,
    now: i64,
) -> Result<eliot_core::Event> {
    let outcome = match request.status {
        TurnResolveStatus::Completed => TurnOutcome::Success,
        TurnResolveStatus::Failed => TurnOutcome::Failure,
        TurnResolveStatus::Interrupted => TurnOutcome::Interrupted,
    };
    let changed = tx
        .execute(
            "UPDATE turns SET state = ?1, status = ?2, outcome = ?3, completed_at = ?4, resolution_source = 'owner'
             WHERE agent = ?5 AND generation = ?6 AND turn_id = ?7 AND state = 'unknown'",
            params![
                turn_state_name(TurnState::Completed),
                request.status.as_str(),
                outcome.as_str(),
                now,
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
