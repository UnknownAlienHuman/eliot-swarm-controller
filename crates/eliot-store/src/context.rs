//! Durable context-injection queue and exact consumer claims.

use super::*;
use eliot_core::{ContextItem, ContextOutcome, InjectArgs, InjectPoint};
use serde::{Deserialize, Serialize};

const CONTEXT_ITEMS_ARG: &str = "context_items";
const MAX_CONTEXT_QUERY_ROWS: usize = 32;
const MAX_CONTEXT_QUERY_BYTES: usize = MAX_FRAME_BYTES;

/// One send dispatch and the FIFO context items committed to that send.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextDispatch {
    pub command: Command,
    pub context_items: Vec<ContextItem>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextClaim {
    consumer_id: String,
    application_point: InjectPoint,
}

#[derive(Clone, Copy)]
struct ContextConsumer<'a> {
    id: &'a str,
    point: InjectPoint,
}

pub(super) fn admit_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    now: i64,
) -> Result<Event> {
    if request.target.is_none()
        || request.selector.is_some()
        || request.request_id.is_some()
        || request.probe
    {
        return Err(Error::new(
            "invalid",
            "inject requires an Agent target and has no request, selector, or probe binding",
        ));
    }
    let args = serde_json::from_value::<InjectArgs>(request.args.clone())
        .map_err(|_| Error::new("invalid", "inject args are malformed"))?;
    if args.point == InjectPoint::NextInput && request.expected_turn_id.is_some() {
        return Err(Error::new(
            "invalid",
            "next_input inject binds its turn only when a send claims it",
        ));
    }
    check_input(&args.text)?;
    let target = request
        .target
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "inject requires an Agent target"))?;
    let agent_num = parse_agent_id(target)?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
    if request
        .expected_generation
        .is_some_and(|expected| expected != agent.generation)
    {
        return Err(Error::new(
            "stale_generation",
            "inject generation does not match the current Agent generation",
        ));
    }
    if agent.generation == 0 || agent.state == AgentState::Created || agent.is_terminal() {
        return Err(Error::new(
            "bad_state",
            "inject requires a live Agent generation",
        ));
    }
    require_context_point_available(&agent, args.point)?;

    let turn_id = match args.point {
        InjectPoint::NextInput => None,
        InjectPoint::TurnSafePoint => {
            if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
                return Err(Error::new(
                    "bad_state",
                    "turn_safe_point inject requires an active turn",
                ));
            }
            let turn_id = agent.active_turn.as_deref().ok_or_else(|| {
                Error::new(
                    "bad_state",
                    "turn_safe_point inject requires an exact active turn",
                )
            })?;
            if request
                .expected_turn_id
                .as_deref()
                .is_some_and(|expected| expected != turn_id)
            {
                return Err(Error::new(
                    "stale_generation",
                    "inject turn does not match the current Agent turn",
                ));
            }
            Some(turn_id.to_owned())
        }
    };

    let event = admit_command(
        tx,
        &CommandInsert {
            turn_id: turn_id.as_deref(),
            ..CommandInsert::admitted(
                principal,
                request,
                payload_digest,
                (agent_num, agent.generation),
                now,
            )
        },
        json!({"command_id": request.id, "verb": "inject"}),
    )?;
    Ok(event)
}

pub(super) fn send_args(args: &Value, items: &[ContextItem]) -> Result<Value> {
    if items.is_empty() {
        return Ok(args.clone());
    }
    let mut args = args.clone();
    let object = args
        .as_object_mut()
        .ok_or_else(|| Error::new("source_integrity", "send args are not an object"))?;
    if object.contains_key(CONTEXT_ITEMS_ARG) {
        return Err(Error::new(
            "source_integrity",
            "send args contain the reserved context_items member",
        ));
    }
    object.insert(
        CONTEXT_ITEMS_ARG.to_owned(),
        serde_json::to_value(items)
            .map_err(|_| Error::new("internal", "context items could not be encoded"))?,
    );
    Ok(args)
}

pub(super) fn claim_next_input_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    agent: &Agent,
    send: &Command,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Vec<ContextItem>> {
    let mut selected = Vec::new();
    let mut selected_ids = Vec::new();
    for record in pending_context_batch(
        tx,
        agent_num,
        agent.generation,
        InjectPoint::NextInput,
        None,
    )? {
        let args = decode_inject_args(&record.command.args)?;
        if args.point != InjectPoint::NextInput {
            continue;
        }
        if args
            .expires_at_ms
            .is_some_and(|expires_at| expires_at <= now)
        {
            reject_pending_context(
                tx,
                &record,
                agent_num,
                agent.generation,
                "expired",
                now,
                events,
            )?;
            continue;
        }
        let item = ContextItem {
            command_id: record.command.id.clone(),
            text: args.text,
            expires_at_ms: args.expires_at_ms,
        };
        let mut candidate = selected.clone();
        candidate.push(item.clone());
        if combined_send_text_len(&send.args, &candidate)? > MAX_INPUT_BYTES {
            reject_pending_context(
                tx,
                &record,
                agent_num,
                agent.generation,
                "frame_too_large",
                now,
                events,
            )?;
            continue;
        }
        let candidate_args = send_args(&send.args, &candidate)?;
        let frame = json!({
            "agent_id": agent.id,
            "generation": agent.generation,
            "command_id": send.id,
            "verb": "send",
            "args": candidate_args,
        });
        match check_rpc_frame("agent.command", &frame) {
            Ok(()) => {
                selected = candidate;
                selected_ids.push(record.command.id.clone());
            }
            Err(error) if error.code == "frame_too_large" => {
                reject_pending_context(
                    tx,
                    &record,
                    agent_num,
                    agent.generation,
                    "frame_too_large",
                    now,
                    events,
                )?;
            }
            Err(error) => return Err(error),
        }
    }

    for command_id in selected_ids {
        claim_context_tx(
            tx,
            &command_id,
            ContextConsumer {
                id: &send.id,
                point: InjectPoint::NextInput,
            },
            agent_num,
            agent.generation,
            now,
            events,
        )?;
    }
    Ok(selected)
}

impl Store {
    /// Claim context for an authenticated native safe-point callback. The
    /// caller proves callback identity; Store rechecks the exact live turn.
    pub fn claim_turn_safe_point_context(
        &mut self,
        agent_id: &str,
        generation: i64,
        turn_id: &str,
        consumer_id: &str,
        now: i64,
    ) -> Result<Change<Vec<ContextItem>>> {
        self.ensure_writer_healthy()?;
        require_nonempty(consumer_id, "consumer_id")?;
        require_nonempty(turn_id, "turn_id")?;
        let result = self.claim_turn_safe_point_context_inner(
            agent_id,
            generation,
            turn_id,
            consumer_id,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    fn claim_turn_safe_point_context_inner(
        &mut self,
        agent_id: &str,
        generation: i64,
        turn_id: &str,
        consumer_id: &str,
        now: i64,
    ) -> Result<Change<Vec<ContextItem>>> {
        let agent_num = parse_agent_id(agent_id)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "context target Agent was not found"))?;
        observer::require_controlled(&agent)?;
        if agent.generation != generation
            || agent.active_turn.as_deref() != Some(turn_id)
            || !matches!(agent.state, AgentState::Busy | AgentState::Waiting)
        {
            return Err(Error::new(
                "stale_generation",
                "context callback does not match the exact current Agent turn",
            ));
        }

        // The callback id is the idempotency key for this exact synchronous
        // request. A recorded empty selection must stay empty if newer context
        // arrives before a transport retry.
        validate_ulid(consumer_id)?;
        let principal = format!("agent:{agent_id}:{generation}");
        let payload = json!({
            "agent_id": agent_id,
            "generation": generation,
            "hook": "before_tool",
            "turn_id": turn_id,
            "consumer_id": consumer_id,
        });
        let payload_digest = canonical::digest(&payload)?;
        if command_binding(&tx, consumer_id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "hook consumer id is already bound to a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, consumer_id)?
        {
            if old_principal != principal
                || old_method != "hook.event"
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "hook consumer id is already bound to another callback",
                ));
            }
            let marker = decode_json(&old_result)?;
            let count = marker
                .get("count")
                .and_then(Value::as_u64)
                .ok_or_else(|| Error::new("source_integrity", "hook claim marker is malformed"))?;
            if count == 0 {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: Vec::new(),
                    events: Vec::new(),
                });
            }
            return Err(Error::new(
                "unknown",
                "hook context was already claimed for this consumer; it will not be redelivered",
            ));
        }
        validate_id_age(consumer_id, now)?;

        let mut events = Vec::new();
        let mut selected = Vec::new();
        let mut selected_ids = Vec::new();
        for record in pending_context_batch(
            &tx,
            agent_num,
            generation,
            InjectPoint::TurnSafePoint,
            Some(turn_id),
        )? {
            let args = decode_inject_args(&record.command.args)?;
            if args.point != InjectPoint::TurnSafePoint
                || record.command.turn_id.as_deref() != Some(turn_id)
            {
                continue;
            }
            if args
                .expires_at_ms
                .is_some_and(|expires_at| expires_at <= now)
            {
                reject_pending_context(
                    &tx,
                    &record,
                    agent_num,
                    generation,
                    "expired",
                    now,
                    &mut events,
                )?;
                continue;
            }
            let item = ContextItem {
                command_id: record.command.id.clone(),
                text: args.text,
                expires_at_ms: args.expires_at_ms,
            };
            let mut candidate = selected.clone();
            candidate.push(item.clone());
            let text_len = candidate
                .iter()
                .fold(0_usize, |sum, item| sum.saturating_add(item.text.len()));
            let text_len =
                text_len.saturating_add(candidate.len().saturating_sub(1).saturating_mul(2));
            let encoded_len = encode_json(&json!({"context": &candidate}))?.len();
            if text_len > MAX_INPUT_BYTES || encoded_len > MAX_FRAME_BYTES {
                reject_pending_context(
                    &tx,
                    &record,
                    agent_num,
                    generation,
                    "frame_too_large",
                    now,
                    &mut events,
                )?;
                continue;
            }
            selected = candidate;
            selected_ids.push(record.command.id.clone());
        }
        for command_id in &selected_ids {
            claim_context_tx(
                &tx,
                command_id,
                ContextConsumer {
                    id: consumer_id,
                    point: InjectPoint::TurnSafePoint,
                },
                agent_num,
                generation,
                now,
                &mut events,
            )?;
        }
        let command_ids_digest = canonical::digest(&json!(selected_ids))?;
        let marker = json!({
            "count": selected_ids.len(),
            "command_ids_digest": command_ids_digest,
        });
        let marker_json = encode_json(&marker)?;
        db!(tx.execute(
            "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
             VALUES (?1, ?2, 'hook.event', ?3, ?4, ?5)",
            params![consumer_id, principal, payload_digest, marker_json, now],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: selected,
            events,
        })
    }
}

pub(super) fn apply_next_input_claims_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    consumer_id: &str,
    turn_id: &str,
    native_event: &NativeEvent,
    now: i64,
) -> Result<Vec<super::native_events::PendingCommandEvent>> {
    let claims = claimed_context_commands(
        tx,
        agent_num,
        generation,
        consumer_id,
        InjectPoint::NextInput,
    )?;
    let receipts = native_event
        .data
        .get("context_outcomes")
        .and_then(Value::as_array);
    if claims.is_empty() {
        if receipts.is_some_and(|receipts| !receipts.is_empty()) {
            return Err(Error::new(
                "source_integrity",
                "turn.started reports context without a matching Store claim",
            ));
        }
        return Ok(Vec::new());
    }
    let receipts = receipts.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "turn.started omitted outcomes for its claimed context",
        )
    })?;
    if receipts.len() != claims.len() {
        return Err(Error::new(
            "source_integrity",
            "turn.started context outcome count does not match the FIFO claim",
        ));
    }

    let mut events = Vec::with_capacity(claims.len());
    for (record, value) in claims.iter().zip(receipts) {
        let outcome = serde_json::from_value::<ContextOutcome>(value.clone()).map_err(|_| {
            Error::new(
                "source_integrity",
                "turn.started context outcome is malformed",
            )
        })?;
        let claim = require_context_claim(record)?;
        validate_context_receipt(record, generation, turn_id, &claim, &outcome)?;
        validate_context_consumer_tx(tx, record, generation, turn_id, &claim)?;
        events.push(apply_context_receipt_tx(
            tx, record, turn_id, &outcome, now,
        )?);
    }
    Ok(events)
}

pub(super) fn settle_turn_safe_point_claims_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    native_event: &NativeEvent,
    now: i64,
) -> Result<Vec<super::native_events::PendingCommandEvent>> {
    let turn_id = required_string(&native_event.data, "turn_id")?;
    let consumer_id = required_string(&native_event.data, "consumer_id")?;
    let claims = claimed_context_commands(
        tx,
        agent_num,
        generation,
        &consumer_id,
        InjectPoint::TurnSafePoint,
    )?;
    let encoded_outcome = native_event.data.get("context_outcome");
    if claims.is_empty() {
        if encoded_outcome.is_some() {
            return Err(Error::new(
                "source_integrity",
                "hook.before_tool reports context without a matching Store claim",
            ));
        }
        return Ok(Vec::new());
    }
    let outcome = encoded_outcome
        .cloned()
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "hook.before_tool omitted the claimed context outcome",
            )
        })
        .and_then(|value| {
            serde_json::from_value::<ContextOutcome>(value)
                .map_err(|_| Error::new("source_integrity", "hook context outcome is malformed"))
        })?;
    if outcome.application_point != InjectPoint::TurnSafePoint || outcome.consumer_id != consumer_id
    {
        return Err(Error::new(
            "source_integrity",
            "hook context outcome does not match its exact callback",
        ));
    }
    let mut events = Vec::with_capacity(claims.len());
    for record in &claims {
        let claim = require_context_claim(record)?;
        if record.command.turn_id.as_deref() != Some(turn_id.as_str()) {
            return Err(Error::new(
                "source_integrity",
                "hook context claim does not match the reported turn",
            ));
        }
        validate_context_receipt(record, generation, &turn_id, &claim, &outcome)?;
        events.push(apply_context_receipt_tx(
            tx, record, &turn_id, &outcome, now,
        )?);
    }
    Ok(events)
}

pub(super) fn settle_send_claims_tx(
    tx: &Transaction<'_>,
    send: &CommandRecord,
    state: CommandState,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(target) = send.target else {
        return Ok(());
    };
    let Some(generation) = send.command.generation else {
        return Ok(());
    };
    let context_state = match state {
        CommandState::Rejected => CommandState::Rejected,
        CommandState::Failed | CommandState::Unknown => CommandState::Unknown,
        _ => return Ok(()),
    };
    for record in claimed_context_commands(
        tx,
        target,
        generation,
        &send.command.id,
        InjectPoint::NextInput,
    )? {
        if !matches!(
            record.state,
            CommandState::Dispatched | CommandState::Unknown
        ) {
            continue;
        }
        let claim = require_context_claim(&record)?;
        let outcome = json!({"claim": claim, "reason": "consumer_input_unconfirmed"});
        update_command_state(
            tx,
            &record.command.id,
            context_state,
            Some(&outcome),
            None,
            now,
        )?;
        let state_name = command_state_name(context_state);
        events.push(insert_daemon_command_event(
            tx,
            now,
            &format!("command.{state_name}"),
            Some(target),
            Some(generation),
            &record.command.id,
            json!({"command_id": record.command.id, "state": state_name, "outcome": outcome}),
        )?);
    }
    Ok(())
}

pub(super) fn cancel_turn_safe_point_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
    now: i64,
) -> Result<Vec<super::native_events::PendingCommandEvent>> {
    let mut events = Vec::new();
    for command_id in pending_turn_safe_point_command_ids(tx, agent_num, generation, turn_id)? {
        let outcome = json!({"reason": "captured_turn_ended"});
        update_command_state(
            tx,
            &command_id,
            CommandState::Cancelled,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(super::native_events::PendingCommandEvent {
            command_id: command_id.clone(),
            data: json!({
                "command_id": command_id,
                "state": "cancelled",
                "outcome": outcome,
            }),
        });
    }
    for (command_id, prior_outcome) in
        dispatched_turn_safe_point_claims(tx, agent_num, generation, turn_id)?
    {
        let outcome =
            unknown_claim_outcome(Some(&prior_outcome), "captured_turn_ended_without_receipt");
        update_command_state(
            tx,
            &command_id,
            CommandState::Unknown,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(super::native_events::PendingCommandEvent {
            command_id: command_id.clone(),
            data: json!({
                "command_id": command_id,
                "state": "unknown",
                "outcome": outcome,
            }),
        });
    }
    Ok(events)
}

pub(super) fn unknown_claim_outcome(outcome: Option<&Value>, reason: &str) -> Value {
    match outcome.and_then(|value| value.get("claim")) {
        Some(claim) => json!({"reason": reason, "claim": claim}),
        None => json!({"reason": reason}),
    }
}

/// Read a bounded FIFO prefix of contexts for one real consumer point.
///
/// The `(created_at, rowid)` keyset walks the partial context index without
/// OFFSET or scanning unrelated command history. Row count and total stored
/// args bytes cap work while the write transaction is held. Rows beyond a
/// budget stay pending in FIFO order for a later consumer request.
fn pending_context_batch(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    point: InjectPoint,
    turn_id: Option<&str>,
) -> Result<Vec<CommandRecord>> {
    const FIRST_SQL: &str = "SELECT id, created_at, rowid, length(CAST(args AS BLOB))
         FROM commands INDEXED BY commands_context_pending
         WHERE target = ?1 AND generation = ?2 AND verb = 'inject'
           AND state IN ('accepted', 'queued')
           AND json_extract(args, '$.point') = ?3 AND turn_id IS ?4
         ORDER BY created_at, rowid LIMIT 1";
    const AFTER_SQL: &str = "SELECT id, created_at, rowid, length(CAST(args AS BLOB))
         FROM commands INDEXED BY commands_context_pending
         WHERE target = ?1 AND generation = ?2 AND verb = 'inject'
           AND state IN ('accepted', 'queued')
           AND json_extract(args, '$.point') = ?3 AND turn_id IS ?4
           AND (created_at, rowid) > (?5, ?6)
         ORDER BY created_at, rowid LIMIT 1";
    let mut first_statement = tx.prepare(FIRST_SQL).map_err(sql_error)?;
    let mut after_statement = tx.prepare(AFTER_SQL).map_err(sql_error)?;
    let point_name = inject_point_name(point);
    let mut cursor = None;
    let mut visited = 0;
    let mut materialized_bytes = 0_usize;
    let mut records = Vec::with_capacity(MAX_CONTEXT_QUERY_ROWS);

    while visited < MAX_CONTEXT_QUERY_ROWS {
        let candidate = if let Some((created_at, rowid)) = cursor {
            let mut rows = after_statement
                .query(params![
                    agent_num, generation, point_name, turn_id, created_at, rowid,
                ])
                .map_err(sql_error)?;
            read_pending_context_candidate(&mut rows)?
        } else {
            let mut rows = first_statement
                .query(params![agent_num, generation, point_name, turn_id])
                .map_err(sql_error)?;
            read_pending_context_candidate(&mut rows)?
        };
        let Some((id, created_at, rowid, args_bytes)) = candidate else {
            break;
        };
        visited += 1;
        cursor = Some((created_at, rowid));

        let args_bytes = usize::try_from(args_bytes).map_err(|_| {
            Error::new(
                "source_integrity",
                "pending context has an invalid stored argument length",
            )
        })?;
        if args_bytes > MAX_CONTEXT_QUERY_BYTES {
            return Err(Error::new(
                "source_integrity",
                "pending context exceeds the maximum command frame size",
            ));
        }
        if args_bytes > MAX_CONTEXT_QUERY_BYTES.saturating_sub(materialized_bytes) {
            break;
        }
        materialized_bytes += args_bytes;
        records.push(
            load_command_record(tx, &id)?
                .ok_or_else(|| Error::new("source_integrity", "context command disappeared"))?,
        );
    }

    Ok(records)
}

fn read_pending_context_candidate(
    rows: &mut rusqlite::Rows<'_>,
) -> Result<Option<(String, i64, i64, i64)>> {
    let Some(row) = rows.next().map_err(sql_error)? else {
        return Ok(None);
    };
    Ok(Some((
        row.get::<_, String>(0).map_err(sql_error)?,
        row.get::<_, i64>(1).map_err(sql_error)?,
        row.get::<_, i64>(2).map_err(sql_error)?,
        row.get::<_, i64>(3).map_err(sql_error)?,
    )))
}

fn pending_turn_safe_point_command_ids(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
) -> Result<Vec<String>> {
    let mut statement = tx
        .prepare(
            "SELECT id FROM commands INDEXED BY commands_context_pending
             WHERE target = ?1 AND generation = ?2 AND verb = 'inject'
               AND state IN ('accepted', 'queued')
               AND json_extract(args, '$.point') = 'turn_safe_point'
               AND turn_id = ?3
             ORDER BY created_at, rowid",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent_num, generation, turn_id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?;
    let mut command_ids = Vec::new();
    for row in rows {
        command_ids.push(row.map_err(sql_error)?);
    }
    Ok(command_ids)
}

fn claimed_context_commands(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    consumer_id: &str,
    point: InjectPoint,
) -> Result<Vec<CommandRecord>> {
    let mut statement = tx
        .prepare(
            "SELECT id FROM commands
             WHERE target = ?1 AND generation = ?2 AND verb = 'inject'
               AND state IN ('dispatched', 'unknown')
             ORDER BY created_at, rowid",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent_num, generation], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(sql_error)?);
    }
    drop(statement);
    let mut claims = Vec::new();
    for id in ids {
        let record = load_command_record(tx, &id)?
            .ok_or_else(|| Error::new("source_integrity", "context claim disappeared"))?;
        let Some(claim) = context_claim(record.command.outcome.as_ref())? else {
            continue;
        };
        if claim.consumer_id == consumer_id && claim.application_point == point {
            claims.push(record);
        }
    }
    Ok(claims)
}

fn dispatched_turn_safe_point_claims(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
) -> Result<Vec<(String, Value)>> {
    let mut statement = tx
        .prepare(
            "SELECT id, outcome FROM commands INDEXED BY commands_target
             WHERE target = ?1 AND generation = ?2 AND verb = 'inject'
               AND state = 'dispatched'
               AND turn_id = ?3
               AND json_extract(outcome, '$.claim.application_point') = 'turn_safe_point'
             ORDER BY created_at, rowid",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![agent_num, generation, turn_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(sql_error)?;
    let mut claims = Vec::new();
    for row in rows {
        let (id, encoded_outcome) = row.map_err(sql_error)?;
        let encoded_outcome = encoded_outcome.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "dispatched context claim has no outcome",
            )
        })?;
        let outcome = decode_json(&encoded_outcome)?;
        let claim = context_claim(Some(&outcome))?.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "dispatched context claim has no claim marker",
            )
        })?;
        if claim.application_point != InjectPoint::TurnSafePoint {
            return Err(Error::new(
                "source_integrity",
                "captured-turn context claim has the wrong application point",
            ));
        }
        claims.push((id, outcome));
    }
    Ok(claims)
}

fn context_claim(outcome: Option<&Value>) -> Result<Option<ContextClaim>> {
    let Some(claim) = outcome.and_then(|value| value.get("claim")) else {
        return Ok(None);
    };
    serde_json::from_value::<ContextClaim>(claim.clone())
        .map(Some)
        .map_err(|_| Error::new("source_integrity", "persisted context claim is malformed"))
}

fn require_context_claim(record: &CommandRecord) -> Result<ContextClaim> {
    if record.command.verb != "inject"
        || !matches!(
            record.state,
            CommandState::Dispatched | CommandState::Unknown
        )
    {
        return Err(Error::new(
            "bad_state",
            "context command is not an unsettled consumer claim",
        ));
    }
    context_claim(record.command.outcome.as_ref())?
        .ok_or_else(|| Error::new("source_integrity", "context command has no durable claim"))
}

fn validate_context_receipt(
    record: &CommandRecord,
    generation: i64,
    turn_id: &str,
    claim: &ContextClaim,
    outcome: &ContextOutcome,
) -> Result<()> {
    if record.command.generation != Some(generation)
        || record
            .command
            .turn_id
            .as_deref()
            .is_some_and(|stored| stored != turn_id)
        || claim.consumer_id != outcome.consumer_id
        || claim.application_point != outcome.application_point
        || outcome.mechanism.trim().is_empty()
        || outcome.consumer_id.trim().is_empty()
    {
        return Err(Error::new(
            "stale_generation",
            "context receipt does not match its exact claim, generation, and turn",
        ));
    }
    Ok(())
}

fn validate_context_consumer_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    generation: i64,
    turn_id: &str,
    claim: &ContextClaim,
) -> Result<()> {
    let target = record
        .target
        .ok_or_else(|| Error::new("source_integrity", "context claim has no Agent target"))?;
    match claim.application_point {
        InjectPoint::NextInput => {
            let send = load_command_record(tx, &claim.consumer_id)?
                .ok_or_else(|| Error::new("stale_generation", "context consumer send is absent"))?;
            if send.target != Some(target)
                || send.command.generation != Some(generation)
                || send.command.verb != "send"
                || send.state != CommandState::Applied
                || send.command.turn_id.as_deref() != Some(turn_id)
            {
                return Err(Error::new(
                    "stale_generation",
                    "context receipt is not bound to its exact applied send turn",
                ));
            }
        }
        InjectPoint::TurnSafePoint => {
            let agent = load_agent_by_num(tx, target)?
                .ok_or_else(|| Error::new("not_found", "context target Agent was not found"))?;
            if agent.generation != generation
                || agent.active_turn.as_deref() != Some(turn_id)
                || !matches!(agent.state, AgentState::Busy | AgentState::Waiting)
            {
                return Err(Error::new(
                    "stale_generation",
                    "context receipt does not match the current captured turn",
                ));
            }
        }
    }
    Ok(())
}

fn apply_context_receipt_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    turn_id: &str,
    outcome: &ContextOutcome,
    now: i64,
) -> Result<super::native_events::PendingCommandEvent> {
    let serialized = serde_json::to_value(outcome)
        .map_err(|_| Error::new("internal", "context outcome could not be encoded"))?;
    update_command_state(
        tx,
        &record.command.id,
        CommandState::Applied,
        Some(&serialized),
        None,
        now,
    )?;
    db!(tx.execute(
        "UPDATE commands SET turn_id = ?1 WHERE id = ?2",
        params![turn_id, record.command.id],
    ));
    Ok(super::native_events::PendingCommandEvent {
        command_id: record.command.id.clone(),
        data: json!({
            "command_id": record.command.id,
            "state": "applied",
            "outcome": serialized,
        }),
    })
}

fn claim_context_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    consumer: ContextConsumer<'_>,
    agent_num: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let record = load_command_record(tx, command_id)?
        .ok_or_else(|| Error::new("source_integrity", "pending context command disappeared"))?;
    if !matches!(record.state, CommandState::Accepted | CommandState::Queued)
        || record.target != Some(agent_num)
        || record.command.generation != Some(generation)
        || decode_inject_args(&record.command.args)?.point != consumer.point
    {
        return Err(Error::new(
            "stale_generation",
            "context command changed before its consumer claim",
        ));
    }
    let claim = ContextClaim {
        consumer_id: consumer.id.to_owned(),
        application_point: consumer.point,
    };
    let outcome = json!({"claim": claim});
    update_command_state(
        tx,
        command_id,
        CommandState::Dispatched,
        Some(&outcome),
        None,
        now,
    )?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        Some(agent_num),
        Some(generation),
        command_id,
        json!({
            "command_id": command_id,
            "verb": "inject",
            "consumer_id": consumer.id,
            "application_point": consumer.point,
        }),
    )?);
    Ok(())
}

fn reject_pending_context(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    agent_num: i64,
    generation: i64,
    code: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let outcome = json!({"code": code});
    update_command_state(
        tx,
        &record.command.id,
        CommandState::Rejected,
        Some(&outcome),
        None,
        now,
    )?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.rejected",
        Some(agent_num),
        Some(generation),
        &record.command.id,
        json!({"command_id": record.command.id, "state": "rejected", "outcome": outcome}),
    )?);
    Ok(())
}

fn require_context_point_available(agent: &Agent, point: InjectPoint) -> Result<()> {
    let Some(context_points) = agent
        .effective
        .as_ref()
        .and_then(|effective| effective.get("context_points"))
    else {
        return Err(Error::new(
            "unsupported",
            "Agent has no consumer for this context application point",
        ));
    };
    let context_points = serde_json::from_value::<Vec<InjectPoint>>(context_points.clone())
        .map_err(|_| {
            Error::new(
                "source_integrity",
                "Agent effective context points are malformed",
            )
        })?;
    if context_points.contains(&point) {
        Ok(())
    } else {
        Err(Error::new(
            "unsupported",
            "Agent has no consumer for this context application point",
        ))
    }
}

fn inject_point_name(point: InjectPoint) -> &'static str {
    match point {
        InjectPoint::NextInput => "next_input",
        InjectPoint::TurnSafePoint => "turn_safe_point",
    }
}

fn decode_inject_args(value: &Value) -> Result<InjectArgs> {
    serde_json::from_value(value.clone())
        .map_err(|_| Error::new("source_integrity", "persisted inject args are malformed"))
}

fn combined_send_text_len(args: &Value, items: &[ContextItem]) -> Result<usize> {
    let send_text = args
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "send command has no text"))?;
    let text_len = items.iter().fold(send_text.len(), |sum, item| {
        sum.saturating_add(item.text.len())
    });
    // The native adapters join claimed context and the original user text
    // with exactly one blank line between each part.
    Ok(text_len.saturating_add(items.len().saturating_mul(2)))
}
