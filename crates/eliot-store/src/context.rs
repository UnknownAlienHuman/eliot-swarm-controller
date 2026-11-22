//! Durable context-injection queue and exact consumer claims.

mod facts;
pub(super) mod notes;

use facts::{edit_note_text, edit_path_key, new_note_paths};

use super::*;
use eliot_core::{
    ContextEditPath, ContextItem, ContextOutcome, InjectArgs, InjectPoint,
    api::harness::MAX_HARNESS_MODEL_ID_BYTES, swarm_note, swarm_note_with_trusted_review_labels,
    turn_started_data,
};
use serde::{Deserialize, Serialize};

const CONTEXT_ITEMS_ARG: &str = "context_items";

const MAX_CONTEXT_QUERY_ROWS: usize = 32;
const MAX_CONTEXT_QUERY_BYTES: usize = MAX_FRAME_BYTES;
const PROJECT_CHECK_PRINCIPAL: &str = "project_check";
enum FactNote<'a> {
    Text { text: &'a str, id: Option<&'a str> },
    Edits(&'a [ContextEditPath]),
}

pub(super) struct FactOrigin<'a> {
    seq: i64,
    paths: &'a [ContextEditPath],
}

type ContextCursor = (i64, i64);
type KeyedCommand = (i64, i64, CommandRecord);
type ContextPage = (Vec<KeyedCommand>, Option<ContextCursor>);

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

struct ClaimedContextRecord {
    command_id: String,
    verb: String,
    target: i64,
    generation: i64,
    turn_id: Option<String>,
    state: CommandState,
    claim: ContextClaim,
    moved: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ContextConsumer<'a> {
    pub(super) id: &'a str,
    pub(super) point: InjectPoint,
}

impl Store {
    /// Most recent proven handoff input that this Agent has already accepted.
    pub fn task_card_handoff_brief(&self, agent: &str, generation: i64) -> Result<Option<Command>> {
        let id: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM commands INDEXED BY commands_target
             WHERE target = ?1 AND generation <= ?2 AND verb = 'send'
               AND link_kind = 'handoff' AND ordinal = 0
             ORDER BY created_at DESC, rowid DESC LIMIT 1",
                params![parse_agent_id(agent)?, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        id.map(|id| self.get_command(&id))
            .transpose()
            .map(|command| {
                command
                    .flatten()
                    .filter(|command| command.state == CommandState::Applied)
            })
    }

    pub fn session_ready_event(&self, agent: &str, generation: i64) -> Result<Option<Event>> {
        let record = self
            .connection
            .query_row(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
             FROM events INDEXED BY events_agent
             WHERE agent = ?1 AND generation = ?2 AND kind = 'session.ready' AND source = 'stream'
             ORDER BY seq DESC LIMIT 1",
                params![parse_agent_id(agent)?, generation],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        record.map(event_from_record).transpose()
    }

    /// Retained native edit paths and observed check execution for one turn.
    pub fn turn_insertion_evidence(
        &self,
        turn: &Turn,
        through: i64,
        check: &[String],
    ) -> Result<(Vec<String>, bool)> {
        let agent = parse_agent_id(&turn.agent)?;
        let Some(started) = turn.started_at else {
            return Ok((Vec::new(), false));
        };
        let mut statement = self
            .connection
            .prepare(
                "SELECT path.value FROM events AS edit INDEXED BY events_agent
             JOIN json_each(CASE
                WHEN edit.kind IN ('hook.after_tool', 'tool.file.edited') THEN json_array(CASE
                  WHEN json_extract(edit.data, '$.edit_path.kind') = 'relative' THEN json_extract(edit.data, '$.edit_path.path')
                  WHEN json_type(edit.data, '$.edit_path') = 'text' THEN json_extract(edit.data, '$.edit_path')
                  ELSE NULL END)
                WHEN json_type(edit.data, '$.changed_paths') = 'array' THEN json_extract(edit.data, '$.changed_paths')
                ELSE '[]' END) AS path
             WHERE edit.agent = ?1 AND edit.generation = ?2 AND edit.seq < ?3
               AND edit.source = 'stream' AND edit.native_id IS NOT NULL
               AND json_extract(edit.data, '$.redacted') IS NOT 1
               AND json_extract(edit.data, '$.turn_id') = ?4
               AND (edit.kind IN ('hook.after_tool', 'tool.file.edited') OR (edit.kind = 'tool.completed'
                    AND json_extract(edit.data, '$.tool') = 'fileChange'
                    AND json_extract(edit.data, '$.status') = 'completed'))
               AND path.type = 'text'
             ORDER BY edit.seq DESC LIMIT 128",
            )
            .map_err(sql_error)?;
        let paths = statement
            .query_map(
                params![agent, turn.generation, through, turn.turn_id],
                |row| row.get(0),
            )
            .map_err(sql_error)?
            .collect::<std::result::Result<Vec<String>, _>>()
            .map_err(sql_error)?;
        let checked = self.connection.query_row(
            "SELECT EXISTS (
                SELECT 1 FROM events AS e INDEXED BY events_agent
                JOIN events AS changed ON changed.seq = json_extract(e.data, '$.edit_seq')
                WHERE e.agent = ?1 AND e.generation = ?2 AND e.seq < ?3 AND e.ts >= ?4
                  AND e.kind = 'project.check.finished' AND e.source = 'project'
                  AND changed.agent = e.agent AND changed.generation = e.generation
                  AND changed.ts >= ?4 AND json_type(e.data, '$.exit_code') = 'integer'
             ) OR EXISTS (
                SELECT 1 FROM events AS start INDEXED BY events_agent
                JOIN events AS done INDEXED BY events_agent
                  ON done.agent = start.agent AND done.generation = start.generation
                  AND done.source = start.source AND done.native_id IS NOT NULL
                  AND done.kind = 'tool.completed' AND done.seq > start.seq AND done.seq < ?3
                  AND json_extract(done.data, '$.turn_id') = ?5
                  AND json_extract(done.data, '$.item_id') = json_extract(start.data, '$.item_id')
                  AND (json_extract(done.data, '$.status') = 'completed'
                       OR json_type(done.data, '$.exit_code') = 'integer')
                WHERE start.agent = ?1 AND start.generation = ?2 AND start.seq < ?3 AND start.ts >= ?4
                  AND start.source = 'stream' AND start.native_id IS NOT NULL AND start.kind = 'tool.started'
                  AND json_extract(start.data, '$.turn_id') = ?5
                  AND json_extract(start.data, '$.command_truncated') = 0
                  AND json_extract(start.data, '$.command') = ?6
             )",
            params![agent, turn.generation, through, started, turn.turn_id, check.join(" ")],
            |row| row.get(0),
        ).map_err(sql_error)?;
        Ok((paths, checked))
    }

    pub fn render_injection(&self, command: &Command, text: &str) -> Result<String> {
        render_injection(&self.connection, command, text)
    }

    /// Replace one unclaimed project diagnostic. The live Agent row chooses
    /// the consumer in the transaction that admits the context.
    pub fn project_check_context(
        &mut self,
        target: &str,
        generation: i64,
        failure: Option<String>,
        fast_check: Option<(i64, i32)>,
        now: i64,
    ) -> Result<Change<Option<String>>> {
        self.ensure_writer_healthy()?;
        let result = self.project_check_context_inner(target, generation, failure, fast_check, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn project_check_context_inner(
        &mut self,
        target: &str,
        generation: i64,
        failure: Option<String>,
        fast_check: Option<(i64, i32)>,
        now: i64,
    ) -> Result<Change<Option<String>>> {
        let agent_num = parse_agent_id(target)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut events = Vec::new();
        let mut value = None;
        if let Some(agent) = load_agent_by_num(&tx, agent_num)?
            && agent.generation == generation
            && !agent.is_terminal()
        {
            if let Some((edit_seq, exit_code)) = fast_check {
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "project.check.finished",
                    "project",
                    Some(agent_num),
                    Some(generation),
                    json!({"edit_seq": edit_seq, "exit_code": exit_code}),
                )?);
            }
            let previous: Option<String> = tx
                .query_row(
                    "SELECT id FROM commands INDEXED BY commands_target
                     WHERE target = ?1 AND generation = ?2 AND principal = ?3
                       AND verb = 'inject' AND state IN ('accepted', 'queued') LIMIT 1",
                    params![agent_num, generation, PROJECT_CHECK_PRINCIPAL],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if let Some(id) = previous {
                let command = load_command_record(&tx, &id)?.ok_or_else(|| {
                    Error::new("source_integrity", "project check context disappeared")
                })?;
                reject_pending_context(
                    &tx,
                    &command,
                    agent_num,
                    generation,
                    "project_check_superseded",
                    now,
                    &mut events,
                )?;
            }
            if let Some(text) = failure {
                let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "inject");
                request.target = Some(target.to_owned());
                request.expected_generation = Some(generation);
                request.args = serde_json::to_value(InjectArgs {
                    text,
                    point: InjectPoint::Soonest,
                    expires_at_ms: agent
                        .active_turn
                        .as_ref()
                        .map(|_| now.saturating_add(600_000)),
                })
                .map_err(|_| {
                    Error::new("internal", "project check context could not be encoded")
                })?;
                let digest = canonical::digest(&admission::command_semantic_payload(
                    &request,
                    request.args.clone(),
                )?)?;
                let event = admit_tx(&tx, PROJECT_CHECK_PRINCIPAL, &request, &digest, now, None)?;
                value = (event.data["application_point"] == "active_turn").then_some(request.id);
                events.push(event);
            }
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value, events })
    }

    /// Reserve a fact note once for the captured turn. Lifecycle notes without
    /// a turn share one pending next-input slot per kind and generation.
    pub fn queue_fact_note(
        &mut self,
        event: &Event,
        kind: &str,
        text: &str,
        id: Option<&str>,
        now: i64,
    ) -> Result<Change<Option<String>>> {
        self.ensure_writer_healthy()?;
        let result = self.queue_fact_note_inner(event, kind, FactNote::Text { text, id }, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn queue_edit_note(
        &mut self,
        event: &Event,
        kind: &str,
        paths: &[ContextEditPath],
        now: i64,
    ) -> Result<Change<Option<String>>> {
        self.ensure_writer_healthy()?;
        let result = self.queue_fact_note_inner(event, kind, FactNote::Edits(paths), now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn queue_fact_note_inner(
        &mut self,
        event: &Event,
        kind: &str,
        note: FactNote<'_>,
        now: i64,
    ) -> Result<Change<Option<String>>> {
        let empty = Change {
            value: None,
            events: Vec::new(),
        };
        let (Some(target), Some(generation)) = (event.agent.as_deref(), event.generation) else {
            return Ok(empty);
        };
        let agent_num = parse_agent_id(target)?;
        let turn_id = event.data["turn_id"].as_str();
        let principal = format!("swarm_note:{kind}");
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let Some(agent) = load_agent_by_num(&tx, agent_num)?.filter(|agent| {
            agent.generation == generation && !agent.is_terminal() && !agent.observe_only
        }) else {
            return Ok(empty);
        };
        if context_points(&agent).is_err() {
            return Ok(empty);
        }
        let mut events = Vec::new();
        if kind == "task_card" && event.data["verb"] == "handoff" {
            let previous: Option<String> = tx
                .query_row(
                    "SELECT id FROM commands INDEXED BY commands_target
                 WHERE target = ?1 AND generation = ?2 AND principal = ?3 AND verb = 'inject'
                   AND turn_id IS NULL AND state IN ('accepted', 'queued') LIMIT 1",
                    params![agent_num, generation, principal],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if let Some(previous) = previous
                && let Some(record) = load_command_record(&tx, &previous)?
            {
                reject_pending_context(
                    &tx,
                    &record,
                    agent_num,
                    generation,
                    "task_card_superseded",
                    now,
                    &mut events,
                )?;
            }
        }
        let (text, id, paths) = match note {
            FactNote::Text { text, id } => (text.to_owned(), id, Vec::new()),
            FactNote::Edits(paths) => {
                let Some(turn) = turn_id else {
                    return Ok(empty);
                };
                let paths = new_note_paths(&tx, agent_num, generation, turn, kind, paths)?;
                if paths.is_empty() {
                    return Ok(empty);
                }
                (edit_note_text(kind, &paths), None, paths)
            }
        };
        let duplicate: bool = if !paths.is_empty() {
            false
        } else if let Some(id) = id {
            tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM commands WHERE id = ?1)",
                [id],
                |row| row.get(0),
            )
            .map_err(sql_error)?
        } else {
            tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM commands INDEXED BY commands_target
                 WHERE target = ?1 AND generation = ?2 AND principal = ?3 AND verb = 'inject'
                   AND ((?4 IS NOT NULL AND (turn_id = ?4 OR expected_turn_id = ?4))
                        OR (turn_id IS NULL AND state IN ('accepted', 'queued', 'dispatched'))))
                 OR EXISTS(SELECT 1 FROM events INDEXED BY events_agent
                     WHERE agent=?1 AND generation=?2 AND kind='command.accepted'
                       AND json_extract(data,'$.note_kind')=?5
                       AND json_extract(data,'$.trigger_seq')=?6)",
                params![agent_num, generation, principal, turn_id, kind, event.seq],
                |row| row.get(0),
            )
            .map_err(sql_error)?
        };
        if duplicate {
            return Ok(empty);
        }
        let id = id.map_or_else(|| ulid::Ulid::new().to_string(), str::to_owned);
        let mut request = SubmitCommand::new(id, "inject");
        request.target = Some(target.to_owned());
        request.expected_generation = Some(generation);
        request.expected_turn_id = turn_id.map(str::to_owned);
        request.args = json!(InjectArgs {
            text,
            point: if turn_id.is_some() {
                InjectPoint::Soonest
            } else {
                InjectPoint::NextInput
            },
            expires_at_ms: None,
        });
        let digest = admission::command_payload_digest(&request)?;
        let accepted = admit_tx(
            &tx,
            &principal,
            &request,
            &digest,
            now,
            Some(FactOrigin {
                seq: event.seq,
                paths: &paths,
            }),
        )?;
        if let Some(turn) = turn_id {
            for path in &paths {
                db!(tx.execute(
                    "INSERT INTO context_noted_paths(agent,generation,kind,path_key,turn_id,note_id) VALUES(?1,?2,?3,?4,?5,?6)",
                    params![agent_num, generation, kind, edit_path_key(path), turn, request.id],
                ));
            }
        }
        let value = (accepted.data["application_point"] == "active_turn").then_some(request.id);
        events.push(accepted);
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value, events })
    }
}

fn render_injection(connection: &Connection, command: &Command, text: &str) -> Result<String> {
    let principal = if command.principal == "owner"
        && command_cause_tx(connection, &command.id)?.is_some_and(|cause| cause.rule.is_some())
    {
        "swarm_rule"
    } else {
        &command.principal
    };
    let labels = if command.verb == "inject" {
        decode_inject_args(&command.args)?.trusted_review_labels
    } else {
        None
    };
    match labels {
        Some(labels) => Ok(swarm_note_with_trusted_review_labels(
            text, principal, &labels,
        )),
        None => Ok(swarm_note(text, principal)),
    }
}

pub(super) fn has_unhealthy_native_stream_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM source_state
             WHERE agent = ?1 AND generation = ?2 AND source = ?3
               AND (state <> 'open' OR health <> 'ok')
         )",
        params![agent_num, generation, INITIAL_SOURCE],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

/// Room a safe-point steer session needs around the context in its native
/// hook response.
const HOOK_NATIVE_ENVELOPE_BYTES: usize = 1024;

fn hook_frame_limit(agent: &Agent) -> usize {
    if agent.steers_at_turn_safe_point() {
        MAX_FRAME_BYTES - HOOK_NATIVE_ENVELOPE_BYTES
    } else {
        MAX_FRAME_BYTES
    }
}

fn hook_response_fits(reply_id: &Value, items: &[ContextItem], frame_limit: usize) -> Result<bool> {
    let reply = json!({
        "jsonrpc": "2.0",
        "id": reply_id,
        "result": {"context": items},
    });
    Ok(encode_json(&reply)?.len().saturating_add(1) <= frame_limit)
}

/// Refuse before acceptance a steer whose Agent has no safe-point consumer or
/// whose framed text can never fit one hook response.
pub(super) fn validate_turn_safe_point_steer_admission(
    agent: &Agent,
    principal: &str,
    command_id: &str,
    text: &str,
) -> Result<()> {
    if !context_points(agent)?.contains(&InjectPoint::TurnSafePoint) {
        return Err(Error::new(
            "unsupported",
            "this Agent has no turn safe point to receive a steer",
        ));
    }
    // Rule-caused owner steers render as "Swarm rule", wider than "owner".
    let frame_principal = if principal == "owner" {
        "swarm_rule"
    } else {
        principal
    };
    let item = ContextItem {
        command_id: command_id.to_owned(),
        text: swarm_note(text, frame_principal),
        expires_at_ms: None,
    };
    let max_reply_id = Value::String("M".repeat(MAX_COMMAND_ID_BYTES));
    if !hook_response_fits(
        &max_reply_id,
        std::slice::from_ref(&item),
        hook_frame_limit(agent),
    )? {
        return Err(Error::new(
            "frame_too_large",
            "framed steer exceeds the 1 MiB hook response bound",
        ));
    }
    Ok(())
}

pub(super) fn admit_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    now: i64,
    fact: Option<FactOrigin<'_>>,
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
    let mut args = serde_json::from_value::<InjectArgs>(request.args.clone())
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
    let points = context_points(&agent)?;
    if args.point == InjectPoint::Soonest {
        if let Some(expected) = request.expected_turn_id.as_deref()
            && agent.active_turn.as_deref() != Some(expected)
            && !captured_turn_exists(tx, agent_num, agent.generation, expected)?
        {
            return Err(Error::new("stale_generation", "inject turn is unknown"));
        }
        let active = matches!(agent.state, AgentState::Busy | AgentState::Waiting)
            && agent.active_turn.is_some()
            && request
                .expected_turn_id
                .as_ref()
                .is_none_or(|turn| agent.active_turn.as_ref() == Some(turn));
        args.point = [InjectPoint::TurnSafePoint, InjectPoint::ActiveTurn]
            .into_iter()
            .find(|point| active && points.contains(point))
            .unwrap_or(InjectPoint::NextInput);
    }
    if !points.contains(&args.point) {
        return Err(Error::new(
            "unsupported",
            "Agent has no consumer for this context application point",
        ));
    }

    let turn_id = match args.point {
        InjectPoint::NextInput => None,
        InjectPoint::Soonest => unreachable!("soonest was resolved above"),
        InjectPoint::TurnSafePoint | InjectPoint::ActiveTurn => {
            let point_name = match args.point {
                InjectPoint::TurnSafePoint => "turn_safe_point",
                _ => "active_turn",
            };
            if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
                return Err(Error::new(
                    "bad_state",
                    format!("{point_name} inject requires an active turn"),
                ));
            }
            let turn_id = agent.active_turn.as_deref().ok_or_else(|| {
                Error::new(
                    "bad_state",
                    format!("{point_name} inject requires an exact active turn"),
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

    let mut resolved = json!(args);
    if let Some(labels) = &request.trusted_review_labels {
        resolved["_trusted_review_labels"] = serde_json::to_value(labels)
            .map_err(|_| Error::new("internal", "trusted review labels could not be encoded"))?;
    }
    let initial_outcome = (args.point == InjectPoint::NextInput).then(|| {
        let delivery = if request.expected_turn_id.is_some() {
            "moved"
        } else {
            "queued"
        };
        json!({"delivery": delivery, "application_point": "next_input"})
    });
    let mut data =
        json!({"command_id": request.id, "verb": "inject", "application_point": args.point});
    if let Some(fact) = fact {
        data["note_kind"] = json!(principal.trim_start_matches("swarm_note:"));
        data["trigger_seq"] = json!(fact.seq);
        if !fact.paths.is_empty() {
            data["paths"] = json!(fact.paths);
        }
    }
    if let Some(outcome) = &initial_outcome {
        data["outcome"] = outcome.clone();
    }
    let event = admit_command(
        tx,
        &CommandInsert {
            turn_id: turn_id.as_deref(),
            args: &resolved,
            outcome: initial_outcome.as_ref(),
            ..CommandInsert::admitted(
                principal,
                request,
                payload_digest,
                (agent_num, agent.generation),
                now,
            )
        },
        data,
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
    let mut selected_outcomes: Vec<ContextOutcome> = Vec::new();
    let mut payload_bytes = send
        .args
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "send command has no text"))?
        .len();
    let mut cursor = None;
    // The mandatory `turn.started` receipt, budgeted in the exact shape the
    // adapters persist (one outcome per claimed item). Fields fixed only at
    // native dispatch are worst-cased by their enforced encoded bounds:
    // command and native turn ids cap at MAX_COMMAND_ID_BYTES, carried
    // models at MAX_HARNESS_MODEL_ID_BYTES, configured effort at
    // MAX_CONFIGURE_EFFORT_LEN. A longer native turn id already fails at
    // commit with `unknown`; real head values are short identifiers.
    let max_turn_id = "T".repeat(MAX_COMMAND_ID_BYTES);
    let max_model = "M".repeat(MAX_HARNESS_MODEL_ID_BYTES);
    let max_effort = "E".repeat(super::admission::MAX_CONFIGURE_EFFORT_LEN);
    let receipt_len = |outcomes: &[ContextOutcome]| -> Result<usize> {
        serde_json::to_vec(&turn_started_data(
            &send.id,
            &max_turn_id,
            Some(&max_model),
            Some(&max_effort),
            outcomes,
        ))
        .map_err(|_| Error::new("internal", "context receipt could not be encoded"))
        .map(|bytes| bytes.len())
    };
    // Intrinsic fitness uses a canonical minimal send: an item that still
    // exceeds the plugin envelope with empty send text can never fit a
    // later send either, so it is rejected with evidence instead of wedging
    // the queue behind every dispatch. Testing against the current send's
    // text and metadata instead would permanently reject an inject that
    // fits a later ordinary send.
    let minimal_send: Value = json!({"text": ""});
    // Worst-case `agent.command` plugin envelope in the exact shape the
    // plugin host transmits: top-level `context` plus the dispatch and
    // configuration overrides. Ids and overrides unknown at claim time use
    // their enforced encoded bounds. Over-estimating only defers an item to
    // a later send; under-estimating would overflow before plugin I/O and
    // have the committed claims settled as rejected.
    let plugin_frame = |args: &Value, context: &[ContextItem]| -> Result<Value> {
        let mut bare = args.clone();
        if let Some(object) = bare.as_object_mut() {
            object.remove(CONTEXT_ITEMS_ARG);
        }
        let mut params = json!({
            "agent_id": agent.id,
            "generation": agent.generation,
            "command_id": send.id,
            "verb": "send",
            "turn_id": send.turn_id.as_deref().unwrap_or(max_turn_id.as_str()),
            "request_id": send.request_id.as_deref().unwrap_or(max_turn_id.as_str()),
            "args": bare,
            "dispatch_seq": i64::MAX,
        });
        if !context.is_empty() {
            params["context"] = serde_json::to_value(context)
                .map_err(|_| Error::new("internal", "context items could not be encoded"))?;
        }
        params["model_override"] = Value::String(max_model.clone());
        params["effort_override"] = Value::String(max_effort.clone());
        params["config_revision"] = json!(i64::MAX);
        Ok(params)
    };
    'scan: loop {
        let (records, next_cursor) = pending_context_batch_page(
            tx,
            agent_num,
            agent.generation,
            InjectPoint::NextInput,
            None,
            cursor,
        )?;
        if records.is_empty() {
            break;
        }
        for (_, _, record) in records {
            let args = decode_inject_args(&record.command.args)?;
            if args.point != InjectPoint::NextInput {
                continue;
            }
            if reject_stale_assignment(tx, &record, agent_num, agent.generation, now, events)? {
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
            if args.text.len() > MAX_TEXT_BYTES {
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
            let item = ContextItem {
                command_id: record.command.id.clone(),
                text: render_injection(tx, &record.command, &args.text)?,
                expires_at_ms: args.expires_at_ms,
            };
            let lone = vec![item.clone()];
            let lone_frame = plugin_frame(&minimal_send, &lone)?;
            match check_rpc_frame("agent.command", &lone_frame) {
                Ok(()) => {}
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
                    continue;
                }
                Err(error) => return Err(error),
            }
            // Anything that fits alone but not with this batch waits for a
            // later send: end the entire scan so later FIFO items are
            // deferred, never rejected against an already-full batch.
            let mut candidate = selected.clone();
            candidate.push(item.clone());
            let candidate_bytes = payload_bytes.saturating_add(args.text.len());
            if candidate_bytes > MAX_TEXT_BYTES {
                break 'scan;
            }
            let frame = plugin_frame(&send.args, &candidate)?;
            match check_rpc_frame("agent.command", &frame) {
                Ok(()) => {
                    let mut candidate_outcomes = selected_outcomes.clone();
                    candidate_outcomes.push(ContextOutcome {
                        mechanism: "send".to_owned(),
                        application_point: InjectPoint::NextInput,
                        consumer_id: send.id.clone(),
                    });
                    if receipt_len(&candidate_outcomes)? > MAX_EVENT_BYTES {
                        break 'scan;
                    }
                    selected_outcomes = candidate_outcomes;
                    selected = candidate;
                    payload_bytes = candidate_bytes;
                    selected_ids.push(record.command.id.clone());
                }
                Err(error) if error.code == "frame_too_large" => break 'scan,
                Err(error) => return Err(error),
            }
        }
        cursor = next_cursor;
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
        callback: (&str, &Value),
        now: i64,
    ) -> Result<Change<Vec<ContextItem>>> {
        self.ensure_writer_healthy()?;
        require_nonempty(consumer_id, "consumer_id")?;
        require_nonempty(turn_id, "turn_id")?;
        if !matches!(callback.0, "before_tool" | "after_tool" | "before_model") {
            return Err(Error::new("invalid", "context hook is not supported"));
        }
        let result = self.claim_turn_safe_point_context_inner(
            agent_id,
            generation,
            turn_id,
            consumer_id,
            callback,
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
        (hook, reply_id): (&str, &Value),
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
            "hook": hook,
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
        crate::ensure_no_digest_id(
            &tx,
            consumer_id,
            "hook consumer id is already used by a completion digest",
        )?;
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
        let mut selected: Vec<ContextItem> = Vec::new();
        let mut selected_ids = Vec::new();
        let mut payload_bytes = 0_usize;
        let in_flight_stop = has_in_flight_stop_tx(&tx, agent_num, generation)?;
        let mut pending_candidates = pending_context_batch(
            &tx,
            agent_num,
            generation,
            InjectPoint::TurnSafePoint,
            Some(turn_id),
        )?;
        if agent.steers_at_turn_safe_point() {
            pending_candidates.extend(pending_turn_steers(&tx, agent_num, generation, turn_id)?);
            pending_candidates.sort_by_key(|(created_at, rowid, _)| (*created_at, *rowid));
        }
        let frame_limit = hook_frame_limit(&agent);
        for (_, _, record) in pending_candidates {
            if record.command.turn_id.as_deref() != Some(turn_id) {
                continue;
            }
            let is_steer = record.command.verb == "steer";
            let (text, rendered, expires_at_ms) = if is_steer {
                if !steer_deliverable_tx(
                    &tx,
                    &record,
                    &agent,
                    in_flight_stop,
                    agent_num,
                    now,
                    &mut events,
                )? {
                    continue;
                }
                let Some((text, rendered)) =
                    render_pending_steer_tx(&tx, &record, agent_num, generation, now, &mut events)?
                else {
                    continue;
                };
                (text, rendered, None)
            } else {
                let args = decode_inject_args(&record.command.args)?;
                if args.point != InjectPoint::TurnSafePoint {
                    continue;
                }
                if reject_stale_assignment(&tx, &record, agent_num, generation, now, &mut events)? {
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
                let rendered = render_injection(&tx, &record.command, &args.text)?;
                (args.text, rendered, args.expires_at_ms)
            };
            let item = ContextItem {
                command_id: record.command.id.clone(),
                text: rendered,
                expires_at_ms,
            };
            let mut candidate = selected.clone();
            candidate.push(item.clone());
            let candidate_bytes = payload_bytes.saturating_add(text.len());
            if candidate_bytes > MAX_TEXT_BYTES
                || !hook_response_fits(reply_id, &candidate, frame_limit)?
            {
                // A steer that fits alone waits for the next hook of its turn,
                // keeping FIFO order; anything else cannot be delivered.
                if is_steer
                    && !selected.is_empty()
                    && text.len() <= MAX_TEXT_BYTES
                    && hook_response_fits(reply_id, std::slice::from_ref(&item), frame_limit)?
                {
                    break;
                }
                if is_steer {
                    reject_pending_steer(
                        &tx,
                        &record,
                        agent_num,
                        generation,
                        "frame_too_large",
                        "framed steer exceeds the 1 MiB hook response bound",
                        now,
                        &mut events,
                    )?;
                } else {
                    reject_pending_context(
                        &tx,
                        &record,
                        agent_num,
                        generation,
                        "frame_too_large",
                        now,
                        &mut events,
                    )?;
                }
                continue;
            }
            selected = candidate;
            payload_bytes = candidate_bytes;
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
        Some(turn_id),
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
    let hook = hook_kind_for_event(&native_event.kind)?;
    let claims = claimed_context_commands(
        tx,
        agent_num,
        generation,
        &consumer_id,
        InjectPoint::TurnSafePoint,
        Some(&turn_id),
    )?;
    let encoded_outcome = native_event.data.get("context_outcome");
    if claims.is_empty() {
        if encoded_outcome.is_some() {
            return Err(Error::new(
                "source_integrity",
                format!("hook.{hook} reports context without a matching Store claim"),
            ));
        }
        require_recorded_empty_safe_point_selection(
            tx,
            agent_num,
            generation,
            &turn_id,
            &consumer_id,
            hook,
        )?;
        return Ok(Vec::new());
    }
    let outcome = encoded_outcome
        .cloned()
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                format!("hook.{hook} omitted the claimed context outcome"),
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
        if record.turn_id.as_deref() != Some(turn_id.as_str()) {
            return Err(Error::new(
                "source_integrity",
                "hook context claim does not match the reported turn",
            ));
        }
        validate_context_receipt(record, generation, &turn_id, &claim, &outcome)?;
        if !receipt_turn_reconciles(tx, agent_num, generation, &turn_id)? {
            return Err(Error::new(
                "stale_generation",
                "hook context receipt does not match the exact captured turn",
            ));
        }
        events.push(apply_context_receipt_tx(
            tx, record, &turn_id, &outcome, now,
        )?);
    }
    Ok(events)
}

fn hook_kind_for_event(kind: &str) -> Result<&'static str> {
    match kind {
        "hook.before_tool" => Ok("before_tool"),
        "hook.after_tool" => Ok("after_tool"),
        "hook.before_model" => Ok("before_model"),
        other => Err(Error::new(
            "unsupported",
            format!("unsupported hook event kind: {other}"),
        )),
    }
}

fn require_recorded_empty_safe_point_selection(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
    consumer_id: &str,
    hook: &str,
) -> Result<()> {
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "context target Agent was not found"))?;
    if agent.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "empty context callback does not match the current generation",
        ));
    }
    let active_matches = agent.active_turn.as_deref() == Some(turn_id)
        && matches!(agent.state, AgentState::Busy | AgentState::Waiting);
    if !active_matches && !captured_turn_exists(tx, agent_num, generation, turn_id)? {
        return Err(Error::new(
            "stale_generation",
            "empty context callback does not match the exact captured turn",
        ));
    }

    let principal = format!("agent:{}:{generation}", agent.id);
    let payload = json!({
        "agent_id": agent.id,
        "generation": generation,
        "hook": hook,
        "turn_id": turn_id,
        "consumer_id": consumer_id,
    });
    let payload_digest = canonical::digest(&payload)?;
    let Some((stored_principal, stored_method, stored_digest, stored_result)) =
        write_binding(tx, consumer_id)?
    else {
        return Err(Error::new(
            "source_integrity",
            format!("hook.{hook} has no recorded empty context selection"),
        ));
    };
    if stored_principal != principal
        || stored_method != "hook.event"
        || stored_digest != payload_digest
    {
        return Err(Error::new(
            "source_integrity",
            "empty context selection does not match its exact callback binding",
        ));
    }
    let marker = decode_json(&stored_result)?;
    let empty_marker = json!({
        "count": 0,
        "command_ids_digest": canonical::digest(&json!([]))?,
    });
    if marker != empty_marker {
        return Err(Error::new(
            "source_integrity",
            format!("hook.{hook} omitted the receipt for a nonempty context selection"),
        ));
    }
    Ok(())
}

fn captured_turn_exists(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM turns
             WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3
         )",
        params![agent_num, generation, turn_id],
        |row| row.get::<_, bool>(0),
    )
    .map_err(sql_error)
}

/// A confirmed receipt reconciles when its exact turn is still the active
/// captured turn, or when that turn was already captured in this generation
/// and has since ended. Selection (claim) always requires the active turn;
/// only the receipt path uses this late allowance, and it never binds to a
/// new turn because the claim filter pins the exact turn and consumer.
fn receipt_turn_reconciles(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
) -> Result<bool> {
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "context target Agent was not found"))?;
    if agent.generation != generation {
        return Ok(false);
    }
    if agent.active_turn.as_deref() == Some(turn_id)
        && matches!(agent.state, AgentState::Busy | AgentState::Waiting)
    {
        return Ok(true);
    }
    captured_turn_exists(tx, agent_num, generation, turn_id)
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
        send.command.turn_id.as_deref(),
    )? {
        let claim = require_context_claim(&record)?;
        let outcome = json!({"claim": claim, "reason": "consumer_input_unconfirmed"});
        update_command_state(
            tx,
            &record.command_id,
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
            &record.command_id,
            json!({
                "command_id": record.command_id,
                "state": state_name,
                "outcome": outcome,
            }),
        )?);
    }
    Ok(())
}

pub(super) fn settle_ended_turn_context_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
    now: i64,
) -> Result<Vec<super::native_events::PendingCommandEvent>> {
    let mut events = Vec::new();
    let agent = load_agent_by_num(tx, agent_num)?;
    let can_move = agent.as_ref().is_some_and(next_input_available);
    let safe_point_steers = agent.as_ref().is_some_and(Agent::steers_at_turn_safe_point);
    for command_id in pending_turn_context_command_ids(tx, agent_num, generation, turn_id)? {
        let record = load_command_record(tx, &command_id)?
            .ok_or_else(|| Error::new("source_integrity", "turn context disappeared"))?;
        let expired = decode_inject_args(&record.command.args)?
            .expires_at_ms
            .is_some_and(|expires| expires <= now);
        if can_move && !expired {
            let outcome = move_context_to_next_input(tx, &command_id, now)?;
            events.push(super::native_events::PendingCommandEvent {
                command_id: command_id.clone(),
                state: CommandState::Accepted,
                data: json!({"command_id": command_id, "outcome": outcome}),
            });
            continue;
        }
        let reason = if expired {
            "expired"
        } else {
            "captured_turn_ended"
        };
        let outcome = json!({"delivery": "cancelled", "reason": reason});
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
            state: CommandState::Cancelled,
            data: json!({
                "command_id": command_id,
                "state": "cancelled",
                "outcome": outcome,
            }),
        });
    }
    let pending_steers = if safe_point_steers {
        pending_turn_steer_command_ids(tx, agent_num, generation, turn_id)?
    } else {
        Vec::new()
    };
    for command_id in pending_steers {
        let outcome = json!({
            "reason": "captured_turn_ended",
            "message": "turn ended before a safe point arrived for steer",
        });
        update_command_state(
            tx,
            &command_id,
            CommandState::Rejected,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(super::native_events::PendingCommandEvent {
            command_id: command_id.clone(),
            state: CommandState::Rejected,
            data: json!({
                "command_id": command_id,
                "state": "rejected",
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
            state: CommandState::Unknown,
            data: json!({
                "command_id": command_id,
                "state": "unknown",
                "outcome": outcome,
            }),
        });
    }
    Ok(events)
}

pub(super) fn move_context_to_next_input(
    tx: &Transaction<'_>,
    id: &str,
    now: i64,
) -> Result<Value> {
    let outcome = json!({"delivery": "moved", "application_point": "next_input"});
    // The original request digest and captured request turn still govern replay.
    db!(tx.execute(
        "UPDATE commands SET args = json_set(args, '$.point', 'next_input'),
            turn_id = NULL, state = 'accepted', outcome = ?2, updated_at = ?3 WHERE id = ?1",
        params![id, encode_json(&outcome)?, now],
    ));
    Ok(outcome)
}

pub(super) fn next_input_available(agent: &Agent) -> bool {
    !agent.is_terminal()
        && context_points(agent).is_ok_and(|points| points.contains(&InjectPoint::NextInput))
}

pub(super) fn unknown_claim_outcome(outcome: Option<&Value>, reason: &str) -> Value {
    match outcome.and_then(|value| value.get("claim")) {
        Some(claim) => {
            let mut unknown = json!({"reason": reason, "claim": claim});
            if outcome.is_some_and(|value| value["moved"] == true) {
                unknown["moved"] = json!(true);
            }
            unknown
        }
        None => json!({"reason": reason}),
    }
}

/// Read one bounded FIFO page of contexts for one real consumer point.
///
/// The `(created_at, rowid)` keyset walks the partial context index without
/// OFFSET or scanning unrelated command history. Row count and total stored
/// args bytes cap each page while the write transaction is held. The caller
/// may continue from the returned key without deferring the remaining queue.
fn pending_context_batch(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    point: InjectPoint,
    turn_id: Option<&str>,
) -> Result<Vec<KeyedCommand>> {
    pending_context_batch_page(tx, agent_num, generation, point, turn_id, None)
        .map(|(records, _)| records)
}

fn pending_context_batch_page(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    point: InjectPoint,
    turn_id: Option<&str>,
    after: Option<ContextCursor>,
) -> Result<ContextPage> {
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
    let mut cursor = after;
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
        let record = load_command_record(tx, &id)?
            .ok_or_else(|| Error::new("source_integrity", "context command disappeared"))?;
        records.push((created_at, rowid, record));
        visited += 1;
        cursor = Some((created_at, rowid));
    }

    Ok((records, cursor))
}

fn pending_turn_steers(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
) -> Result<Vec<KeyedCommand>> {
    let mut statement = tx
        .prepare(
            "SELECT id, created_at, rowid, length(CAST(args AS BLOB))
             FROM commands INDEXED BY commands_turn_steer_pending
             WHERE target = ?1 AND generation = ?2 AND verb = 'steer'
               AND state IN ('accepted', 'queued')
               AND turn_id = ?3
             ORDER BY created_at, rowid LIMIT ?4",
        )
        .map_err(sql_error)?;
    let mut rows = statement
        .query(params![
            agent_num,
            generation,
            turn_id,
            MAX_CONTEXT_QUERY_ROWS as i64
        ])
        .map_err(sql_error)?;
    let mut materialized_bytes = 0_usize;
    let mut records = Vec::new();
    while let Some((id, created_at, rowid, args_bytes)) = read_pending_context_candidate(&mut rows)?
    {
        let args_bytes = usize::try_from(args_bytes).map_err(|_| {
            Error::new(
                "source_integrity",
                "pending steer has an invalid stored argument length",
            )
        })?;
        if args_bytes > MAX_CONTEXT_QUERY_BYTES {
            return Err(Error::new(
                "source_integrity",
                "pending steer exceeds the maximum command frame size",
            ));
        }
        if args_bytes > MAX_CONTEXT_QUERY_BYTES.saturating_sub(materialized_bytes) {
            break;
        }
        materialized_bytes += args_bytes;
        let record = load_command_record(tx, &id)?
            .ok_or_else(|| Error::new("source_integrity", "steer command disappeared"))?;
        records.push((created_at, rowid, record));
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

fn pending_turn_context_command_ids(
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
               AND json_extract(args, '$.point') IN ('turn_safe_point', 'active_turn')
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

fn pending_turn_steer_command_ids(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
) -> Result<Vec<String>> {
    let mut statement = tx
        .prepare(
            "SELECT id FROM commands INDEXED BY commands_turn_steer_pending
             WHERE target = ?1 AND generation = ?2 AND verb = 'steer'
               AND state IN ('accepted', 'queued')
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
    turn_id: Option<&str>,
) -> Result<Vec<ClaimedContextRecord>> {
    let (sql, required_turn_id) = match point {
        InjectPoint::Soonest => {
            return Err(Error::new("source_integrity", "unresolved context point"));
        }
        InjectPoint::NextInput => (
            "SELECT c.id, c.verb, c.turn_id, c.state, c.outcome
             FROM commands AS c INDEXED BY commands_target
             WHERE c.target = ?1 AND c.generation = ?2 AND c.verb = 'inject'
               AND c.state IN ('dispatched', 'unknown') AND c.turn_id IS NULL
               AND json_extract(c.outcome, '$.claim.consumer_id') = ?3
               AND json_extract(c.outcome, '$.claim.application_point') = 'next_input'
               AND EXISTS(
                   SELECT 1 FROM commands AS send
                   WHERE send.id = ?3 AND send.target = ?1 AND send.generation = ?2
                     AND send.verb = 'send' AND send.turn_id IS ?4
               )
             ORDER BY c.created_at, c.rowid",
            false,
        ),
        InjectPoint::TurnSafePoint => (
            "SELECT c.id, c.verb, c.turn_id, c.state, c.outcome
             FROM commands AS c INDEXED BY commands_target
             WHERE c.target = ?1 AND c.generation = ?2 AND c.verb IN ('inject', 'steer')
               AND c.state IN ('dispatched', 'unknown') AND c.turn_id = ?4
               AND json_extract(c.outcome, '$.claim.consumer_id') = ?3
               AND json_extract(c.outcome, '$.claim.application_point') = 'turn_safe_point'
             ORDER BY c.created_at, c.rowid",
            true,
        ),
        InjectPoint::ActiveTurn => (
            "SELECT c.id, c.verb, c.turn_id, c.state, c.outcome
             FROM commands AS c INDEXED BY commands_target
             WHERE c.target = ?1 AND c.generation = ?2 AND c.verb = 'inject'
               AND c.state IN ('dispatched', 'unknown') AND c.turn_id = ?4
               AND json_extract(c.outcome, '$.claim.consumer_id') = ?3
               AND json_extract(c.outcome, '$.claim.application_point') = 'active_turn'
             ORDER BY c.created_at, c.rowid",
            true,
        ),
    };
    if required_turn_id && turn_id.is_none() {
        return Err(Error::new(
            "source_integrity",
            "turn-safe-point claim lookup has no exact turn",
        ));
    }
    let mut statement = tx.prepare(sql).map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![agent_num, generation, consumer_id, turn_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let mut claims = Vec::new();
    for row in rows {
        let (command_id, verb, stored_turn_id, state, encoded_outcome) = row.map_err(sql_error)?;
        let state = match state.as_str() {
            "dispatched" => CommandState::Dispatched,
            "unknown" => CommandState::Unknown,
            _ => {
                return Err(Error::new(
                    "source_integrity",
                    "context claim query returned an invalid state",
                ));
            }
        };
        let encoded_outcome = encoded_outcome
            .ok_or_else(|| Error::new("source_integrity", "context claim has no stored outcome"))?;
        let outcome = decode_json(&encoded_outcome)?;
        let claim = context_claim(Some(&outcome))?.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "context claim has no durable claim marker",
            )
        })?;
        if claim.consumer_id != consumer_id || claim.application_point != point {
            return Err(Error::new(
                "source_integrity",
                "context claim metadata differs from its indexed selection",
            ));
        }
        claims.push(ClaimedContextRecord {
            command_id,
            verb,
            target: agent_num,
            generation,
            turn_id: stored_turn_id,
            state,
            claim,
            moved: outcome["moved"] == true,
        });
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
             WHERE target = ?1 AND generation = ?2 AND verb IN ('inject', 'steer')
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

fn require_context_claim(record: &ClaimedContextRecord) -> Result<ContextClaim> {
    if !matches!(
        record.state,
        CommandState::Dispatched | CommandState::Unknown
    ) {
        return Err(Error::new(
            "bad_state",
            "context command is not an unsettled consumer claim",
        ));
    }
    Ok(record.claim.clone())
}

fn validate_context_receipt(
    record: &ClaimedContextRecord,
    generation: i64,
    turn_id: &str,
    claim: &ContextClaim,
    outcome: &ContextOutcome,
) -> Result<()> {
    if record.generation != generation
        || record
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

/// Validate the retained outcome of a directly dispatched `active_turn`
/// inject before it may settle Applied: the exact-turn native steer ack
/// must carry the typed `ContextOutcome` for this command.
pub(super) fn validate_active_turn_applied_outcome(
    record: &CommandRecord,
    outcome: Option<&Value>,
) -> Result<()> {
    let Some(outcome) = outcome else {
        return Err(Error::new(
            "source_integrity",
            "active_turn inject cannot be applied without its exact native result",
        ));
    };
    let receipt = outcome.get("context_outcome").cloned().ok_or_else(|| {
        Error::new(
            "source_integrity",
            "active_turn inject result has no context outcome",
        )
    })?;
    let receipt: ContextOutcome = serde_json::from_value(receipt).map_err(|_| {
        Error::new(
            "source_integrity",
            "active_turn context outcome is malformed",
        )
    })?;
    if receipt.application_point != InjectPoint::ActiveTurn
        || receipt.consumer_id != record.command.id
        || receipt.mechanism.trim().is_empty()
        || outcome.get("command_id").and_then(Value::as_str) != Some(record.command.id.as_str())
        || outcome.get("turn_id").and_then(Value::as_str) != record.command.turn_id.as_deref()
    {
        return Err(Error::new(
            "source_integrity",
            "active_turn result does not match its exact command, turn, and consumer binding",
        ));
    }
    Ok(())
}

fn validate_context_consumer_tx(
    tx: &Transaction<'_>,
    record: &ClaimedContextRecord,
    generation: i64,
    turn_id: &str,
    claim: &ContextClaim,
) -> Result<()> {
    let target = record.target;
    match claim.application_point {
        InjectPoint::Soonest => {
            return Err(Error::new("source_integrity", "unresolved context receipt"));
        }
        InjectPoint::NextInput => {
            let send_matches = tx
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM commands
                         WHERE id = ?1 AND target = ?2 AND generation = ?3
                           AND verb = 'send' AND state = 'applied' AND turn_id = ?4
                     )",
                    params![claim.consumer_id, target, generation, turn_id],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(sql_error)?;
            if !send_matches {
                return Err(Error::new(
                    "stale_generation",
                    "context receipt is not bound to its exact applied send turn",
                ));
            }
        }
        InjectPoint::TurnSafePoint => {
            // Hook receipts (`before_tool`/`before_model`) may confirm after
            // the native terminal won the race: an exact active turn or an
            // already-captured historical turn in this generation reconciles.
            // Selection never uses this allowance.
            if !receipt_turn_reconciles(tx, target, generation, turn_id)? {
                return Err(Error::new(
                    "stale_generation",
                    "context receipt does not match the exact captured turn",
                ));
            }
        }
        InjectPoint::ActiveTurn => {
            // Immediate native dispatch keeps its strict armor: only the
            // exact active turn of a live Agent may validate, with no late
            // or historical allowance.
            let agent = load_agent_by_num(tx, target)?
                .ok_or_else(|| Error::new("not_found", "context target Agent was not found"))?;
            if agent.generation != generation
                || agent.active_turn.as_deref() != Some(turn_id)
                || !matches!(agent.state, AgentState::Busy | AgentState::Waiting)
            {
                return Err(Error::new(
                    "stale_generation",
                    "context receipt does not match the exact captured turn",
                ));
            }
        }
    }
    Ok(())
}

fn apply_context_receipt_tx(
    tx: &Transaction<'_>,
    record: &ClaimedContextRecord,
    turn_id: &str,
    outcome: &ContextOutcome,
    now: i64,
) -> Result<super::native_events::PendingCommandEvent> {
    let mut serialized = serde_json::to_value(outcome)
        .map_err(|_| Error::new("internal", "context outcome could not be encoded"))?;
    serialized["delivery"] = json!(if outcome.application_point == InjectPoint::NextInput {
        "delivered_next_input"
    } else {
        "delivered_in_turn"
    });
    if record.verb == "steer" {
        serialized["command_id"] = json!(record.command_id);
        serialized["turn_id"] = json!(turn_id);
    }
    if record.moved {
        serialized["moved"] = json!(true);
    }
    update_command_state(
        tx,
        &record.command_id,
        CommandState::Applied,
        Some(&serialized),
        None,
        now,
    )?;
    db!(tx.execute(
        "UPDATE commands SET turn_id = ?1 WHERE id = ?2",
        params![turn_id, record.command_id],
    ));
    Ok(super::native_events::PendingCommandEvent {
        command_id: record.command_id.clone(),
        state: CommandState::Applied,
        data: json!({
            "command_id": record.command_id,
            "state": "applied",
            "outcome": serialized,
        }),
    })
}

pub(super) fn claim_context_tx(
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
    let point_matches = match record.command.verb.as_str() {
        "inject" => decode_inject_args(&record.command.args)?.point == consumer.point,
        "steer" => consumer.point == InjectPoint::TurnSafePoint && record.command.turn_id.is_some(),
        _ => false,
    };
    if !matches!(record.state, CommandState::Accepted | CommandState::Queued)
        || record.target != Some(agent_num)
        || record.command.generation != Some(generation)
        || !point_matches
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
    let mut outcome = json!({"claim": claim});
    if record
        .command
        .outcome
        .as_ref()
        .is_some_and(|value| value["delivery"] == "moved")
    {
        outcome["moved"] = json!(true);
    }
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
            "verb": record.command.verb,
            "consumer_id": consumer.id,
            "application_point": consumer.point,
        }),
    )?);
    Ok(())
}

/// Recheck a pending steer at its safe point with the gates a dispatched
/// command passes. `false` means it is held by its rule order or was settled.
fn steer_deliverable_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    agent: &Agent,
    in_flight_stop: bool,
    agent_num: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<bool> {
    let refusal = match rules::gate_rule_action_dispatch_tx(tx, &record.command.id, now, events) {
        Ok(rules::RuleActionDispatchGate::Ready) if in_flight_stop => Some(Error::new(
            "bad_state",
            "steer cannot be delivered while a stop is in flight",
        )),
        Ok(rules::RuleActionDispatchGate::Ready) => {
            check_pending_steer_dispatch(tx, record, agent).err()
        }
        Ok(rules::RuleActionDispatchGate::Held | rules::RuleActionDispatchGate::Cancelled) => {
            return Ok(false);
        }
        Err(error) => Some(error),
    };
    let Some(error) = refusal else {
        return Ok(true);
    };
    reject_pending_steer(
        tx,
        record,
        agent_num,
        agent.generation,
        &error.code,
        &error.message,
        now,
        events,
    )?;
    Ok(false)
}

/// Read and frame one pending steer. A steer that cannot be rendered is
/// rejected alone so the rest of the hook batch is still delivered.
fn render_pending_steer_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    agent_num: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<(String, String)>> {
    let rendered = required_string(&record.command.args, "text").and_then(|text| {
        let framed = render_injection(tx, &record.command, &text)?;
        Ok((text, framed))
    });
    match rendered {
        Ok(rendered) => Ok(Some(rendered)),
        Err(error) => {
            reject_pending_steer(
                tx,
                record,
                agent_num,
                generation,
                &error.code,
                &error.message,
                now,
                events,
            )?;
            Ok(None)
        }
    }
}

fn check_pending_steer_dispatch(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    agent: &Agent,
) -> Result<()> {
    project_assignment::check_dispatch(tx, record)?;
    projects::validate_project_command_dispatch_tx(tx, record)?;
    admission::require_reply_authority(&record.command.principal, &record.command.verb, agent)?;
    if record.command.principal.starts_with("agent:") {
        let caller = hierarchy::parse_caller(tx, &record.command.principal)?;
        hierarchy::caller_may_command(tx, &caller, agent)?;
    }
    provider_holds::require_dispatch_tx(tx, agent, &record.command)?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "The operation's distinct inputs remain explicit at its established call boundary."
)]
fn reject_pending_steer(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    agent_num: i64,
    generation: i64,
    code: &str,
    message: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let outcome = json!({"code": code, "reason": message});
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

fn reject_stale_assignment(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    agent_num: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<bool> {
    match project_assignment::check_dispatch(tx, record) {
        Ok(()) => Ok(false),
        Err(error)
            if matches!(
                error.code.as_str(),
                "stale_assignment" | "not_allowed" | "stale_generation"
            ) =>
        {
            reject_pending_context(tx, record, agent_num, generation, &error.code, now, events)?;
            Ok(true)
        }
        Err(error) => Err(error),
    }
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
    let mut outcome = json!({"code": code, "delivery": "cancelled"});
    if record
        .command
        .outcome
        .as_ref()
        .is_some_and(|value| value["delivery"] == "moved")
    {
        outcome["moved"] = json!(true);
    }
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

pub(super) fn context_points(agent: &Agent) -> Result<Vec<InjectPoint>> {
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
    serde_json::from_value(context_points.clone()).map_err(|_| {
        Error::new(
            "source_integrity",
            "Agent effective context points are malformed",
        )
    })
}

fn inject_point_name(point: InjectPoint) -> &'static str {
    match point {
        InjectPoint::Soonest => "soonest",
        InjectPoint::NextInput => "next_input",
        InjectPoint::TurnSafePoint => "turn_safe_point",
        InjectPoint::ActiveTurn => "active_turn",
    }
}

pub(super) fn decode_inject_args(value: &Value) -> Result<eliot_core::RetainedInjectArgs> {
    eliot_core::decode_retained_inject_args(value)
        .map_err(|_| Error::new("source_integrity", "persisted inject args are malformed"))
}
