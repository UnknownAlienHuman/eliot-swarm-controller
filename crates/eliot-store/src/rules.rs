//! Durable cursor and command admission for the first event-rule slice.
//!
//! The daemon owns rule parsing and matching. Store owns the persisted definition,
//! event cursor, action accounting, and the parent/child admission transaction.

use super::*;

const RULE_ACTION_WINDOW_MS: i64 = 60_000;
const RULE_ACTION_LIMIT: i64 = 10;
const GLOBAL_RULE_ACTION_LIMIT: i64 = 120;
const RULE_COMMAND_ID_DOMAIN: &str = "eliot-rule-command-v1";
const RULE_TRIGGER_EVENT: &str = "turn.completed";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleDefinitionInput {
    pub id: String,
    pub canonical_definition: String,
    pub definition_digest: [u8; 32],
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuleCursorOutcome {
    /// A scanned event did not match. Cursor-only; this emits no event.
    NoMatch,
    /// A contiguous disabled range was deliberately skipped.
    Disabled,
    /// A deterministic definition/action rejection was recorded.
    Rejected { code: String },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuleEventAdmission {
    pub state: core::api::RuleStateView,
    pub parent: Command,
    pub child: Option<Command>,
    pub events: Vec<Event>,
    pub is_new: bool,
}

#[derive(Clone, Debug)]
struct StoredRuleState {
    view: core::api::RuleStateView,
    canonical_definition: String,
    definition_digest: [u8; 32],
}

struct RuleEventKey<'a> {
    principal: &'a str,
    state: &'a core::api::RuleStateView,
    event_seq: i64,
    parent_id: String,
    child_id: String,
}

impl<'a> RuleEventKey<'a> {
    fn new(
        principal: &'a str,
        state: &'a core::api::RuleStateView,
        event_seq: i64,
    ) -> Result<Self> {
        Ok(Self {
            principal,
            state,
            event_seq,
            parent_id: derived_rule_id("parent", principal, &state.id, state.revision, event_seq)?,
            child_id: derived_rule_id("action-0", principal, &state.id, state.revision, event_seq)?,
        })
    }
}

enum RuleParentOutcome<'a> {
    Admitted,
    Rejected { code: &'a str },
}

impl Store {
    /// Create a rule at the transaction's event high-water, or read the unchanged
    /// persisted definition. Semantic replacement is unsupported in this slice.
    pub fn ensure_rule_state(
        &mut self,
        principal: &str,
        input: &RuleDefinitionInput,
        now: i64,
    ) -> Result<core::api::RuleStateView> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            validate_principal_rule(principal, &input.id)?;
            validate_definition_input(input)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;

            if let Some(existing) = load_rule_state(&tx, principal, &input.id)? {
                if existing.canonical_definition != input.canonical_definition
                    || existing.definition_digest != input.definition_digest
                    || existing.view.enabled != input.enabled
                {
                    return Err(Error::new(
                        "unsupported",
                        "semantic rule edits are unsupported in this slice",
                    ));
                }
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(existing.view);
            }

            let cursor = high_water_tx(&tx)?;
            tx.execute(
                "INSERT INTO rule_state(
                     principal, rule, revision, current_definition, definition_digest,
                     current_cursor, enabled, paused_reason, updated_at
                 ) VALUES (?1, ?2, 1, ?3, ?4, ?5, ?6, NULL, ?7)",
                params![
                    principal,
                    input.id,
                    input.canonical_definition,
                    input.definition_digest.as_slice(),
                    cursor,
                    input.enabled,
                    now,
                ],
            )
            .map_err(sql_error)?;
            let view = core::api::RuleStateView {
                id: input.id.clone(),
                revision: 1,
                current_cursor: cursor,
                enabled: input.enabled,
                paused_reason: None,
            };
            commit_transaction(tx, &self.write_faulted)?;
            Ok(view)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn get_rule_state(
        &self,
        principal: &str,
        id: &str,
    ) -> Result<Option<core::api::RuleStateView>> {
        require_nonempty(principal, "principal")?;
        require_nonempty(id, "rule id")?;
        Ok(load_rule_state(&self.connection, principal, id)?.map(|state| state.view))
    }

    /// Advance a scanned event range. NoMatch is deliberately cursor-only so a
    /// `rule.*` bookkeeping event cannot create another bookkeeping event.
    pub fn advance_rule_cursor(
        &mut self,
        principal: &str,
        state: &core::api::RuleStateView,
        through: i64,
        outcome: RuleCursorOutcome,
        now: i64,
    ) -> Result<Change<core::api::RuleStateView>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            require_nonempty(principal, "principal")?;
            require_nonempty(&state.id, "rule id")?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let stored = load_rule_state(&tx, principal, &state.id)?
                .ok_or_else(|| Error::new("not_found", "rule state was not found"))?;

            // A replay after a successful commit is a no-op. It must not emit a
            // second skip/rejection event or move the cursor backwards.
            if through <= stored.view.current_cursor {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: stored.view,
                    events: Vec::new(),
                });
            }
            if stored.view != *state {
                return Err(Error::new("bad_state", "rule cursor view is stale"));
            }
            if through > high_water_tx(&tx)? {
                return Err(Error::new(
                    "invalid",
                    "rule cursor cannot advance past the captured event high-water",
                ));
            }

            let mut events = Vec::new();
            match outcome {
                RuleCursorOutcome::NoMatch => {}
                RuleCursorOutcome::Disabled => {
                    if stored.view.enabled {
                        return Err(Error::new(
                            "bad_state",
                            "an enabled rule cannot advance as a disabled range",
                        ));
                    }
                    // Only record a range that contains trigger events. A range
                    // containing only rule bookkeeping is cursor-only, preventing
                    // disabled rules from amplifying their own skip metadata.
                    if has_trigger_in_range(&tx, stored.view.current_cursor, through)? {
                        events.push(insert_rule_event(
                            &tx,
                            now,
                            "rule.skipped",
                            json!({
                                "rule": state.id,
                                "revision": state.revision,
                                "from_seq": stored.view.current_cursor + 1,
                                "through_seq": through,
                                "reason": "disabled",
                            }),
                        )?);
                    }
                }
                RuleCursorOutcome::Rejected { code } => {
                    if code.trim().is_empty() || code.len() > 128 {
                        return Err(Error::new("invalid", "rule rejection code is invalid"));
                    }
                    let kind: String = tx
                        .query_row("SELECT kind FROM events WHERE seq = ?1", [through], |row| {
                            row.get(0)
                        })
                        .optional()
                        .map_err(sql_error)?
                        .ok_or_else(|| {
                            Error::new("not_found", "rejected rule event was not found")
                        })?;
                    if kind != RULE_TRIGGER_EVENT {
                        return Err(Error::new(
                            "bad_state",
                            "rule rejection bookkeeping is only for turn.completed events",
                        ));
                    }
                    events.push(insert_rule_event(
                        &tx,
                        now,
                        "rule.rejected",
                        json!({
                            "rule": state.id,
                            "revision": state.revision,
                            "from_seq": stored.view.current_cursor + 1,
                            "through_seq": through,
                            "code": code,
                        }),
                    )?);
                }
            }

            let view = update_rule_cursor_tx(&tx, principal, &stored.view, through, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: view,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Atomically admit one matched stop, its fixed parent/child link, rate
    /// accounting, the event cursor, and committed rule/command events.
    pub fn admit_rule_stop(
        &mut self,
        principal: &str,
        state: &core::api::RuleStateView,
        event_seq: i64,
        request: SubmitCommand,
        now: i64,
    ) -> Result<RuleEventAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.admit_rule_stop_inner(principal, state, event_seq, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn admit_rule_stop_inner(
        &mut self,
        principal: &str,
        state: &core::api::RuleStateView,
        event_seq: i64,
        mut request: SubmitCommand,
        now: i64,
    ) -> Result<RuleEventAdmission> {
        require_nonempty(principal, "principal")?;
        require_nonempty(&state.id, "rule id")?;
        if event_seq <= 0 {
            return Err(Error::new("invalid", "event sequence must be positive"));
        }

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let stored = load_rule_state(&tx, principal, &state.id)?
            .ok_or_else(|| Error::new("not_found", "rule state was not found"))?;
        let key = RuleEventKey::new(principal, state, event_seq)?;

        if let Some(parent) = load_command(&tx, &key.parent_id)? {
            if parent.principal != principal
                || parent.verb != "rule"
                || parent.args.get("rule").and_then(Value::as_str) != Some(state.id.as_str())
                || parent.args.get("event_seq").and_then(Value::as_i64) != Some(event_seq)
            {
                return Err(Error::new(
                    "source_integrity",
                    "derived rule parent id is bound to another action",
                ));
            }
            if stored.view.current_cursor < event_seq {
                return Err(Error::new(
                    "source_integrity",
                    "rule parent exists before its event cursor commit",
                ));
            }
            let child = load_rule_child(&tx, &key.parent_id, &key.child_id)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(RuleEventAdmission {
                state: stored.view,
                parent,
                child,
                events: Vec::new(),
                is_new: false,
            });
        }
        if stored.view.current_cursor >= event_seq {
            return Err(Error::new(
                "bad_state",
                "rule event was already scanned without its deterministic parent",
            ));
        }
        if stored.view != *state {
            return Err(Error::new("bad_state", "rule cursor view is stale"));
        }
        if !stored.view.enabled {
            return Err(Error::new(
                "bad_state",
                "disabled rules cannot admit actions",
            ));
        }

        let event = load_rule_trigger(&tx, event_seq)?;
        if event.kind != RULE_TRIGGER_EVENT {
            return Err(Error::new(
                "invalid",
                "rule stop admission requires a turn.completed event",
            ));
        }
        let binding_error = stop_binding_error(&event, &request);
        request.id = key.child_id.clone();

        if let Some(code) = binding_error {
            return reject_rule_event(tx, &self.write_faulted, &key, &code, now);
        }

        let (rule_count, global_count) = rule_action_counts(&tx, principal, &state.id, now)?;
        if rule_count >= RULE_ACTION_LIMIT || global_count >= GLOBAL_RULE_ACTION_LIMIT {
            return reject_rule_event(tx, &self.write_faulted, &key, "rate_limited", now);
        }

        // submit_tx validates and admits the ordinary stop command. Its stop path
        // performs all deterministic checks before inserting the child row; the
        // surrounding transaction rolls every write back on persistence failure.
        let child_admission = match submit_tx(&tx, principal, request, None, now, true) {
            Ok(admission) => admission,
            Err(error) if deterministic_rule_rejection(&error) => {
                return reject_rule_event(tx, &self.write_faulted, &key, &error.code, now);
            }
            Err(error) => return Err(error),
        };
        if !child_admission.is_new {
            return Err(Error::new(
                "source_integrity",
                "rule child exists without its deterministic parent",
            ));
        }

        let child = child_admission.command;
        let (parent, parent_event) =
            insert_rule_parent(&tx, &key, RuleParentOutcome::Admitted, now)?;
        let linked = tx
            .execute(
                "UPDATE commands
                 SET parent_command = ?1, link_kind = 'rule_action', ordinal = 0
                 WHERE id = ?2 AND parent_command IS NULL
                   AND link_kind IS NULL AND ordinal IS NULL",
                params![key.parent_id, child.id],
            )
            .map_err(sql_error)?;
        if linked != 1 {
            return Err(Error::new(
                "source_integrity",
                "new rule child could not be bound to its parent",
            ));
        }

        let mut events = child_admission.events;
        events.push(parent_event);
        events.push(insert_rule_event(
            &tx,
            now,
            "rule.fired",
            json!({
                "rule": key.state.id,
                "revision": key.state.revision,
                "event_seq": key.event_seq,
                "parent_command": key.parent_id,
                "child_command": child.id,
            }),
        )?);
        let view = update_rule_cursor_tx(&tx, key.principal, &stored.view, key.event_seq, now)?;
        commit_transaction(tx, &self.write_faulted)?;

        Ok(RuleEventAdmission {
            state: view,
            parent,
            child: Some(child),
            events,
            is_new: true,
        })
    }

    pub fn recover_rule_children(&self, after: Option<&str>, limit: usize) -> Result<Vec<Command>> {
        validate_limit(limit)?;
        if let Some(after) = after {
            validate_ulid(after)?;
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, principal, method, target, expected_generation, generation,
                        expected_turn_id, turn_id, request_id, verb, args, state, outcome,
                        created_at, updated_at, probe
                 FROM commands
                 WHERE link_kind = 'rule_action' AND ordinal = 0
                   AND state IN ('accepted','queued')
                   AND (?1 IS NULL OR id > ?1)
                 ORDER BY id LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after, limit as i64], command_record_from_row)
            .map_err(sql_error)?;
        let mut commands = Vec::new();
        for row in rows {
            commands.push(command_from_raw(row.map_err(sql_error)?)?.command);
        }
        enforce_page_bytes(&mut commands)?;
        Ok(commands)
    }
}

fn validate_principal_rule(principal: &str, rule: &str) -> Result<()> {
    require_nonempty(principal, "principal")?;
    require_nonempty(rule, "rule id")
}

fn validate_definition_input(input: &RuleDefinitionInput) -> Result<()> {
    if input.canonical_definition.len() > 128 * 1024 {
        return Err(Error::new(
            "frame_too_large",
            "canonical rule definition exceeds 128 KiB",
        ));
    }
    let value: Value = serde_json::from_str(&input.canonical_definition)
        .map_err(|_| Error::new("invalid", "canonical rule definition is invalid JSON"))?;
    let canonical = serde_json::to_string(&value)
        .map_err(|_| Error::new("invalid", "canonical rule definition cannot be encoded"))?;
    if canonical != input.canonical_definition {
        return Err(Error::new(
            "invalid",
            "rule definition must use canonical JSON encoding",
        ));
    }
    let digest = core::canonical_json_sha256(&value)
        .map_err(|_| Error::new("invalid", "rule definition cannot be canonicalized"))?;
    if digest != input.definition_digest {
        return Err(Error::new(
            "source_integrity",
            "rule definition digest does not match its canonical content",
        ));
    }
    Ok(())
}

fn load_rule_state(
    connection: &Connection,
    principal: &str,
    id: &str,
) -> Result<Option<StoredRuleState>> {
    let row = connection
        .query_row(
            "SELECT revision, current_definition, definition_digest,
                    current_cursor, enabled, paused_reason
             FROM rule_state WHERE principal = ?1 AND rule = ?2",
            params![principal, id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some((
        revision,
        canonical_definition,
        digest_bytes,
        current_cursor,
        enabled,
        paused_reason,
    )) = row
    else {
        return Ok(None);
    };
    let definition_digest: [u8; 32] = digest_bytes.try_into().map_err(|_| {
        Error::new(
            "source_integrity",
            "stored rule digest has the wrong length",
        )
    })?;
    Ok(Some(StoredRuleState {
        view: core::api::RuleStateView {
            id: id.to_owned(),
            revision,
            current_cursor,
            enabled,
            paused_reason,
        },
        canonical_definition,
        definition_digest,
    }))
}

fn high_water_tx(tx: &Transaction<'_>) -> Result<i64> {
    tx.query_row(
        "SELECT MAX(
             COALESCE((SELECT MAX(seq) FROM events), 0),
             COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)
         )",
        [],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn has_trigger_in_range(tx: &Transaction<'_>, after: i64, through: i64) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM events
             WHERE seq > ?1 AND seq <= ?2 AND kind = ?3
         )",
        params![after, through, RULE_TRIGGER_EVENT],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn update_rule_cursor_tx(
    tx: &Transaction<'_>,
    principal: &str,
    state: &core::api::RuleStateView,
    through: i64,
    now: i64,
) -> Result<core::api::RuleStateView> {
    let changed = tx
        .execute(
            "UPDATE rule_state
             SET current_cursor = ?1, updated_at = ?2
             WHERE principal = ?3 AND rule = ?4 AND revision = ?5
               AND current_cursor = ?6",
            params![
                through,
                now,
                principal,
                state.id,
                state.revision,
                state.current_cursor,
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new("bad_state", "rule cursor changed concurrently"));
    }
    let mut next = state.clone();
    next.current_cursor = through;
    Ok(next)
}

fn insert_rule_event(tx: &Transaction<'_>, now: i64, kind: &str, data: Value) -> Result<Event> {
    insert_daemon_event(tx, now, kind, "rule", None, None, data)
}

fn derived_rule_id(
    purpose: &str,
    principal: &str,
    rule: &str,
    revision: i64,
    event_seq: i64,
) -> Result<String> {
    let digest = canonical::digest(&json!({
        "domain": RULE_COMMAND_ID_DOMAIN,
        "purpose": purpose,
        "principal": principal,
        "rule": rule,
        "revision": revision,
        "event_seq": event_seq,
    }))?;
    let bytes: [u8; 16] = digest[..16]
        .try_into()
        .map_err(|_| Error::new("internal", "rule command digest is too short"))?;
    Ok(Ulid::from_bytes(bytes).to_string())
}

fn load_rule_trigger(tx: &Transaction<'_>, event_seq: i64) -> Result<StoredRuleTrigger> {
    tx.query_row(
        "SELECT kind, agent, generation FROM events WHERE seq = ?1",
        [event_seq],
        |row| {
            Ok(StoredRuleTrigger {
                kind: row.get(0)?,
                agent: row.get(1)?,
                generation: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(sql_error)?
    .ok_or_else(|| Error::new("not_found", "rule trigger event was not found"))
}

struct StoredRuleTrigger {
    kind: String,
    agent: Option<i64>,
    generation: Option<i64>,
}

fn stop_binding_error(event: &StoredRuleTrigger, request: &SubmitCommand) -> Option<String> {
    let agent_id = event.agent.map(format_agent_id);
    let valid_args = request.args.as_object().is_some_and(|args| {
        args.iter()
            .all(|(key, value)| key == "force" && value == &Value::Bool(false))
    });
    if request.verb != "stop"
        || request.target != agent_id
        || request.expected_generation != event.generation
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
        || !valid_args
    {
        Some("rule_action_binding_invalid".to_owned())
    } else {
        None
    }
}

fn rule_action_counts(
    tx: &Transaction<'_>,
    principal: &str,
    rule: &str,
    now: i64,
) -> Result<(i64, i64)> {
    let since = now.saturating_sub(RULE_ACTION_WINDOW_MS);
    let per_rule = tx
        .query_row(
            "SELECT COUNT(*)
             FROM commands action
             JOIN commands parent ON parent.id = action.parent_command
             WHERE action.link_kind = 'rule_action' AND action.ordinal = 0
               AND action.created_at >= ?1 AND action.created_at <= ?2
               AND parent.principal = ?3
               AND json_extract(parent.args, '$.rule') = ?4",
            params![since, now, principal, rule],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let global = tx
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE link_kind = 'rule_action' AND ordinal = 0
               AND created_at >= ?1 AND created_at <= ?2",
            params![since, now],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok((per_rule, global))
}

fn deterministic_rule_rejection(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "invalid"
            | "not_allowed"
            | "not_found"
            | "unsupported"
            | "bad_state"
            | "stale_generation"
            | "frame_too_large"
    )
}

fn insert_rule_parent(
    tx: &Transaction<'_>,
    key: &RuleEventKey<'_>,
    outcome_kind: RuleParentOutcome<'_>,
    now: i64,
) -> Result<(Command, Event)> {
    if write_exists(tx, &key.parent_id)? {
        return Err(Error::new("id_conflict", "rule parent id is already used"));
    }
    let (child_ids, state, rejection_code) = match outcome_kind {
        RuleParentOutcome::Admitted => (vec![key.child_id.clone()], CommandState::Applied, None),
        RuleParentOutcome::Rejected { code } => (Vec::new(), CommandState::Rejected, Some(code)),
    };
    let args = json!({
        "rule": key.state.id,
        "revision": key.state.revision,
        "event_seq": key.event_seq,
    });
    let outcome = match rejection_code {
        Some(code) => json!({"children": child_ids, "code": code}),
        None => json!({"children": child_ids}),
    };
    let digest = canonical::digest(&json!({
        "principal": key.principal,
        "verb": "rule",
        "args": args,
        "outcome": outcome,
    }))?;
    let command = CommandInsert {
        id: &key.parent_id,
        principal: key.principal,
        method: COMMAND_METHOD,
        payload_digest: &digest,
        target: None,
        expected_generation: None,
        generation: None,
        expected_turn_id: None,
        parent_command: None,
        link_kind: None,
        ordinal: None,
        turn_id: None,
        request_id: None,
        probe: false,
        verb: "rule",
        args: &args,
        state,
        outcome: Some(&outcome),
        created_at: now,
        updated_at: now,
    };
    let event = admit_command(
        tx,
        &command,
        json!({
            "command_id": key.parent_id,
            "rule": key.state.id,
            "revision": key.state.revision,
            "event_seq": key.event_seq,
            "children": child_ids,
        }),
    )?;
    Ok((
        Command {
            id: key.parent_id.clone(),
            principal: key.principal.to_owned(),
            method: COMMAND_METHOD.to_owned(),
            verb: "rule".to_owned(),
            target: None,
            expected_generation: None,
            generation: None,
            args,
            state,
            turn_id: None,
            request_id: None,
            outcome: Some(outcome),
            created_at: now,
            updated_at: now,
            probe: false,
        },
        event,
    ))
}

fn rule_rejected_data(key: &RuleEventKey<'_>, code: &str) -> Value {
    json!({
        "rule": key.state.id,
        "revision": key.state.revision,
        "event_seq": key.event_seq,
        "parent_command": key.parent_id,
        "code": code,
    })
}

fn reject_rule_event(
    tx: Transaction<'_>,
    write_faulted: &Cell<bool>,
    key: &RuleEventKey<'_>,
    code: &str,
    now: i64,
) -> Result<RuleEventAdmission> {
    let (parent, parent_event) =
        insert_rule_parent(&tx, key, RuleParentOutcome::Rejected { code }, now)?;
    let mut events = vec![parent_event];
    events.push(insert_rule_event(
        &tx,
        now,
        "rule.rejected",
        rule_rejected_data(key, code),
    )?);
    let view = update_rule_cursor_tx(&tx, key.principal, key.state, key.event_seq, now)?;
    commit_transaction(tx, write_faulted)?;
    Ok(RuleEventAdmission {
        state: view,
        parent,
        child: None,
        events,
        is_new: true,
    })
}

fn load_rule_child(
    tx: &Transaction<'_>,
    parent_id: &str,
    expected_child_id: &str,
) -> Result<Option<Command>> {
    let child_id: Option<String> = tx
        .query_row(
            "SELECT id FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal = 0",
            [parent_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(child_id) = child_id else {
        return Ok(None);
    };
    if child_id != expected_child_id {
        return Err(Error::new(
            "source_integrity",
            "rule parent child id does not match its deterministic action id",
        ));
    }
    load_command(tx, &child_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn store() -> Store {
        Store::open(Path::new(":memory:")).unwrap()
    }

    fn definition(id: &str, enabled: bool) -> RuleDefinitionInput {
        let value = json!({"id": id, "enabled": enabled});
        RuleDefinitionInput {
            id: id.to_owned(),
            canonical_definition: serde_json::to_string(&value).unwrap(),
            definition_digest: core::canonical_json_sha256(&value).unwrap(),
            enabled,
        }
    }

    fn insert_test_event(store: &mut Store, kind: &str, now: i64) -> i64 {
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let event =
            insert_daemon_event(&tx, now, kind, "rule_test", None, None, json!({})).unwrap();
        tx.commit().unwrap();
        event.seq
    }

    fn insert_test_agent(store: &mut Store) -> (i64, String) {
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        tx.execute(
            "INSERT INTO agents(generation, project, harness, profile, state, created_at, updated_at)
             VALUES (1, 'project-test', 'fake', 'fake', 'idle', 1, 1)",
            [],
        )
        .unwrap();
        let agent = tx.last_insert_rowid();
        tx.commit().unwrap();
        (agent, format_agent_id(agent))
    }

    fn insert_test_turn_completed(store: &mut Store, agent: i64, now: i64) -> i64 {
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let event = insert_event(
            &tx,
            EventInsert {
                ts: now,
                kind: RULE_TRIGGER_EVENT,
                family: Some("turn"),
                agent: Some(agent),
                generation: Some(1),
                source: "stream",
                native_id: None,
                cursor: None,
                payload_digest: None,
                subject: None,
                data: json!({"status":"completed"}),
            },
        )
        .unwrap();
        tx.commit().unwrap();
        event.seq
    }

    fn stop_request(agent: &str) -> SubmitCommand {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "stop");
        request.target = Some(agent.to_owned());
        request.expected_generation = Some(1);
        request
    }

    #[test]
    fn new_rule_seeds_once_and_changed_startup_definition_does_not_reseed() {
        let mut store = store();
        let first_event = insert_test_event(&mut store, "fixture", 1);
        let first = definition("r1", true);
        let view = store.ensure_rule_state("owner", &first, 2).unwrap();
        assert_eq!(view.current_cursor, first_event);

        let later_event = insert_test_event(&mut store, "fixture", 3);
        assert_eq!(store.ensure_rule_state("owner", &first, 4).unwrap(), view);

        let changed = definition("r1", false);
        assert_eq!(
            store
                .ensure_rule_state("owner", &changed, 5)
                .unwrap_err()
                .code,
            "unsupported"
        );
        assert_eq!(store.get_rule_state("owner", "r1").unwrap().unwrap(), view);
        assert!(later_event > view.current_cursor);
    }

    #[test]
    fn no_match_and_disabled_range_bookkeeping_reaches_quiescence() {
        let mut store = store();
        let enabled = store
            .ensure_rule_state("owner", &definition("enabled", true), 1)
            .unwrap();
        let no_match_seq = insert_test_event(&mut store, "rule.skipped", 2);
        let no_match = store
            .advance_rule_cursor(
                "owner",
                &enabled,
                no_match_seq,
                RuleCursorOutcome::NoMatch,
                3,
            )
            .unwrap();
        assert!(no_match.events.is_empty());
        let high_water_after_no_match = store.high_water().unwrap();

        let disabled = store
            .ensure_rule_state("owner", &definition("disabled", false), 4)
            .unwrap();
        let trigger_seq = insert_test_event(&mut store, "turn.completed", 5);
        let skipped = store
            .advance_rule_cursor(
                "owner",
                &disabled,
                trigger_seq,
                RuleCursorOutcome::Disabled,
                6,
            )
            .unwrap();
        assert_eq!(skipped.events.len(), 1);
        assert_eq!(skipped.events[0].kind, "rule.skipped");

        // The disabled marker itself is bookkeeping, so advancing through it
        // cannot create another marker (or feed another disabled rule).
        let marker_seq = skipped.events[0].seq;
        let marker = store
            .advance_rule_cursor(
                "owner",
                &skipped.value,
                marker_seq,
                RuleCursorOutcome::NoMatch,
                7,
            )
            .unwrap();
        assert!(marker.events.is_empty());
        assert_eq!(store.high_water().unwrap(), marker_seq);
        assert_eq!(high_water_after_no_match, no_match_seq);

        let disabled_again = store
            .advance_rule_cursor(
                "owner",
                &marker.value,
                marker_seq,
                RuleCursorOutcome::Disabled,
                8,
            )
            .unwrap();
        assert!(disabled_again.events.is_empty());
        assert_eq!(store.high_water().unwrap(), marker_seq);
    }

    #[test]
    fn cursor_persistence_failure_rolls_back_and_faults_writer() {
        let mut store = store();
        let view = store
            .ensure_rule_state("owner", &definition("r1", true), 1)
            .unwrap();
        let event_seq = insert_test_event(&mut store, "fixture", 2);
        store
            .connection
            .execute_batch(
                "CREATE TRIGGER fail_rule_cursor BEFORE UPDATE OF current_cursor ON rule_state
                 BEGIN SELECT RAISE(ABORT, 'injected rule cursor failure'); END;",
            )
            .unwrap();

        assert_eq!(
            store
                .advance_rule_cursor("owner", &view, event_seq, RuleCursorOutcome::NoMatch, 3,)
                .unwrap_err()
                .code,
            "internal"
        );
        assert!(store.is_write_faulted());
        assert_eq!(store.get_rule_state("owner", "r1").unwrap().unwrap(), view);
        assert_eq!(store.high_water().unwrap(), event_seq);
    }

    #[test]
    fn stop_admission_counts_only_children_and_recovers_undispatched_children() {
        let mut store = store();
        let (agent_num, agent_id) = insert_test_agent(&mut store);
        let mut state = store
            .ensure_rule_state("owner", &definition("stop-rule", true), 10_000)
            .unwrap();
        let now = 10_000;

        for _ in 0..RULE_ACTION_LIMIT {
            let event_seq = insert_test_turn_completed(&mut store, agent_num, now);
            let admission = store
                .admit_rule_stop("owner", &state, event_seq, stop_request(&agent_id), now)
                .unwrap();
            assert!(admission.is_new);
            let child = admission.child.as_ref().expect("admitted stop child");
            assert_eq!(child.verb, "stop");
            assert_eq!(admission.parent.state, CommandState::Applied);
            assert_eq!(
                admission.parent.outcome.as_ref().unwrap()["children"][0],
                child.id
            );
            let fired = admission
                .events
                .iter()
                .find(|event| event.kind == "rule.fired")
                .expect("committed rule firing event");
            assert_eq!(fired.data["parent_command"], admission.parent.id);
            assert_eq!(fired.data["child_command"], child.id);
            state = admission.state;
        }

        let rate_event_seq = insert_test_turn_completed(&mut store, agent_num, now);
        let limited = store
            .admit_rule_stop(
                "owner",
                &state,
                rate_event_seq,
                stop_request(&agent_id),
                now,
            )
            .unwrap();
        assert!(limited.is_new);
        assert!(limited.child.is_none());
        assert_eq!(limited.parent.state, CommandState::Rejected);
        assert_eq!(
            limited.parent.outcome.as_ref().unwrap()["children"],
            json!([])
        );
        assert_eq!(
            limited.parent.outcome.as_ref().unwrap()["code"],
            "rate_limited"
        );
        assert!(
            limited
                .events
                .iter()
                .any(|event| event.kind == "rule.rejected")
        );
        assert_eq!(limited.state.current_cursor, rate_event_seq);

        let first_page = store.recover_rule_children(None, 4).unwrap();
        assert_eq!(first_page.len(), 4);
        let after = first_page.last().unwrap().id.clone();
        let second_page = store.recover_rule_children(Some(&after), 100).unwrap();
        assert_eq!(second_page.len(), RULE_ACTION_LIMIT as usize - 4);
        assert!(second_page.first().unwrap().id > after);
        let pending_after_pages = first_page
            .into_iter()
            .chain(second_page)
            .collect::<Vec<_>>();
        assert_eq!(pending_after_pages.len(), RULE_ACTION_LIMIT as usize);
        assert!(
            pending_after_pages
                .iter()
                .all(|command| command.state == CommandState::Accepted)
        );

        store
            .connection
            .execute(
                "UPDATE commands SET state = 'dispatched' WHERE id = ?1",
                [&pending_after_pages[0].id],
            )
            .unwrap();
        let pending_after_dispatch = store.recover_rule_children(None, 100).unwrap();
        assert_eq!(pending_after_dispatch.len(), RULE_ACTION_LIMIT as usize - 1);
        assert!(pending_after_dispatch.iter().all(|command| {
            matches!(command.state, CommandState::Accepted | CommandState::Queued)
        }));
    }
}
