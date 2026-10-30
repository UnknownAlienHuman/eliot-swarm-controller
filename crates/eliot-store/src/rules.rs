//! Durable cursor and command admission for the first event-rule slice.
//!
//! The daemon owns rule parsing and matching. Store owns the persisted definition,
//! event cursor, action accounting, and the parent/child admission transaction.

use super::*;

#[path = "rule_history.rs"]
mod rule_history;

const RULE_ACTION_WINDOW_MS: i64 = 60_000;
const RULE_ACTION_LIMIT: i64 = 10;
const GLOBAL_RULE_ACTION_LIMIT: i64 = 120;
const RULE_COMMAND_ID_DOMAIN: &str = "eliot-rule-command-v1";

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct RuleCatchUpAction {
    pub event_seq: i64,
    pub outcome: RuleCatchUpActionOutcome,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum RuleCatchUpActionOutcome {
    Command(SubmitCommand),
    Rejected { code: String },
}

impl RuleCatchUpAction {
    pub fn command(event_seq: i64, request: SubmitCommand) -> Self {
        Self {
            event_seq,
            outcome: RuleCatchUpActionOutcome::Command(request),
        }
    }

    pub fn rejected(event_seq: i64, code: impl Into<String>) -> Self {
        Self {
            event_seq,
            outcome: RuleCatchUpActionOutcome::Rejected { code: code.into() },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuleCatchUpAdmission {
    pub parent: Command,
    pub children: Vec<Command>,
    pub events: Vec<Event>,
    pub is_new: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleDefinitionInput {
    pub id: String,
    pub canonical_definition: String,
    pub definition_digest: [u8; 32],
    pub enabled: bool,
}

impl RuleDefinitionInput {
    pub fn validated_value(&self) -> Result<serde_json::Value> {
        if self.canonical_definition.len() > 128 * 1024 {
            return Err(Error::new(
                "frame_too_large",
                "canonical rule definition exceeds 128 KiB",
            ));
        }
        let value: serde_json::Value = serde_json::from_str(&self.canonical_definition)
            .map_err(|_| Error::new("invalid", "canonical rule definition is invalid JSON"))?;
        let canonical = serde_json::to_string(&value)
            .map_err(|_| Error::new("invalid", "canonical rule definition cannot be encoded"))?;
        if canonical != self.canonical_definition {
            return Err(Error::new(
                "invalid",
                "rule definition must use canonical JSON encoding",
            ));
        }
        let digest = core::canonical_json_sha256(&value)
            .map_err(|_| Error::new("invalid", "rule definition cannot be canonicalized"))?;
        if digest != self.definition_digest {
            return Err(Error::new(
                "source_integrity",
                "rule definition digest does not match its canonical content",
            ));
        }
        rule_limit_from_value(&value)?;
        Ok(value)
    }
}

fn rule_limit_from_value(value: &serde_json::Value) -> Result<Option<core::api::RuleLimit>> {
    let Some(value) = value.get("limit") else {
        return Ok(None);
    };
    let limit: core::api::RuleLimit = serde_json::from_value(value.clone())
        .map_err(|_| Error::new("invalid", "rule limit is invalid"))?;
    limit.validate()?;
    Ok(Some(limit))
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
    /// Absent only for a cause-depth-suppressed event, which advances the
    /// cursor without creating a parent or action command.
    pub parent: Option<Command>,
    pub child: Option<Command>,
    pub events: Vec<Event>,
    pub is_new: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RuleActionDispatchGate {
    Ready,
    Held,
    Cancelled,
}

/// Store-owned processing input for the daemon worker. The public Core view
/// carries cursor progress; serialized definitions stay inside this Store API.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RuleProcessingState {
    pub view: core::api::RuleStateView,
    pub current_definition: String,
    pub retiring_definition: Option<String>,
}

#[derive(Clone, Debug)]
struct StoredRuleState {
    view: core::api::RuleStateView,
    canonical_definition: String,
    definition_digest: [u8; 32],
    retiring_definition: Option<String>,
}

#[derive(Clone, Copy)]
enum RuleFiringIdentity {
    Event {
        event_seq: i64,
    },
    Timer {
        schedule_revision: i64,
        slot_ms: i64,
    },
    Manual,
}

struct RuleFiringKey<'a> {
    principal: &'a str,
    state: &'a core::api::RuleStateView,
    trigger_cause: Option<core::EventCause>,
    identity: RuleFiringIdentity,
    verb: &'static str,
    args: Value,
    parent_digest: Option<Vec<u8>>,
    parent_id: String,
    child_id: String,
}

impl<'a> RuleFiringKey<'a> {
    fn event(
        principal: &'a str,
        state: &'a core::api::RuleStateView,
        revision: i64,
        event_seq: i64,
        trigger_cause: Option<core::EventCause>,
    ) -> Result<Self> {
        let mut args = json!({
            "rule": state.id,
            "revision": revision,
            "event_seq": event_seq,
        });
        if let Some(project) = &state.project {
            args["project"] = json!(project);
        }
        Ok(Self {
            principal,
            state,
            trigger_cause,
            identity: RuleFiringIdentity::Event { event_seq },
            verb: "rule",
            args,
            parent_digest: None,
            parent_id: derived_rule_id("parent", principal, &state.id, revision, event_seq)?,
            child_id: derived_rule_id("action-0", principal, &state.id, revision, event_seq)?,
        })
    }

    fn timer(
        principal: &'a str,
        state: &'a core::api::RuleStateView,
        schedule_revision: i64,
        slot_ms: i64,
    ) -> Result<Self> {
        let parent_id =
            derived_timer_id("parent", principal, &state.id, schedule_revision, slot_ms)?;
        let child_id =
            derived_timer_id("action-0", principal, &state.id, schedule_revision, slot_ms)?;
        let mut args = json!({
            "rule": state.id,
            "schedule_revision": schedule_revision,
            "scheduled_at_ms": slot_ms,
        });
        if let Some(project) = &state.project {
            args["project"] = json!(project);
        }
        Ok(Self {
            principal,
            state,
            trigger_cause: None,
            identity: RuleFiringIdentity::Timer {
                schedule_revision,
                slot_ms,
            },
            verb: "rule",
            args,
            parent_digest: None,
            parent_id,
            child_id,
        })
    }

    fn manual(
        principal: &'a str,
        state: &'a core::api::RuleStateView,
        parent_id: &'a str,
        parent_digest: Vec<u8>,
    ) -> Result<Self> {
        let mut args = json!({"rule": state.id});
        if let Some(project) = &state.project {
            args["project"] = json!(project);
        }
        Ok(Self {
            principal,
            state,
            trigger_cause: None,
            identity: RuleFiringIdentity::Manual,
            verb: "timer.run",
            args,
            parent_digest: Some(parent_digest),
            parent_id: parent_id.to_owned(),
            child_id: derived_manual_timer_id(principal, &state.id, parent_id)?,
        })
    }

    fn command_event_data(&self, children: Vec<String>) -> Value {
        let mut data = self.args.clone();
        let object = data.as_object_mut().expect("firing args are an object");
        object.insert("command_id".to_owned(), json!(self.parent_id));
        object.insert("children".to_owned(), json!(children));
        data
    }

    fn fired_data(&self, child_id: &str) -> Value {
        let mut data = self.args.clone();
        let object = data.as_object_mut().expect("firing args are an object");
        object.insert("parent_command".to_owned(), json!(self.parent_id));
        object.insert("child_command".to_owned(), json!(child_id));
        data
    }

    fn rejected_data(&self, code: &str) -> Value {
        let mut data = self.args.clone();
        let object = data.as_object_mut().expect("firing args are an object");
        object.insert("parent_command".to_owned(), json!(self.parent_id));
        object.insert("code".to_owned(), json!(code));
        data
    }

    fn command_cause(&self, command_id: &str) -> Result<core::EventCause> {
        let depth = next_rule_cause_depth(self.trigger_cause.as_ref());
        if depth > 5 {
            return Err(Error::new(
                "bad_state",
                "rule cause depth exceeds the firing limit",
            ));
        }
        Ok(core::EventCause {
            command_id: command_id.to_owned(),
            rule: Some(self.state.id.clone()),
            depth,
        })
    }

    fn matches_parent(&self, parent: &Command, principal: &str) -> bool {
        if parent.principal != principal || parent.verb != self.verb || parent.args != self.args {
            return false;
        }
        match self.identity {
            RuleFiringIdentity::Event { event_seq } => {
                parent.args.get("event_seq").and_then(Value::as_i64) == Some(event_seq)
            }
            RuleFiringIdentity::Timer {
                schedule_revision,
                slot_ms,
            } => {
                parent.args.get("schedule_revision").and_then(Value::as_i64)
                    == Some(schedule_revision)
                    && parent.args.get("scheduled_at_ms").and_then(Value::as_i64) == Some(slot_ms)
            }
            RuleFiringIdentity::Manual => parent.id == self.parent_id,
        }
    }
}

fn next_rule_cause_depth(trigger_cause: Option<&core::EventCause>) -> u8 {
    trigger_cause.map_or(1, |cause| cause.depth.saturating_add(1))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerDefinitionInput {
    pub id: String,
    pub schedule_revision: i64,
    pub schedule: core::api::TimerSchedule,
    pub enabled: bool,
    pub initial_next_due_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TimerSlotCommit {
    pub scheduled_at_ms: i64,
    pub next_due_at_ms: Option<i64>,
    pub action: Option<SubmitCommand>,
    pub skip_code: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TimerSlotAdmission {
    pub state: core::api::TimerStateView,
    pub parent: Option<Command>,
    pub child: Option<Command>,
    pub events: Vec<Event>,
    pub is_new: bool,
    pub blocked: bool,
}

enum RuleParentOutcome<'a> {
    Admitted,
    Rejected { code: &'a str },
}

impl Store {
    /// Create or reconcile one rule using the transaction's captured high-water.
    pub fn ensure_rule_state(
        &mut self,
        principal: &str,
        input: &RuleDefinitionInput,
        now: i64,
    ) -> Result<core::api::RuleStateView> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            validate_principal_rule(principal, &input.id)?;
            let value = input.validated_value()?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let project = validate_rule_principal_tx(&tx, principal)?;
            validate_project_rule_definition(project.as_ref(), &value)?;

            let high_water = high_water_tx(&tx)?;
            let mut events = Vec::new();
            let view = ensure_rule_state_tx(&tx, principal, input, high_water, now, &mut events)?;
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
        validate_rule_principal_read(&self.connection, principal)?;
        require_nonempty(id, "rule id")?;
        Ok(load_rule_state(&self.connection, principal, id)?.map(|state| state.view))
    }

    /// Reconcile a startup or watcher snapshot. A single captured event high-water
    /// fences every definition edit in this transaction; timer schedule inputs use
    /// the same registration sample supplied by the daemon.
    pub fn reconcile_rule_definitions(
        &mut self,
        principal: &str,
        definitions: &[RuleDefinitionInput],
        timers: &[TimerDefinitionInput],
        now: i64,
    ) -> Result<Change<Vec<RuleProcessingState>>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            require_nonempty(principal, "principal")?;
            rule_project_id(principal)?;
            let mut definition_ids = HashSet::new();
            let mut values = Vec::with_capacity(definitions.len());
            for definition in definitions {
                validate_principal_rule(principal, &definition.id)?;
                values.push(definition.validated_value()?);
                if !definition_ids.insert(definition.id.as_str()) {
                    return Err(Error::new("invalid", "rule IDs must be unique"));
                }
            }
            let mut timer_ids = HashSet::new();
            for timer in timers {
                if !definition_ids.contains(timer.id.as_str())
                    || !timer_ids.insert(timer.id.as_str())
                {
                    return Err(Error::new(
                        "invalid",
                        "timer inputs must name unique rules in the snapshot",
                    ));
                }
            }

            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let project = validate_rule_principal_tx(&tx, principal)?;
            for value in &values {
                validate_project_rule_definition(project.as_ref(), value)?;
            }
            let high_water = high_water_tx(&tx)?;
            let mut events = Vec::new();
            let existing_ids = rule_ids_tx(&tx, principal)?;
            for id in existing_ids {
                if !definition_ids.contains(id.as_str()) {
                    disable_deleted_rule_tx(&tx, principal, &id, high_water, now, &mut events)?;
                }
            }

            for definition in definitions {
                ensure_rule_state_tx(&tx, principal, definition, high_water, now, &mut events)?;
            }

            for timer in timers {
                let definition = definitions
                    .iter()
                    .find(|definition| definition.id == timer.id)
                    .expect("timer IDs were checked against definitions");
                let stored = load_rule_state(&tx, principal, &timer.id)?
                    .ok_or_else(|| Error::new("internal", "reconciled rule state is absent"))?;
                // A new edit is deferred while the previous revision retires. Do
                // not apply its timer schedule until the matching definition does.
                if stored.canonical_definition == definition.canonical_definition
                    && stored.view.enabled == definition.enabled
                {
                    ensure_timer_state_tx(&tx, principal, timer, now)?;
                }
            }

            for definition in definitions {
                let stored = load_rule_state(&tx, principal, &definition.id)?
                    .ok_or_else(|| Error::new("internal", "reconciled rule state is absent"))?;
                if stored.canonical_definition == definition.canonical_definition
                    && stored.view.enabled == definition.enabled
                    && !timer_ids.contains(definition.id.as_str())
                {
                    tx.execute(
                        "DELETE FROM timers WHERE principal = ?1 AND rule = ?2",
                        params![principal, definition.id],
                    )
                    .map_err(sql_error)?;
                }
            }

            let states = list_rule_processing_states_tx(&tx, principal, None, MAX_PAGE_ENTRIES)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: states,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn list_rule_processing_states(
        &self,
        principal: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RuleProcessingState>> {
        validate_rule_principal_read(&self.connection, principal)?;
        validate_limit(limit)?;
        if let Some(after_id) = after_id {
            require_nonempty(after_id, "rule cursor")?;
        }
        list_rule_processing_states_tx(&self.connection, principal, after_id, limit)
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
            validate_rule_principal_tx(&tx, principal)?;
            let stored = load_rule_state(&tx, principal, &state.id)?
                .ok_or_else(|| Error::new("not_found", "rule state was not found"))?;

            let retiring = rule_cursor_is_retiring(&stored.view, through);
            let cursor = if retiring {
                stored.view.retiring_cursor.ok_or_else(|| {
                    Error::new("source_integrity", "retiring cursor is incomplete")
                })?
            } else {
                stored.view.current_cursor
            };
            // A replay after a successful commit is a no-op. It must not emit a
            // second skip/rejection event or move either lane backwards.
            if through <= cursor {
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
            if retiring && through > stored.view.retire_through.unwrap_or_default() {
                return Err(Error::new(
                    "invalid",
                    "retiring rule cursor cannot advance past its captured boundary",
                ));
            }
            let revision = if retiring {
                stored.view.retiring_revision.ok_or_else(|| {
                    Error::new("source_integrity", "retiring revision is incomplete")
                })?
            } else {
                stored.view.revision
            };

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
                    if has_non_rule_event_in_range(&tx, cursor, through)? {
                        events.push(insert_rule_event(
                            &tx,
                            now,
                            "rule.skipped",
                            json!({
                                "rule": state.id,
                                "revision": revision,
                                "from_seq": cursor + 1,
                                "through_seq": through,
                                "reason": "disabled",
                            }),
                        )?);
                    }
                }
                RuleCursorOutcome::Rejected { code } => {
                    validate_rule_rejection_code(&code)?;
                    let trigger = load_rule_trigger(&tx, through)?;
                    // A malformed rule-triggered bookkeeping event is consumed
                    // silently so a rule cannot generate an endless rejection chain.
                    // Also suppress a would-be sixth-level derivative before
                    // writing the rejection marker; the cursor still commits below.
                    if !trigger.kind.starts_with("rule.")
                        && next_rule_cause_depth(trigger.cause.as_ref()) <= 5
                    {
                        events.push(insert_rule_event_with_cause(
                            &tx,
                            now,
                            "rule.rejected",
                            json!({
                                "rule": state.id,
                                "revision": revision,
                                "from_seq": cursor + 1,
                                "through_seq": through,
                                "code": code,
                            }),
                            trigger.cause,
                        )?);
                    }
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

    /// Atomically discard an overlarge retained range, clear retirement, and
    /// pause the rule. The caller supplies its captured high-water; Store counts
    /// committed rows in the range rather than relying on sequence gaps.
    pub fn settle_rule_backlog(
        &mut self,
        principal: &str,
        state: &core::api::RuleStateView,
        through: i64,
        max_events: i64,
        now: i64,
    ) -> Result<Change<RuleProcessingState>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            require_nonempty(principal, "principal")?;
            state.validate()?;
            if max_events < 0 {
                return Err(Error::new("invalid", "maximum rule backlog is negative"));
            }
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            validate_rule_principal_tx(&tx, principal)?;
            let stored = load_rule_state(&tx, principal, &state.id)?
                .ok_or_else(|| Error::new("not_found", "rule state was not found"))?;
            if stored.view != *state {
                return Err(Error::new("bad_state", "rule backlog view is stale"));
            }
            let high_water = high_water_tx(&tx)?;
            if through > high_water || through < state.current_cursor {
                return Err(Error::new("invalid", "rule backlog boundary is invalid"));
            }
            let oldest = if state.enabled {
                state
                    .retiring_cursor
                    .filter(|_| state.retire_through.is_some())
                    .map_or(state.current_cursor, |retiring| {
                        retiring.min(state.current_cursor)
                    })
            } else {
                through
            };
            let retained: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE seq > ?1 AND seq <= ?2",
                    params![oldest, through],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let mut events = Vec::new();
            let view = if state.enabled && state.paused_reason.is_none() && retained > max_events {
                tx.execute(
                    "UPDATE rule_state
                     SET current_cursor = ?1, retiring_revision = NULL,
                         retiring_definition = NULL, retiring_cursor = NULL,
                         retire_through = NULL, paused_reason = 'backlog_exceeded',
                         updated_at = ?2
                     WHERE principal = ?3 AND rule = ?4 AND revision = ?5
                       AND current_cursor = ?6
                       AND retiring_revision IS ?7 AND retiring_cursor IS ?8
                       AND retire_through IS ?9",
                    params![
                        through,
                        now,
                        principal,
                        state.id,
                        state.revision,
                        state.current_cursor,
                        state.retiring_revision,
                        state.retiring_cursor,
                        state.retire_through,
                    ],
                )
                .map_err(sql_error)?;
                if tx.changes() != 1 {
                    return Err(Error::new(
                        "bad_state",
                        "rule backlog state changed concurrently",
                    ));
                }
                if has_non_rule_event_in_range(&tx, oldest, through)? {
                    events.push(insert_rule_event(
                        &tx,
                        now,
                        "rule.skipped",
                        json!({
                            "rule": state.id,
                            "revision": state.revision,
                            "from_seq": oldest + 1,
                            "through_seq": through,
                            "reason": "backlog_exceeded",
                        }),
                    )?);
                }
                load_rule_processing_state(&tx, principal, &state.id)?
                    .ok_or_else(|| Error::new("internal", "paused rule state disappeared"))?
            } else {
                RuleProcessingState::from(stored)
            };
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: view,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Atomically admit one matched rule action, its fixed parent/child link,
    /// rate accounting, the event cursor, and committed rule/command events.
    pub fn admit_rule_action(
        &mut self,
        principal: &str,
        state: &core::api::RuleStateView,
        event_seq: i64,
        request: SubmitCommand,
        now: i64,
    ) -> Result<RuleEventAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.admit_rule_action_inner(principal, state, event_seq, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn admit_rule_action_inner(
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
        let trigger = load_rule_trigger(&tx, event_seq)?;
        let retiring = rule_cursor_is_retiring(&stored.view, event_seq);
        let cursor = if retiring {
            stored
                .view
                .retiring_cursor
                .ok_or_else(|| Error::new("source_integrity", "retiring cursor is incomplete"))?
        } else {
            stored.view.current_cursor
        };
        let revision = if retiring {
            stored
                .view
                .retiring_revision
                .ok_or_else(|| Error::new("source_integrity", "retiring revision is incomplete"))?
        } else {
            stored.view.revision
        };
        let key = RuleFiringKey::event(
            principal,
            &stored.view,
            revision,
            event_seq,
            trigger.cause.clone(),
        )?;

        if let Some(parent) = load_command(&tx, &key.parent_id)? {
            if !key.matches_parent(&parent, principal) {
                return Err(Error::new(
                    "source_integrity",
                    "derived rule parent id is bound to another action",
                ));
            }
            if cursor < event_seq {
                return Err(Error::new(
                    "source_integrity",
                    "rule parent exists before its event cursor commit",
                ));
            }
            let child = load_rule_child(&tx, &key.parent_id, &key.child_id)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(RuleEventAdmission {
                state: stored.view,
                parent: Some(parent),
                child,
                events: Vec::new(),
                is_new: false,
            });
        }
        if cursor >= event_seq {
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
        validate_rule_principal_tx(&tx, principal)?;

        let depth = next_rule_cause_depth(trigger.cause.as_ref());
        if depth > 5 {
            let view = update_rule_cursor_tx(&tx, principal, &stored.view, event_seq, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(RuleEventAdmission {
                state: view,
                parent: None,
                child: None,
                events: Vec::new(),
                is_new: true,
            });
        }
        let binding_error = if request.verb == "run" {
            rule_run_action_binding_error(&tx, principal, &state.id, revision, event_seq, &request)?
        } else {
            rule_action_binding_error(&trigger, &request)
        };
        if let Some(code) = binding_error {
            request.id = key.child_id.clone();
            return reject_rule_event(tx, &self.write_faulted, &key, &code, now);
        }
        let canonical_definition = if retiring {
            stored
                .retiring_definition
                .as_deref()
                .ok_or_else(|| Error::new("source_integrity", "retiring definition is absent"))?
        } else {
            &stored.canonical_definition
        };
        if should_suppress_own_event_send_tx(
            &tx,
            principal,
            &state.id,
            canonical_definition,
            &request,
            trigger.cause.as_ref(),
        )? {
            let view = update_rule_cursor_tx(&tx, principal, &stored.view, event_seq, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(RuleEventAdmission {
                state: view,
                parent: None,
                child: None,
                events: Vec::new(),
                is_new: true,
            });
        }
        if let Err(error) = validate_project_rule_action_tx(&tx, principal, &request) {
            if core::is_deterministic_pre_admission_rejection(&error.code) {
                request.id = key.child_id.clone();
                return reject_rule_event(tx, &self.write_faulted, &key, &error.code, now);
            }
            return Err(error);
        }
        request.id = key.child_id.clone();

        let limit = rule_action_limit(&stored)?;
        let (scoped_count, global_count) = rule_action_counts(
            &tx,
            principal,
            &state.id,
            request.target.as_deref(),
            &limit,
            now,
        )?;
        if rule_limit_reached(scoped_count, limit.max) || global_count >= GLOBAL_RULE_ACTION_LIMIT {
            return reject_rule_event(tx, &self.write_faulted, &key, "rate_limited", now);
        }

        // submit_tx validates and admits the ordinary rule action. Its verb path
        // performs the normal deterministic checks before inserting the child;
        // the surrounding transaction rolls every write back on persistence failure.
        let (parent, child, events) = match admit_rule_action_tx(&tx, &key, request, now) {
            Ok(admission) => admission,
            Err(error) if core::is_deterministic_pre_admission_rejection(&error.code) => {
                return reject_rule_event(tx, &self.write_faulted, &key, &error.code, now);
            }
            Err(error) => return Err(error),
        };
        let view = update_rule_cursor_tx(&tx, key.principal, &stored.view, event_seq, now)?;
        commit_transaction(tx, &self.write_faulted)?;

        Ok(RuleEventAdmission {
            state: view,
            parent: Some(parent),
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
        if self.write_faulted.get() {
            return Ok(Vec::new());
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, principal, method, target, expected_generation, generation,
                        expected_turn_id, turn_id, request_id, verb, args, state, outcome,
                        created_at, updated_at, probe
                 FROM commands
                 WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL
                   AND state IN ('accepted','queued')
                   AND NOT EXISTS (
                       SELECT 1 FROM commands earlier
                       WHERE earlier.parent_command = commands.parent_command
                         AND earlier.link_kind = 'rule_action'
                         AND earlier.ordinal < commands.ordinal
                         AND earlier.state IN ('accepted','queued','dispatched','unknown')
                   )
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

    /// Persist a timer's immutable schedule beside its owning rule state. The
    /// daemon supplies calculations; Store captures an omitted interval anchor
    /// exactly once and keeps the numeric UTC-millisecond cursor.
    pub fn ensure_timer_state(
        &mut self,
        principal: &str,
        input: &TimerDefinitionInput,
        now: i64,
    ) -> Result<core::api::TimerStateView> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            validate_rule_principal_tx(&tx, principal)?;
            let view = ensure_timer_state_tx(&tx, principal, input, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(view)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn get_timer_state(
        &self,
        principal: &str,
        id: &str,
    ) -> Result<Option<core::api::TimerStateView>> {
        validate_principal_rule(principal, id)?;
        validate_rule_principal_read(&self.connection, principal)?;
        Ok(load_timer_state(&self.connection, principal, id)?.map(|timer| timer.view))
    }

    /// Return a bounded page of persisted due timers. The caller computes the
    /// selected slot outside the Store lock and supplies it to commit_timer_slot.
    pub fn list_due_timer_states(
        &self,
        principal: &str,
        after_id: Option<&str>,
        due_by_ms: i64,
        limit: usize,
    ) -> Result<Vec<core::api::TimerStateView>> {
        validate_rule_principal_read(&self.connection, principal)?;
        validate_limit(limit)?;
        if let Some(after_id) = after_id {
            require_nonempty(after_id, "timer cursor")?;
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT t.principal, t.rule, t.schedule_revision, t.trigger, t.anchor,
                        t.last_key, t.next_due_at, t.catch_up, r.enabled, r.paused_reason
                 FROM timers t
                 JOIN rule_state r ON r.principal = t.principal AND r.rule = t.rule
                 WHERE t.principal = ?1 AND (?2 IS NULL OR t.rule > ?2)
                   AND t.next_due_at IS NOT NULL AND t.next_due_at <= ?3
                   AND r.enabled = 1 AND r.paused_reason IS NULL
                 ORDER BY t.rule LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![principal, after_id, due_by_ms, limit as i64],
                timer_record_from_row,
            )
            .map_err(sql_error)?;
        let mut timers = Vec::new();
        for row in rows {
            timers.push(timer_view_from_record(row.map_err(sql_error)?)?);
        }
        enforce_page_bytes(&mut timers)?;
        Ok(timers)
    }

    /// Commit one scheduled slot with its ordinary rule-action child, fixed parent,
    /// action accounting, and numeric cursor in one writer transaction.
    pub fn commit_timer_slot(
        &mut self,
        principal: &str,
        state: &core::api::TimerStateView,
        slot: TimerSlotCommit,
        now: i64,
    ) -> Result<TimerSlotAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.commit_timer_slot_inner(principal, state, slot, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn commit_timer_slot_inner(
        &mut self,
        principal: &str,
        state: &core::api::TimerStateView,
        slot: TimerSlotCommit,
        now: i64,
    ) -> Result<TimerSlotAdmission> {
        require_nonempty(principal, "principal")?;
        state.validate()?;
        validate_timer_slot(&slot, now)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let rule = load_rule_state(&tx, principal, &state.id)?
            .ok_or_else(|| Error::new("not_found", "timer rule was not found"))?;
        let stored = load_timer_state(&tx, principal, &state.id)?
            .ok_or_else(|| Error::new("not_found", "timer state was not found"))?;
        let key = RuleFiringKey::timer(
            principal,
            &rule.view,
            state.schedule_revision,
            slot.scheduled_at_ms,
        )?;
        if let Some(parent) = load_command(&tx, &key.parent_id)? {
            if !key.matches_parent(&parent, principal) {
                return Err(Error::new(
                    "source_integrity",
                    "timer parent id is bound to another scheduled slot",
                ));
            }
            if stored
                .view
                .last_key
                .is_none_or(|last| last < slot.scheduled_at_ms)
            {
                return Err(Error::new(
                    "source_integrity",
                    "timer parent exists before its slot cursor commit",
                ));
            }
            let child = load_rule_child(&tx, &key.parent_id, &key.child_id)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(TimerSlotAdmission {
                state: stored.view,
                parent: Some(parent),
                child,
                events: Vec::new(),
                is_new: false,
                blocked: false,
            });
        }
        if stored
            .view
            .last_key
            .is_some_and(|last| slot.scheduled_at_ms < last)
        {
            return Err(Error::new(
                "bad_state",
                "timer slot is older than the persisted cursor",
            ));
        }
        if stored.view.last_key == Some(slot.scheduled_at_ms) {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(TimerSlotAdmission {
                state: stored.view,
                parent: None,
                child: None,
                events: Vec::new(),
                is_new: false,
                blocked: false,
            });
        }
        validate_rule_principal_tx(&tx, principal)?;
        if stored.view != *state || !stored.view.enabled || !rule.view.enabled {
            return Err(Error::new("bad_state", "timer state is stale or disabled"));
        }
        if stored.view.schedule_revision != state.schedule_revision
            || stored
                .view
                .next_due_at
                .is_none_or(|due| slot.scheduled_at_ms < due)
        {
            return Err(Error::new(
                "bad_state",
                "timer slot is not at or after the persisted next due time",
            ));
        }
        if slot
            .next_due_at_ms
            .is_some_and(|next| next <= slot.scheduled_at_ms)
        {
            return Err(Error::new(
                "invalid",
                "next timer due time must be after its committed slot",
            ));
        }
        if has_open_rule_actions_tx(&tx, principal, &state.id)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(TimerSlotAdmission {
                state: stored.view,
                parent: None,
                child: None,
                events: Vec::new(),
                is_new: false,
                blocked: true,
            });
        }

        let mut events = Vec::new();
        let mut parent = None;
        let mut child = None;
        if let Some(action) = slot.action {
            let rejected = if !valid_rule_action_request(&action, false) {
                Some("rule_action_binding_invalid".to_owned())
            } else {
                match validate_project_rule_action_tx(&tx, principal, &action) {
                    Err(error) if core::is_deterministic_pre_admission_rejection(&error.code) => {
                        Some(error.code)
                    }
                    Err(error) => return Err(error),
                    Ok(_) => {
                        let limit = rule_action_limit(&rule)?;
                        let (scoped_count, global) = rule_action_counts(
                            &tx,
                            principal,
                            &state.id,
                            action.target.as_deref(),
                            &limit,
                            now,
                        )?;
                        if rule_limit_reached(scoped_count, limit.max)
                            || global >= GLOBAL_RULE_ACTION_LIMIT
                        {
                            Some("rate_limited".to_owned())
                        } else {
                            None
                        }
                    }
                }
            };
            if let Some(code) = rejected {
                let (rejected_parent, event) = reject_timer_fire_tx(&tx, &key, &code, now)?;
                parent = Some(rejected_parent);
                events.extend(event);
            } else {
                match admit_rule_action_tx(&tx, &key, action, now) {
                    Ok((admitted_parent, admitted_child, admitted_events)) => {
                        parent = Some(admitted_parent);
                        child = Some(admitted_child);
                        events.extend(admitted_events);
                    }
                    Err(error) if core::is_deterministic_pre_admission_rejection(&error.code) => {
                        let (rejected_parent, rejected_events) =
                            reject_timer_fire_tx(&tx, &key, &error.code, now)?;
                        parent = Some(rejected_parent);
                        events.extend(rejected_events);
                    }
                    Err(error) => return Err(error),
                }
            }
        } else {
            events.push(insert_rule_event(
                &tx,
                now,
                "rule.skipped",
                json!({
                    "rule": state.id,
                    "schedule_revision": state.schedule_revision,
                    "scheduled_at_ms": slot.scheduled_at_ms,
                    "reason": slot.skip_code,
                }),
            )?);
        }

        let next_state = update_timer_slot_tx(
            &tx,
            principal,
            &stored,
            slot.scheduled_at_ms,
            slot.next_due_at_ms,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(TimerSlotAdmission {
            state: next_state,
            parent,
            child,
            events,
            is_new: true,
            blocked: false,
        })
    }

    /// Resolve a retained manual timer parent before the Router reevaluates
    /// current timer, mode, or action gates.
    pub fn replay_timer_run(
        &self,
        principal: &str,
        request: &core::api::TimerRunRequest,
    ) -> Result<Option<Command>> {
        require_nonempty(principal, "principal")?;
        request.validate()?;
        validate_ulid(&request.id)?;
        Ok(replay_timer_run_tx(&self.connection, principal, request)?.map(|(parent, _)| parent))
    }

    /// Atomically admit a manual run using the external mutation ULID as its
    /// parent ID. It never advances the scheduled timer cursor.
    pub fn admit_manual_timer_run(
        &mut self,
        principal: &str,
        request: &core::api::TimerRunRequest,
        state: &core::api::TimerStateView,
        expected_rule_revision: i64,
        action: SubmitCommand,
        now: i64,
    ) -> Result<TimerSlotAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.admit_manual_timer_run_inner(
            principal,
            request,
            state,
            expected_rule_revision,
            action,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    fn admit_manual_timer_run_inner(
        &mut self,
        principal: &str,
        request: &core::api::TimerRunRequest,
        state: &core::api::TimerStateView,
        expected_rule_revision: i64,
        action: SubmitCommand,
        now: i64,
    ) -> Result<TimerSlotAdmission> {
        require_nonempty(principal, "principal")?;
        request.validate()?;
        validate_ulid(&request.id)?;
        let digest = timer_run_digest(request)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some((parent, child)) = replay_timer_run_tx(&tx, principal, request)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(TimerSlotAdmission {
                state: state.clone(),
                parent: Some(parent),
                child,
                events: Vec::new(),
                is_new: false,
                blocked: false,
            });
        }
        let project = validate_rule_principal_tx(&tx, principal)?;
        if request.project.as_deref()
            != project
                .as_ref()
                .map(|registration| registration.id.as_str())
        {
            return Err(Error::new(
                "not_allowed",
                "timer project does not match its rule principal",
            ));
        }
        let rule = load_rule_state(&tx, principal, &request.rule)?
            .ok_or_else(|| Error::new("not_found", "timer rule was not found"))?;
        if rule.view.revision != expected_rule_revision {
            return Err(Error::new(
                "bad_state",
                "timer rule definition changed before manual run admission",
            ));
        }
        state.validate()?;
        if write_exists(&tx, &request.id)? {
            return Err(Error::new(
                "id_conflict",
                "timer mutation id is already used",
            ));
        }
        validate_id_age(&request.id, now)?;
        if !valid_rule_action_request(&action, false) {
            return Err(Error::new(
                "invalid",
                "timer action is not a valid rule action",
            ));
        }
        validate_project_rule_action_tx(&tx, principal, &action)?;
        if state.id != request.rule {
            return Err(Error::new(
                "invalid",
                "timer state does not match the request",
            ));
        }
        let timer = load_timer_state(&tx, principal, &request.rule)?
            .ok_or_else(|| Error::new("not_found", "timer state was not found"))?;
        if timer.view != *state || !timer.view.enabled || !rule.view.enabled {
            return Err(Error::new("bad_state", "timer is disabled or paused"));
        }
        if has_open_rule_actions_tx(&tx, principal, &request.rule)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(TimerSlotAdmission {
                state: timer.view,
                parent: None,
                child: None,
                events: Vec::new(),
                is_new: false,
                blocked: true,
            });
        }
        let limit = rule_action_limit(&rule)?;
        let (scoped_count, global) = rule_action_counts(
            &tx,
            principal,
            &request.rule,
            action.target.as_deref(),
            &limit,
            now,
        )?;
        if rule_limit_reached(scoped_count, limit.max) || global >= GLOBAL_RULE_ACTION_LIMIT {
            return Err(Error::new(
                "bad_state",
                "timer action rate limit is reached",
            ));
        }

        let key = RuleFiringKey::manual(principal, &rule.view, &request.id, digest)?;
        match admit_rule_action_tx(&tx, &key, action, now) {
            Ok((parent, child, events)) => {
                commit_transaction(tx, &self.write_faulted)?;
                Ok(TimerSlotAdmission {
                    state: timer.view,
                    parent: Some(parent),
                    child: Some(child),
                    events,
                    is_new: true,
                    blocked: false,
                })
            }
            Err(error) if core::is_deterministic_pre_admission_rejection(&error.code) => {
                let (parent, events) = reject_timer_fire_tx(&tx, &key, &error.code, now)?;
                commit_transaction(tx, &self.write_faulted)?;
                Ok(TimerSlotAdmission {
                    state: timer.view,
                    parent: Some(parent),
                    child: None,
                    events,
                    is_new: true,
                    blocked: false,
                })
            }
            Err(error) => Err(error),
        }
    }
}

fn ensure_timer_state_tx(
    tx: &Transaction<'_>,
    principal: &str,
    input: &TimerDefinitionInput,
    registration_ms: i64,
) -> Result<core::api::TimerStateView> {
    require_nonempty(principal, "principal")?;
    if input.schedule_revision <= 0 {
        return Err(Error::new(
            "invalid",
            "timer schedule revision must be positive",
        ));
    }
    input.schedule.validate()?;
    let trigger = serde_json::to_string(&input.schedule)
        .map_err(|_| Error::new("invalid", "timer schedule cannot be encoded"))?;
    let rule = load_rule_state(tx, principal, &input.id)?
        .ok_or_else(|| Error::new("not_found", "timer rule was not found"))?;
    let catch_up = rule_catch_up(&rule)?;
    if rule.view.enabled != input.enabled {
        return Err(Error::new(
            "bad_state",
            "timer enabled state differs from its rule definition",
        ));
    }

    if let Some(existing) = load_timer_state(tx, principal, &input.id)? {
        if input.schedule_revision < existing.view.schedule_revision {
            return Err(Error::new("bad_state", "timer schedule revision is stale"));
        }
        if input.schedule_revision == existing.view.schedule_revision {
            if existing.trigger != trigger || existing.catch_up != catch_up {
                return Err(Error::new(
                    "bad_state",
                    "timer schedule or catch_up changed without a new schedule revision",
                ));
            }
            return Ok(existing.view);
        }

        validate_timer_definition(input, registration_ms)?;
        let anchor = match &input.schedule {
            core::api::TimerSchedule::Interval {
                anchor: Some(anchor),
                ..
            } => *anchor,
            core::api::TimerSchedule::Interval { anchor: None, .. }
            | core::api::TimerSchedule::Cron { .. }
            | core::api::TimerSchedule::Once { .. } => registration_ms,
        };
        let changed = tx
            .execute(
                "UPDATE timers
                 SET schedule_revision = ?1, trigger = ?2, anchor = ?3,
                     last_key = NULL, next_due_at = ?4, catch_up = ?5
                 WHERE principal = ?6 AND rule = ?7 AND schedule_revision = ?8",
                params![
                    input.schedule_revision,
                    trigger,
                    anchor,
                    input.initial_next_due_at,
                    catch_up_name(catch_up),
                    principal,
                    input.id,
                    existing.view.schedule_revision,
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "bad_state",
                "timer schedule changed concurrently",
            ));
        }
        return Ok(core::api::TimerStateView {
            id: input.id.clone(),
            project: rule_project_id(principal)?,
            schedule_revision: input.schedule_revision,
            schedule: input.schedule.clone(),
            anchor,
            last_key: None,
            next_due_at: input.initial_next_due_at,
            enabled: rule.view.enabled && rule.view.paused_reason.is_none(),
        });
    }

    validate_timer_definition(input, registration_ms)?;
    let anchor = match &input.schedule {
        core::api::TimerSchedule::Interval {
            anchor: Some(anchor),
            ..
        } => *anchor,
        core::api::TimerSchedule::Interval { anchor: None, .. }
        | core::api::TimerSchedule::Cron { .. }
        | core::api::TimerSchedule::Once { .. } => registration_ms,
    };
    tx.execute(
        "INSERT INTO timers(
             principal, rule, schedule_revision, trigger, anchor,
             last_key, next_due_at, catch_up
         ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7)",
        params![
            principal,
            input.id,
            input.schedule_revision,
            trigger,
            anchor,
            input.initial_next_due_at,
            catch_up_name(catch_up),
        ],
    )
    .map_err(sql_error)?;
    Ok(core::api::TimerStateView {
        id: input.id.clone(),
        project: rule_project_id(principal)?,
        schedule_revision: input.schedule_revision,
        schedule: input.schedule.clone(),
        anchor,
        last_key: None,
        next_due_at: input.initial_next_due_at,
        enabled: rule.view.enabled && rule.view.paused_reason.is_none(),
    })
}

fn validate_principal_rule(principal: &str, rule: &str) -> Result<()> {
    rule_project_id(principal)?;
    require_nonempty(rule, "rule id")
}

fn rule_project_id(principal: &str) -> Result<Option<String>> {
    if principal == "owner" {
        return Ok(None);
    }
    let Some(project) = principal.strip_prefix("project:") else {
        return Err(Error::new(
            "not_allowed",
            "rule principal is not authorized",
        ));
    };
    if project.is_empty() || project.trim() != project || project.contains(':') {
        return Err(Error::new("invalid", "rule project principal is invalid"));
    }
    Ok(Some(project.to_owned()))
}

fn validate_rule_principal_tx(
    tx: &Transaction<'_>,
    principal: &str,
) -> Result<Option<core::api::ProjectRegistrationView>> {
    let Some(project_id) = rule_project_id(principal)? else {
        return Ok(None);
    };
    let registration = super::projects::project_registration_tx(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project rule registration is absent"))?;
    registration
        .validate()
        .map_err(|_| Error::new("source_integrity", "stored project registration is invalid"))?;
    if registration.id != project_id {
        return Err(Error::new(
            "source_integrity",
            "project registration does not match its principal",
        ));
    }
    Ok(Some(registration))
}

fn validate_rule_principal_read(connection: &Connection, principal: &str) -> Result<()> {
    let Some(project_id) = rule_project_id(principal)? else {
        return Ok(());
    };
    let registration = super::projects::project_registration_tx(connection, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project rule registration is absent"))?;
    registration
        .validate()
        .map_err(|_| Error::new("source_integrity", "stored project registration is invalid"))?;
    if registration.id != project_id {
        return Err(Error::new(
            "source_integrity",
            "project registration does not match its principal",
        ));
    }
    Ok(())
}

fn validate_project_rule_definition(
    project: Option<&core::api::ProjectRegistrationView>,
    value: &Value,
) -> Result<()> {
    let Some(project) = project else {
        return Ok(());
    };
    let actions = value
        .get("do")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("invalid", "project rule actions are invalid"))?;
    if actions.iter().any(|action| {
        action
            .get("action")
            .and_then(Value::as_str)
            .is_none_or(|action| !project.actions.iter().any(|approved| approved == action))
    }) {
        return Err(Error::new(
            "not_allowed",
            "project rule uses an action not approved by its registration",
        ));
    }
    Ok(())
}

fn validate_project_rule_action_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
) -> Result<Option<core::api::ProjectRegistrationView>> {
    if request.verb == "run" {
        if rule_project_id(principal)?.is_none() {
            return Ok(None);
        }
        let payload_digest = [0_u8; 32];
        let command = CommandInsert::targetless_admitted(principal, request, &payload_digest, 0);
        super::projects::capture_project_command_binding_tx(tx, &command)?;
        return Ok(None);
    }
    if rule_project_id(principal)?.is_none() {
        return Ok(None);
    }
    let target = request
        .target
        .as_deref()
        .ok_or_else(|| Error::new("not_allowed", "project rule action has no target"))?;
    let generation = request
        .expected_generation
        .ok_or_else(|| Error::new("not_allowed", "project rule action has no generation"))?;
    super::projects::validate_project_rule_admission_tx(
        tx,
        principal,
        target,
        generation,
        &request.verb,
    )
}

struct StoredTimerState {
    view: core::api::TimerStateView,
    trigger: String,
    catch_up: core::api::CatchUp,
}

struct TimerRecord {
    principal: String,
    id: String,
    schedule_revision: i64,
    trigger: String,
    anchor: i64,
    last_key: Option<i64>,
    next_due_at: Option<i64>,
    catch_up: String,
    enabled: bool,
    paused_reason: Option<String>,
}

fn timer_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TimerRecord> {
    Ok(TimerRecord {
        principal: row.get(0)?,
        id: row.get(1)?,
        schedule_revision: row.get(2)?,
        trigger: row.get(3)?,
        anchor: row.get(4)?,
        last_key: row.get(5)?,
        next_due_at: row.get(6)?,
        catch_up: row.get(7)?,
        enabled: row.get(8)?,
        paused_reason: row.get(9)?,
    })
}

fn timer_view_from_record(record: TimerRecord) -> Result<core::api::TimerStateView> {
    parse_catch_up(&record.catch_up)?;
    let schedule = serde_json::from_str(&record.trigger)
        .map_err(|_| Error::new("source_integrity", "stored timer schedule is invalid"))?;
    let view = core::api::TimerStateView {
        id: record.id,
        project: rule_project_id(&record.principal)?,
        schedule_revision: record.schedule_revision,
        schedule,
        anchor: record.anchor,
        last_key: record.last_key,
        next_due_at: record.next_due_at,
        enabled: record.enabled && record.paused_reason.is_none(),
    };
    view.validate()
        .map_err(|_| Error::new("source_integrity", "stored timer state is invalid"))?;
    Ok(view)
}

fn load_timer_state(
    connection: &Connection,
    principal: &str,
    rule: &str,
) -> Result<Option<StoredTimerState>> {
    let record = connection
        .query_row(
            "SELECT t.principal, t.rule, t.schedule_revision, t.trigger, t.anchor,
                    t.last_key, t.next_due_at, t.catch_up, r.enabled, r.paused_reason
             FROM timers t
             JOIN rule_state r ON r.principal = t.principal AND r.rule = t.rule
             WHERE t.principal = ?1 AND t.rule = ?2",
            params![principal, rule],
            timer_record_from_row,
        )
        .optional()
        .map_err(sql_error)?;
    let Some(record) = record else {
        return Ok(None);
    };
    let trigger = record.trigger.clone();
    let catch_up = parse_catch_up(&record.catch_up)?;
    let view = timer_view_from_record(record)?;
    Ok(Some(StoredTimerState {
        view,
        trigger,
        catch_up,
    }))
}

fn parse_catch_up(value: &str) -> Result<core::api::CatchUp> {
    match value {
        "latest" => Ok(core::api::CatchUp::Latest),
        "none" => Ok(core::api::CatchUp::None),
        _ => Err(Error::new(
            "source_integrity",
            "stored timer catch_up is invalid",
        )),
    }
}

fn ensure_rule_state_tx(
    tx: &Transaction<'_>,
    principal: &str,
    input: &RuleDefinitionInput,
    high_water: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<core::api::RuleStateView> {
    if let Some(existing) = load_rule_state(tx, principal, &input.id)? {
        if existing.canonical_definition == input.canonical_definition
            && existing.definition_digest == input.definition_digest
            && existing.view.enabled == input.enabled
        {
            return Ok(existing.view);
        }

        // Only one retiring revision is retained. Keep the old current
        // definition until it drains; the daemon retries the latest snapshot.
        if existing.view.retiring_revision.is_some() && input.enabled {
            return Ok(existing.view);
        }

        let revision = existing
            .view
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::new("invalid", "rule revision overflows"))?;
        let should_retire = existing.view.enabled && input.enabled;
        let retiring = should_retire && existing.view.current_cursor < high_water;
        let skipped_from = existing
            .view
            .retiring_cursor
            .map_or(existing.view.current_cursor, |cursor| {
                cursor.min(existing.view.current_cursor)
            });

        if existing.view.enabled
            && !input.enabled
            && skipped_from < high_water
            && has_non_rule_event_in_range(tx, skipped_from, high_water)?
        {
            events.push(insert_rule_event(
                tx,
                now,
                "rule.skipped",
                json!({
                    "rule": input.id,
                    "revision": existing.view.revision,
                    "from_seq": skipped_from + 1,
                    "through_seq": high_water,
                    "reason": "disabled",
                }),
            )?);
        }

        let changed = tx
            .execute(
                "UPDATE rule_state
                 SET revision = ?1, current_definition = ?2, definition_digest = ?3,
                     current_cursor = ?4, enabled = ?5,
                     retiring_revision = ?6, retiring_definition = ?7,
                     retiring_cursor = ?8, retire_through = ?9, updated_at = ?10
                 WHERE principal = ?11 AND rule = ?12 AND revision = ?13
                   AND current_cursor = ?14",
                params![
                    revision,
                    input.canonical_definition,
                    input.definition_digest.as_slice(),
                    high_water,
                    input.enabled,
                    retiring.then_some(existing.view.revision),
                    retiring.then_some(existing.canonical_definition.as_str()),
                    retiring.then_some(existing.view.current_cursor),
                    retiring.then_some(high_water),
                    now,
                    principal,
                    input.id,
                    existing.view.revision,
                    existing.view.current_cursor,
                ],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "bad_state",
                "rule definition changed concurrently",
            ));
        }
        return load_rule_state(tx, principal, &input.id)?
            .map(|stored| stored.view)
            .ok_or_else(|| Error::new("internal", "updated rule state disappeared"));
    }

    tx.execute(
        "INSERT INTO rule_state(
             principal, rule, revision, current_definition, definition_digest,
             current_cursor, enabled, paused_reason, updated_at,
             retiring_revision, retiring_definition, retiring_cursor, retire_through
         ) VALUES (?1, ?2, 1, ?3, ?4, ?5, ?6, NULL, ?7, NULL, NULL, NULL, NULL)",
        params![
            principal,
            input.id,
            input.canonical_definition,
            input.definition_digest.as_slice(),
            high_water,
            input.enabled,
            now,
        ],
    )
    .map_err(sql_error)?;
    Ok(core::api::RuleStateView {
        id: input.id.clone(),
        project: rule_project_id(principal)?,
        revision: 1,
        current_cursor: high_water,
        retiring_revision: None,
        retiring_cursor: None,
        retire_through: None,
        enabled: input.enabled,
        paused_reason: None,
    })
}

fn disable_deleted_rule_tx(
    tx: &Transaction<'_>,
    principal: &str,
    id: &str,
    high_water: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(existing) = load_rule_state(tx, principal, id)? else {
        return Ok(());
    };
    let oldest = existing
        .view
        .retiring_cursor
        .map_or(existing.view.current_cursor, |retiring| {
            retiring.min(existing.view.current_cursor)
        });
    if existing.view.enabled
        && oldest < high_water
        && has_non_rule_event_in_range(tx, oldest, high_water)?
    {
        events.push(insert_rule_event(
            tx,
            now,
            "rule.skipped",
            json!({
                "rule": id,
                "revision": existing.view.revision,
                "from_seq": oldest + 1,
                "through_seq": high_water,
                "reason": "deleted",
            }),
        )?);
    }
    tx.execute(
        "UPDATE rule_state
         SET current_cursor = ?1, enabled = 0,
             retiring_revision = NULL, retiring_definition = NULL,
             retiring_cursor = NULL, retire_through = NULL, updated_at = ?2
         WHERE principal = ?3 AND rule = ?4 AND revision = ?5
           AND current_cursor = ?6
           AND retiring_revision IS ?7 AND retiring_cursor IS ?8
           AND retire_through IS ?9",
        params![
            high_water,
            now,
            principal,
            id,
            existing.view.revision,
            existing.view.current_cursor,
            existing.view.retiring_revision,
            existing.view.retiring_cursor,
            existing.view.retire_through,
        ],
    )
    .map_err(sql_error)?;
    if tx.changes() != 1 {
        return Err(Error::new("bad_state", "deleted rule changed concurrently"));
    }
    Ok(())
}

fn rule_ids_tx(connection: &Connection, principal: &str) -> Result<Vec<String>> {
    let mut statement = connection
        .prepare("SELECT rule FROM rule_state WHERE principal = ?1 ORDER BY rule")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([principal], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)
}

impl From<StoredRuleState> for RuleProcessingState {
    fn from(stored: StoredRuleState) -> Self {
        Self {
            view: stored.view,
            current_definition: stored.canonical_definition,
            retiring_definition: stored.retiring_definition,
        }
    }
}

fn load_rule_processing_state(
    connection: &Connection,
    principal: &str,
    id: &str,
) -> Result<Option<RuleProcessingState>> {
    Ok(load_rule_state(connection, principal, id)?.map(RuleProcessingState::from))
}

fn list_rule_processing_states_tx(
    connection: &Connection,
    principal: &str,
    after_id: Option<&str>,
    limit: usize,
) -> Result<Vec<RuleProcessingState>> {
    let mut statement = connection
        .prepare(
            "SELECT rule FROM rule_state
             WHERE principal = ?1 AND (?2 IS NULL OR rule > ?2)
             ORDER BY rule LIMIT ?3",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![principal, after_id, limit as i64], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?;
    let ids = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    drop(statement);
    let mut states = Vec::with_capacity(ids.len());
    for id in ids {
        let state = load_rule_processing_state(connection, principal, &id)?
            .ok_or_else(|| Error::new("source_integrity", "listed rule state disappeared"))?;
        states.push(state);
    }
    enforce_page_bytes(&mut states)?;
    Ok(states)
}

fn replay_timer_run_tx(
    connection: &Connection,
    principal: &str,
    request: &core::api::TimerRunRequest,
) -> Result<Option<(Command, Option<Command>)>> {
    let digest = timer_run_digest(request)?;
    let Some((old_principal, old_method, old_digest)) = command_binding(connection, &request.id)?
    else {
        return Ok(None);
    };
    if old_principal != principal || old_method != COMMAND_METHOD || old_digest != digest {
        return Err(Error::new(
            "id_conflict",
            "timer mutation id is already bound to another request",
        ));
    }
    let parent = load_command(connection, &request.id)?
        .ok_or_else(|| Error::new("internal", "timer command binding has no command row"))?;
    if parent.verb != "timer.run" || request.validate_response(&parent).is_err() {
        return Err(Error::new(
            "source_integrity",
            "timer command binding does not match its retained parent",
        ));
    }
    let child = load_linked_command(connection, &request.id)?;
    Ok(Some((parent, child)))
}

fn timer_run_digest(request: &core::api::TimerRunRequest) -> Result<Vec<u8>> {
    let mut value = json!({
        "target": null,
        "verb": "timer.run",
        "args": {"rule": request.rule},
        "expected_generation": null,
        "expected_turn_id": null,
    });
    if let Some(project) = &request.project {
        value["args"]["project"] = json!(project);
    }
    canonical::digest(&value)
}

fn rule_catch_up(rule: &StoredRuleState) -> Result<core::api::CatchUp> {
    let value: Value = serde_json::from_str(&rule.canonical_definition)
        .map_err(|_| Error::new("source_integrity", "stored rule definition is invalid"))?;
    match value.get("catch_up") {
        None => Ok(core::api::CatchUp::None),
        Some(Value::String(value)) => parse_catch_up(value),
        Some(_) => Err(Error::new(
            "source_integrity",
            "stored rule catch_up is invalid",
        )),
    }
}

fn rule_action_limit(rule: &StoredRuleState) -> Result<core::api::RuleLimit> {
    rule_action_limit_for_definition(&rule.canonical_definition)
}

fn rule_action_limit_for_definition(definition: &str) -> Result<core::api::RuleLimit> {
    let value: Value = serde_json::from_str(definition)
        .map_err(|_| Error::new("source_integrity", "stored rule definition is invalid"))?;
    rule_limit_from_value(&value)
        .map_err(|_| Error::new("source_integrity", "stored rule limit is invalid"))
        .map(|limit| {
            limit.unwrap_or(core::api::RuleLimit {
                max: RULE_ACTION_LIMIT as u64,
                per: core::api::RuleLimitScope::Rule,
                window_ms: RULE_ACTION_WINDOW_MS,
            })
        })
}

pub(super) struct ScriptRunOrigin {
    pub principal: String,
    pub rule: String,
    pub cause: core::EventCause,
    limit: core::api::RuleLimit,
}

pub(super) fn resolve_script_run_origin_tx(
    tx: &Transaction<'_>,
    run: &Command,
    frozen: &core::api::FrozenScriptRun,
) -> Result<ScriptRunOrigin> {
    let link: Option<(Option<String>, Option<String>, Option<i64>)> = tx
        .query_row(
            "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
            [&run.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let (Some(parent_id), Some(link_kind), Some(ordinal)) = link.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "script Run command has no stored parent link",
        )
    })?
    else {
        return Err(Error::new(
            "source_integrity",
            "script Run command has incomplete parent ordering metadata",
        ));
    };
    if link_kind != "rule_action" || ordinal < 0 {
        return Err(Error::new(
            "source_integrity",
            "script Run command is not an ordered rule action",
        ));
    }
    let parent = load_command_record(tx, &parent_id)?
        .ok_or_else(|| Error::new("source_integrity", "script Run rule parent is missing"))?;
    if !matches!(parent.command.verb.as_str(), "rule" | "rule.catch_up")
        || parent.command.principal != run.principal
    {
        return Err(Error::new(
            "source_integrity",
            "script Run has an unsupported or differently-authorized rule parent",
        ));
    }
    let firing_children = parent
        .command
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("source_integrity", "script rule parent has no child set"))?;
    if !firing_children
        .iter()
        .any(|child| child.as_str() == Some(run.id.as_str()))
    {
        return Err(Error::new(
            "source_integrity",
            "script Run is absent from its rule parent's fixed child set",
        ));
    }
    let rule = parent
        .command
        .args
        .get("rule")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "script rule parent has no rule id"))?
        .to_owned();
    let revision = parent
        .command
        .args
        .get("revision")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::new("source_integrity", "script rule parent has no revision"))?;
    let event_seq = match parent.command.verb.as_str() {
        "rule" => parent
            .command
            .args
            .get("event_seq")
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::new("source_integrity", "script rule parent has no event id"))?,
        "rule.catch_up" => {
            let after_seq = parent
                .command
                .args
                .get("after_seq")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    Error::new("source_integrity", "catch-up parent has no lower bound")
                })?;
            let through_seq = parent
                .command
                .args
                .get("through_seq")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    Error::new("source_integrity", "catch-up parent has no upper bound")
                })?;
            let event_seq = frozen.context.event.seq;
            if event_seq <= after_seq || event_seq > through_seq {
                return Err(Error::new(
                    "source_integrity",
                    "script Run event is outside its stored catch-up range",
                ));
            }
            event_seq
        }
        _ => unreachable!("parent verb was checked above"),
    };
    if frozen.context.rule.id != rule
        || frozen.context.rule.revision != revision
        || frozen.context.event.seq != event_seq
    {
        return Err(Error::new(
            "source_integrity",
            "stored script context differs from its rule parent",
        ));
    }
    let event = scripts::load_event_tx(tx, event_seq)?;
    if frozen.context.event != event || frozen.context.subject != event.subject {
        return Err(Error::new(
            "source_integrity",
            "stored script context differs from its committed event",
        ));
    }

    let cause = command_cause_tx(tx, &run.id)?
        .ok_or_else(|| Error::new("source_integrity", "script Run has no stored cause"))?;
    let expected_depth = next_rule_cause_depth(event.cause.as_ref());
    if cause.command_id != run.id
        || cause.rule.as_deref() != Some(rule.as_str())
        || cause.depth != expected_depth
        || cause.depth > 5
    {
        return Err(Error::new(
            "source_integrity",
            "script Run cause does not match its verified rule and event",
        ));
    }
    let parent_cause = command_cause_tx(tx, &parent_id)?
        .ok_or_else(|| Error::new("source_integrity", "script rule parent has no cause"))?;
    let parent_depth_matches = match parent.command.verb.as_str() {
        "rule" => parent_cause.depth == cause.depth,
        "rule.catch_up" => parent_cause.depth == 1,
        _ => false,
    };
    if parent_cause.command_id != parent_id
        || parent_cause.rule.as_deref() != Some(rule.as_str())
        || !parent_depth_matches
    {
        return Err(Error::new(
            "source_integrity",
            "script Run cause differs from its stored rule parent",
        ));
    }

    let stored = load_rule_state(tx, &run.principal, &rule)?
        .ok_or_else(|| Error::new("source_integrity", "script rule state was removed"))?;
    let definition = if stored.view.revision == revision {
        stored.canonical_definition.as_str()
    } else if stored.view.retiring_revision == Some(revision) {
        stored
            .retiring_definition
            .as_deref()
            .ok_or_else(|| Error::new("source_integrity", "retiring script rule is absent"))?
    } else {
        return Err(Error::new(
            "source_integrity",
            "script rule revision is no longer retained",
        ));
    };
    let limit = rule_action_limit_for_definition(definition)?;
    Ok(ScriptRunOrigin {
        principal: run.principal.clone(),
        rule,
        cause,
        limit,
    })
}

pub(super) fn validate_script_output_rate_tx(
    tx: &Transaction<'_>,
    origin: &ScriptRunOrigin,
    target_agent: Option<&str>,
    now: i64,
) -> Result<()> {
    let (scoped_count, global_count) = rule_action_counts(
        tx,
        &origin.principal,
        &origin.rule,
        target_agent,
        &origin.limit,
        now,
    )?;
    if rule_limit_reached(scoped_count, origin.limit.max)
        || global_count >= GLOBAL_RULE_ACTION_LIMIT
    {
        return Err(Error::new(
            "rate_limited",
            "script output exceeds the saved rule action rate limit",
        ));
    }
    Ok(())
}

fn catch_up_name(catch_up: core::api::CatchUp) -> &'static str {
    match catch_up {
        core::api::CatchUp::Latest => "latest",
        core::api::CatchUp::None => "none",
    }
}

fn validate_timer_definition(input: &TimerDefinitionInput, registration_ms: i64) -> Result<()> {
    require_nonempty(&input.id, "timer id")?;
    if input.schedule_revision <= 0 {
        return Err(Error::new(
            "invalid",
            "timer schedule revision must be positive",
        ));
    }
    input.schedule.validate()?;
    match &input.schedule {
        core::api::TimerSchedule::Interval { period_ms, anchor } => {
            let effective_anchor = anchor.unwrap_or(registration_ms);
            let elapsed = i128::from(registration_ms) - i128::from(effective_anchor);
            let periods = if elapsed < 0 {
                1
            } else {
                elapsed / i128::from(*period_ms) + 1
            };
            let expected = i128::from(effective_anchor) + periods * i128::from(*period_ms);
            let expected = i64::try_from(expected)
                .map_err(|_| Error::new("invalid", "first timer slot overflows epoch millis"))?;
            if input.initial_next_due_at != Some(expected) {
                return Err(Error::new(
                    "invalid",
                    "interval timer first due time does not match its captured anchor",
                ));
            }
        }
        core::api::TimerSchedule::Once { at } => {
            let expected = (*at > registration_ms).then_some(*at);
            if input.initial_next_due_at != expected {
                return Err(Error::new(
                    "invalid",
                    "once timer first due time does not match its UTC instant",
                ));
            }
        }
        core::api::TimerSchedule::Cron { .. } => {
            if input
                .initial_next_due_at
                .is_some_and(|due| due <= registration_ms)
            {
                return Err(Error::new(
                    "invalid",
                    "cron timer first due time must be after registration",
                ));
            }
        }
    }
    Ok(())
}

fn validate_timer_slot(slot: &TimerSlotCommit, now: i64) -> Result<()> {
    if slot.scheduled_at_ms > now {
        return Err(Error::new("invalid", "timer slot is in the future"));
    }
    match (&slot.action, &slot.skip_code) {
        (Some(_), None) | (None, Some(_)) => {}
        _ => {
            return Err(Error::new(
                "invalid",
                "timer slot must contain either one action or one skip reason",
            ));
        }
    }
    if let Some(code) = &slot.skip_code
        && (code.trim().is_empty() || code.len() > 128)
    {
        return Err(Error::new("invalid", "timer skip reason is invalid"));
    }
    Ok(())
}

pub(super) fn valid_rule_action_request(request: &SubmitCommand, allow_interrupt: bool) -> bool {
    if request.validate().is_err()
        || !request
            .target
            .as_deref()
            .is_some_and(|target| core::parse_agent_id(target).is_ok())
        || !request
            .expected_generation
            .is_some_and(|generation| generation >= 0)
        || request.request_id.is_some()
        || request.probe
    {
        return false;
    }

    match request.verb.as_str() {
        "stop" => {
            request.expected_turn_id.is_none()
                && request.args.as_object().is_some_and(|args| {
                    args.iter()
                        .all(|(key, value)| key == "force" && value == &Value::Bool(false))
                })
        }
        "send" => {
            request.expected_turn_id.is_none()
                && request.args.as_object().is_some_and(|args| {
                    args.len() == 1 && args.get("text").is_some_and(Value::is_string)
                })
        }
        "interrupt" if allow_interrupt => {
            request
                .expected_turn_id
                .as_deref()
                .is_some_and(|turn_id| !turn_id.is_empty())
                && request.args.as_object().is_some_and(|args| args.is_empty())
        }
        _ => false,
    }
}

fn valid_targetless_rule_run_request(request: &SubmitCommand) -> bool {
    request.validate().is_ok()
        && request.verb == "run"
        && request.target.is_none()
        && request.expected_generation.is_none()
        && request.expected_turn_id.is_none()
        && request.request_id.is_none()
        && !request.probe
        && request.args.is_object()
}

pub(super) fn rule_run_action_binding_error(
    tx: &Transaction<'_>,
    principal: &str,
    rule: &str,
    revision: i64,
    event_seq: i64,
    request: &SubmitCommand,
) -> Result<Option<String>> {
    if !valid_targetless_rule_run_request(request) {
        return Ok(Some("rule_action_binding_invalid".to_owned()));
    }
    match scripts::validate_rule_run_context_tx(
        tx,
        principal,
        rule,
        revision,
        event_seq,
        &request.args,
    ) {
        Ok(()) => Ok(None),
        Err(error) if error.code == "invalid" => Ok(Some("rule_action_binding_invalid".to_owned())),
        Err(error) => Err(error),
    }
}

/// Gate a rule-action child at the Store dispatch boundary. A held successor is
/// durably queued; a failed predecessor cancels every later undispatched sibling.
pub(super) fn gate_rule_action_dispatch_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<RuleActionDispatchGate> {
    let link: Option<(Option<String>, Option<String>, Option<i64>)> = tx
        .query_row(
            "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
            [command_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((parent_id, link_kind, ordinal)) = link else {
        return Err(Error::new(
            "not_found",
            "rule-action dispatch command was not found",
        ));
    };
    if link_kind.as_deref() != Some("rule_action") {
        return Ok(RuleActionDispatchGate::Ready);
    }
    let (Some(parent_id), Some(ordinal)) = (parent_id, ordinal) else {
        return Err(Error::new(
            "source_integrity",
            "rule-action child has incomplete parent ordering metadata",
        ));
    };
    if ordinal < 0 {
        return Err(Error::new(
            "source_integrity",
            "rule-action ordinal is negative",
        ));
    }
    let parent = load_command_record(tx, &parent_id)?
        .ok_or_else(|| Error::new("source_integrity", "rule-action parent is missing"))?;
    if !matches!(
        parent.command.verb.as_str(),
        "rule" | "timer.run" | "rule.catch_up" | "run"
    ) {
        return Err(Error::new(
            "source_integrity",
            "rule-action parent has an unsupported verb",
        ));
    }
    if parent.command.verb == "run" && parent.command.state != CommandState::Applied {
        return Err(Error::new(
            "source_integrity",
            "script output children cannot dispatch before the Run child set is committed",
        ));
    }
    let expected_children = parent
        .command
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "rule-action parent has no fixed child set",
            )
        })?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::new("source_integrity", "rule-action child ID is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut statement = tx
        .prepare(
            "SELECT id, principal, ordinal, state, target, generation, expected_generation
             FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL
             ORDER BY ordinal",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([&parent_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })
        .map_err(sql_error)?;
    let sibling_rows = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    drop(statement);
    let child_ids = sibling_rows
        .iter()
        .map(|(id, _, _, _, _, _, _)| id.clone())
        .collect::<Vec<_>>();
    if child_ids != expected_children {
        return Err(Error::new(
            "source_integrity",
            "rule-action child rows differ from the parent's fixed child set",
        ));
    }
    let current_index = usize::try_from(ordinal)
        .map_err(|_| Error::new("source_integrity", "rule-action ordinal is invalid"))?;
    if current_index >= sibling_rows.len()
        || sibling_rows[current_index].0 != command_id
        || sibling_rows.iter().enumerate().any(|(index, row)| {
            i64::try_from(index).ok() != Some(row.2) || row.1 != parent.command.principal
        })
    {
        return Err(Error::new(
            "source_integrity",
            "rule-action sibling ordering is inconsistent",
        ));
    }
    if current_index == 0 {
        return Ok(RuleActionDispatchGate::Ready);
    }

    let mut failed_predecessor = None;
    for (index, row) in sibling_rows.iter().take(current_index).enumerate() {
        match parse_enum::<CommandState>(&row.3, "command state")? {
            CommandState::Applied => {}
            CommandState::Rejected | CommandState::Failed | CommandState::Cancelled => {
                failed_predecessor = Some(index);
                break;
            }
            CommandState::Accepted
            | CommandState::Queued
            | CommandState::Dispatched
            | CommandState::Unknown => {
                let current = load_command_record(tx, command_id)?.ok_or_else(|| {
                    Error::new("source_integrity", "rule-action child disappeared")
                })?;
                if current.state == CommandState::Accepted {
                    let target = current.target.ok_or_else(|| {
                        Error::new("source_integrity", "rule-action child has no target")
                    })?;
                    let generation = current
                        .command
                        .generation
                        .or(current.command.expected_generation)
                        .ok_or_else(|| {
                            Error::new("source_integrity", "rule-action child has no generation")
                        })?;
                    update_command_state(tx, command_id, CommandState::Queued, None, None, now)?;
                    events.push(insert_daemon_command_event(
                        tx,
                        now,
                        "command.queued",
                        Some(target),
                        Some(generation),
                        command_id,
                        json!({
                            "command_id": command_id,
                            "reason": "rule_action_predecessor_pending",
                        }),
                    )?);
                }
                return Ok(RuleActionDispatchGate::Held);
            }
        }
    }
    let Some(failed_index) = failed_predecessor else {
        return Ok(RuleActionDispatchGate::Ready);
    };
    for row in sibling_rows.iter().skip(failed_index + 1) {
        let sibling_state: CommandState = parse_enum(&row.3, "command state")?;
        if !matches!(sibling_state, CommandState::Accepted | CommandState::Queued) {
            continue;
        }
        let target = row.4.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "undispatched rule-action child has no target",
            )
        })?;
        let generation = row.5.or(row.6).ok_or_else(|| {
            Error::new(
                "source_integrity",
                "undispatched rule-action child has no generation",
            )
        })?;
        cancel_pending_command_tx(
            tx,
            target,
            generation,
            &row.0,
            "rule_action_predecessor_terminal",
            now,
            events,
        )?;
    }
    Ok(RuleActionDispatchGate::Cancelled)
}

fn has_open_rule_actions_tx(tx: &Transaction<'_>, principal: &str, rule: &str) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM commands child
             JOIN commands parent ON parent.id = child.parent_command
             WHERE child.link_kind = 'rule_action' AND child.ordinal IS NOT NULL
               AND child.state IN ('accepted','queued','dispatched','unknown')
                AND (
                    (parent.principal = ?1
                     AND json_extract(parent.args, '$.rule') = ?2)
                    OR EXISTS (
                        SELECT 1 FROM commands root
                        WHERE parent.verb = 'run'
                          AND parent.principal = ?1
                          AND parent.link_kind = 'rule_action'
                          AND parent.ordinal IS NOT NULL
                          AND root.id = parent.parent_command
                          AND root.principal = ?1
                          AND root.verb IN ('rule','rule.catch_up')
                          AND json_extract(root.args, '$.rule') = ?2
                    )
                )
         )",
        params![principal, rule],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn update_timer_slot_tx(
    tx: &Transaction<'_>,
    principal: &str,
    stored: &StoredTimerState,
    slot_ms: i64,
    next_due_at_ms: Option<i64>,
) -> Result<core::api::TimerStateView> {
    let changed = tx
        .execute(
            "UPDATE timers SET last_key = ?1, next_due_at = ?2
             WHERE principal = ?3 AND rule = ?4 AND schedule_revision = ?5
               AND last_key IS ?6 AND next_due_at IS ?7",
            params![
                slot_ms,
                next_due_at_ms,
                principal,
                stored.view.id,
                stored.view.schedule_revision,
                stored.view.last_key,
                stored.view.next_due_at,
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new("bad_state", "timer cursor changed concurrently"));
    }
    let mut view = stored.view.clone();
    view.last_key = Some(slot_ms);
    view.next_due_at = next_due_at_ms;
    Ok(view)
}

fn reject_timer_fire_tx(
    tx: &Transaction<'_>,
    key: &RuleFiringKey<'_>,
    code: &str,
    now: i64,
) -> Result<(Command, Vec<Event>)> {
    let parent_cause = key.command_cause(&key.parent_id)?;
    let (parent, mut parent_event) =
        insert_rule_parent(tx, key, RuleParentOutcome::Rejected { code }, now)?;
    annotate_new_command_cause_tx(tx, &parent.id, &mut parent_event, parent_cause.clone())?;
    let rejected = insert_rule_event_with_cause(
        tx,
        now,
        "rule.rejected",
        key.rejected_data(code),
        Some(parent_cause),
    )?;
    Ok((parent, vec![parent_event, rejected]))
}

fn load_rule_state(
    connection: &Connection,
    principal: &str,
    id: &str,
) -> Result<Option<StoredRuleState>> {
    let row = connection
        .query_row(
            "SELECT revision, current_definition, definition_digest,
                    current_cursor, enabled, paused_reason, retiring_revision,
                    retiring_definition, retiring_cursor, retire_through
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
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, Option<i64>>(9)?,
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
        retiring_revision,
        retiring_definition,
        retiring_cursor,
        retire_through,
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
    let view = core::api::RuleStateView {
        id: id.to_owned(),
        project: rule_project_id(principal)?,
        revision,
        current_cursor,
        retiring_revision,
        retiring_cursor,
        retire_through,
        enabled,
        paused_reason,
    };
    view.validate()
        .map_err(|_| Error::new("source_integrity", "stored rule cursor state is invalid"))?;
    if view.retiring_revision.is_some() != retiring_definition.is_some() {
        return Err(Error::new(
            "source_integrity",
            "stored retiring rule definition and cursor state disagree",
        ));
    }
    Ok(Some(StoredRuleState {
        view,
        canonical_definition,
        definition_digest,
        retiring_definition,
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

fn has_non_rule_event_in_range(tx: &Transaction<'_>, after: i64, through: i64) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM events
             WHERE seq > ?1 AND seq <= ?2 AND kind NOT LIKE 'rule.%'
         )",
        params![after, through],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn rule_cursor_is_retiring(state: &core::api::RuleStateView, through: i64) -> bool {
    state
        .retire_through
        .is_some_and(|boundary| through <= boundary)
        && state.retiring_revision.is_some()
        && state.retiring_cursor.is_some()
}

fn update_rule_cursor_tx(
    tx: &Transaction<'_>,
    principal: &str,
    state: &core::api::RuleStateView,
    through: i64,
    now: i64,
) -> Result<core::api::RuleStateView> {
    let retiring = rule_cursor_is_retiring(state, through);
    let changed = if retiring {
        let boundary = state
            .retire_through
            .ok_or_else(|| Error::new("source_integrity", "retiring boundary is incomplete"))?;
        if through > boundary {
            return Err(Error::new(
                "invalid",
                "retiring cursor cannot advance past its boundary",
            ));
        }
        let complete = through == boundary;
        tx.execute(
            "UPDATE rule_state
             SET
                 retiring_revision = CASE WHEN ?2 THEN NULL ELSE retiring_revision END,
                 retiring_definition = CASE WHEN ?2 THEN NULL ELSE retiring_definition END,
                 retiring_cursor = CASE WHEN ?2 THEN NULL ELSE ?1 END,
                 retire_through = CASE WHEN ?2 THEN NULL ELSE retire_through END,
                 updated_at = ?3
             WHERE principal = ?4 AND rule = ?5 AND revision = ?6
               AND current_cursor = ?7 AND retiring_revision = ?8
               AND retiring_cursor = ?9 AND retire_through = ?10",
            params![
                through,
                complete,
                now,
                principal,
                state.id,
                state.revision,
                state.current_cursor,
                state.retiring_revision,
                state.retiring_cursor,
                state.retire_through,
            ],
        )
        .map_err(sql_error)?
    } else {
        tx.execute(
            "UPDATE rule_state
             SET current_cursor = ?1, updated_at = ?2
             WHERE principal = ?3 AND rule = ?4 AND revision = ?5
               AND current_cursor = ?6
               AND retiring_revision IS ?7 AND retiring_cursor IS ?8
               AND retire_through IS ?9",
            params![
                through,
                now,
                principal,
                state.id,
                state.revision,
                state.current_cursor,
                state.retiring_revision,
                state.retiring_cursor,
                state.retire_through,
            ],
        )
        .map_err(sql_error)?
    };
    if changed != 1 {
        return Err(Error::new("bad_state", "rule cursor changed concurrently"));
    }
    let mut next = state.clone();
    if retiring {
        if through == state.retire_through.expect("retiring boundary was checked") {
            next.retiring_revision = None;
            next.retiring_cursor = None;
            next.retire_through = None;
        } else {
            next.retiring_cursor = Some(through);
        }
    } else {
        next.current_cursor = through;
    }
    Ok(next)
}

fn insert_rule_event(tx: &Transaction<'_>, now: i64, kind: &str, data: Value) -> Result<Event> {
    insert_rule_event_with_cause(tx, now, kind, data, None)
}

fn insert_rule_event_with_cause(
    tx: &Transaction<'_>,
    now: i64,
    kind: &str,
    data: Value,
    cause: Option<core::EventCause>,
) -> Result<Event> {
    let input = DaemonEventInput::new(now, kind, "rule", None, None, data);
    let input = match cause {
        Some(cause) => input.with_cause(cause),
        None => input,
    };
    insert_daemon_event_input(tx, input)
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

fn derived_timer_id(
    purpose: &str,
    principal: &str,
    rule: &str,
    schedule_revision: i64,
    slot_ms: i64,
) -> Result<String> {
    let digest = canonical::digest(&json!({
        "domain": RULE_COMMAND_ID_DOMAIN,
        "purpose": purpose,
        "principal": principal,
        "rule": rule,
        "schedule_revision": schedule_revision,
        "scheduled_at_ms": slot_ms,
    }))?;
    let bytes: [u8; 16] = digest[..16]
        .try_into()
        .map_err(|_| Error::new("internal", "timer command digest is too short"))?;
    Ok(Ulid::from_bytes(bytes).to_string())
}

fn derived_manual_timer_id(principal: &str, rule: &str, mutation_id: &str) -> Result<String> {
    let digest = canonical::digest(&json!({
        "domain": RULE_COMMAND_ID_DOMAIN,
        "purpose": "action-0",
        "principal": principal,
        "rule": rule,
        "mutation_id": mutation_id,
    }))?;
    let bytes: [u8; 16] = digest[..16]
        .try_into()
        .map_err(|_| Error::new("internal", "manual timer digest is too short"))?;
    Ok(Ulid::from_bytes(bytes).to_string())
}

fn load_rule_trigger(tx: &Transaction<'_>, event_seq: i64) -> Result<StoredRuleTrigger> {
    tx.query_row(
        "SELECT kind, agent, generation, cause,
                CASE WHEN json_type(data, '$.turn_id') = 'text'
                     THEN json_extract(data, '$.turn_id') END
         FROM events WHERE seq = ?1",
        [event_seq],
        |row| {
            Ok(StoredRuleTrigger {
                kind: row.get(0)?,
                agent: row.get(1)?,
                generation: row.get(2)?,
                encoded_cause: row.get(3)?,
                turn_id: row.get(4)?,
                cause: None,
            })
        },
    )
    .optional()
    .map_err(sql_error)?
    .ok_or_else(|| Error::new("not_found", "rule trigger event was not found"))
    .map(|mut trigger| {
        trigger.cause = decode_event_cause(trigger.encoded_cause.as_deref(), None);
        trigger
    })
}

struct StoredRuleTrigger {
    kind: String,
    agent: Option<i64>,
    generation: Option<i64>,
    encoded_cause: Option<String>,
    turn_id: Option<String>,
    cause: Option<core::EventCause>,
}

fn rule_action_binding_error(event: &StoredRuleTrigger, request: &SubmitCommand) -> Option<String> {
    if !valid_rule_action_request(request, true) {
        return Some("rule_action_binding_invalid".to_owned());
    }
    if request.target != event.agent.map(format_agent_id)
        || request.expected_generation != event.generation
        || (request.verb == "interrupt"
            && request.expected_turn_id.as_deref() != event.turn_id.as_deref())
    {
        return Some("rule_action_binding_invalid".to_owned());
    }
    None
}

fn should_suppress_own_event_send_tx(
    tx: &Transaction<'_>,
    principal: &str,
    rule: &str,
    canonical_definition: &str,
    request: &SubmitCommand,
    trigger_cause: Option<&core::EventCause>,
) -> Result<bool> {
    if request.verb != "send" {
        return Ok(false);
    }
    let definition: Value = serde_json::from_str(canonical_definition)
        .map_err(|_| Error::new("source_integrity", "stored rule definition is invalid"))?;
    if definition
        .get("allow_self")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(false);
    }
    let Some(cause) = trigger_cause else {
        return Ok(false);
    };
    let Some(verified) = command_cause_tx(tx, &cause.command_id)? else {
        return Ok(false);
    };
    if verified.command_id != cause.command_id || verified.depth != cause.depth {
        return Ok(false);
    }

    // Resolve the origin row, following its parent link for an action child.
    // EventCause.rule is descriptive and is not authority for this check.
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM commands origin
             WHERE origin.id = ?1 AND origin.principal = ?2
               AND (
                   (origin.verb IN ('rule','timer.run','rule.catch_up')
                    AND json_extract(origin.args, '$.rule') = ?3)
                   OR EXISTS (
                       SELECT 1 FROM commands parent
                       WHERE parent.id = origin.parent_command
                         AND origin.link_kind = 'rule_action'
                         AND origin.ordinal IS NOT NULL
                         AND parent.principal = ?2
                         AND parent.verb IN ('rule','timer.run','rule.catch_up')
                         AND json_extract(parent.args, '$.rule') = ?3
                   )
                    OR EXISTS (
                        SELECT 1 FROM commands run
                        JOIN commands root ON root.id = run.parent_command
                        WHERE run.id = origin.parent_command
                          AND origin.link_kind = 'rule_action'
                          AND origin.ordinal IS NOT NULL
                          AND run.verb = 'run'
                          AND run.principal = ?2
                          AND run.link_kind = 'rule_action'
                          AND run.ordinal IS NOT NULL
                          AND root.principal = ?2
                          AND root.verb IN ('rule','rule.catch_up')
                          AND json_extract(root.args, '$.rule') = ?3
                    )
               )
         )",
        params![cause.command_id, principal, rule],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn rule_action_counts(
    tx: &Transaction<'_>,
    principal: &str,
    rule: &str,
    target_agent: Option<&str>,
    limit: &core::api::RuleLimit,
    now: i64,
) -> Result<(i64, i64)> {
    if limit.max == 0 || limit.window_ms <= 0 {
        return Err(Error::new("invalid", "rule limit must be positive"));
    }
    let since = now.saturating_sub(limit.window_ms);
    let scoped = match &limit.per {
        core::api::RuleLimitScope::Rule => tx
            .query_row(
                "SELECT COUNT(*)
                 FROM commands action
                 JOIN commands parent ON parent.id = action.parent_command
                 WHERE action.link_kind = 'rule_action' AND action.ordinal IS NOT NULL
                   AND action.created_at >= ?1 AND action.created_at <= ?2
                    AND EXISTS (
                        SELECT 1 FROM commands root
                        WHERE root.principal = ?3
                          AND root.verb IN ('rule','timer.run','rule.catch_up')
                          AND json_extract(root.args, '$.rule') = ?4
                          AND (
                              root.id = parent.id
                              OR (parent.verb = 'run'
                                  AND parent.parent_command = root.id
                                  AND parent.principal = root.principal
                                  AND parent.link_kind = 'rule_action'
                                  AND parent.ordinal IS NOT NULL)
                          )
                          AND action.principal = root.principal
                    )",
                params![since, now, principal, rule],
                |row| row.get(0),
            )
            .map_err(sql_error)?,
        core::api::RuleLimitScope::Agent => match target_agent {
            // A targetless Run consumes rule/global capacity, but it has no
            // Agent target and must not inherit one from its trigger event.
            None => 0,
            Some(target_agent) => {
                let target_agent = parse_agent_id(target_agent)?;
                tx.query_row(
                    "SELECT COUNT(*)
                     FROM commands action
                     JOIN commands parent ON parent.id = action.parent_command
                     WHERE action.link_kind = 'rule_action' AND action.ordinal IS NOT NULL
                       AND action.created_at >= ?1 AND action.created_at <= ?2
                        AND EXISTS (
                            SELECT 1 FROM commands root
                            WHERE root.principal = ?3
                              AND root.verb IN ('rule','timer.run','rule.catch_up')
                              AND json_extract(root.args, '$.rule') = ?4
                              AND (
                                  root.id = parent.id
                                  OR (parent.verb = 'run'
                                      AND parent.parent_command = root.id
                                      AND parent.principal = root.principal
                                      AND parent.link_kind = 'rule_action'
                                      AND parent.ordinal IS NOT NULL)
                              )
                        )
                        AND action.principal = ?3
                        AND action.target = ?5",
                    params![since, now, principal, rule, target_agent],
                    |row| row.get(0),
                )
                .map_err(sql_error)?
            }
        },
    };
    // The global guard spans owner and project principals and stays fixed at
    // 120 admitted action children per minute.
    let global = tx
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL
               AND created_at >= ?1 AND created_at <= ?2",
            params![now.saturating_sub(RULE_ACTION_WINDOW_MS), now],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok((scoped, global))
}

fn rule_limit_reached(count: i64, max: u64) -> bool {
    u64::try_from(count).is_ok_and(|count| count >= max)
}

fn validate_rule_rejection_code(code: &str) -> Result<()> {
    if code.trim().is_empty() || code.len() > 128 {
        return Err(Error::new("invalid", "rule rejection code is invalid"));
    }
    Ok(())
}

fn insert_rule_parent(
    tx: &Transaction<'_>,
    key: &RuleFiringKey<'_>,
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
    let args = &key.args;
    let outcome = match rejection_code {
        Some(code) => json!({"children": child_ids, "code": code}),
        None => json!({"children": child_ids}),
    };
    let digest = match &key.parent_digest {
        Some(digest) => digest.clone(),
        None => canonical::digest(&json!({
            "principal": key.principal,
            "verb": key.verb,
            "args": args,
            "outcome": outcome,
        }))?,
    };
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
        verb: key.verb,
        args,
        state,
        outcome: Some(&outcome),
        created_at: now,
        updated_at: now,
    };
    let event = admit_command(tx, &command, key.command_event_data(child_ids.clone()))?;
    Ok((
        Command {
            id: key.parent_id.clone(),
            principal: key.principal.to_owned(),
            method: COMMAND_METHOD.to_owned(),
            verb: key.verb.to_owned(),
            target: None,
            expected_generation: None,
            generation: None,
            args: (*args).clone(),
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

fn admit_rule_action_tx(
    tx: &Transaction<'_>,
    key: &RuleFiringKey<'_>,
    mut request: SubmitCommand,
    now: i64,
) -> Result<(Command, Command, Vec<Event>)> {
    let parent_cause = key.command_cause(&key.parent_id)?;
    let child_cause = key.command_cause(&key.child_id)?;
    request.id = key.child_id.clone();
    let mut child_admission = submit_tx(tx, key.principal, request, None, now, true)?;
    if !child_admission.is_new {
        return Err(Error::new(
            "source_integrity",
            "rule child exists without its deterministic parent",
        ));
    }

    let child = child_admission.command;
    annotate_rule_command_event(tx, &child.id, &mut child_admission.events, child_cause)?;
    let (parent, mut parent_event) = insert_rule_parent(tx, key, RuleParentOutcome::Admitted, now)?;
    annotate_new_command_cause_tx(tx, &parent.id, &mut parent_event, parent_cause.clone())?;
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
    events.push(insert_rule_event_with_cause(
        tx,
        now,
        "rule.fired",
        key.fired_data(&child.id),
        Some(parent_cause),
    )?);
    Ok((parent, child, events))
}

pub(super) fn annotate_rule_command_event(
    tx: &Transaction<'_>,
    command_id: &str,
    events: &mut [Event],
    cause: core::EventCause,
) -> Result<()> {
    let event = events
        .iter_mut()
        .find(|event| event.data.get("command_id").and_then(Value::as_str) == Some(command_id))
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "new rule command has no matching returned command event",
            )
        })?;
    annotate_new_command_cause_tx(tx, command_id, event, cause)
}

fn reject_rule_event(
    tx: Transaction<'_>,
    write_faulted: &Cell<bool>,
    key: &RuleFiringKey<'_>,
    code: &str,
    now: i64,
) -> Result<RuleEventAdmission> {
    let RuleFiringIdentity::Event { event_seq } = key.identity else {
        return Err(Error::new(
            "internal",
            "event rejection has a non-event key",
        ));
    };
    let parent_cause = key.command_cause(&key.parent_id)?;
    let (parent, mut parent_event) =
        insert_rule_parent(&tx, key, RuleParentOutcome::Rejected { code }, now)?;
    annotate_new_command_cause_tx(&tx, &parent.id, &mut parent_event, parent_cause.clone())?;
    let mut events = vec![parent_event];
    events.push(insert_rule_event_with_cause(
        &tx,
        now,
        "rule.rejected",
        key.rejected_data(code),
        Some(parent_cause),
    )?);
    let view = update_rule_cursor_tx(&tx, key.principal, key.state, event_seq, now)?;
    commit_transaction(tx, write_faulted)?;
    Ok(RuleEventAdmission {
        state: view,
        parent: Some(parent),
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

    fn insert_test_rule_action(
        store: &mut Store,
        principal: &str,
        rule: &str,
        target: &str,
        generation: i64,
        now: i64,
    ) {
        let parent_id = Ulid::new().to_string();
        let child_id = Ulid::new().to_string();
        let target_agent = parse_agent_id(target).unwrap();
        ensure_test_project_action_scope(store, principal, target_agent, generation, now);
        let payload_digest = [0_u8; 32];
        let args = json!({"rule": rule});
        let outcome = json!({"children": [child_id]});
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let parent = CommandInsert {
            id: &parent_id,
            principal,
            method: COMMAND_METHOD,
            payload_digest: &payload_digest,
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
            state: CommandState::Applied,
            outcome: Some(&outcome),
            created_at: now,
            updated_at: now,
        };
        admit_command(
            &tx,
            &parent,
            json!({"command_id": parent_id, "children": [child_id]}),
        )
        .unwrap();

        let child_args = json!({});
        let child = CommandInsert {
            id: &child_id,
            principal,
            method: COMMAND_METHOD,
            payload_digest: &payload_digest,
            target: Some(target_agent),
            expected_generation: Some(generation),
            generation: Some(generation),
            expected_turn_id: None,
            parent_command: Some(&parent_id),
            link_kind: Some("rule_action"),
            ordinal: Some(0),
            turn_id: None,
            request_id: None,
            probe: false,
            verb: "stop",
            args: &child_args,
            state: CommandState::Accepted,
            outcome: None,
            created_at: now,
            updated_at: now,
        };
        insert_command(&tx, &child, &ordinary_command_cause(&child_id)).unwrap();
        tx.commit().unwrap();
    }

    fn ensure_test_project_action_scope(
        store: &mut Store,
        principal: &str,
        agent: i64,
        generation: i64,
        now: i64,
    ) {
        let Some(project) = principal.strip_prefix("project:") else {
            return;
        };
        let checkout = format!("synthetic-checkout-{project}");
        store
            .upsert_project_registration(
                "owner",
                &ProjectRegistrationInput {
                    id: project.to_owned(),
                    checkout: checkout.clone(),
                    repo_common_dir: format!("synthetic-common-{project}"),
                    profiles: vec!["fake".to_owned()],
                    actions: vec!["stop".to_owned()],
                    interpreters: std::collections::BTreeMap::new(),
                    run_env: Vec::new(),
                },
                now,
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE agents SET project = ?1, profile = 'fake', workspace = ?2,
                     generation = ?3 WHERE id = ?4",
                params![project, checkout, generation, agent],
            )
            .unwrap();
    }

    fn insert_test_turn_completed(store: &mut Store, agent: i64, now: i64) -> i64 {
        insert_test_turn_completed_with_cause(store, agent, now, None)
    }

    fn insert_test_turn_completed_with_cause(
        store: &mut Store,
        agent: i64,
        now: i64,
        cause: Option<core::EventCause>,
    ) -> i64 {
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let event = insert_event(
            &tx,
            EventInsert {
                ts: now,
                kind: "turn.completed",
                family: Some("turn"),
                agent: Some(agent),
                generation: Some(1),
                source: "stream",
                native_id: None,
                cursor: None,
                payload_digest: None,
                subject: None,
                cause,
                data: json!({
                    "status": "completed",
                    "cause": {"command_id": "forged", "depth": 5}
                }),
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

    fn interrupt_request(agent: &str, generation: i64, turn_id: Option<&str>) -> SubmitCommand {
        let mut request = SubmitCommand::new("", "interrupt");
        request.target = Some(agent.to_owned());
        request.expected_generation = Some(generation);
        request.expected_turn_id = turn_id.map(str::to_owned);
        request
    }

    fn set_interrupt_test_turn(store: &mut Store, agent: i64, turn_id: &str) {
        store
            .connection
            .execute(
                "UPDATE agents SET harness = 'codex', state = 'busy', active_turn = ?1
                 WHERE id = ?2",
                params![turn_id, agent],
            )
            .unwrap();
    }

    fn insert_test_interrupt_event(
        store: &mut Store,
        agent: i64,
        generation: i64,
        turn_id: Option<&str>,
        now: i64,
    ) -> i64 {
        let data = match turn_id {
            Some(turn_id) => json!({"turn_id": turn_id}),
            None => json!({}),
        };
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let event = insert_event(
            &tx,
            EventInsert {
                ts: now,
                kind: "turn.started",
                family: Some("turn"),
                agent: Some(agent),
                generation: Some(generation),
                source: "stream",
                native_id: None,
                cursor: None,
                payload_digest: None,
                subject: None,
                cause: None,
                data,
            },
        )
        .unwrap();
        tx.commit().unwrap();
        event.seq
    }

    #[test]
    fn event_interrupt_binds_exact_turn_and_replays_the_same_child() {
        let mut store = store();
        let (agent, agent_id) = insert_test_agent(&mut store);
        set_interrupt_test_turn(&mut store, agent, "turn-captured");
        let rule = definition("interrupt-rule", true);
        let state = store.ensure_rule_state("owner", &rule, 10).unwrap();
        let event_seq =
            insert_test_interrupt_event(&mut store, agent, 1, Some("turn-captured"), 11);
        let request = interrupt_request(&agent_id, 1, Some("turn-captured"));
        assert!(valid_rule_action_request(&request, true));
        assert!(!valid_rule_action_request(&request, false));

        let first = store
            .admit_rule_action("owner", &state, event_seq, request.clone(), 12)
            .unwrap();
        assert!(first.is_new);
        let parent_id = first.parent.as_ref().expect("admitted parent").id.clone();
        let child = first.child.as_ref().expect("admitted interrupt child");
        assert_eq!(child.verb, "interrupt");
        assert_eq!(child.target.as_deref(), Some(agent_id.as_str()));
        assert_eq!(child.expected_generation, Some(1));
        assert_eq!(child.turn_id.as_deref(), Some("turn-captured"));
        assert_eq!(child.state, CommandState::Accepted);
        let child_id = child.id.clone();
        let retained_expected_turn: Option<String> = store
            .connection
            .query_row(
                "SELECT expected_turn_id FROM commands WHERE id = ?1",
                [&child_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_expected_turn.as_deref(), Some("turn-captured"));

        let replay = store
            .admit_rule_action("owner", &state, event_seq, request, 13)
            .unwrap();
        assert!(!replay.is_new);
        assert_eq!(replay.parent.as_ref().unwrap().id, parent_id);
        assert_eq!(replay.child.as_ref().unwrap().id, child_id);
        assert!(replay.events.is_empty());
        assert!(store.get_command(&child_id).unwrap().is_some());
    }

    #[test]
    fn event_interrupt_rejects_wrong_agent_generation_or_missing_captured_turn() {
        for mismatch in [
            "agent",
            "generation",
            "missing_event_turn",
            "missing_request_turn",
        ] {
            let mut store = store();
            let (agent, agent_id) = insert_test_agent(&mut store);
            set_interrupt_test_turn(&mut store, agent, "turn-captured");
            if mismatch == "generation" {
                store
                    .connection
                    .execute("UPDATE agents SET generation = 2 WHERE id = ?1", [agent])
                    .unwrap();
            }
            let other = if mismatch == "agent" {
                let (other, other_id) = insert_test_agent(&mut store);
                set_interrupt_test_turn(&mut store, other, "turn-captured");
                Some(other_id)
            } else {
                None
            };
            let rule = definition("interrupt-negative", true);
            let state = store.ensure_rule_state("owner", &rule, 20).unwrap();
            let event_turn = (mismatch != "missing_event_turn").then_some("turn-captured");
            let event_seq = insert_test_interrupt_event(&mut store, agent, 1, event_turn, 21);
            let (target, generation) = match mismatch {
                "agent" => (other.as_deref().unwrap(), 1),
                "generation" => (agent_id.as_str(), 2),
                "missing_event_turn" | "missing_request_turn" => (agent_id.as_str(), 1),
                _ => unreachable!(),
            };
            let request_turn = (mismatch != "missing_request_turn").then_some("turn-captured");
            let request = interrupt_request(target, generation, request_turn);

            let admission = store
                .admit_rule_action("owner", &state, event_seq, request, 22)
                .unwrap();
            assert!(admission.child.is_none(), "{mismatch} created a child");
            assert_eq!(admission.state.current_cursor, event_seq);
            let parent = admission.parent.expect("committed rejection parent");
            assert_eq!(parent.state, CommandState::Rejected);
            assert_eq!(parent.outcome.as_ref().unwrap()["children"], json!([]));
            assert_eq!(
                parent.outcome.as_ref().unwrap()["code"],
                "rule_action_binding_invalid"
            );
            let child_id =
                derived_rule_id("action-0", "owner", &rule.id, state.revision, event_seq).unwrap();
            assert!(store.get_command(&child_id).unwrap().is_none());
        }
    }

    #[test]
    fn historical_interrupt_does_not_retarget_a_new_active_turn() {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 10;
        let (agent, agent_id) = insert_test_agent(&mut store);
        set_interrupt_test_turn(&mut store, agent, "turn-current");
        let rule = definition("history-interrupt", true);
        let state = store.ensure_rule_state("owner", &rule, now - 2).unwrap();
        let event_seq =
            insert_test_interrupt_event(&mut store, agent, 1, Some("turn-historical"), now - 1);
        let advanced = store
            .advance_rule_cursor("owner", &state, event_seq, RuleCursorOutcome::NoMatch, now)
            .unwrap();
        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            rule.id.as_str(),
            state.current_cursor,
            Some(event_seq),
        );
        let actions = [RuleCatchUpAction::command(
            event_seq,
            interrupt_request(&agent_id, 1, Some("turn-historical")),
        )];
        let high_water = store.high_water().unwrap();

        let first = store
            .admit_rule_catch_up(
                "owner",
                &request,
                &advanced.value,
                event_seq,
                &actions,
                now + 1,
            )
            .unwrap();
        assert!(first.is_new);
        assert_eq!(first.parent.state, CommandState::Applied);
        assert!(first.children.is_empty());
        let outcome = first.parent.outcome.as_ref().expect("catch-up outcome");
        assert_eq!(outcome["children"], json!([]));
        assert!(outcome.get("code").is_none());
        let rejected = first
            .events
            .iter()
            .find(|event| {
                event.kind == "rule.rejected"
                    && event.data["parent_command"] == request.id
                    && event.data["event_seq"] == event_seq
            })
            .expect("stale historical interrupt rejection marker");
        assert_eq!(rejected.data["code"], "stale_generation");
        assert_eq!(
            first
                .events
                .iter()
                .filter(|event| {
                    event.kind == "rule.rejected"
                        && event.data["parent_command"] == request.id
                        && event.data["event_seq"] == event_seq
                })
                .count(),
            1
        );
        let child_id =
            derived_rule_id("action-0", "owner", &rule.id, state.revision, event_seq).unwrap();
        assert!(store.get_command(&child_id).unwrap().is_none());
        let committed_high_water = store.high_water().unwrap();
        assert!(committed_high_water > high_water);
        assert_eq!(
            store
                .get_rule_state("owner", &rule.id)
                .unwrap()
                .unwrap()
                .current_cursor,
            event_seq
        );

        let replay = store
            .admit_rule_catch_up(
                "owner",
                &request,
                &advanced.value,
                event_seq,
                &actions,
                now + 2,
            )
            .unwrap();
        assert!(!replay.is_new);
        assert_eq!(replay.parent.id, first.parent.id);
        assert_eq!(replay.parent.state, CommandState::Applied);
        assert!(replay.children.is_empty());
        assert!(replay.events.is_empty());
        assert_eq!(store.high_water().unwrap(), committed_high_water);
    }

    #[test]
    fn catch_up_rejections_do_not_consume_rate_before_a_valid_child() {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 10;
        let (agent, agent_id) = insert_test_agent(&mut store);
        set_interrupt_test_turn(&mut store, agent, "turn-current");

        let mut rule = definition("history-interrupt-limit", true);
        let mut canonical: Value = serde_json::from_str(&rule.canonical_definition).unwrap();
        canonical["limit"] = json!({
            "max": 1,
            "per": "rule",
            "window_ms": RULE_ACTION_WINDOW_MS,
        });
        rule.canonical_definition = serde_json::to_string(&canonical).unwrap();
        rule.definition_digest = core::canonical_json_sha256(&canonical).unwrap();

        let state = store.ensure_rule_state("owner", &rule, now - 3).unwrap();
        let stale_event =
            insert_test_interrupt_event(&mut store, agent, 1, Some("turn-historical"), now - 3);
        let current_event =
            insert_test_interrupt_event(&mut store, agent, 1, Some("turn-current"), now - 2);
        let prepared_rejection_event =
            insert_test_interrupt_event(&mut store, agent, 1, Some("turn-current"), now - 1);
        assert!(stale_event < current_event && current_event < prepared_rejection_event);
        let advanced = store
            .advance_rule_cursor(
                "owner",
                &state,
                prepared_rejection_event,
                RuleCursorOutcome::NoMatch,
                now + 1,
            )
            .unwrap();
        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            rule.id.as_str(),
            state.current_cursor,
            Some(prepared_rejection_event),
        );
        let actions = [
            RuleCatchUpAction::command(
                stale_event,
                interrupt_request(&agent_id, 1, Some("turn-historical")),
            ),
            RuleCatchUpAction::command(
                current_event,
                interrupt_request(&agent_id, 1, Some("turn-current")),
            ),
            RuleCatchUpAction::rejected(prepared_rejection_event, "invalid"),
        ];

        let admission = store
            .admit_rule_catch_up(
                "owner",
                &request,
                &advanced.value,
                prepared_rejection_event,
                &actions,
                now + 2,
            )
            .unwrap();
        assert!(admission.is_new);
        assert_eq!(admission.parent.state, CommandState::Applied);
        assert_eq!(admission.children.len(), 1);
        let admitted_child = &admission.children[0];
        assert_eq!(admitted_child.verb, "interrupt");
        assert_eq!(admitted_child.turn_id.as_deref(), Some("turn-current"));
        assert_eq!(
            admission.parent.outcome.as_ref().unwrap()["children"],
            json!([admitted_child.id.clone()])
        );
        let rejected_child_id = derived_rule_id(
            "action-0",
            "owner",
            &rule.id,
            state.revision,
            prepared_rejection_event,
        )
        .unwrap();
        assert!(store.get_command(&rejected_child_id).unwrap().is_none());
        let failed_submit_child_id =
            derived_rule_id("action-0", "owner", &rule.id, state.revision, stale_event).unwrap();
        assert!(
            store
                .get_command(&failed_submit_child_id)
                .unwrap()
                .is_none()
        );
        let failed_submit_rejected = admission
            .events
            .iter()
            .find(|event| {
                event.kind == "rule.rejected"
                    && event.data["event_seq"] == stale_event
                    && event.data["parent_command"] == request.id
            })
            .expect("failed ordinary submit has a rejection marker");
        assert_eq!(failed_submit_rejected.data["code"], "stale_generation");
        let rejected = admission
            .events
            .iter()
            .find(|event| {
                event.kind == "rule.rejected"
                    && event.data["event_seq"] == prepared_rejection_event
                    && event.data["parent_command"] == request.id
            })
            .expect("prepared rejection has a marker");
        assert_eq!(rejected.data["code"], "invalid");
        let fired = admission
            .events
            .iter()
            .find(|event| {
                event.kind == "rule.fired"
                    && event.data["event_seq"] == current_event
                    && event.data["child_command"] == admitted_child.id
            })
            .expect("valid second action is admitted");
        assert_eq!(fired.data["parent_command"], request.id);

        let linked_children: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM commands
                 WHERE parent_command = ?1 AND link_kind = 'rule_action'",
                [&admission.parent.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(linked_children, 1);

        let high_water = store.high_water().unwrap();
        let replay = store
            .admit_rule_catch_up(
                "owner",
                &request,
                &advanced.value,
                prepared_rejection_event,
                &actions,
                now + 3,
            )
            .unwrap();
        assert!(!replay.is_new);
        assert_eq!(replay.parent.id, admission.parent.id);
        assert_eq!(replay.children[0].id, admitted_child.id);
        assert!(replay.events.is_empty());
        assert_eq!(store.high_water().unwrap(), high_water);
    }

    #[test]
    fn new_rule_seeds_once_and_changed_definition_retires_old_revision() {
        let mut store = store();
        let first_event = insert_test_event(&mut store, "fixture", 1);
        let first = definition("r1", true);
        let seeded = store
            .reconcile_rule_definitions("owner", std::slice::from_ref(&first), &[], 2)
            .unwrap();
        let view = seeded.value[0].view.clone();
        assert_eq!(view.current_cursor, first_event);

        let later_event = insert_test_event(&mut store, "fixture", 3);
        let unchanged = store
            .reconcile_rule_definitions("owner", &[first], &[], 4)
            .unwrap();
        assert_eq!(unchanged.value[0].view, view);

        let changed = definition("r1", false);
        let reconciled = store
            .reconcile_rule_definitions("owner", std::slice::from_ref(&changed), &[], 5)
            .unwrap();
        let updated = reconciled.value[0].view.clone();
        assert_eq!(updated.revision, view.revision + 1);
        assert_eq!(updated.current_cursor, later_event);
        assert!(!updated.enabled);
        assert!(updated.retiring_revision.is_none());
        assert_eq!(reconciled.events.len(), 1);
        assert_eq!(reconciled.events[0].kind, "rule.skipped");
        assert_eq!(
            store
                .reconcile_rule_definitions("owner", &[changed], &[], 6)
                .unwrap()
                .value[0]
                .view,
            updated
        );
        assert!(later_event > view.current_cursor);
    }

    #[test]
    fn enabled_definition_edit_retires_through_captured_high_water() {
        let mut store = store();
        let first_event = insert_test_event(&mut store, "fixture", 1);
        let old = definition("r1", true);
        let old_view = store.ensure_rule_state("owner", &old, 2).unwrap();
        let boundary = insert_test_event(&mut store, "fixture", 3);
        let changed = RuleDefinitionInput {
            canonical_definition: serde_json::to_string(
                &json!({"id":"r1","enabled":true,"new":true}),
            )
            .unwrap(),
            definition_digest: core::canonical_json_sha256(
                &json!({"id":"r1","enabled":true,"new":true}),
            )
            .unwrap(),
            ..old
        };

        let view = store.ensure_rule_state("owner", &changed, 4).unwrap();
        assert_eq!(view.revision, old_view.revision + 1);
        assert_eq!(view.current_cursor, boundary);
        assert_eq!(view.retiring_revision, Some(old_view.revision));
        assert_eq!(view.retiring_cursor, Some(first_event));
        assert_eq!(view.retire_through, Some(boundary));

        let advanced = store
            .advance_rule_cursor("owner", &view, boundary, RuleCursorOutcome::NoMatch, 5)
            .unwrap();
        assert!(advanced.value.retiring_revision.is_none());
        assert!(advanced.value.retiring_cursor.is_none());
        assert!(advanced.value.retire_through.is_none());
    }

    #[test]
    fn retiring_revision_admits_with_its_own_deterministic_key() {
        let mut store = store();
        let (agent, agent_id) = insert_test_agent(&mut store);
        let old = definition("stop-rule", true);
        let old_view = store.ensure_rule_state("owner", &old, 1).unwrap();
        let event_seq = insert_test_turn_completed(&mut store, agent, 2);
        let changed_value = json!({"id":"stop-rule","enabled":true,"matcher":"updated"});
        let changed = RuleDefinitionInput {
            canonical_definition: serde_json::to_string(&changed_value).unwrap(),
            definition_digest: core::canonical_json_sha256(&changed_value).unwrap(),
            ..old
        };
        let current = store.ensure_rule_state("owner", &changed, 3).unwrap();
        assert_eq!(current.retiring_revision, Some(old_view.revision));

        let admission = store
            .admit_rule_action("owner", &current, event_seq, stop_request(&agent_id), 4)
            .unwrap();
        let parent = admission.parent.expect("retiring rule parent");
        assert_eq!(parent.args["revision"], old_view.revision);
        assert_eq!(
            parent.id,
            derived_rule_id("parent", "owner", "stop-rule", old_view.revision, event_seq).unwrap()
        );
        assert!(admission.child.is_some());
        assert!(admission.state.retiring_revision.is_none());
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

        // A disabled consumer of the marker must not create another marker.
        let marker_seq = skipped.events[0].seq;
        let marker = store
            .advance_rule_cursor(
                "owner",
                &skipped.value,
                marker_seq,
                RuleCursorOutcome::Disabled,
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
    fn backlog_guard_counts_committed_rows_then_pauses_and_clears_retirement() {
        let mut store = store();
        let mut state = store
            .ensure_rule_state("owner", &definition("r1", true), 1)
            .unwrap();
        let starting_cursor = state.current_cursor;
        let first = insert_test_event(&mut store, "fixture", 2);
        assert_eq!(first, starting_cursor + 1);
        let deleted = insert_test_event(&mut store, "fixture", 3);
        let third = insert_test_event(&mut store, "fixture", 4);
        store
            .connection
            .execute("DELETE FROM events WHERE seq = ?1", [deleted])
            .unwrap();

        let below_limit = store
            .settle_rule_backlog("owner", &state, third, 2, 5)
            .unwrap();
        assert_eq!(below_limit.value.view.current_cursor, state.current_cursor);
        assert!(below_limit.value.view.paused_reason.is_none());

        let fourth = insert_test_event(&mut store, "fixture", 6);
        let over_limit = store
            .settle_rule_backlog("owner", &state, fourth, 2, 7)
            .unwrap();
        state = over_limit.value.view.clone();
        assert_eq!(state.current_cursor, fourth);
        assert_eq!(state.paused_reason.as_deref(), Some("backlog_exceeded"));
        assert!(state.retiring_revision.is_none());
        assert_eq!(over_limit.events.len(), 1);
        assert_eq!(over_limit.events[0].data["from_seq"], starting_cursor + 1);
        assert_eq!(over_limit.events[0].data["through_seq"], fourth);
    }

    #[test]
    fn verified_depth_suppresses_without_parent_child_or_event() {
        let mut store = store();
        let (agent, agent_id) = insert_test_agent(&mut store);
        let state = store
            .ensure_rule_state("owner", &definition("stop-rule", true), 1)
            .unwrap();
        let trigger_cause = core::EventCause {
            command_id: Ulid::new().to_string(),
            rule: Some("upstream".to_owned()),
            depth: 5,
        };
        let event_seq =
            insert_test_turn_completed_with_cause(&mut store, agent, 2, Some(trigger_cause));

        let suppressed = store
            .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), 3)
            .unwrap();
        assert!(suppressed.is_new);
        assert_eq!(suppressed.state.current_cursor, event_seq);
        assert!(suppressed.parent.is_none());
        assert!(suppressed.child.is_none());
        assert!(suppressed.events.is_empty());
        assert_eq!(store.high_water().unwrap(), event_seq);
    }

    #[test]
    fn rejected_cursor_suppresses_sixth_depth_and_rule_marker_quiesces() {
        let mut store = store();
        let (agent, _) = insert_test_agent(&mut store);
        let state_five = store
            .ensure_rule_state("owner", &definition("r5", true), 1)
            .unwrap();
        let cause_five = core::EventCause {
            command_id: Ulid::new().to_string(),
            rule: Some("upstream".to_owned()),
            depth: 5,
        };
        let event_five =
            insert_test_turn_completed_with_cause(&mut store, agent, 2, Some(cause_five));
        let events_before = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        let commands_before = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();

        let suppressed = store
            .advance_rule_cursor(
                "owner",
                &state_five,
                event_five,
                RuleCursorOutcome::Rejected {
                    code: "render_error".to_owned(),
                },
                3,
            )
            .unwrap();
        assert_eq!(suppressed.value.current_cursor, event_five);
        assert!(suppressed.events.is_empty());
        assert_eq!(store.high_water().unwrap(), event_five);
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            events_before
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            commands_before
        );

        let state_four = store
            .ensure_rule_state("owner", &definition("r4", true), 4)
            .unwrap();
        let cause_four = core::EventCause {
            command_id: Ulid::new().to_string(),
            rule: Some("upstream".to_owned()),
            depth: 4,
        };
        let event_four =
            insert_test_turn_completed_with_cause(&mut store, agent, 5, Some(cause_four.clone()));
        let rejected = store
            .advance_rule_cursor(
                "owner",
                &state_four,
                event_four,
                RuleCursorOutcome::Rejected {
                    code: "render_error".to_owned(),
                },
                6,
            )
            .unwrap();
        assert_eq!(rejected.value.current_cursor, event_four);
        assert_eq!(rejected.events.len(), 1);
        assert_eq!(rejected.events[0].kind, "rule.rejected");
        assert_eq!(rejected.events[0].cause, Some(cause_four));

        let marker_seq = rejected.events[0].seq;
        let marker = store
            .advance_rule_cursor(
                "owner",
                &rejected.value,
                marker_seq,
                RuleCursorOutcome::Rejected {
                    code: "render_error".to_owned(),
                },
                7,
            )
            .unwrap();
        assert_eq!(marker.value.current_cursor, marker_seq);
        assert!(marker.events.is_empty());
        assert_eq!(store.high_water().unwrap(), marker_seq);
    }

    #[test]
    fn global_rule_guard_counts_all_principals_with_custom_windows() {
        assert_eq!(GLOBAL_RULE_ACTION_LIMIT, 120);
        assert_eq!(RULE_ACTION_WINDOW_MS, 60_000);

        let mut store = store();
        let now = 100_000;
        let action_at = now - RULE_ACTION_WINDOW_MS / 2;
        let custom_window_ms = RULE_ACTION_WINDOW_MS / 4;
        let (agent, agent_id) = insert_test_agent(&mut store);
        assert!(action_at >= now - RULE_ACTION_WINDOW_MS);
        assert!(action_at < now - custom_window_ms);

        for index in 0..GLOBAL_RULE_ACTION_LIMIT {
            let (principal, rule) = match index % 4 {
                0 => ("owner", "stop-rule"),
                1 => ("project:alpha", "alpha-rule"),
                2 => ("owner", "other-rule"),
                _ => ("project:beta", "beta-rule"),
            };
            insert_test_rule_action(&mut store, principal, rule, &agent_id, 1, action_at);
        }

        let action_ids_before = {
            let mut statement = store
                .connection
                .prepare(
                    "SELECT id FROM commands
                     WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL
                     ORDER BY id",
                )
                .unwrap();
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(action_ids_before.len(), GLOBAL_RULE_ACTION_LIMIT as usize);

        let mut custom_definition = definition("stop-rule", true);
        let mut canonical_definition: Value =
            serde_json::from_str(&custom_definition.canonical_definition).unwrap();
        canonical_definition["limit"] = json!({
            "max": 121,
            "per": "rule",
            "window_ms": custom_window_ms,
        });
        custom_definition.canonical_definition =
            serde_json::to_string(&canonical_definition).unwrap();
        custom_definition.definition_digest =
            core::canonical_json_sha256(&canonical_definition).unwrap();

        let state = store
            .ensure_rule_state("owner", &custom_definition, now - 1)
            .unwrap();
        let event_seq = insert_test_turn_completed(&mut store, agent, now);
        let admission = store
            .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), now)
            .unwrap();

        assert!(admission.is_new);
        assert!(admission.child.is_none());
        let rejected_parent = admission.parent.as_ref().expect("rejected rule parent");
        assert_eq!(rejected_parent.state, CommandState::Rejected);
        assert_eq!(
            rejected_parent.outcome.as_ref().unwrap()["children"],
            json!([])
        );
        assert_eq!(
            rejected_parent.outcome.as_ref().unwrap()["code"],
            "rate_limited"
        );
        let rejected_parent_id = rejected_parent.id.clone();

        let replay = store
            .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), now)
            .unwrap();
        assert!(!replay.is_new);
        assert!(replay.child.is_none());
        assert_eq!(
            replay.parent.as_ref().expect("retained rule parent").id,
            rejected_parent_id
        );

        let action_ids_after = {
            let mut statement = store
                .connection
                .prepare(
                    "SELECT id FROM commands
                     WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL
                     ORDER BY id",
                )
                .unwrap();
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(action_ids_after, action_ids_before);
    }

    #[test]
    fn finite_history_catch_up_replays_before_current_gates() {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
        let (agent, agent_id) = insert_test_agent(&mut store);
        let event_seq = insert_test_turn_completed(&mut store, agent, now - 1);
        let state = store
            .ensure_rule_state("owner", &definition("history-rule", true), now)
            .unwrap();
        let captured_through = state.current_cursor;
        assert!(state.retiring_revision.is_none());
        assert!(event_seq <= captured_through);

        let after_seq = event_seq - 1;
        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            "history-rule",
            after_seq,
            Some(captured_through),
        );
        let actions = [RuleCatchUpAction::command(
            event_seq,
            stop_request(&agent_id),
        )];
        let first = store
            .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
            .unwrap();

        assert!(first.is_new);
        assert_eq!(first.parent.verb, "rule.catch_up");
        assert_eq!(first.parent.args["revision"].as_i64(), Some(state.revision));
        assert_eq!(first.children.len(), 1);
        let child = &first.children[0];
        assert_eq!(child.target.as_deref(), Some(agent_id.as_str()));
        assert_eq!(child.expected_generation, Some(1));
        assert_eq!(child.generation, Some(1));
        let child_id = child.id.clone();
        let parent_id = first.parent.id.clone();

        let disabled_state = store
            .ensure_rule_state("owner", &definition("history-rule", false), now + 1)
            .unwrap();
        assert!(!disabled_state.enabled);
        assert!(disabled_state.revision > state.revision);
        let high_water_after_edit = store.high_water().unwrap();

        // Reuse the old view and exact request after the live definition changed.
        let replay = store
            .admit_rule_catch_up(
                "owner",
                &request,
                &state,
                captured_through,
                &actions,
                now + 2,
            )
            .unwrap();
        assert!(!replay.is_new);
        assert_eq!(replay.parent.id, parent_id);
        assert_eq!(
            replay.parent.args["revision"].as_i64(),
            Some(state.revision)
        );
        assert_eq!(replay.children.len(), 1);
        assert_eq!(replay.children[0].id, child_id);
        assert!(replay.events.is_empty());
        assert_eq!(store.high_water().unwrap(), high_water_after_edit);

        let changed_request = core::api::RuleCatchUpRequest::new(
            request.id.clone(),
            "history-rule",
            after_seq,
            Some(captured_through + 1),
        );
        let conflict = store
            .admit_rule_catch_up(
                "owner",
                &changed_request,
                &disabled_state,
                captured_through + 1,
                &[],
                now + 3,
            )
            .unwrap_err();
        assert_eq!(conflict.code, "id_conflict");
        assert_eq!(store.high_water().unwrap(), high_water_after_edit);
    }

    #[test]
    fn finite_history_catch_up_unknown_child_blocks_new_work() {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
        let (agent, agent_id) = insert_test_agent(&mut store);
        let event_seq = insert_test_turn_completed(&mut store, agent, now - 1);
        let state = store
            .ensure_rule_state("owner", &definition("history-rule", true), now)
            .unwrap();
        let captured_through = state.current_cursor;
        assert!(event_seq <= captured_through);

        insert_test_rule_action(&mut store, "owner", "history-rule", &agent_id, 1, now);
        let unknown_child_id: String = store
            .connection
            .query_row(
                "SELECT id FROM commands
                 WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            store
                .connection
                .execute(
                    "UPDATE commands SET state = 'unknown' WHERE id = ?1",
                    [&unknown_child_id],
                )
                .unwrap(),
            1
        );

        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            "history-rule",
            event_seq - 1,
            Some(captured_through),
        );
        let actions = [RuleCatchUpAction::command(
            event_seq,
            stop_request(&agent_id),
        )];
        let commands_before: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_before: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let high_water_before = store.high_water().unwrap();

        let error = store
            .admit_rule_catch_up(
                "owner",
                &request,
                &state,
                captured_through,
                &actions,
                now + 1,
            )
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        let commands_after: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_after: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let unknown_state: String = store
            .connection
            .query_row(
                "SELECT state FROM commands WHERE id = ?1",
                [&unknown_child_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(commands_after, commands_before);
        assert_eq!(events_after, events_before);
        assert_eq!(store.high_water().unwrap(), high_water_before);
        assert_eq!(unknown_state, "unknown");
    }

    #[test]
    fn finite_history_catch_up_uses_authoritative_coverage_floor() {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
        let (agent, _) = insert_test_agent(&mut store);
        let first_seq = insert_test_turn_completed(&mut store, agent, now - 1);
        let missing_seq = insert_test_turn_completed(&mut store, agent, now - 1);
        let last_seq = insert_test_turn_completed(&mut store, agent, now - 1);
        let state = store
            .ensure_rule_state("owner", &definition("history-rule", true), now)
            .unwrap();
        let captured_through = state.current_cursor;
        assert!(last_seq <= captured_through);

        assert_eq!(
            store
                .connection
                .execute(
                    "DELETE FROM events WHERE seq IN (?1, ?2)",
                    params![first_seq, missing_seq],
                )
                .unwrap(),
            2
        );
        let coverage_floor: i64 = store
            .connection
            .query_row(
                "SELECT pruned_through FROM rule_history_coverage WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(coverage_floor, 0);

        let sparse_request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            "history-rule",
            0,
            Some(captured_through),
        );
        let sparse = store
            .admit_rule_catch_up("owner", &sparse_request, &state, captured_through, &[], now)
            .unwrap();
        assert!(sparse.is_new);
        assert!(sparse.children.is_empty());

        assert_eq!(
            store
                .connection
                .execute(
                    "UPDATE rule_history_coverage SET pruned_through = ?1 WHERE singleton = 1",
                    [missing_seq],
                )
                .unwrap(),
            1
        );
        let gap_id = Ulid::new();
        let gap_now = i64::try_from(gap_id.timestamp_ms()).unwrap() + 1;
        let gap_request = core::api::RuleCatchUpRequest::new(
            gap_id.to_string(),
            "history-rule",
            0,
            Some(captured_through),
        );
        let commands_before_gap: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_before_gap: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let high_water_before_gap = store.high_water().unwrap();

        let error = store
            .admit_rule_catch_up(
                "owner",
                &gap_request,
                &state,
                captured_through,
                &[],
                gap_now,
            )
            .unwrap_err();
        assert_eq!(error.code, "history_gap");
        let commands_after_gap: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_after_gap: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(commands_after_gap, commands_before_gap);
        assert_eq!(events_after_gap, events_before_gap);
        assert_eq!(store.high_water().unwrap(), high_water_before_gap);
    }

    #[test]
    fn finite_history_catch_up_oversized_result_rolls_back() {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
        let (agent, agent_id) = insert_test_agent(&mut store);
        let action_count = RULE_ACTION_LIMIT as usize + 1;
        let mut event_seqs = Vec::with_capacity(action_count);
        for _ in 0..action_count {
            event_seqs.push(insert_test_turn_completed(&mut store, agent, now - 1));
        }

        let rule_id = "r".repeat(48 * 1024);
        let rule_definition = definition(&rule_id, true);
        let state = store
            .ensure_rule_state("owner", &rule_definition, now)
            .unwrap();
        let captured_through = state.current_cursor;
        assert!(
            event_seqs
                .last()
                .is_some_and(|seq| *seq <= captured_through)
        );
        assert!(rule_id.len() + 1024 < MAX_EVENT_BYTES);

        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            rule_id.clone(),
            0,
            Some(captured_through),
        );
        let action_request = stop_request(&agent_id);
        let actions = event_seqs
            .iter()
            .map(|event_seq| RuleCatchUpAction::command(*event_seq, action_request.clone()))
            .collect::<Vec<_>>();
        let encoded_actions = serde_json::to_vec(&actions).unwrap();
        assert!(encoded_actions.len() <= MAX_PAGE_BYTES);
        // Each history marker repeats the rule in both data and cause.
        assert!(actions.len() * 2 * rule_id.len() > MAX_PAGE_BYTES);

        let commands_before: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_before: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let high_water_before = store.high_water().unwrap();

        let first_error = match store.admit_rule_catch_up(
            "owner",
            &request,
            &state,
            captured_through,
            &actions,
            now,
        ) {
            Err(error) => error,
            Ok(_) => panic!("oversized catch-up result was admitted"),
        };
        assert_eq!(first_error.code, "frame_too_large");
        assert!(first_error.message.contains("bounded response size"));
        let commands_after_first: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_after_first: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(commands_after_first, commands_before);
        assert_eq!(events_after_first, events_before);
        assert_eq!(store.high_water().unwrap(), high_water_before);

        let retry_error = match store.admit_rule_catch_up(
            "owner",
            &request,
            &state,
            captured_through,
            &actions,
            now + 1,
        ) {
            Err(error) => error,
            Ok(_) => panic!("oversized catch-up retry was admitted"),
        };
        assert_eq!(retry_error.code, "frame_too_large");
        assert!(retry_error.message.contains("bounded response size"));
        let commands_after_retry: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_after_retry: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(commands_after_retry, commands_before);
        assert_eq!(events_after_retry, events_before);
        assert_eq!(store.high_water().unwrap(), high_water_before);
    }

    #[test]
    fn finite_history_catch_up_allows_cursor_only_progress() {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
        let (agent, agent_id) = insert_test_agent(&mut store);
        let historical_event = insert_test_turn_completed(&mut store, agent, now - 1);
        let seeded_state = store
            .ensure_rule_state("owner", &definition("history-rule", true), now)
            .unwrap();
        let captured_through = seeded_state.current_cursor;
        assert!(seeded_state.retiring_revision.is_none());
        assert!(historical_event <= captured_through);

        let later_unmatched_event = insert_test_event(&mut store, "fixture.unmatched", now + 1);
        let advanced = store
            .advance_rule_cursor(
                "owner",
                &seeded_state,
                later_unmatched_event,
                RuleCursorOutcome::NoMatch,
                now + 2,
            )
            .unwrap();
        assert!(advanced.events.is_empty());
        assert_eq!(advanced.value.current_cursor, later_unmatched_event);
        assert!(advanced.value.current_cursor > seeded_state.current_cursor);
        assert_eq!(advanced.value.revision, seeded_state.revision);

        let after_seq = historical_event - 1;
        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            "history-rule",
            after_seq,
            Some(captured_through),
        );
        let actions = [RuleCatchUpAction::command(
            historical_event,
            stop_request(&agent_id),
        )];
        let admission = store
            .admit_rule_catch_up(
                "owner",
                &request,
                &seeded_state,
                captured_through,
                &actions,
                now + 3,
            )
            .unwrap();
        assert!(admission.is_new);
        assert_eq!(admission.parent.id, request.id);
        assert_eq!(admission.children.len(), 1);
        assert_eq!(
            admission.children[0].target.as_deref(),
            Some(agent_id.as_str())
        );
        assert_eq!(admission.children[0].expected_generation, Some(1));

        let current_state = store
            .get_rule_state("owner", "history-rule")
            .unwrap()
            .unwrap();
        assert_eq!(current_state.current_cursor, advanced.value.current_cursor);
        assert_eq!(current_state.revision, seeded_state.revision);

        let wrong_id = Ulid::new();
        let wrong_now = i64::try_from(wrong_id.timestamp_ms()).unwrap() + 1;
        let wrong_request = core::api::RuleCatchUpRequest::new(
            wrong_id.to_string(),
            "history-rule",
            after_seq,
            Some(captured_through),
        );
        let mut wrong_revision_state = current_state.clone();
        wrong_revision_state.revision += 1;
        let commands_before_wrong_revision: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_before_wrong_revision: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let high_water_before_wrong_revision = store.high_water().unwrap();

        let error = store
            .admit_rule_catch_up(
                "owner",
                &wrong_request,
                &wrong_revision_state,
                captured_through,
                &[],
                wrong_now,
            )
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        let commands_after_wrong_revision: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_after_wrong_revision: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            commands_after_wrong_revision,
            commands_before_wrong_revision
        );
        assert_eq!(events_after_wrong_revision, events_before_wrong_revision);
        assert_eq!(
            store.high_water().unwrap(),
            high_water_before_wrong_revision
        );
        assert_eq!(
            store
                .get_rule_state("owner", "history-rule")
                .unwrap()
                .unwrap(),
            current_state
        );
    }

    #[test]
    fn catch_up_children_wait_for_predecessor_before_recovery_or_dispatch() {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
        let (first_agent, first_target) = insert_test_agent(&mut store);
        let (second_agent, second_target) = insert_test_agent(&mut store);
        let first_event = insert_test_turn_completed(&mut store, first_agent, now - 1);
        let second_event = insert_test_turn_completed(&mut store, second_agent, now - 1);

        let mut rule_definition = definition("ordered-rule", true);
        let mut canonical_definition: Value =
            serde_json::from_str(&rule_definition.canonical_definition).unwrap();
        canonical_definition["limit"] = json!({
            "max": 2,
            "per": "rule",
            "window_ms": RULE_ACTION_WINDOW_MS,
        });
        rule_definition.canonical_definition =
            serde_json::to_string(&canonical_definition).unwrap();
        rule_definition.definition_digest =
            core::canonical_json_sha256(&canonical_definition).unwrap();
        let state = store
            .ensure_rule_state("owner", &rule_definition, now)
            .unwrap();
        let captured_through = state.current_cursor;
        assert!(second_event <= captured_through);

        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            "ordered-rule",
            first_event - 1,
            Some(captured_through),
        );
        let actions = [
            RuleCatchUpAction::command(first_event, stop_request(&first_target)),
            RuleCatchUpAction::command(second_event, stop_request(&second_target)),
        ];
        let admission = store
            .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
            .unwrap();
        assert!(admission.is_new);
        assert_eq!(admission.children.len(), 2);
        assert_ne!(admission.children[0].target, admission.children[1].target);

        let first_child_id = admission.children[0].id.clone();
        let second_child_id = admission.children[1].id.clone();
        let first_link: (String, i64) = store
            .connection
            .query_row(
                "SELECT parent_command, ordinal FROM commands WHERE id = ?1",
                [&first_child_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let second_link: (String, i64) = store
            .connection
            .query_row(
                "SELECT parent_command, ordinal FROM commands WHERE id = ?1",
                [&second_child_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(first_link, (admission.parent.id.clone(), 0));
        assert_eq!(second_link, (admission.parent.id.clone(), 1));

        for (step, predecessor_state) in ["accepted", "queued", "dispatched", "unknown"]
            .into_iter()
            .enumerate()
        {
            match predecessor_state {
                "accepted" => {}
                "queued" => {
                    assert_eq!(
                        store
                            .connection
                            .execute(
                                "UPDATE commands SET state = 'queued' WHERE id = ?1",
                                [&first_child_id],
                            )
                            .unwrap(),
                        1
                    );
                }
                "dispatched" => {
                    assert_eq!(
                        store
                            .dispatch(&first_child_id, now + 10 + step as i64)
                            .unwrap()
                            .value
                            .state,
                        CommandState::Dispatched
                    );
                }
                "unknown" => {
                    assert_eq!(
                        store
                            .settle(
                                &first_child_id,
                                CommandState::Unknown,
                                Some(json!({"reason": "fixture uncertainty"})),
                                now + 10 + step as i64,
                            )
                            .unwrap()
                            .value
                            .state,
                        CommandState::Unknown
                    );
                }
                _ => unreachable!(),
            }

            let expected_state = match predecessor_state {
                "accepted" => CommandState::Accepted,
                "queued" => CommandState::Queued,
                "dispatched" => CommandState::Dispatched,
                "unknown" => CommandState::Unknown,
                _ => unreachable!(),
            };
            assert_eq!(
                store.get_command(&first_child_id).unwrap().unwrap().state,
                expected_state
            );

            let recoverable = store.recover_rule_children(None, 100).unwrap();
            assert!(
                !recoverable
                    .iter()
                    .any(|command| command.id == second_child_id),
                "child 2 became recoverable while child 1 was {predecessor_state}"
            );
            let held = store
                .dispatch(&second_child_id, now + 20 + step as i64)
                .unwrap();
            assert_eq!(
                held.value.state,
                CommandState::Queued,
                "child 2 was not held while child 1 was {predecessor_state}"
            );
            assert!(
                !held
                    .events
                    .iter()
                    .any(|event| event.kind == "command.dispatched")
            );
            if predecessor_state == "accepted" {
                let queued = held
                    .events
                    .iter()
                    .find(|event| event.kind == "command.queued")
                    .expect("held child emits its queued transition");
                assert_eq!(queued.data["reason"], "rule_action_predecessor_pending");
            } else {
                assert!(held.events.is_empty());
            }
            assert_eq!(
                store.get_command(&second_child_id).unwrap().unwrap().state,
                CommandState::Queued
            );
            let child_two_dispatches: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM events
                     WHERE kind = 'command.dispatched'
                       AND json_extract(data, '$.command_id') = ?1",
                    [&second_child_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(child_two_dispatches, 0);
        }

        assert_eq!(
            store
                .settle(
                    &first_child_id,
                    CommandState::Applied,
                    Some(json!({"result": "stopped"})),
                    now + 30,
                )
                .unwrap()
                .value
                .state,
            CommandState::Applied
        );
        let recoverable = store.recover_rule_children(None, 100).unwrap();
        assert!(
            recoverable
                .iter()
                .any(|command| command.id == second_child_id)
        );
        assert_eq!(
            store
                .dispatch(&second_child_id, now + 31)
                .unwrap()
                .value
                .state,
            CommandState::Dispatched
        );
    }

    #[test]
    fn catch_up_terminal_predecessor_cancels_unstarted_sibling() {
        for terminal_predecessor in ["failed", "rejected", "cancelled"] {
            let mut store = store();
            let request_id = Ulid::new();
            let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
            let (first_agent, first_target) = insert_test_agent(&mut store);
            let (second_agent, second_target) = insert_test_agent(&mut store);
            let first_event = insert_test_turn_completed(&mut store, first_agent, now - 1);
            let second_event = insert_test_turn_completed(&mut store, second_agent, now - 1);

            let mut rule_definition = definition("ordered-rule", true);
            let mut canonical_definition: Value =
                serde_json::from_str(&rule_definition.canonical_definition).unwrap();
            canonical_definition["limit"] = json!({
                "max": 2,
                "per": "rule",
                "window_ms": RULE_ACTION_WINDOW_MS,
            });
            rule_definition.canonical_definition =
                serde_json::to_string(&canonical_definition).unwrap();
            rule_definition.definition_digest =
                core::canonical_json_sha256(&canonical_definition).unwrap();
            let state = store
                .ensure_rule_state("owner", &rule_definition, now)
                .unwrap();
            let captured_through = state.current_cursor;
            assert!(second_event <= captured_through);

            let request = core::api::RuleCatchUpRequest::new(
                request_id.to_string(),
                "ordered-rule",
                first_event - 1,
                Some(captured_through),
            );
            let actions = [
                RuleCatchUpAction::command(first_event, stop_request(&first_target)),
                RuleCatchUpAction::command(second_event, stop_request(&second_target)),
            ];
            let admission = store
                .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
                .unwrap();
            assert_eq!(admission.children.len(), 2);
            let first_child_id = admission.children[0].id.clone();
            let second_child_id = admission.children[1].id.clone();

            let expected_predecessor_state = match terminal_predecessor {
                "failed" => {
                    store.dispatch(&first_child_id, now + 1).unwrap();
                    store
                        .settle(
                            &first_child_id,
                            CommandState::Failed,
                            Some(json!({"reason": "fixture failure"})),
                            now + 2,
                        )
                        .unwrap();
                    CommandState::Failed
                }
                "rejected" => {
                    store
                        .settle(
                            &first_child_id,
                            CommandState::Rejected,
                            Some(json!({"reason": "fixture rejection"})),
                            now + 1,
                        )
                        .unwrap();
                    CommandState::Rejected
                }
                "cancelled" => {
                    store.mark_lost(&first_target, 1, now + 1).unwrap();
                    CommandState::Cancelled
                }
                _ => unreachable!(),
            };
            assert_eq!(
                store.get_command(&first_child_id).unwrap().unwrap().state,
                expected_predecessor_state
            );

            let recoverable = store.recover_rule_children(None, 100).unwrap();
            assert!(
                recoverable
                    .iter()
                    .any(|command| command.id == second_child_id),
                "Store recovery must surface the terminal follower for cancellation"
            );
            let cancelled = store.dispatch(&second_child_id, now + 3).unwrap();
            assert_eq!(cancelled.value.state, CommandState::Cancelled);
            let cancelled_event = cancelled
                .events
                .iter()
                .find(|event| event.kind == "command.cancelled")
                .expect("Store gate cancels the unstarted follower");
            assert_eq!(
                cancelled_event.data["reason"],
                "rule_action_predecessor_terminal"
            );
            let recoverable_after_cancel = store.recover_rule_children(None, 100).unwrap();
            assert!(
                !recoverable_after_cancel
                    .iter()
                    .any(|command| command.id == second_child_id)
            );
            assert_eq!(
                store.get_command(&second_child_id).unwrap().unwrap().state,
                CommandState::Cancelled,
                "child 2 was not cancelled after child 1 became {terminal_predecessor}"
            );
            let child_two_dispatches: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM events
                     WHERE kind = 'command.dispatched'
                       AND json_extract(data, '$.command_id') = ?1",
                    [&second_child_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(child_two_dispatches, 0);
        }
    }

    #[test]
    fn send_self_suppression_uses_verified_principal_and_rule_identity() {
        let mut store = store();
        let now = 20_000;
        let (agent, agent_id) = insert_test_agent(&mut store);
        ensure_test_project_action_scope(&mut store, "project:alpha", agent, 1, now);

        let owner_value = json!({"id": "same-rule", "enabled": true, "allow_self": false});
        let owner_definition = RuleDefinitionInput {
            id: "same-rule".to_owned(),
            canonical_definition: serde_json::to_string(&owner_value).unwrap(),
            definition_digest: core::canonical_json_sha256(&owner_value).unwrap(),
            enabled: true,
        };
        let project_value = json!({
            "id": "same-rule",
            "enabled": true,
            "allow_self": false,
            "do": [{"action": "stop"}],
        });
        let project_definition = RuleDefinitionInput {
            id: "same-rule".to_owned(),
            canonical_definition: serde_json::to_string(&project_value).unwrap(),
            definition_digest: core::canonical_json_sha256(&project_value).unwrap(),
            enabled: true,
        };
        let owner_state = store
            .ensure_rule_state("owner", &owner_definition, now)
            .unwrap();
        let project_state = store
            .ensure_rule_state("project:alpha", &project_definition, now)
            .unwrap();

        let project_event = insert_test_turn_completed(&mut store, agent, now + 1);
        let project_source = store
            .admit_rule_action(
                "project:alpha",
                &project_state,
                project_event,
                stop_request(&agent_id),
                now + 2,
            )
            .unwrap();
        let project_child = project_source.child.as_ref().expect("project rule child");
        let project_cause = command_cause_tx(&store.connection, &project_child.id)
            .unwrap()
            .expect("Store-verified project rule cause");
        assert_eq!(project_cause.rule.as_deref(), Some("same-rule"));
        assert_eq!(project_cause.depth, 1);
        let project_origin: (String, String) = store
            .connection
            .query_row(
                "SELECT parent.principal, json_extract(parent.args, '$.rule')
                 FROM commands child JOIN commands parent ON parent.id = child.parent_command
                 WHERE child.id = ?1",
                [&project_child.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            project_origin,
            ("project:alpha".to_owned(), "same-rule".to_owned())
        );
        store
            .settle(
                &project_child.id,
                CommandState::Rejected,
                Some(json!({"reason": "source fixture complete"})),
                now + 3,
            )
            .unwrap();

        let cross_namespace_event =
            insert_test_turn_completed_with_cause(&mut store, agent, now + 4, Some(project_cause));
        let mut cross_namespace_send = SubmitCommand::new(Ulid::new().to_string(), "send");
        cross_namespace_send.target = Some(agent_id.clone());
        cross_namespace_send.expected_generation = Some(1);
        cross_namespace_send.args = json!({"text": "same rule ID from another namespace"});
        let cross_namespace = store
            .admit_rule_action(
                "owner",
                &owner_state,
                cross_namespace_event,
                cross_namespace_send,
                now + 5,
            )
            .unwrap();
        let foreign_send = cross_namespace
            .child
            .as_ref()
            .expect("different principal is not a self send");
        assert_eq!(foreign_send.verb, "send");
        assert_eq!(
            foreign_send.args["text"],
            "same rule ID from another namespace"
        );
        store
            .settle(
                &foreign_send.id,
                CommandState::Rejected,
                Some(json!({"reason": "close namespace fixture"})),
                now + 6,
            )
            .unwrap();

        let owner_source_event = insert_test_turn_completed(&mut store, agent, now + 7);
        let owner_source = store
            .admit_rule_action(
                "owner",
                &cross_namespace.state,
                owner_source_event,
                stop_request(&agent_id),
                now + 8,
            )
            .unwrap();
        let owner_child = owner_source.child.as_ref().expect("owner rule child");
        let owner_cause = command_cause_tx(&store.connection, &owner_child.id)
            .unwrap()
            .expect("Store-verified owner rule cause");
        let owner_origin: (String, String) = store
            .connection
            .query_row(
                "SELECT parent.principal, json_extract(parent.args, '$.rule')
                 FROM commands child JOIN commands parent ON parent.id = child.parent_command
                 WHERE child.id = ?1",
                [&owner_child.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(owner_origin, ("owner".to_owned(), "same-rule".to_owned()));
        store
            .settle(
                &owner_child.id,
                CommandState::Rejected,
                Some(json!({"reason": "verified origin fixture"})),
                now + 9,
            )
            .unwrap();

        let own_event =
            insert_test_turn_completed_with_cause(&mut store, agent, now + 10, Some(owner_cause));
        let mut own_send = SubmitCommand::new(Ulid::new().to_string(), "send");
        own_send.target = Some(agent_id);
        own_send.expected_generation = Some(1);
        own_send.args = json!({"text": "verified self send"});
        let commands_before_suppression: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let high_water_before_suppression = store.high_water().unwrap();
        let suppressed = store
            .admit_rule_action("owner", &owner_source.state, own_event, own_send, now + 11)
            .unwrap();
        assert!(suppressed.is_new);
        assert!(suppressed.parent.is_none());
        assert!(suppressed.child.is_none());
        assert!(suppressed.events.is_empty());
        assert_eq!(suppressed.state.current_cursor, own_event);
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            commands_before_suppression
        );
        assert_eq!(store.high_water().unwrap(), high_water_before_suppression);
    }

    #[test]
    fn allow_self_send_permits_cause_depth_through_five_only() {
        let mut store = store();
        let (agent, agent_id) = insert_test_agent(&mut store);
        let definition_value = json!({
            "id": "bounded-send",
            "enabled": true,
            "allow_self": true,
        });
        let rule_definition = RuleDefinitionInput {
            id: "bounded-send".to_owned(),
            canonical_definition: serde_json::to_string(&definition_value).unwrap(),
            definition_digest: core::canonical_json_sha256(&definition_value).unwrap(),
            enabled: true,
        };
        let mut state = store
            .ensure_rule_state("owner", &rule_definition, 10_000)
            .unwrap();
        let mut cause = None;
        let mut now = 10_001;

        for expected_depth in 1_u8..=4 {
            let event_seq =
                insert_test_turn_completed_with_cause(&mut store, agent, now, cause.clone());
            let admission = store
                .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), now + 1)
                .unwrap();
            let child = admission.child.expect("caused stop child");
            let child_cause = command_cause_tx(&store.connection, &child.id)
                .unwrap()
                .expect("verified Store cause");
            assert_eq!(child_cause.rule.as_deref(), Some("bounded-send"));
            assert_eq!(child_cause.depth, expected_depth);
            store
                .settle(
                    &child.id,
                    CommandState::Rejected,
                    Some(json!({"reason": "close bounded cause fixture"})),
                    now + 2,
                )
                .unwrap();
            state = admission.state;
            cause = Some(child_cause);
            now += 3;
        }

        let send_event = insert_test_turn_completed_with_cause(&mut store, agent, now, cause);
        let mut send = SubmitCommand::new(Ulid::new().to_string(), "send");
        send.target = Some(agent_id.clone());
        send.expected_generation = Some(1);
        send.args = json!({"text": "bounded self send"});
        let admission = store
            .admit_rule_action("owner", &state, send_event, send, now + 1)
            .unwrap();
        let send_child = admission
            .child
            .as_ref()
            .expect("allow_self sends at depth five");
        assert_eq!(send_child.verb, "send");
        assert_eq!(send_child.args["text"], "bounded self send");
        let send_cause = command_cause_tx(&store.connection, &send_child.id)
            .unwrap()
            .expect("verified depth-five send cause");
        assert_eq!(send_cause.rule.as_deref(), Some("bounded-send"));
        assert_eq!(send_cause.depth, 5);
        store
            .settle(
                &send_child.id,
                CommandState::Rejected,
                Some(json!({"reason": "close bounded send fixture"})),
                now + 2,
            )
            .unwrap();

        let sixth_event =
            insert_test_turn_completed_with_cause(&mut store, agent, now + 3, Some(send_cause));
        let commands_before_suppression: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let bounded = store
            .admit_rule_action(
                "owner",
                &admission.state,
                sixth_event,
                SubmitCommand {
                    id: Ulid::new().to_string(),
                    target: Some(agent_id),
                    verb: "send".to_owned(),
                    args: json!({"text": "over depth bound"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now + 4,
            )
            .unwrap();
        assert!(bounded.is_new);
        assert!(bounded.parent.is_none());
        assert!(bounded.child.is_none());
        assert!(bounded.events.is_empty());
        assert_eq!(bounded.state.current_cursor, sixth_event);
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            commands_before_suppression
        );
    }

    #[test]
    fn unknown_send_is_not_replayed_by_live_history_or_recovery() {
        let mut store = store();
        let now = 30_000;
        let (live_agent, live_agent_id) = insert_test_agent(&mut store);
        let (history_agent, history_agent_id) = insert_test_agent(&mut store);
        let live_definition = definition("live-unknown-send", true);
        let live_state = store
            .ensure_rule_state("owner", &live_definition, now)
            .unwrap();
        let live_event = insert_test_turn_completed(&mut store, live_agent, now + 1);
        let mut live_send = SubmitCommand::new(Ulid::new().to_string(), "send");
        live_send.target = Some(live_agent_id.clone());
        live_send.expected_generation = Some(1);
        live_send.args = json!({"text": "live uncertain send"});
        let live_admission = store
            .admit_rule_action("owner", &live_state, live_event, live_send.clone(), now + 2)
            .unwrap();
        let live_child = live_admission.child.as_ref().expect("live send child");
        let live_child_id = live_child.id.clone();
        assert_eq!(live_child.verb, "send");
        store.dispatch(&live_child_id, now + 3).unwrap();
        store
            .settle(
                &live_child_id,
                CommandState::Unknown,
                Some(json!({"reason": "fixture uncertain send"})),
                now + 4,
            )
            .unwrap();
        assert_eq!(
            store.get_command(&live_child_id).unwrap().unwrap().state,
            CommandState::Unknown
        );
        let live_recovery = store.recover_rule_children(None, 100).unwrap();
        assert!(
            !live_recovery
                .iter()
                .any(|command| command.id == live_child_id)
        );

        let commands_before_live_replay: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_before_live_replay = store.high_water().unwrap();
        let live_replay = store
            .admit_rule_action("owner", &live_state, live_event, live_send, now + 5)
            .unwrap();
        assert!(!live_replay.is_new);
        assert_eq!(
            live_replay.parent.unwrap().id,
            live_admission.parent.unwrap().id
        );
        assert_eq!(live_replay.child.unwrap().id, live_child_id);
        assert!(live_replay.events.is_empty());
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            commands_before_live_replay
        );
        assert_eq!(store.high_water().unwrap(), events_before_live_replay);

        let history_event = insert_test_turn_completed(&mut store, history_agent, now + 6);
        let history_state = store
            .ensure_rule_state("owner", &definition("history-unknown-send", true), now + 7)
            .unwrap();
        let history_id = Ulid::new();
        let history_now = i64::try_from(history_id.timestamp_ms()).unwrap() + 1;
        let history_request = core::api::RuleCatchUpRequest::new(
            history_id.to_string(),
            "history-unknown-send",
            history_event - 1,
            Some(history_state.current_cursor),
        );
        let mut history_send = SubmitCommand::new(Ulid::new().to_string(), "send");
        history_send.target = Some(history_agent_id);
        history_send.expected_generation = Some(1);
        history_send.args = json!({"text": "history uncertain send"});
        let history_actions = [RuleCatchUpAction::command(history_event, history_send)];
        let history_admission = store
            .admit_rule_catch_up(
                "owner",
                &history_request,
                &history_state,
                history_state.current_cursor,
                &history_actions,
                history_now,
            )
            .unwrap();
        assert_eq!(history_admission.children.len(), 1);
        let history_child_id = history_admission.children[0].id.clone();
        store.dispatch(&history_child_id, history_now + 1).unwrap();
        store
            .settle(
                &history_child_id,
                CommandState::Unknown,
                Some(json!({"reason": "fixture uncertain history send"})),
                history_now + 2,
            )
            .unwrap();
        let commands_before_history_replay: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        let events_before_history_replay = store.high_water().unwrap();
        let history_replay = store
            .replay_rule_catch_up("owner", &history_request)
            .unwrap()
            .expect("retained catch-up parent");
        assert!(!history_replay.is_new);
        assert_eq!(history_replay.parent.id, history_request.id);
        assert_eq!(history_replay.children.len(), 1);
        assert_eq!(history_replay.children[0].id, history_child_id);
        assert_eq!(history_replay.children[0].state, CommandState::Unknown);
        assert!(history_replay.events.is_empty());

        let recovered = store.recover_rule_children(None, 100).unwrap();
        assert!(
            !recovered
                .iter()
                .any(|command| { command.id == live_child_id || command.id == history_child_id })
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            commands_before_history_replay
        );
        assert_eq!(store.high_water().unwrap(), events_before_history_replay);
    }

    #[test]
    fn per_agent_limit_is_scoped_by_principal_and_rule_across_generations() {
        let mut store = store();
        let now = 10_000;
        let (alpha_agent_num, alpha_agent) = insert_test_agent(&mut store);
        let (_, beta_agent) = insert_test_agent(&mut store);
        let owner_rule = store
            .ensure_rule_state("owner", &definition("same-rule", true), now - 3)
            .unwrap();
        let owner_event = insert_test_turn_completed(&mut store, alpha_agent_num, now - 2);
        store
            .admit_rule_action(
                "owner",
                &owner_rule,
                owner_event,
                stop_request(&alpha_agent),
                now - 1,
            )
            .unwrap();
        insert_test_rule_action(
            &mut store,
            "project:alpha",
            "same-rule",
            &alpha_agent,
            1,
            now - 3,
        );
        store
            .connection
            .execute(
                "UPDATE agents SET generation = 2 WHERE id = ?1",
                [alpha_agent_num],
            )
            .unwrap();
        insert_test_rule_action(
            &mut store,
            "project:alpha",
            "same-rule",
            &alpha_agent,
            2,
            now - 2,
        );
        insert_test_rule_action(
            &mut store,
            "project:alpha",
            "other-rule",
            &alpha_agent,
            2,
            now - 1,
        );
        insert_test_rule_action(&mut store, "project:beta", "same-rule", &beta_agent, 1, now);

        let limit = core::api::RuleLimit {
            max: 2,
            per: core::api::RuleLimitScope::Agent,
            window_ms: RULE_ACTION_WINDOW_MS,
        };
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .unwrap();
        let (same_rule, global) = rule_action_counts(
            &tx,
            "project:alpha",
            "same-rule",
            Some(&alpha_agent),
            &limit,
            now,
        )
        .unwrap();
        let (other_rule, _) = rule_action_counts(
            &tx,
            "project:alpha",
            "other-rule",
            Some(&alpha_agent),
            &limit,
            now,
        )
        .unwrap();
        let (other_project, _) = rule_action_counts(
            &tx,
            "project:beta",
            "same-rule",
            Some(&beta_agent),
            &limit,
            now,
        )
        .unwrap();
        tx.rollback().unwrap();

        assert_eq!(same_rule, 2);
        assert!(rule_limit_reached(same_rule, limit.max));
        assert_eq!(other_rule, 1);
        assert!(!rule_limit_reached(other_rule, limit.max));
        assert_eq!(other_project, 1);
        assert!(!rule_limit_reached(other_project, limit.max));
        assert_eq!(global, 5);
    }

    #[test]
    fn malformed_legacy_command_cause_is_unbound_not_a_read_failure() {
        let mut store = store();
        let (agent, agent_id) = insert_test_agent(&mut store);
        let state = store
            .ensure_rule_state("owner", &definition("stop-rule", true), 1)
            .unwrap();
        let event_seq = insert_test_turn_completed(&mut store, agent, 2);
        let admission = store
            .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), 3)
            .unwrap();
        let child = admission.child.unwrap();
        store
            .connection
            .execute(
                "UPDATE commands SET cause = 'legacy-untyped-value' WHERE id = ?1",
                [&child.id],
            )
            .unwrap();

        assert_eq!(
            command_cause_tx(&store.connection, &child.id).unwrap(),
            None
        );
        assert!(
            store
                .get_rule_state("owner", "stop-rule")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .list_rule_processing_states("owner", None, 10)
                .unwrap()
                .len(),
            1
        );
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
                .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), now)
                .unwrap();
            assert!(admission.is_new);
            let child = admission.child.as_ref().expect("admitted stop child");
            assert_eq!(child.verb, "stop");
            let parent = admission.parent.as_ref().expect("admitted rule parent");
            assert_eq!(parent.state, CommandState::Applied);
            assert_eq!(parent.outcome.as_ref().unwrap()["children"][0], child.id);
            let child_event = admission
                .events
                .iter()
                .find(|event| {
                    event.kind == "command.accepted"
                        && event.data["command_id"].as_str() == Some(child.id.as_str())
                })
                .expect("child accepted event");
            assert_eq!(
                child_event.cause,
                Some(core::EventCause {
                    command_id: child.id.clone(),
                    rule: Some("stop-rule".to_owned()),
                    depth: 1,
                })
            );
            assert_eq!(
                command_cause_tx(&store.connection, &child.id)
                    .unwrap()
                    .as_ref(),
                child_event.cause.as_ref()
            );
            let child_event_cause: Option<String> = store
                .connection
                .query_row(
                    "SELECT cause FROM events WHERE seq = ?1",
                    [child_event.seq],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                decode_event_cause(child_event_cause.as_deref(), Some(&child.id)).as_ref(),
                child_event.cause.as_ref()
            );
            let parent_event = admission
                .events
                .iter()
                .find(|event| event.data["command_id"].as_str() == Some(parent.id.as_str()))
                .expect("parent command event");
            assert_eq!(
                parent_event.cause,
                Some(core::EventCause {
                    command_id: parent.id.clone(),
                    rule: Some("stop-rule".to_owned()),
                    depth: 1,
                })
            );
            assert_eq!(
                command_cause_tx(&store.connection, &parent.id)
                    .unwrap()
                    .as_ref(),
                parent_event.cause.as_ref()
            );
            let fired = admission
                .events
                .iter()
                .find(|event| event.kind == "rule.fired")
                .expect("committed rule firing event");
            assert_eq!(fired.data["parent_command"], parent.id);
            assert_eq!(fired.data["child_command"], child.id);
            assert_eq!(fired.cause.as_ref(), parent_event.cause.as_ref());
            state = admission.state;
        }

        let rate_event_seq = insert_test_turn_completed(&mut store, agent_num, now);
        let limited = store
            .admit_rule_action(
                "owner",
                &state,
                rate_event_seq,
                stop_request(&agent_id),
                now,
            )
            .unwrap();
        assert!(limited.is_new);
        assert!(limited.child.is_none());
        let rejected_parent = limited.parent.as_ref().expect("rejected rule parent");
        assert_eq!(rejected_parent.state, CommandState::Rejected);
        assert_eq!(
            rejected_parent.outcome.as_ref().unwrap()["children"],
            json!([])
        );
        assert_eq!(
            rejected_parent.outcome.as_ref().unwrap()["code"],
            "rate_limited"
        );
        let rejected = limited
            .events
            .iter()
            .find(|event| event.kind == "rule.rejected")
            .expect("committed rejection event");
        assert_eq!(
            rejected
                .cause
                .as_ref()
                .map(|cause| cause.command_id.as_str()),
            Some(rejected_parent.id.as_str())
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

    #[test]
    fn faulted_writer_holds_rule_child_recovery_without_mutating_commands() {
        let mut store = store();
        let (_, agent_id) = insert_test_agent(&mut store);
        insert_test_rule_action(&mut store, "owner", "faulted-recovery", &agent_id, 1, 100);
        insert_test_rule_action(&mut store, "owner", "faulted-recovery", &agent_id, 1, 101);

        let seeded = store.recover_rule_children(None, 10).unwrap();
        assert_eq!(seeded.len(), 2);
        assert!(
            seeded
                .iter()
                .all(|command| command.state == CommandState::Accepted)
        );
        store
            .connection
            .execute(
                // Seed both states accepted by recovery without building a predecessor chain.
                "UPDATE commands SET state = 'queued' WHERE id = ?1",
                [&seeded[0].id],
            )
            .unwrap();

        let healthy = store.recover_rule_children(None, 10).unwrap();
        assert_eq!(healthy.len(), 2);
        assert!(
            healthy
                .iter()
                .any(|command| command.state == CommandState::Accepted)
        );
        assert!(
            healthy
                .iter()
                .any(|command| command.state == CommandState::Queued)
        );

        store.write_faulted.set(true);
        assert!(store.recover_rule_children(None, 10).unwrap().is_empty());
        assert!(store.is_write_faulted());

        let accepted = healthy
            .iter()
            .find(|command| command.state == CommandState::Accepted)
            .unwrap();
        let error = store.dispatch(&accepted.id, 102).unwrap_err();
        assert_eq!(error.code, "unavailable");
        for command in &healthy {
            let retained = store.get_command(&command.id).unwrap().unwrap();
            assert_eq!(retained.state, command.state);
        }
        let dispatched: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dispatched, 0);
    }
}
