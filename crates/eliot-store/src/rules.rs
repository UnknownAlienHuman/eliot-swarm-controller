//! Durable cursor and command admission for the first event-rule slice.
//!
//! The daemon owns rule parsing and matching. Store owns the persisted definition,
//! event cursor, action accounting, and the parent/child admission transaction.

use super::*;

#[path = "rule_history.rs"]
mod rule_history;
#[path = "rule_timers.rs"]
mod rule_timers;

pub(super) use rule_history::gate_rule_action_dispatch_tx;
pub use rule_history::{RuleCatchUpAction, RuleCatchUpActionOutcome, RuleCatchUpAdmission};
pub use rule_timers::{TimerDefinitionInput, TimerSlotAdmission, TimerSlotCommit};

const RULE_ACTION_WINDOW_MS: i64 = 60_000;
const RULE_ACTION_LIMIT: i64 = 10;
const GLOBAL_RULE_ACTION_LIMIT: i64 = 120;
const RULE_COMMAND_ID_DOMAIN: &str = "eliot-rule-command-v1";

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
    /// A configured-capacity preparation refusal retains bounded Owner remedy
    /// text for automation readback; all other errors use `Rejected`.
    RejectedWithCapacity {
        code: String,
        message: String,
        failure_where: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuleEventAdmission {
    pub state: core::api::RuleStateView,
    /// Absent only for a cause-depth-suppressed event, which advances the
    /// cursor without creating a parent or action command.
    pub parent: Option<Command>,
    pub child: Option<Admission>,
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
    pub source_present: bool,
    pub current_definition: String,
    pub retiring_definition: Option<String>,
}

#[derive(Clone, Debug)]
struct StoredRuleState {
    view: core::api::RuleStateView,
    source_present: bool,
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
    script_context: Option<core::api::FrozenScriptContext>,
    run_rate_limit: Option<core::api::RuleLimit>,
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
            script_context: None,
            run_rate_limit: None,
            parent_digest: None,
            parent_id: derived_rule_id("parent", principal, &state.id, revision, event_seq)?,
            child_id: derived_rule_id("action-0", principal, &state.id, revision, event_seq)?,
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

    fn rejected_data(&self, rejection: &Error) -> Value {
        let mut data = self.args.clone();
        let object = data.as_object_mut().expect("firing args are an object");
        object.insert("parent_command".to_owned(), json!(self.parent_id));
        object.insert("code".to_owned(), json!(rejection.code));
        if let Some((message, where_)) = capacity_metadata(rejection) {
            object.insert("message".to_owned(), json!(message));
            object.insert("where".to_owned(), json!(where_));
        }
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
            remote_call: self
                .trigger_cause
                .as_ref()
                .and_then(|cause| cause.remote_call.clone()),
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

enum RuleParentOutcome<'a> {
    Admitted,
    Rejected { error: &'a Error },
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
            let view =
                ensure_rule_state_tx(&tx, principal, input, true, high_water, now, &mut events)?;
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
        absent_from_source: &[&str],
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
            let absent: HashSet<_> = absent_from_source.iter().copied().collect();
            if !absent.is_subset(&definition_ids) {
                return Err(Error::new(
                    "invalid",
                    "retiring rule IDs must belong to the reconciled definitions",
                ));
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
                ensure_rule_state_tx(
                    &tx,
                    principal,
                    definition,
                    !absent.contains(definition.id.as_str()),
                    high_water,
                    now,
                    &mut events,
                )?;
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
                    rule_timers::ensure_timer_state_tx(&tx, principal, timer, now)?;
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
                    events.extend(rule_rejection_event(
                        &tx,
                        &state.id,
                        revision,
                        cursor + 1..=through,
                        &code,
                        None,
                        now,
                    )?);
                }
                RuleCursorOutcome::RejectedWithCapacity {
                    code,
                    message,
                    failure_where,
                } => {
                    if code != "capacity_unavailable" || failure_where != "store.admission.capacity"
                    {
                        return Err(Error::new(
                            "invalid",
                            "rule capacity rejection metadata is invalid",
                        ));
                    }
                    events.extend(rule_rejection_event(
                        &tx,
                        &state.id,
                        revision,
                        cursor + 1..=through,
                        &code,
                        Some((&message, &failure_where)),
                        now,
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

    /// Resolve an already committed event firing before the caller prepares or
    /// freezes the action. The captured definition revision is part of its key.
    pub fn replay_rule_event_action(
        &self,
        principal: &str,
        state: &core::api::RuleStateView,
        revision: i64,
        event_seq: i64,
    ) -> Result<Option<RuleEventAdmission>> {
        require_nonempty(principal, "principal")?;
        require_nonempty(&state.id, "rule id")?;
        if revision <= 0 || event_seq <= 0 {
            return Err(Error::new(
                "invalid",
                "rule replay revision and event sequence must be positive",
            ));
        }
        let stored = load_rule_state(&self.connection, principal, &state.id)?
            .ok_or_else(|| Error::new("not_found", "rule state was not found"))?;
        load_retained_rule_event_action(&self.connection, principal, &stored, revision, event_seq)
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
        if let Some(replay) =
            load_retained_rule_event_action(&tx, principal, &stored, revision, event_seq)?
        {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(replay);
        }
        let trigger = load_rule_trigger(&tx, event_seq)?;
        let mut key = RuleFiringKey::event(
            principal,
            &stored.view,
            revision,
            event_seq,
            trigger.cause.clone(),
        )?;

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
            let error = Error::new(code.clone(), code);
            return reject_rule_event(tx, &self.write_faulted, &key, &error, now);
        }
        let canonical_definition = if retiring {
            stored
                .retiring_definition
                .as_deref()
                .ok_or_else(|| Error::new("source_integrity", "retiring definition is absent"))?
        } else {
            &stored.canonical_definition
        };
        if should_suppress_own_event_send_or_run_tx(
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
                return reject_rule_event(tx, &self.write_faulted, &key, &error, now);
            }
            return Err(error);
        }
        request.id = key.child_id.clone();

        // Rate-limit a retiring event under its own captured revision, not the
        // edited current definition that replaced it (see the retirement
        // handoff in docs/spec/automation.md). `canonical_definition` above
        // already selected the retiring definition for this event.
        let limit = rule_action_limit_for_definition(canonical_definition)?;
        let (scoped_count, global_count) = rule_action_counts(
            &tx,
            principal,
            &state.id,
            request.target.as_deref(),
            &limit,
            now,
        )?;
        if rule_limit_reached(scoped_count, limit.max) || global_count >= GLOBAL_RULE_ACTION_LIMIT {
            let error = Error::new("rate_limited", "rule action rate limit reached");
            return reject_rule_event(tx, &self.write_faulted, &key, &error, now);
        }

        if request.verb == "run" {
            // Freeze the admission rate policy in the committed rule parent:
            // script output validates against this saved policy plus current
            // authority, without requiring the definition revision to remain
            // retained.
            key.run_rate_limit = Some(limit);
        }

        // submit_tx validates and admits the ordinary rule action. Its verb path
        // performs the normal deterministic checks before inserting the child;
        // the surrounding transaction rolls every write back on persistence failure.
        let limits = self.hierarchy_limits;
        let policy = self.capacity_policy.as_deref();
        let (parent, child_admission, events) =
            match admit_rule_action_tx(&tx, &key, request, now, limits, policy) {
                Ok(admission) => admission,
                Err(error) if core::is_deterministic_pre_admission_rejection(&error.code) => {
                    return reject_rule_event(tx, &self.write_faulted, &key, &error, now);
                }
                Err(error) => return Err(error),
            };
        let view = update_rule_cursor_tx(&tx, key.principal, &stored.view, event_seq, now)?;
        commit_transaction(tx, &self.write_faulted)?;

        Ok(RuleEventAdmission {
            state: view,
            parent: Some(parent),
            child: Some(child_admission),
            events,
            is_new: true,
        })
    }

    pub fn recover_rule_children(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Admission>> {
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
                        created_at, updated_at, probe, selector, config_revision
                 FROM commands
                 WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL
                   AND (
                       state IN ('accepted','queued')
                       OR (verb IN ('launch','resume') AND state = 'applied'
                           AND EXISTS (
                               SELECT 1 FROM commands input
                               WHERE input.parent_command = commands.id
                                 AND input.link_kind = 'send' AND input.ordinal = 0
                                 AND input.state IN ('accepted','queued')
                           ))
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM commands earlier
                       WHERE earlier.parent_command = commands.parent_command
                         AND earlier.link_kind = 'rule_action'
                         AND earlier.ordinal < commands.ordinal
                         AND (
                             earlier.state IN ('accepted','queued','dispatched','unknown')
                             OR (earlier.verb IN ('launch','resume')
                                 AND earlier.state = 'applied'
                                 AND EXISTS (
                                     SELECT 1 FROM commands input
                                     WHERE input.parent_command = earlier.id
                                       AND input.link_kind = 'send' AND input.ordinal = 0
                                       AND input.state IN ('accepted','queued','dispatched','unknown')
                                 ))
                         )
                   )
                   AND (?1 IS NULL OR id > ?1)
                 ORDER BY id LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after, limit as i64], command_record_from_row)
            .map_err(sql_error)?;
        let mut admissions = Vec::new();
        for row in rows {
            let command = command_from_raw(row.map_err(sql_error)?)?.command;
            admissions.push(retained_action_admission(&self.connection, command)?);
        }
        trim_recovery_admission_page(&mut admissions)?;
        Ok(admissions)
    }
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
    if request.verb == "launch" {
        return super::projects::validate_project_rule_launch_action_tx(
            tx,
            principal,
            &request.args,
        );
    }
    if request.verb == "run" {
        if rule_project_id(principal)?.is_none() {
            return Ok(None);
        }
        let payload_digest = [0_u8; 32];
        let command = CommandInsert::targetless_admitted(principal, request, &payload_digest, 0);
        super::projects::capture_project_command_binding_tx(tx, &command)?;
        return Ok(None);
    }
    if matches!(request.verb.as_str(), "notify" | "emit") {
        if rule_project_id(principal)?.is_none() {
            return Ok(None);
        }
        let payload_digest = [0_u8; 32];
        let command = CommandInsert::targetless_admitted(principal, request, &payload_digest, 0);
        super::projects::capture_project_command_binding_tx(tx, &command)?;
        return Ok(None);
    }
    if matches!(request.verb.as_str(), "mail.send" | "var.set") {
        if rule_project_id(principal)?.is_none() {
            return Ok(None);
        }
        let payload_digest = [0_u8; 32];
        let command = CommandInsert::targetless_admitted(principal, request, &payload_digest, 0);
        super::projects::capture_project_command_binding_tx(tx, &command)?;
        return Ok(None);
    }
    if request.verb == "git.fetch" {
        let Some(project_id) = rule_project_id(principal)? else {
            return Ok(None);
        };
        let frozen = super::projects::decode_frozen_git_fetch(&request.args)?;
        if frozen.repository != project_id {
            return Err(Error::new(
                "not_allowed",
                "Project git.fetch repository must match its saved principal",
            ));
        }
        let registration = super::projects::validate_git_fetch_intent_tx(tx, principal, &frozen)?;
        return Ok(Some(registration));
    }
    if request.verb == "resume" {
        let target = request
            .target
            .as_deref()
            .ok_or_else(|| Error::new("not_allowed", "project resume has no Agent target"))?;
        let generation = request.expected_generation.ok_or_else(|| {
            Error::new("not_allowed", "project resume has no exact old generation")
        })?;
        let has_linked_input = request
            .args
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty());
        return super::projects::validate_project_resume_action_tx(
            tx,
            principal,
            target,
            generation,
            request.expected_generation,
            has_linked_input,
        );
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

fn ensure_rule_state_tx(
    tx: &Transaction<'_>,
    principal: &str,
    input: &RuleDefinitionInput,
    source_present: bool,
    high_water: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<core::api::RuleStateView> {
    if let Some(existing) = load_rule_state(tx, principal, &input.id)? {
        if existing.source_present != source_present {
            tx.execute(
                "UPDATE rule_state SET source_present = ?1 WHERE principal = ?2 AND rule = ?3",
                params![source_present, principal, input.id],
            )
            .map_err(sql_error)?;
        }
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
             retiring_revision, retiring_definition, retiring_cursor, retire_through, source_present
         ) VALUES (?1, ?2, 1, ?3, ?4, ?5, ?6, NULL, ?7, NULL, NULL, NULL, NULL, ?8)",
        params![
            principal,
            input.id,
            input.canonical_definition,
            input.definition_digest.as_slice(),
            high_water,
            input.enabled,
            now,
            source_present,
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
         SET current_cursor = ?1, enabled = 0, source_present = 0,
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
            source_present: stored.source_present,
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
    match &frozen.context {
        core::api::FrozenScriptContext::Event { .. } => {
            resolve_event_script_run_origin_tx(tx, run, frozen)
        }
        core::api::FrozenScriptContext::Timer { .. } => {
            rule_timers::resolve_timer_script_run_origin_tx(tx, run, frozen)
        }
    }
}

pub(super) fn validate_script_run_admitted_origin_tx(
    tx: &Transaction<'_>,
    run: &Command,
    frozen: &core::api::FrozenScriptRun,
) -> Result<Command> {
    match &frozen.context {
        core::api::FrozenScriptContext::Event { .. } => {
            validate_event_script_run_admitted_origin_tx(tx, run, frozen)
        }
        core::api::FrozenScriptContext::Timer { .. } => {
            rule_timers::validate_timer_script_run_admitted_origin_tx(tx, run, frozen)
        }
    }
}

fn validate_event_script_run_admitted_origin_tx(
    tx: &Transaction<'_>,
    run: &Command,
    frozen: &core::api::FrozenScriptRun,
) -> Result<Command> {
    let core::api::FrozenScriptContext::Event {
        event: frozen_event,
        subject: frozen_subject,
        rule: frozen_rule,
    } = &frozen.context
    else {
        return Err(Error::new(
            "source_integrity",
            "event rule Run has no frozen event context",
        ));
    };
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
            let event_seq = frozen_event.seq;
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
    if frozen_rule.id != rule || frozen_rule.revision != revision || frozen_event.seq != event_seq {
        return Err(Error::new(
            "source_integrity",
            "stored script context differs from its rule parent",
        ));
    }
    let event = scripts::load_event_tx(tx, event_seq)?;
    if frozen_event.as_ref() != &event || frozen_subject != &event.subject {
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

    Ok(parent.command)
}

fn resolve_event_script_run_origin_tx(
    tx: &Transaction<'_>,
    run: &Command,
    frozen: &core::api::FrozenScriptRun,
) -> Result<ScriptRunOrigin> {
    let core::api::FrozenScriptContext::Event {
        rule: frozen_rule, ..
    } = &frozen.context
    else {
        return Err(Error::new(
            "source_integrity",
            "event rule Run has no frozen event context",
        ));
    };
    let parent = validate_event_script_run_admitted_origin_tx(tx, run, frozen)?;
    if let Some(limit) = script_run_parent_rate_limit(parent.outcome.as_ref())? {
        // The admission froze this Run's rate policy in its committed parent,
        // so successful output stays valid after the definition revision is
        // retired or replaced. Provenance and current authority were verified
        // above; the live definition pair is not consulted.
        let cause = command_cause_tx(tx, &run.id)?
            .ok_or_else(|| Error::new("source_integrity", "script Run has no stored cause"))?;
        return Ok(ScriptRunOrigin {
            principal: run.principal.clone(),
            rule: frozen_rule.id.clone(),
            cause,
            limit,
        });
    }
    // Rows committed before the parent policy freeze fall back to the retained
    // current/retiring definition pair.
    let stored = load_rule_state(tx, &run.principal, &frozen_rule.id)?
        .ok_or_else(|| Error::new("source_integrity", "script rule state was removed"))?;
    let definition = if stored.view.revision == frozen_rule.revision {
        stored.canonical_definition.as_str()
    } else if stored.view.retiring_revision == Some(frozen_rule.revision) {
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
    let cause = command_cause_tx(tx, &run.id)?
        .ok_or_else(|| Error::new("source_integrity", "script Run has no stored cause"))?;
    Ok(ScriptRunOrigin {
        principal: run.principal.clone(),
        rule: frozen_rule.id.clone(),
        cause,
        limit,
    })
}

/// Read the admission rate policy a rule parent froze for its Run child.
/// `None` is a row committed before the freeze; callers fall back to the
/// retained current/retiring definition lookup for those rows.
fn script_run_parent_rate_limit(outcome: Option<&Value>) -> Result<Option<core::api::RuleLimit>> {
    let Some(value) = outcome.and_then(|outcome| outcome.get("rate_limit")) else {
        return Ok(None);
    };
    let limit = serde_json::from_value::<core::api::RuleLimit>(value.clone())
        .map_err(|_| Error::new("source_integrity", "saved rule rate policy is invalid"))?;
    limit
        .validate()
        .map_err(|_| Error::new("source_integrity", "saved rule rate policy is invalid"))?;
    Ok(Some(limit))
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
        "resume" => {
            request.expected_turn_id.is_none()
                && request.args.as_object().is_some_and(|args| {
                    args.iter()
                        .all(|(key, value)| key == "text" && (value.is_string() || value.is_null()))
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

fn valid_event_rule_action_request(request: &SubmitCommand) -> bool {
    if request.verb == "launch" {
        return valid_targetless_rule_launch_request(request);
    }
    if request.verb == "git.fetch" {
        return valid_targetless_git_fetch_request(request);
    }
    if matches!(request.verb.as_str(), "notify" | "emit") {
        return valid_targetless_user_event_request(request);
    }
    if matches!(request.verb.as_str(), "mail.send" | "var.set") {
        return valid_targetless_coordination_request(request);
    }
    if request.verb == "inject" {
        return valid_inject_request(request);
    }
    let reply = matches!(request.verb.as_str(), "approve" | "deny" | "answer");
    if request.verb != "steer" && !reply {
        return valid_rule_action_request(request, true);
    }
    let common = request.validate().is_ok()
        && request
            .target
            .as_deref()
            .is_some_and(|target| core::parse_agent_id(target).is_ok())
        && request
            .expected_generation
            .is_some_and(|generation| generation >= 0)
        && request
            .expected_turn_id
            .as_deref()
            .is_some_and(|turn_id| !turn_id.is_empty())
        && !request.probe;
    if !common {
        return false;
    }
    if reply {
        return request
            .request_id
            .as_deref()
            .is_some_and(|request_id| !request_id.is_empty());
    }
    request.request_id.is_none()
        && request
            .args
            .as_object()
            .is_some_and(|args| args.len() == 1 && args.get("text").is_some_and(Value::is_string))
}

fn valid_inject_request(request: &SubmitCommand) -> bool {
    if request.validate().is_err()
        || !request
            .target
            .as_deref()
            .is_some_and(|target| core::parse_agent_id(target).is_ok())
        || request
            .expected_generation
            .is_some_and(|generation| generation < 0)
        || request.request_id.is_some()
        || request.probe
    {
        return false;
    }
    if !request.args.is_object() {
        return false;
    }
    let Ok(args) = serde_json::from_value::<eliot_core::InjectArgs>(request.args.clone()) else {
        return false;
    };
    match args.point {
        eliot_core::InjectPoint::NextInput => request.expected_turn_id.is_none(),
        eliot_core::InjectPoint::TurnSafePoint => request
            .expected_turn_id
            .as_deref()
            .is_some_and(|turn_id| !turn_id.is_empty()),
        eliot_core::InjectPoint::ActiveTurn => false,
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

fn valid_targetless_user_event_request(request: &SubmitCommand) -> bool {
    request.validate().is_ok()
        && matches!(request.verb.as_str(), "notify" | "emit")
        && request.target.is_none()
        && request.expected_generation.is_none()
        && request.expected_turn_id.is_none()
        && request.request_id.is_none()
        && !request.probe
        && request.args.is_object()
}

fn valid_targetless_coordination_request(request: &SubmitCommand) -> bool {
    request.validate().is_ok()
        && matches!(request.verb.as_str(), "mail.send" | "var.set")
        && request.target.is_none()
        && request.expected_generation.is_none()
        && request.expected_turn_id.is_none()
        && request.request_id.is_none()
        && !request.probe
        && request.args.is_object()
}

fn valid_targetless_git_fetch_request(request: &SubmitCommand) -> bool {
    request.validate().is_ok()
        && request.verb == "git.fetch"
        && request.target.is_none()
        && request.expected_generation.is_none()
        && request.expected_turn_id.is_none()
        && request.request_id.is_none()
        && !request.probe
        && super::projects::decode_frozen_git_fetch(&request.args).is_ok()
}

fn valid_targetless_rule_launch_request(request: &SubmitCommand) -> bool {
    request.validate().is_ok()
        && request.verb == "launch"
        && request.target.is_none()
        && request.expected_generation == Some(0)
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

fn has_open_rule_actions(connection: &Connection, principal: &str, rule: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
             SELECT 1 FROM commands child
             JOIN commands parent ON parent.id = child.parent_command
             WHERE child.link_kind = 'rule_action' AND child.ordinal IS NOT NULL
               AND (
                   child.state IN ('accepted','queued','dispatched','unknown')
                   OR (child.verb IN ('launch','resume') AND EXISTS (
                       SELECT 1 FROM commands input
                       WHERE input.parent_command = child.id
                         AND input.link_kind = 'send' AND input.ordinal = 0
                         AND input.state IN ('accepted','queued','dispatched','unknown')
                   ))
               )
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
                          AND root.verb IN ('rule','timer.run','rule.catch_up')
                          AND json_extract(root.args, '$.rule') = ?2
                    )
                )
         )",
            params![principal, rule],
            |row| row.get(0),
        )
        .map_err(sql_error)
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
                    retiring_definition, retiring_cursor, retire_through, source_present
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
                    row.get::<_, bool>(10)?,
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
        source_present,
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
        source_present,
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

fn load_rule_trigger(tx: &Transaction<'_>, event_seq: i64) -> Result<StoredRuleTrigger> {
    tx.query_row(
        "SELECT kind, agent, generation, cause,
                CASE WHEN json_type(data, '$.turn_id') = 'text'
                     THEN json_extract(data, '$.turn_id') END,
                CASE WHEN json_type(data, '$.request_id') = 'text'
                     THEN json_extract(data, '$.request_id') END
         FROM events WHERE seq = ?1",
        [event_seq],
        |row| {
            Ok(StoredRuleTrigger {
                kind: row.get(0)?,
                agent: row.get(1)?,
                generation: row.get(2)?,
                encoded_cause: row.get(3)?,
                turn_id: row.get(4)?,
                request_id: row.get(5)?,
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
    request_id: Option<String>,
    cause: Option<core::EventCause>,
}

fn rule_action_binding_error(event: &StoredRuleTrigger, request: &SubmitCommand) -> Option<String> {
    if !valid_event_rule_action_request(request) {
        return Some("rule_action_binding_invalid".to_owned());
    }
    if matches!(
        request.verb.as_str(),
        "launch" | "git.fetch" | "notify" | "emit" | "mail.send" | "var.set"
    ) {
        return None;
    }
    if request.target != event.agent.map(format_agent_id)
        || request.expected_generation != event.generation
        || ((matches!(
            request.verb.as_str(),
            "interrupt" | "steer" | "approve" | "deny" | "answer"
        ) || (request.verb == "inject" && request.args["point"] == "turn_safe_point"))
            && request.expected_turn_id.as_deref() != event.turn_id.as_deref())
        || (matches!(request.verb.as_str(), "approve" | "deny" | "answer")
            && (event.kind != "request.opened"
                || request.request_id.as_deref() != event.request_id.as_deref()))
    {
        return Some("rule_action_binding_invalid".to_owned());
    }
    None
}

fn should_suppress_own_event_send_or_run_tx(
    tx: &Transaction<'_>,
    principal: &str,
    rule: &str,
    canonical_definition: &str,
    request: &SubmitCommand,
    trigger_cause: Option<&core::EventCause>,
) -> Result<bool> {
    let has_rule_input = matches!(request.verb.as_str(), "send" | "run")
        || (request.verb == "resume"
            && request
                .args
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.trim().is_empty()));
    if !has_rule_input {
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
                          AND root.verb IN ('rule','timer.run','rule.catch_up')
                          AND json_extract(root.args, '$.rule') = ?3
                    )
                    OR EXISTS (
                        SELECT 1 FROM commands action
                        WHERE action.id = origin.parent_command
                          AND origin.link_kind = 'send'
                          AND action.principal = ?2
                          AND action.link_kind = 'rule_action'
                          AND action.ordinal IS NOT NULL
                          AND (
                              EXISTS (
                                  SELECT 1 FROM commands root
                                  WHERE root.id = action.parent_command
                                    AND root.principal = ?2
                                    AND root.verb IN ('rule','timer.run','rule.catch_up')
                                    AND json_extract(root.args, '$.rule') = ?3
                              )
                              OR EXISTS (
                                  SELECT 1 FROM commands run
                                  JOIN commands root ON root.id = run.parent_command
                                  WHERE run.id = action.parent_command
                                    AND run.verb = 'run'
                                    AND run.principal = ?2
                                    AND run.link_kind = 'rule_action'
                                    AND run.ordinal IS NOT NULL
                                    AND root.principal = ?2
                                    AND root.verb IN ('rule','timer.run','rule.catch_up')
                                    AND json_extract(root.args, '$.rule') = ?3
                              )
                          )
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

fn capacity_metadata(error: &Error) -> Option<(&str, &str)> {
    (error.code == "capacity_unavailable"
        && error.failure_where.as_deref() == Some("store.admission.capacity"))
    .then_some((error.message.as_str(), "store.admission.capacity"))
}

fn rule_rejection_event(
    tx: &Transaction<'_>,
    rule: &str,
    revision: i64,
    range: std::ops::RangeInclusive<i64>,
    code: &str,
    capacity: Option<(&str, &str)>,
    now: i64,
) -> Result<Option<Event>> {
    validate_rule_rejection_code(code)?;
    let trigger = load_rule_trigger(tx, *range.end())?;
    if trigger.kind.starts_with("rule.") || next_rule_cause_depth(trigger.cause.as_ref()) > 5 {
        return Ok(None);
    }
    let mut data = json!({
        "rule": rule,
        "revision": revision,
        "from_seq": range.start(),
        "through_seq": range.end(),
        "code": code,
    });
    if let Some((message, failure_where)) = capacity {
        data["message"] = json!(message);
        data["where"] = json!(failure_where);
    }
    insert_rule_event_with_cause(tx, now, "rule.rejected", data, trigger.cause).map(Some)
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
    let (child_ids, state, rejection_code) = match &outcome_kind {
        RuleParentOutcome::Admitted => (vec![key.child_id.clone()], CommandState::Applied, None),
        RuleParentOutcome::Rejected { error } => (
            Vec::new(),
            CommandState::Rejected,
            Some(error.code.as_str()),
        ),
    };
    let args = &key.args;
    let mut outcome = match rejection_code {
        Some(code) => json!({"children": child_ids, "code": code}),
        None => json!({"children": child_ids}),
    };
    if let RuleParentOutcome::Rejected { error } = &outcome_kind
        && let Some((message, where_)) = capacity_metadata(error)
    {
        outcome["message"] = json!(message);
        outcome["where"] = json!(where_);
    }
    if rejection_code.is_none()
        && let Some(script_context) = &key.script_context
    {
        outcome["script_context"] = serde_json::to_value(script_context)
            .map_err(|_| Error::new("internal", "timer script context cannot be encoded"))?;
    }
    if rejection_code.is_none()
        && let Some(rate_limit) = &key.run_rate_limit
    {
        outcome["rate_limit"] = serde_json::to_value(rate_limit)
            .map_err(|_| Error::new("internal", "rule rate policy cannot be encoded"))?;
    }
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
        selector: None,
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
            selector: None,
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
            config_revision: 0,
            config: None,
        },
        event,
    ))
}

fn admit_rule_action_tx(
    tx: &Transaction<'_>,
    key: &RuleFiringKey<'_>,
    mut request: SubmitCommand,
    now: i64,
    limits: core::api::HierarchyLimits,
    policy: Option<&capacity::PolicyLoader>,
) -> Result<(Command, Admission, Vec<Event>)> {
    let parent_cause = key.command_cause(&key.parent_id)?;
    let child_cause = key.command_cause(&key.child_id)?;
    request.id = key.child_id.clone();
    // Ordinary admission may have inserted an Agent/cursor before a later
    // transactional capacity gate refuses it. A rejected firing must retain
    // none of those partial writes when its rejection is committed.
    tx.execute_batch("SAVEPOINT rule_child_admission")
        .map_err(sql_error)?;
    let mut child_admission =
        match admission::submit_tx(tx, key.principal, request, None, now, true, limits, policy) {
            Ok(admission) => {
                tx.execute_batch("RELEASE rule_child_admission")
                    .map_err(sql_error)?;
                admission
            }
            Err(error) => {
                tx.execute_batch("ROLLBACK TO rule_child_admission; RELEASE rule_child_admission")
                    .map_err(sql_error)?;
                return Err(error);
            }
        };
    if !child_admission.is_new {
        return Err(Error::new(
            "source_integrity",
            "rule child exists without its deterministic parent",
        ));
    }

    let child_id = child_admission.command.id.clone();
    annotate_rule_command_event(
        tx,
        &child_id,
        &mut child_admission.events,
        child_cause.clone(),
    )?;
    if let Some(linked_command) = &child_admission.linked_command {
        annotate_rule_command_event(
            tx,
            &linked_command.id,
            &mut child_admission.events,
            core::EventCause {
                command_id: linked_command.id.clone(),
                rule: child_cause.rule.clone(),
                remote_call: child_cause.remote_call.clone(),
                depth: child_cause.depth,
            },
        )?;
    }
    let (parent, mut parent_event) = insert_rule_parent(tx, key, RuleParentOutcome::Admitted, now)?;
    annotate_new_command_cause_tx(tx, &parent.id, &mut parent_event, parent_cause.clone())?;
    let linked = tx
        .execute(
            "UPDATE commands
             SET parent_command = ?1, link_kind = 'rule_action', ordinal = 0
             WHERE id = ?2 AND parent_command IS NULL
               AND link_kind IS NULL AND ordinal IS NULL",
            params![key.parent_id, child_id],
        )
        .map_err(sql_error)?;
    if linked != 1 {
        return Err(Error::new(
            "source_integrity",
            "new rule child could not be bound to its parent",
        ));
    }

    let mut events = std::mem::take(&mut child_admission.events);
    events.push(parent_event);
    events.push(insert_rule_event_with_cause(
        tx,
        now,
        "rule.fired",
        key.fired_data(&child_id),
        Some(parent_cause),
    )?);
    Ok((parent, child_admission, events))
}

pub(super) fn annotate_rule_command_event(
    tx: &Transaction<'_>,
    command_id: &str,
    events: &mut [Event],
    cause: core::EventCause,
) -> Result<()> {
    annotate_admission_command_cause_tx(tx, command_id, events, cause)
}

fn reject_rule_event(
    tx: Transaction<'_>,
    write_faulted: &Cell<bool>,
    key: &RuleFiringKey<'_>,
    error: &Error,
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
        insert_rule_parent(&tx, key, RuleParentOutcome::Rejected { error }, now)?;
    annotate_new_command_cause_tx(&tx, &parent.id, &mut parent_event, parent_cause.clone())?;
    let mut events = vec![parent_event];
    events.push(insert_rule_event_with_cause(
        &tx,
        now,
        "rule.rejected",
        key.rejected_data(error),
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
    connection: &Connection,
    parent_id: &str,
    expected_child_id: &str,
) -> Result<Option<Admission>> {
    let child_id: Option<String> = connection
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
    let command = load_command(connection, &child_id)?
        .ok_or_else(|| Error::new("source_integrity", "rule parent child command is missing"))?;
    Ok(Some(retained_action_admission(connection, command)?))
}

pub(super) fn retained_action_admission(
    connection: &Connection,
    command: Command,
) -> Result<Admission> {
    let linked_command = if matches!(command.verb.as_str(), "launch" | "resume") {
        load_linked_command(connection, &command.id)?
    } else {
        None
    };
    if command.verb == "launch" {
        let expects_linked_send = command.args.get("text").is_some_and(|text| !text.is_null());
        match (expects_linked_send, linked_command.as_ref()) {
            (true, Some(linked))
                if linked.id == format!("{}/send", command.id)
                    && linked.principal == command.principal
                    && linked.method == command.method
                    && linked.verb == "send"
                    && linked.target == command.target
                    && linked.expected_generation == Some(1)
                    && linked.generation == Some(1)
                    && linked.args.get("text") == command.args.get("text") => {}
            (false, None) => {}
            _ => {
                return Err(Error::new(
                    "source_integrity",
                    "rule launch linked-send row differs from its retained command",
                ));
            }
        }
    } else if command.verb == "resume" {
        match linked_command.as_ref() {
            Some(linked) => {
                let old_generation = command
                    .expected_generation
                    .or(command.generation)
                    .ok_or_else(|| {
                        Error::new("source_integrity", "rule resume has no old generation")
                    })?;
                let next_generation = old_generation.checked_add(1).ok_or_else(|| {
                    Error::new("source_integrity", "rule resume generation overflow")
                })?;
                let parent_target = command
                    .target
                    .as_deref()
                    .map(parse_agent_id)
                    .transpose()?
                    .ok_or_else(|| {
                        Error::new("source_integrity", "rule resume has no Agent target")
                    })?;
                let linked_record =
                    load_command_record(connection, &linked.id)?.ok_or_else(|| {
                        Error::new("source_integrity", "resume input row disappeared")
                    })?;
                let linked_turn_binding_matches = match (
                    linked_record.state,
                    linked_record.command.turn_id.as_deref(),
                ) {
                    (CommandState::Applied, Some(turn_id)) => {
                        match (linked_record.target, linked_record.command.generation) {
                            (Some(agent), Some(generation)) => {
                                super::load_turn(connection, agent, generation, turn_id)?.is_some()
                            }
                            _ => false,
                        }
                    }
                    (CommandState::Applied, None) => false,
                    (_, None) => true,
                    (_, Some(_)) => false,
                };
                let exact_input = linked.id == format!("{}/send", command.id)
                    && linked.principal == command.principal
                    && linked.method == command.method
                    && linked.verb == "send"
                    && linked.target == command.target
                    && linked.expected_generation == Some(next_generation)
                    && linked.generation == Some(next_generation)
                    && linked.state == linked_record.state
                    && linked.turn_id == linked_record.command.turn_id
                    && linked.args == linked_record.command.args
                    && linked_record.target == Some(parent_target)
                    && linked_record.expected_turn_id.is_none()
                    && linked_record.command.id == linked.id
                    && linked_record.command.principal == linked.principal
                    && linked_record.command.method == linked.method
                    && linked_record.command.expected_generation == Some(next_generation)
                    && linked_record.command.generation == Some(next_generation)
                    && linked_record.command.request_id.is_none()
                    && linked_turn_binding_matches
                    && linked.request_id.is_none()
                    && !linked.probe
                    && linked.args.as_object().is_some_and(|args| {
                        args.len() == 1 && args.get("text").is_some_and(Value::is_string)
                    })
                    && super::resume_parent_payload_matches(connection, &command, &linked.args)?;
                if !exact_input {
                    return Err(Error::new(
                        "source_integrity",
                        "rule resume linked-send row differs from its retained request",
                    ));
                }
            }
            None => {
                let no_input =
                    super::resume_parent_payload_matches(connection, &command, &json!({}))?
                        || super::resume_parent_payload_matches(
                            connection,
                            &command,
                            &json!({"text": null}),
                        )?;
                if !no_input {
                    return Err(Error::new(
                        "source_integrity",
                        "rule resume is missing its retained linked input",
                    ));
                }
            }
        }
    }
    Ok(Admission {
        command,
        linked_command,
        events: Vec::new(),
        is_new: false,
    })
}

fn trim_recovery_admission_page(admissions: &mut Vec<Admission>) -> Result<()> {
    let mut command_bytes = 2_usize;
    let mut linked_bytes = 2_usize;
    let mut has_linked_command = false;
    let mut keep = 0_usize;

    for (index, admission) in admissions.iter().enumerate() {
        if index == MAX_PAGE_ENTRIES {
            break;
        }
        let command_len = serde_json::to_vec(&admission.command)
            .map_err(|_| Error::new("internal", "recovered command cannot be encoded"))?
            .len();
        let next_command_bytes = command_bytes
            .saturating_add(command_len)
            .saturating_add(if index == 0 { 0 } else { 1 });
        let mut next_linked_bytes = linked_bytes;
        let mut next_has_linked_command = has_linked_command;
        if let Some(linked) = &admission.linked_command {
            let linked_len = serde_json::to_vec(linked)
                .map_err(|_| Error::new("internal", "recovered linked command cannot be encoded"))?
                .len();
            next_linked_bytes = next_linked_bytes
                .saturating_add(linked_len)
                .saturating_add(if has_linked_command { 1 } else { 0 });
            next_has_linked_command = true;
        }
        if next_command_bytes.saturating_add(next_linked_bytes) > MAX_PAGE_BYTES {
            // Preserve one already-admitted launch and its linked input as a singleton.
            if keep == 0 {
                keep = index + 1;
            }
            break;
        }
        command_bytes = next_command_bytes;
        linked_bytes = next_linked_bytes;
        has_linked_command = next_has_linked_command;
        keep = index + 1;
    }

    admissions.truncate(keep);
    Ok(())
}
fn ensure_admission_page(admissions: &[Admission]) -> Result<()> {
    if admissions.len() > MAX_PAGE_ENTRIES {
        return Err(Error::new(
            "frame_too_large",
            "admission page exceeds the bounded entry count",
        ));
    }
    let commands = admissions
        .iter()
        .map(|admission| &admission.command)
        .collect::<Vec<_>>();
    let linked = admissions
        .iter()
        .filter_map(|admission| admission.linked_command.as_ref())
        .collect::<Vec<_>>();
    let command_bytes = serde_json::to_vec(&commands)
        .map_err(|_| Error::new("internal", "recovered commands cannot be encoded"))?;
    let linked_bytes = serde_json::to_vec(&linked)
        .map_err(|_| Error::new("internal", "recovered linked commands cannot be encoded"))?;
    if command_bytes.len().saturating_add(linked_bytes.len()) > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "recovered rule admissions exceed the page byte bound",
        ));
    }
    Ok(())
}

fn load_retained_rule_event_action(
    connection: &Connection,
    principal: &str,
    stored: &StoredRuleState,
    revision: i64,
    event_seq: i64,
) -> Result<Option<RuleEventAdmission>> {
    let key = RuleFiringKey::event(principal, &stored.view, revision, event_seq, None)?;
    let Some(parent) = load_command(connection, &key.parent_id)? else {
        return Ok(None);
    };
    if !key.matches_parent(&parent, principal) {
        return Err(Error::new(
            "source_integrity",
            "derived rule parent id is bound to another action",
        ));
    }
    if stored.view.current_cursor < event_seq
        && stored
            .view
            .retiring_cursor
            .is_none_or(|cursor| cursor < event_seq)
    {
        return Err(Error::new(
            "source_integrity",
            "rule parent exists before its event cursor commit",
        ));
    }
    let expected_children = match &parent.state {
        CommandState::Applied => vec![key.child_id.clone()],
        CommandState::Rejected => Vec::new(),
        _ => {
            return Err(Error::new(
                "source_integrity",
                "retained rule parent has a nonterminal admission state",
            ));
        }
    };
    let recorded_children = parent
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "retained rule parent has no fixed child set",
            )
        })?
        .iter()
        .map(|child| {
            child
                .as_str()
                .ok_or_else(|| Error::new("source_integrity", "retained rule child ID is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    if recorded_children
        .iter()
        .map(|child| (*child).to_owned())
        .collect::<Vec<_>>()
        != expected_children
    {
        return Err(Error::new(
            "source_integrity",
            "retained rule parent child set differs from its deterministic action",
        ));
    }
    let child = load_rule_child(connection, &key.parent_id, &key.child_id)?;
    if child.is_some() != (parent.state == CommandState::Applied) {
        return Err(Error::new(
            "source_integrity",
            "retained rule child rows differ from the parent outcome",
        ));
    }
    Ok(Some(RuleEventAdmission {
        state: stored.view.clone(),
        parent: Some(parent),
        child,
        events: Vec::new(),
        is_new: false,
    }))
}
