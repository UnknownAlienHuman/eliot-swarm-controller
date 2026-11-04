//! Timer schedules, due slots, and manual runs beside their owning rule state.
//!
//! The daemon computes due slots; Store persists the schedule, the slot cursor,
//! and the ordinary firing admission. Event-rule traversal stays in `rules`.

use super::*;

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
    pub child: Option<Admission>,
    pub events: Vec<Event>,
    pub is_new: bool,
    pub blocked: bool,
}

impl Store {
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

    /// Read a committed timer slot before its caller prepares or freezes an action.
    ///
    /// `state` and `scheduled_at_ms` identify the retained slot only. Current timer
    /// enablement, rate limits and project registration are deliberately not checked
    /// on replay. The returned state is reread from Store so a stale caller snapshot
    /// cannot repeat the timer cursor.
    pub fn replay_timer_slot(
        &self,
        principal: &str,
        state: &core::api::TimerStateView,
        scheduled_at_ms: i64,
    ) -> Result<Option<TimerSlotAdmission>> {
        require_nonempty(principal, "principal")?;
        state.validate()?;
        let parent_id = derived_timer_id(
            "parent",
            principal,
            &state.id,
            state.schedule_revision,
            scheduled_at_ms,
        )?;
        let Some(parent) = load_command(&self.connection, &parent_id)? else {
            return Ok(None);
        };
        if parent.principal != principal
            || parent.verb != "rule"
            || parent.args.get("rule").and_then(Value::as_str) != Some(state.id.as_str())
            || parent.args.get("schedule_revision").and_then(Value::as_i64)
                != Some(state.schedule_revision)
            || parent.args.get("scheduled_at_ms").and_then(Value::as_i64) != Some(scheduled_at_ms)
            || parent.args.get("project").and_then(Value::as_str) != state.project.as_deref()
        {
            return Err(Error::new(
                "source_integrity",
                "scheduled timer parent does not match its slot identity",
            ));
        }
        let expected_children = parent
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.get("children"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "scheduled timer parent has no child set",
                )
            })?;
        let child_id = derived_timer_id(
            "action-0",
            principal,
            &state.id,
            state.schedule_revision,
            scheduled_at_ms,
        )?;
        if expected_children.len() > 1
            || expected_children
                .first()
                .is_some_and(|child| child.as_str() != Some(child_id.as_str()))
        {
            return Err(Error::new(
                "source_integrity",
                "scheduled timer parent has an invalid fixed child set",
            ));
        }
        let child = load_rule_child(&self.connection, &parent.id, &child_id)?;
        if expected_children.is_empty() == child.is_some() {
            return Err(Error::new(
                "source_integrity",
                "scheduled timer child rows differ from the retained parent",
            ));
        }
        let state = load_timer_state(&self.connection, principal, &state.id)?
            .ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "scheduled timer parent has no retained timer state",
                )
            })?
            .view;
        Ok(Some(TimerSlotAdmission {
            state,
            parent: Some(parent),
            child,
            events: Vec::new(),
            is_new: false,
            blocked: false,
        }))
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
        let mut key = RuleFiringKey::timer(
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
            let action_rejection = if action.verb == "run" {
                let expected_context = expected_timer_script_context(
                    &rule.view,
                    &stored.view,
                    core::api::ScriptTimerInvocation::Scheduled {
                        scheduled_at_ms: slot.scheduled_at_ms,
                    },
                )?;
                match validate_timer_script_run_request(&action, &expected_context) {
                    Ok(()) => {
                        key.script_context = Some(expected_context);
                        None
                    }
                    Err(error)
                        if error.code == "rule_action_binding_invalid"
                            || core::is_deterministic_pre_admission_rejection(&error.code) =>
                    {
                        Some(error.code)
                    }
                    Err(error) => return Err(error),
                }
            } else if !valid_timer_action_request(&action) {
                Some("rule_action_binding_invalid".to_owned())
            } else {
                None
            };
            let rejected = if let Some(code) = action_rejection {
                Some(code)
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
                let limits = self.hierarchy_limits;
                match admit_rule_action_tx(&tx, &key, action, now, limits) {
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
        if action.verb == "run" {
            if !valid_targetless_rule_run_request(&action) {
                return Err(Error::new(
                    "invalid",
                    "timer Run must be targetless and have no Agent bindings",
                ));
            }
        } else if !valid_timer_action_request(&action) {
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
        let timer_context = if action.verb == "run" {
            let expected = expected_timer_script_context(
                &rule.view,
                &timer.view,
                core::api::ScriptTimerInvocation::Manual {
                    parent_command_id: request.id.clone(),
                },
            )?;
            validate_timer_script_run_request(&action, &expected)?;
            Some(expected)
        } else {
            None
        };
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

        let mut key = RuleFiringKey::manual(principal, &rule.view, &request.id, digest)?;
        key.script_context = timer_context;
        let limits = self.hierarchy_limits;
        match admit_rule_action_tx(&tx, &key, action, now, limits) {
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

pub(super) fn ensure_timer_state_tx(
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

fn replay_timer_run_tx(
    connection: &Connection,
    principal: &str,
    request: &core::api::TimerRunRequest,
) -> Result<Option<(Command, Option<Admission>)>> {
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
    let child_id = derived_manual_timer_id(principal, &request.rule, &request.id)?;
    let child = load_rule_child(connection, &request.id, &child_id)?;
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

pub(super) fn resolve_timer_script_run_origin_tx(
    tx: &Transaction<'_>,
    run: &Command,
    frozen: &core::api::FrozenScriptRun,
) -> Result<ScriptRunOrigin> {
    let core::api::FrozenScriptContext::Timer {
        timer: timer_context,
        rule: script_rule,
    } = &frozen.context
    else {
        return Err(Error::new(
            "source_integrity",
            "timer Run has no frozen timer context",
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
    let (Some(parent_id), Some(link_kind), Some(ordinal)) =
        link.ok_or_else(|| Error::new("source_integrity", "timer Run has no stored parent link"))?
    else {
        return Err(Error::new(
            "source_integrity",
            "timer Run has incomplete parent ordering metadata",
        ));
    };
    if link_kind != "rule_action" || ordinal < 0 {
        return Err(Error::new(
            "source_integrity",
            "timer Run is not an ordered rule action",
        ));
    }
    let parent = load_command_record(tx, &parent_id)?
        .ok_or_else(|| Error::new("source_integrity", "timer Run parent is missing"))?;
    if parent.command.principal != run.principal
        || parent.command.state != CommandState::Applied
        || !matches!(parent.command.verb.as_str(), "rule" | "timer.run")
    {
        return Err(Error::new(
            "source_integrity",
            "timer Run has an unsupported or differently-authorized parent",
        ));
    }
    let firing_children = parent
        .command
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("source_integrity", "timer parent has no fixed child set"))?;
    if !firing_children
        .iter()
        .any(|child| child.as_str() == Some(run.id.as_str()))
    {
        return Err(Error::new(
            "source_integrity",
            "timer Run is absent from its parent fixed child set",
        ));
    }
    let rule = parent
        .command
        .args
        .get("rule")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "timer parent has no rule id"))?
        .to_owned();
    if script_rule.id != rule {
        return Err(Error::new(
            "source_integrity",
            "frozen timer context rule differs from its parent",
        ));
    }
    let stored_context = parent
        .command
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("script_context"))
        .cloned()
        .ok_or_else(|| Error::new("source_integrity", "timer Run parent has no script context"))
        .and_then(|value| {
            serde_json::from_value::<core::api::FrozenScriptContext>(value).map_err(|_| {
                Error::new("source_integrity", "stored timer script context is invalid")
            })
        })?;
    if stored_context != frozen.context {
        return Err(Error::new(
            "source_integrity",
            "frozen timer context differs from its stored parent snapshot",
        ));
    }
    timer_context
        .validate()
        .map_err(|_| Error::new("source_integrity", "frozen timer context is invalid"))?;

    let project = rule_project_id(&run.principal)?;
    if parent.command.args.get("project").and_then(Value::as_str) != project.as_deref() {
        return Err(Error::new(
            "source_integrity",
            "timer parent project differs from its stored principal",
        ));
    }
    match parent.command.verb.as_str() {
        "rule" => {
            let schedule_revision = parent
                .command
                .args
                .get("schedule_revision")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "scheduled timer parent has no schedule revision",
                    )
                })?;
            let scheduled_at_ms = parent
                .command
                .args
                .get("scheduled_at_ms")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    Error::new("source_integrity", "scheduled timer parent has no slot key")
                })?;
            if parent.command.args.get("event_seq").is_some()
                || timer_context.schedule_revision != schedule_revision
                || timer_context.invocation
                    != (core::api::ScriptTimerInvocation::Scheduled { scheduled_at_ms })
                || derived_timer_id(
                    "parent",
                    &run.principal,
                    &rule,
                    schedule_revision,
                    scheduled_at_ms,
                )?
                .as_str()
                    != parent.command.id.as_str()
            {
                return Err(Error::new(
                    "source_integrity",
                    "scheduled timer context differs from its stored slot identity",
                ));
            }
        }
        "timer.run" => {
            if timer_context.invocation
                != (core::api::ScriptTimerInvocation::Manual {
                    parent_command_id: parent.command.id.clone(),
                })
            {
                return Err(Error::new(
                    "source_integrity",
                    "manual timer context differs from its stored parent identity",
                ));
            }
            let mut request =
                core::api::TimerRunRequest::new(parent.command.id.clone(), rule.clone());
            if let Some(project) = project {
                request = request.in_project(project);
            }
            request
                .validate_response(&parent.command)
                .map_err(|_| Error::new("source_integrity", "manual timer parent is invalid"))?;
            let digest = timer_run_digest(&request)?;
            let binding = command_binding(tx, &parent.command.id)?.ok_or_else(|| {
                Error::new("source_integrity", "manual timer parent binding is absent")
            })?;
            if binding.0 != run.principal || binding.1 != COMMAND_METHOD || binding.2 != digest {
                return Err(Error::new(
                    "source_integrity",
                    "manual timer parent differs from its request binding",
                ));
            }
        }
        _ => unreachable!("timer parent verb was checked above"),
    }

    let cause = command_cause_tx(tx, &run.id)?
        .ok_or_else(|| Error::new("source_integrity", "timer Run has no stored cause"))?;
    let parent_cause = command_cause_tx(tx, &parent_id)?
        .ok_or_else(|| Error::new("source_integrity", "timer parent has no stored cause"))?;
    if parent_cause.command_id != parent_id
        || parent_cause.rule.as_deref() != Some(rule.as_str())
        || parent_cause.depth != 1
        || cause.command_id != run.id
        || cause.rule.as_deref() != Some(rule.as_str())
        || cause.depth != parent_cause.depth
        || cause.depth > 5
    {
        return Err(Error::new(
            "source_integrity",
            "timer Run cause differs from its verified parent",
        ));
    }

    let stored = load_rule_state(tx, &run.principal, &rule)?
        .ok_or_else(|| Error::new("source_integrity", "timer rule state was removed"))?;
    let revision = script_rule.revision;
    let definition = if stored.view.revision == revision {
        stored.canonical_definition.as_str()
    } else if stored.view.retiring_revision == Some(revision) {
        stored
            .retiring_definition
            .as_deref()
            .ok_or_else(|| Error::new("source_integrity", "retiring timer rule is absent"))?
    } else {
        return Err(Error::new(
            "source_integrity",
            "timer rule revision is no longer retained",
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

fn valid_timer_action_request(request: &SubmitCommand) -> bool {
    match request.verb.as_str() {
        "launch" => valid_targetless_rule_launch_request(request),
        "git.fetch" => valid_targetless_git_fetch_request(request),
        _ => valid_rule_action_request(request, false),
    }
}

fn expected_timer_script_context(
    rule: &core::api::RuleStateView,
    timer: &core::api::TimerStateView,
    invocation: core::api::ScriptTimerInvocation,
) -> Result<core::api::FrozenScriptContext> {
    if rule.id != timer.id {
        return Err(Error::new(
            "source_integrity",
            "timer and rule state identities differ",
        ));
    }
    let context = core::api::FrozenScriptContext::Timer {
        timer: core::api::ScriptTimerContext {
            schedule_revision: timer.schedule_revision,
            schedule: timer.schedule.clone(),
            anchor: timer.anchor,
            invocation,
        },
        rule: core::api::ScriptRuleContext {
            id: rule.id.clone(),
            revision: rule.revision,
        },
    };
    context
        .validate()
        .map_err(|_| Error::new("source_integrity", "stored timer script context is invalid"))?;
    Ok(context)
}

fn validate_timer_script_run_request(
    request: &SubmitCommand,
    expected: &core::api::FrozenScriptContext,
) -> Result<()> {
    if !valid_targetless_rule_run_request(request) {
        return Err(Error::new(
            "rule_action_binding_invalid",
            "timer Run must be targetless and have no Agent bindings",
        ));
    }
    let frozen = scripts::decode_frozen_run(&request.args).map_err(|_| {
        Error::new(
            "rule_action_binding_invalid",
            "timer Run arguments are not a valid frozen script snapshot",
        )
    })?;
    if &frozen.context != expected {
        return Err(Error::new(
            "rule_action_binding_invalid",
            "frozen timer context differs from its stored firing identity",
        ));
    }
    Ok(())
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
