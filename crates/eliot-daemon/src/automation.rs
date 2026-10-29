mod definition;
mod reload;
mod schedule;
pub(crate) use definition::RuleDefinition;

use crate::{common::now_ms, logging, router::Router};
use eliot_core::{
    Error, Result, SecretPolicy, SubmitCommand,
    api::{CatchUp, RuleStateView, TimerSchedule, TimerStateView},
    canonical_json_sha256,
};
use eliot_store::{
    RuleCursorOutcome, RuleDefinitionInput, RuleProcessingState, TimerDefinitionInput,
    TimerSlotCommit,
};
use serde_json::json;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{broadcast, watch};

const RULE_EVENT_PAGE_SIZE: usize = 128;
const TIMER_PAGE_SIZE: usize = 128;
const MAX_RULE_EVENT_BACKLOG: i64 = 100_000;
const RULE_RECONCILE_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReloadNoticeKey {
    Snapshot([u8; 32]),
    Rejection([u8; 32]),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReloadNoticeContext {
    deferred: Option<ReloadNoticeKey>,
    rejected: Option<ReloadNoticeKey>,
}

enum TimerDecision {
    NotDue,
    Fire {
        scheduled_at_ms: i64,
        next_due_at_ms: Option<i64>,
        live_armed: bool,
    },
    Skip {
        scheduled_at_ms: i64,
        next_due_at_ms: Option<i64>,
        code: String,
    },
}

pub(crate) struct ActiveRule {
    definition: RuleDefinition,
    state: RuleStateView,
    retiring: Option<RetiringRule>,
    timer: Option<ActiveTimer>,
}

struct RetiringRule {
    definition: RuleDefinition,
    revision: i64,
    cursor: i64,
    through: i64,
}

struct ActiveTimer {
    state: TimerStateView,
    catch_up: CatchUp,
    armed_due_at_ms: Option<i64>,
    blocked: bool,
}

pub(crate) async fn initialize_global_rules(
    router: &Router,
    data_root: &Path,
    policy: &SecretPolicy,
) -> Result<(Vec<ActiveRule>, ReloadNoticeContext)> {
    let mut notices = ReloadNoticeContext::default();
    let prior = router.global_rule_processing().await?;
    let registration_now = now_ms();
    let (snapshot, from_file) = match reload::load(data_root) {
        Ok(snapshot) => (snapshot, true),
        Err(error) if !prior.is_empty() => {
            report_rejected_source(policy, &error, &mut notices)?;
            (reload::from_processing(&prior)?, false)
        }
        Err(error) => return Err(error),
    };
    let (rules, candidate_from_file) =
        match apply_global_rule_snapshot(router, &snapshot, None, registration_now).await {
            Ok(rules) => (rules, from_file),
            Err(error)
                if !prior.is_empty()
                    && matches!(
                        error.code.as_str(),
                        "frame_too_large" | "invalid" | "secret_in_payload" | "unsupported"
                    ) =>
            {
                report_rejected_reload(policy, &snapshot.inputs, &error, &mut notices)?;
                let saved = reload::from_processing(&prior)?;
                (
                    apply_global_rule_snapshot(router, &saved, None, registration_now).await?,
                    false,
                )
            }
            Err(error) => return Err(error),
        };
    if candidate_from_file
        && rules.iter().any(|rule| rule.retiring.is_some())
        && !snapshot.matches(
            &rules
                .iter()
                .map(|rule| rule.definition.clone())
                .collect::<Vec<_>>(),
        )?
    {
        report_deferred_reload(policy, &snapshot.inputs, &mut notices)?;
    }
    Ok((rules, notices))
}

fn reload_notice_key(inputs: &[RuleDefinitionInput]) -> Result<ReloadNoticeKey> {
    let mut ordered = inputs.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.id.cmp(&right.id));
    let value = json!({
        "rules": ordered
            .iter()
            .map(|input| json!({
                "id": input.id,
                "digest": input.definition_digest,
                "enabled": input.enabled,
            }))
            .collect::<Vec<_>>(),
    });
    canonical_json_sha256(&value)
        .map(ReloadNoticeKey::Snapshot)
        .map_err(|_| Error::new("internal", "reload notice could not be canonicalized"))
}

fn claim_notice(key: ReloadNoticeKey, last_notice: &mut Option<ReloadNoticeKey>) -> bool {
    if last_notice.as_ref() == Some(&key) {
        return false;
    }
    *last_notice = Some(key);
    true
}

fn report_deferred_reload(
    policy: &SecretPolicy,
    inputs: &[RuleDefinitionInput],
    notices: &mut ReloadNoticeContext,
) -> Result<()> {
    if claim_deferred_reload(inputs, notices)? {
        logging::record(policy, "daemon.rule_reload_deferred", &[], None);
    }
    Ok(())
}

fn claim_deferred_reload(
    inputs: &[RuleDefinitionInput],
    notices: &mut ReloadNoticeContext,
) -> Result<bool> {
    let key = reload_notice_key(inputs)?;
    if notices.rejected.as_ref() == Some(&key) {
        return Ok(false);
    }
    Ok(claim_notice(key, &mut notices.deferred))
}

fn report_rejected_reload(
    policy: &SecretPolicy,
    inputs: &[RuleDefinitionInput],
    error: &Error,
    notices: &mut ReloadNoticeContext,
) -> Result<()> {
    if claim_notice(reload_notice_key(inputs)?, &mut notices.rejected) {
        logging::record(policy, "daemon.rule_reload_rejected", &[], Some(error));
    }
    Ok(())
}

fn report_rejected_source(
    policy: &SecretPolicy,
    error: &Error,
    notices: &mut ReloadNoticeContext,
) -> Result<()> {
    let details = json!({"code": error.code, "message": error.message});
    let key = ReloadNoticeKey::Rejection(
        canonical_json_sha256(&details)
            .map_err(|_| Error::new("internal", "reload notice could not be canonicalized"))?,
    );
    if claim_notice(key, &mut notices.rejected) {
        logging::record(policy, "daemon.rule_reload_rejected", &[], Some(error));
    }
    Ok(())
}

async fn apply_global_rule_snapshot(
    router: &Router,
    snapshot: &reload::RuleSnapshot,
    previous: Option<&[ActiveRule]>,
    registration_now: i64,
) -> Result<Vec<ActiveRule>> {
    router
        .validate_global_rule_definitions(&snapshot.inputs)
        .await?;
    let prior = router.global_rule_processing().await?;
    let effective = defer_retiring_edits(snapshot, &prior)?;
    let timers = timer_inputs(router, &effective, &prior, registration_now).await?;
    let states = router
        .initialize_global_rules(&effective.inputs, &timers, registration_now)
        .await?;
    let active = active_rules(router, states, previous, registration_now).await?;
    let cached = active
        .iter()
        .map(|rule| (rule.state.revision, rule.definition.clone()))
        .collect::<Vec<_>>();
    router.install_global_rule_definitions(&cached);
    Ok(active)
}

fn defer_retiring_edits(
    snapshot: &reload::RuleSnapshot,
    prior: &[RuleProcessingState],
) -> Result<reload::RuleSnapshot> {
    let mut definitions: HashMap<String, RuleDefinition> = snapshot
        .definitions
        .iter()
        .cloned()
        .map(|definition| (definition.id.clone(), definition))
        .collect();
    let mut inputs: HashMap<String, RuleDefinitionInput> = snapshot
        .inputs
        .iter()
        .cloned()
        .map(|input| (input.id.clone(), input))
        .collect();

    for state in prior
        .iter()
        .filter(|state| state.view.retiring_revision.is_some())
    {
        let stored = reload::from_canonical(&state.current_definition)?;
        let changed = definitions
            .get(&state.view.id)
            .map(|candidate| {
                Ok(candidate.enabled != stored.enabled
                    || candidate.semantic_digest()? != stored.semantic_digest()?)
            })
            .transpose()?
            .unwrap_or(true);
        if !changed {
            continue;
        }
        let definition_digest = stored.semantic_digest()?;
        inputs.insert(
            state.view.id.clone(),
            RuleDefinitionInput {
                id: state.view.id.clone(),
                canonical_definition: state.current_definition.clone(),
                definition_digest,
                enabled: stored.enabled,
            },
        );
        definitions.insert(state.view.id.clone(), stored);
    }

    let mut ids = definitions.keys().cloned().collect::<Vec<_>>();
    ids.sort();
    let definitions = ids.iter().filter_map(|id| definitions.remove(id)).collect();
    let inputs = ids.iter().filter_map(|id| inputs.remove(id)).collect();
    Ok(reload::RuleSnapshot {
        definitions,
        inputs,
    })
}

async fn timer_inputs(
    router: &Router,
    snapshot: &reload::RuleSnapshot,
    prior: &[RuleProcessingState],
    registration_now: i64,
) -> Result<Vec<TimerDefinitionInput>> {
    let prior_by_id: HashMap<_, _> = prior
        .iter()
        .map(|state| (state.view.id.as_str(), state))
        .collect();
    let mut inputs = Vec::new();
    for definition in &snapshot.definitions {
        let Some(schedule) = definition.schedule() else {
            continue;
        };
        let old_definition = prior_by_id
            .get(definition.id.as_str())
            .map(|state| reload::from_canonical(&state.current_definition))
            .transpose()?;
        let schedule_changed = old_definition
            .as_ref()
            .map(RuleDefinition::schedule_digest)
            .transpose()?
            != Some(definition.schedule_digest()?);
        let existing = router.get_global_timer(&definition.id).await?;
        let schedule_revision =
            match &existing {
                Some(timer) if schedule_changed => timer
                    .schedule_revision
                    .checked_add(1)
                    .ok_or_else(|| Error::new("invalid", "timer schedule revision overflows"))?,
                Some(timer) => timer.schedule_revision,
                None => prior_by_id
                    .get(definition.id.as_str())
                    .map_or(Ok(1), |state| {
                        state.view.revision.checked_add(1).ok_or_else(|| {
                            Error::new("invalid", "timer schedule revision overflows")
                        })
                    })?,
            };
        let initial_next_due_at = match (&existing, schedule_changed) {
            (Some(timer), false) => timer.next_due_at,
            _ => initial_next_due_at(schedule, registration_now)?,
        };
        inputs.push(TimerDefinitionInput {
            id: definition.id.clone(),
            schedule_revision,
            schedule: schedule.clone(),
            enabled: definition.enabled,
            initial_next_due_at,
        });
    }
    Ok(inputs)
}

async fn active_rules(
    router: &Router,
    states: Vec<RuleProcessingState>,
    previous: Option<&[ActiveRule]>,
    armed_at: i64,
) -> Result<Vec<ActiveRule>> {
    let mut active = Vec::with_capacity(states.len());
    for processing in states {
        let definition = reload::from_canonical(&processing.current_definition)?;
        if definition.id != processing.view.id {
            return Err(Error::new(
                "source_integrity",
                "stored rule definition does not match its state ID",
            ));
        }
        let retiring = match processing.retiring_definition {
            Some(canonical) => {
                let definition = reload::from_canonical(&canonical)?;
                let (Some(revision), Some(cursor), Some(through)) = (
                    processing.view.retiring_revision,
                    processing.view.retiring_cursor,
                    processing.view.retire_through,
                ) else {
                    return Err(Error::new(
                        "source_integrity",
                        "retiring rule definition has incomplete cursor state",
                    ));
                };
                Some(RetiringRule {
                    definition,
                    revision,
                    cursor,
                    through,
                })
            }
            None => {
                if processing.view.retiring_revision.is_some()
                    || processing.view.retiring_cursor.is_some()
                    || processing.view.retire_through.is_some()
                {
                    return Err(Error::new(
                        "source_integrity",
                        "retiring rule cursor has no stored definition",
                    ));
                }
                None
            }
        };
        let timer = if definition.schedule().is_some() {
            let state = router
                .get_global_timer(&definition.id)
                .await?
                .ok_or_else(|| Error::new("source_integrity", "timer rule has no timer state"))?;
            let old_timer = previous
                .and_then(|rules| {
                    rules
                        .iter()
                        .find(|rule| rule.definition.id == definition.id)
                })
                .and_then(|rule| rule.timer.as_ref());
            let unchanged_schedule = old_timer.is_some_and(|old| {
                old.state.schedule_revision == state.schedule_revision
                    && old.state.schedule == state.schedule
            });
            Some(ActiveTimer {
                armed_due_at_ms: if unchanged_schedule {
                    old_timer.and_then(|old| old.armed_due_at_ms)
                } else {
                    state.next_due_at.filter(|due| *due >= armed_at)
                },
                state,
                catch_up: *definition.catch_up(),
                blocked: unchanged_schedule && old_timer.is_some_and(|old| old.blocked),
            })
        } else {
            None
        };
        active.push(ActiveRule {
            definition,
            state: processing.view,
            retiring,
            timer,
        });
    }
    Ok(active)
}

pub(crate) async fn run_global_rules(
    router: Router,
    mut rules: Vec<ActiveRule>,
    data_root: PathBuf,
    policy: Arc<SecretPolicy>,
    initial_notices: ReloadNoticeContext,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    // Bus messages are wake hints; durable Store pages remain authoritative.
    // Register before the first high-water read so no event can fall between them.
    let mut events = router.subscribe_rule_events();
    let mut progress = router.progress_receiver();
    let mut watcher = match reload::RuleWatcher::watch(&data_root) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            logging::record(&policy, "daemon.rule_watch_unavailable", &[], Some(&error));
            None
        }
    };
    let mut reconcile = tokio::time::interval(RULE_RECONCILE_INTERVAL);
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut notices = initial_notices;
    let mut last_applied_inputs = None;
    if let Ok(snapshot) = reload::load(&data_root) {
        let current = rules
            .iter()
            .map(|rule| rule.definition.clone())
            .collect::<Vec<_>>();
        if snapshot.matches(&current).unwrap_or(false) {
            last_applied_inputs = Some(snapshot.inputs);
            notices.deferred = None;
            notices.rejected = None;
        } else if rules
            .iter()
            .any(|rule| rule.state.retiring_revision.is_some())
        {
            let key = reload_notice_key(&snapshot.inputs)?;
            if notices.rejected.as_ref() != Some(&key) {
                match router
                    .validate_global_rule_definitions(&snapshot.inputs)
                    .await
                {
                    Ok(()) => report_deferred_reload(&policy, &snapshot.inputs, &mut notices)?,
                    Err(error)
                        if matches!(
                            error.code.as_str(),
                            "frame_too_large" | "invalid" | "secret_in_payload" | "unsupported"
                        ) =>
                    {
                        report_rejected_reload(&policy, &snapshot.inputs, &error, &mut notices)?;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
    loop {
        if *shutdown.borrow() || !router.rule_firings_open() {
            return Ok(());
        }
        let high_water = router.rule_high_water().await?;
        if !reconcile_event_rules(&router, &mut rules, high_water, now_ms()).await? {
            return Ok(());
        }

        if !reconcile_due_timers(&router, &mut rules, now_ms()).await? {
            return Ok(());
        }

        let timer_wait = next_timer_wait(&rules, now_ms());
        let mut reload_requested = false;
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            changed = progress.changed() => {
                if changed.is_err() || !router.rule_firings_open() {
                    return Ok(());
                }
            }
            received = events.recv() => match received {
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(Error::new("unavailable", "event bus is closed"));
                }
            },
            _ = reconcile.tick() => reload_requested = true,
            _ = async {
                if let Some(watcher) = &mut watcher {
                    let _ = watcher.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => reload_requested = true,
            _ = async {
                if let Some(wait) = timer_wait {
                    tokio::time::sleep(wait).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
        for rule in &mut rules {
            if let Some(timer) = &mut rule.timer {
                timer.blocked = false;
            }
        }
        if !reload_requested {
            continue;
        }

        let snapshot = match reload::load(&data_root) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                report_rejected_source(&policy, &error, &mut notices)?;
                continue;
            }
        };
        let snapshot_key = reload_notice_key(&snapshot.inputs)?;
        if notices.rejected.as_ref() == Some(&snapshot_key) {
            continue;
        }
        if last_applied_inputs.as_ref() == Some(&snapshot.inputs) {
            notices.deferred = None;
            notices.rejected = None;
            continue;
        }
        let current_definitions = rules
            .iter()
            .map(|rule| rule.definition.clone())
            .collect::<Vec<_>>();
        if snapshot.matches(&current_definitions)? {
            last_applied_inputs = Some(snapshot.inputs.clone());
            notices.deferred = None;
            notices.rejected = None;
            continue;
        }
        let retirement_pending = rules
            .iter()
            .any(|rule| rule.state.retiring_revision.is_some());
        if retirement_pending && notices.deferred.as_ref() == Some(&snapshot_key) {
            continue;
        }
        match apply_global_rule_snapshot(&router, &snapshot, Some(&rules), now_ms()).await {
            Ok(updated) => {
                let updated_definitions = updated
                    .iter()
                    .map(|rule| rule.definition.clone())
                    .collect::<Vec<_>>();
                let deferred = updated
                    .iter()
                    .any(|rule| rule.state.retiring_revision.is_some())
                    && !snapshot.matches(&updated_definitions)?;
                if deferred {
                    report_deferred_reload(&policy, &snapshot.inputs, &mut notices)?;
                } else {
                    last_applied_inputs = Some(snapshot.inputs.clone());
                    notices.deferred = None;
                    notices.rejected = None;
                }
                rules = updated;
            }
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "frame_too_large" | "invalid" | "secret_in_payload" | "unsupported"
                ) =>
            {
                report_rejected_reload(&policy, &snapshot.inputs, &error, &mut notices)?;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn reconcile_event_rules(
    router: &Router,
    rules: &mut [ActiveRule],
    high_water: i64,
    now: i64,
) -> Result<bool> {
    for rule in rules {
        if rule.state.paused_reason.as_deref() == Some("backlog_exceeded") {
            continue;
        }
        if rule.state.current_cursor > high_water
            || rule
                .state
                .retiring_cursor
                .is_some_and(|cursor| cursor > high_water)
        {
            return Err(Error::new(
                "source_integrity",
                "rule cursor is above the event high-water mark",
            ));
        }
        refresh_retiring(rule)?;
        let oldest_needed = [
            rule.state.enabled.then_some(rule.state.current_cursor),
            rule.retiring
                .as_ref()
                .filter(|retiring| retiring.definition.enabled)
                .map(|retiring| retiring.cursor),
        ]
        .into_iter()
        .flatten()
        .min();
        if oldest_needed
            .is_some_and(|cursor| high_water.saturating_sub(cursor) > MAX_RULE_EVENT_BACKLOG)
        {
            let processing = router
                .settle_rule_backlog(&rule.state, high_water, MAX_RULE_EVENT_BACKLOG, now)
                .await?;
            rule.state = processing.view;
            refresh_retiring(rule)?;
            if rule.state.paused_reason.as_deref() == Some("backlog_exceeded") {
                continue;
            }
        }

        if let Some(retiring) = &rule.retiring {
            if retiring.definition.enabled && rule.state.paused_reason.is_none() {
                let through = retiring.through.min(high_water);
                if retiring.cursor < through
                    && !process_rule_cursor(router, rule, true, through).await?
                {
                    return Ok(false);
                }
            } else if retiring.cursor < retiring.through.min(high_water) {
                advance_rule_cursor(
                    router,
                    rule,
                    true,
                    retiring.through.min(high_water),
                    RuleCursorOutcome::NoMatch,
                )
                .await?;
            }
        }

        if rule.state.current_cursor < high_water {
            if rule.state.paused_reason.is_some() {
                continue;
            }
            if !rule.state.enabled || !rule.definition.enabled {
                advance_rule_cursor(router, rule, false, high_water, RuleCursorOutcome::Disabled)
                    .await?;
            } else if !process_rule_cursor(router, rule, false, high_water).await? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn process_rule_cursor(
    router: &Router,
    rule: &mut ActiveRule,
    retiring: bool,
    through: i64,
) -> Result<bool> {
    let definition = if retiring {
        rule.retiring
            .as_ref()
            .ok_or_else(|| Error::new("source_integrity", "retiring definition is absent"))?
            .definition
            .clone()
    } else {
        rule.definition.clone()
    };
    while let Some(cursor) = rule_cursor(rule, retiring) {
        if cursor >= through {
            break;
        }
        let page = router
            .read_rule_events(cursor, through, RULE_EVENT_PAGE_SIZE)
            .await?;
        if page.is_empty() {
            return Err(Error::new(
                "history_gap",
                "rule cursor is behind unavailable event history",
            ));
        }
        let mut no_match_through = None;
        for event in page {
            if event.seq <= rule_cursor(rule, retiring).unwrap_or(through) {
                continue;
            }
            let stop = match definition::evaluate(&definition, &event) {
                Ok(Some(stop)) => stop,
                Ok(None) => {
                    no_match_through = Some(event.seq);
                    continue;
                }
                Err(error) => {
                    flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
                    advance_rule_cursor(
                        router,
                        rule,
                        retiring,
                        event.seq,
                        RuleCursorOutcome::Rejected { code: error.code },
                    )
                    .await?;
                    continue;
                }
            };

            flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
            if !router.rule_firings_open() {
                return Ok(false);
            }
            let prepared = match router.prepare_owner_submission(stop_command(stop)).await {
                Ok(prepared) => prepared,
                Err(_) if !router.rule_firings_open() => return Ok(false),
                Err(error) if deterministic_rejection(&error.code) => {
                    advance_rule_cursor(
                        router,
                        rule,
                        retiring,
                        event.seq,
                        RuleCursorOutcome::Rejected { code: error.code },
                    )
                    .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(admission) = router
                .admit_rule_stop(&rule.state, event.seq, prepared)
                .await?
            else {
                return Ok(false);
            };
            rule.state = admission.state;
            refresh_retiring(rule)?;
            // Router schedules only a new child after Store commit and lock release.
        }
        flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
    }
    Ok(true)
}

fn rule_cursor(rule: &ActiveRule, retiring: bool) -> Option<i64> {
    if retiring {
        rule.retiring.as_ref().map(|state| state.cursor)
    } else {
        Some(rule.state.current_cursor)
    }
}

async fn advance_rule_cursor(
    router: &Router,
    rule: &mut ActiveRule,
    _retiring: bool,
    through: i64,
    outcome: RuleCursorOutcome,
) -> Result<()> {
    rule.state = router
        .advance_rule_cursor(&rule.state, through, outcome)
        .await?;
    refresh_retiring(rule)
}

async fn flush_rule_no_match(
    router: &Router,
    rule: &mut ActiveRule,
    retiring: bool,
    through: &mut Option<i64>,
) -> Result<()> {
    if let Some(through) = through.take()
        && through > rule_cursor(rule, retiring).unwrap_or(through)
    {
        advance_rule_cursor(router, rule, retiring, through, RuleCursorOutcome::NoMatch).await?;
    }
    Ok(())
}

fn refresh_retiring(rule: &mut ActiveRule) -> Result<()> {
    match (
        rule.state.retiring_revision,
        rule.state.retiring_cursor,
        rule.state.retire_through,
        rule.retiring.as_mut(),
    ) {
        (None, None, None, None) => Ok(()),
        (None, None, None, Some(_)) => {
            rule.retiring = None;
            Ok(())
        }
        (Some(revision), Some(cursor), Some(through), Some(retiring))
            if revision == retiring.revision =>
        {
            retiring.cursor = cursor;
            retiring.through = through;
            Ok(())
        }
        _ => Err(Error::new(
            "source_integrity",
            "retiring definition and cursor state do not match",
        )),
    }
}

fn initial_next_due_at(schedule: &TimerSchedule, registration_now: i64) -> Result<Option<i64>> {
    match schedule {
        TimerSchedule::Interval { anchor, .. } => {
            let anchor = anchor.unwrap_or(registration_now);
            // The explicit anchor sets the phase; creation starts at the first
            // slot after registration, so an old phase never replays history.
            schedule::next_after(schedule, anchor, registration_now)
        }
        TimerSchedule::Once { .. } | TimerSchedule::Cron { .. } => {
            schedule::next_after(schedule, 0, registration_now)
        }
    }
}

async fn reconcile_due_timers(router: &Router, rules: &mut [ActiveRule], now: i64) -> Result<bool> {
    let mut after_id = None;
    loop {
        let page = router
            .list_due_global_timers(after_id.as_deref(), now, TIMER_PAGE_SIZE)
            .await?;
        let page_len = page.len();
        if page_len == 0 {
            return Ok(true);
        }

        for state in page {
            after_id = Some(state.id.clone());
            let rule = rules
                .iter_mut()
                .find(|rule| {
                    rule.timer
                        .as_ref()
                        .is_some_and(|timer| timer.state.id == state.id)
                })
                .ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "stored timer has no loaded rule definition",
                    )
                })?;
            let Some(timer) = rule.timer.as_mut() else {
                return Err(Error::new(
                    "source_integrity",
                    "stored timer has no loaded rule definition",
                ));
            };
            if timer.state.next_due_at != state.next_due_at {
                timer.armed_due_at_ms = None;
            }
            timer.state = state;
            if timer.blocked {
                continue;
            }

            let (scheduled_at_ms, next_due_at_ms, action, skip_code, armed_slot) =
                match timer_decision(timer, now)? {
                    TimerDecision::NotDue => continue,
                    TimerDecision::Skip {
                        scheduled_at_ms,
                        next_due_at_ms,
                        code,
                    } => (scheduled_at_ms, next_due_at_ms, None, Some(code), None),
                    TimerDecision::Fire {
                        scheduled_at_ms,
                        next_due_at_ms,
                        live_armed,
                    } => {
                        let prepared = match rule.definition.evaluate_timer() {
                            Ok(stop) => router.prepare_owner_submission(stop_command(stop)).await,
                            Err(error) if deterministic_rejection(&error.code) => Err(error),
                            Err(error) => return Err(error),
                        };
                        match prepared {
                            Ok(prepared) => (
                                scheduled_at_ms,
                                next_due_at_ms,
                                Some(prepared.request),
                                None,
                                live_armed.then_some(scheduled_at_ms),
                            ),
                            Err(error) if deterministic_rejection(&error.code) => (
                                scheduled_at_ms,
                                next_due_at_ms,
                                None,
                                Some(error.code),
                                None,
                            ),
                            Err(_error) if !router.rule_firings_open() => return Ok(false),
                            Err(error) => return Err(error),
                        }
                    }
                };

            let slot = TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms,
                action,
                skip_code,
            };
            let Some(admission) = router
                .commit_global_timer_slot(&timer.state, slot, now)
                .await?
            else {
                return Ok(false);
            };
            timer.state = admission.state;
            timer.blocked = admission.blocked;
            timer.armed_due_at_ms = if timer.blocked {
                armed_slot
                    .or(timer.armed_due_at_ms)
                    .filter(|due| timer.state.next_due_at == Some(*due))
            } else {
                timer.state.next_due_at.filter(|due| *due > now)
            };
        }

        if page_len < TIMER_PAGE_SIZE {
            return Ok(true);
        }
    }
}

fn timer_decision(timer: &ActiveTimer, now: i64) -> Result<TimerDecision> {
    let Some(due) = timer.state.next_due_at else {
        return Ok(TimerDecision::NotDue);
    };
    if due > now {
        return Ok(TimerDecision::NotDue);
    }

    let anchor = interval_anchor(&timer.state);
    if timer.armed_due_at_ms == Some(due) || due == now {
        return Ok(TimerDecision::Fire {
            scheduled_at_ms: due,
            next_due_at_ms: schedule::next_after(&timer.state.schedule, anchor, now)?,
            live_armed: true,
        });
    }

    let after = timer
        .state
        .last_key
        .unwrap_or_else(|| due.saturating_sub(1));
    let scheduled_at_ms = schedule::latest_due(&timer.state.schedule, anchor, after, now)?
        .ok_or_else(|| Error::new("source_integrity", "due timer has no due schedule slot"))?;
    match timer.catch_up {
        CatchUp::None => Ok(TimerDecision::Skip {
            scheduled_at_ms,
            next_due_at_ms: schedule::next_after(&timer.state.schedule, anchor, now)?,
            code: "missed".to_owned(),
        }),
        CatchUp::Latest => Ok(TimerDecision::Fire {
            scheduled_at_ms,
            next_due_at_ms: schedule::next_after(&timer.state.schedule, anchor, scheduled_at_ms)?,
            live_armed: false,
        }),
    }
}

fn interval_anchor(state: &TimerStateView) -> i64 {
    match &state.schedule {
        TimerSchedule::Interval { .. } => state.anchor,
        // Cron and once derive occurrences from their UTC schedule and cursor;
        // their persisted Store anchor is intentionally unused.
        TimerSchedule::Cron { .. } | TimerSchedule::Once { .. } => 0,
    }
}

fn next_timer_wait(rules: &[ActiveRule], now: i64) -> Option<Duration> {
    rules
        .iter()
        .filter_map(|rule| rule.timer.as_ref())
        .filter_map(|timer| timer_wake_delay(timer, now))
        .min()
}

fn timer_wake_delay(timer: &ActiveTimer, now: i64) -> Option<Duration> {
    if !timer.state.enabled || timer.blocked {
        return None;
    }
    let next_due = timer.state.next_due_at?;
    let delay_ms = (i128::from(next_due) - i128::from(now)).max(0);
    Some(Duration::from_millis(
        u64::try_from(delay_ms).unwrap_or(u64::MAX),
    ))
}

fn stop_command(stop: definition::StopAction) -> SubmitCommand {
    // This unbound request ID is used only by Router preparation. The atomic
    // Store rule admission assigns the deterministic rule/event child ID.
    SubmitCommand {
        id: String::new(),
        target: Some(stop.target),
        verb: "stop".to_owned(),
        args: json!({}),
        expected_generation: Some(stop.expected_generation),
        expected_turn_id: None,
        request_id: None,
        probe: false,
    }
}

fn deterministic_rejection(code: &str) -> bool {
    matches!(
        code,
        "bad_state"
            | "frame_too_large"
            | "invalid"
            | "not_allowed"
            | "not_found"
            | "secret_in_payload"
            | "stale_generation"
            | "unsupported"
    )
}

#[cfg(test)]
mod timer_tests {
    use super::*;

    fn timer() -> ActiveTimer {
        ActiveTimer {
            state: TimerStateView {
                id: "interval".to_owned(),
                schedule_revision: 1,
                schedule: TimerSchedule::Interval {
                    period_ms: 10,
                    anchor: None,
                },
                anchor: 100,
                last_key: None,
                next_due_at: Some(110),
                enabled: true,
            },
            catch_up: CatchUp::None,
            armed_due_at_ms: Some(110),
            blocked: false,
        }
    }

    #[test]
    fn startup_rejected_snapshot_is_not_deferred_and_new_pending_edit_reports_once() {
        let input = |digest| RuleDefinitionInput {
            id: "rule-a".to_owned(),
            canonical_definition: "{}".to_owned(),
            definition_digest: [digest; 32],
            enabled: true,
        };
        let first = input(1);
        let changed = input(2);
        let mut notices = ReloadNoticeContext::default();
        assert!(claim_notice(
            reload_notice_key(std::slice::from_ref(&first)).unwrap(),
            &mut notices.rejected,
        ));
        assert!(!claim_deferred_reload(std::slice::from_ref(&first), &mut notices).unwrap());
        assert!(claim_deferred_reload(std::slice::from_ref(&changed), &mut notices).unwrap());
        assert!(!claim_deferred_reload(std::slice::from_ref(&changed), &mut notices).unwrap());
    }

    #[test]
    fn new_once_timer_has_only_a_due_slot_strictly_after_registration() {
        let past = TimerSchedule::Once { at: 99 };
        assert_eq!(initial_next_due_at(&past, 100).unwrap(), None);

        let future = TimerSchedule::Once { at: 101 };
        assert_eq!(initial_next_due_at(&future, 100).unwrap(), Some(101));

        let at_boundary = TimerSchedule::Once { at: 100 };
        assert_eq!(initial_next_due_at(&at_boundary, 100).unwrap(), None);
    }

    #[test]
    fn live_armed_slot_fires_while_restart_catch_up_coalesces_and_blocked_waits() {
        let mut timer = timer();
        assert!(matches!(
            timer_decision(&timer, 135).unwrap(),
            TimerDecision::Fire {
                scheduled_at_ms: 110,
                next_due_at_ms: Some(140),
                live_armed: true,
            }
        ));

        timer.armed_due_at_ms = None;
        assert!(matches!(
            timer_decision(&timer, 135).unwrap(),
            TimerDecision::Skip {
                scheduled_at_ms: 130,
                next_due_at_ms: Some(140),
                ..
            }
        ));
        timer.catch_up = CatchUp::Latest;
        assert!(matches!(
            timer_decision(&timer, 135).unwrap(),
            TimerDecision::Fire {
                scheduled_at_ms: 130,
                next_due_at_ms: Some(140),
                live_armed: false,
            }
        ));

        timer.blocked = true;
        assert_eq!(timer_wake_delay(&timer, 135), None);
    }
}
