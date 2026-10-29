mod definition;
mod schedule;
pub(crate) use definition::RuleDefinition;

use crate::{common::now_ms, router::Router};
use eliot_core::{
    Error, Result, SubmitCommand,
    api::{CatchUp, RuleStateView, TimerSchedule, TimerStateView},
};
use eliot_store::{RuleCursorOutcome, RuleDefinitionInput, TimerDefinitionInput, TimerSlotCommit};
use serde_json::json;
use std::{
    collections::HashSet,
    fs::{self, File},
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::{broadcast, watch};

const RULE_EVENT_PAGE_SIZE: usize = 128;
const TIMER_PAGE_SIZE: usize = 128;

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
    timer: Option<ActiveTimer>,
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
) -> Result<Vec<ActiveRule>> {
    let (definitions, inputs) = load_global_rules(data_root)?;
    // One sample anchors every initial calculation and the Store seed. Preflight
    // every timer before rule initialization can persist any definition.
    let registration_now = now_ms();
    let mut timer_seeds: Vec<Option<(Option<TimerStateView>, Option<i64>)>> =
        Vec::with_capacity(definitions.len());
    for definition in &definitions {
        let seed = if let Some(schedule) = definition.schedule() {
            let existing = router.get_global_timer(&definition.id).await?;
            let next_due = match &existing {
                Some(timer) => timer.next_due_at,
                None => initial_next_due_at(schedule, registration_now)?,
            };
            Some((existing, next_due))
        } else {
            None
        };
        timer_seeds.push(seed);
    }

    let states = router
        .initialize_global_rules(&inputs, registration_now)
        .await?;
    if definitions.len() != states.len() {
        return Err(Error::new(
            "internal",
            "global rule initialization returned an incomplete state set",
        ));
    }
    router.install_global_rule_definitions(&definitions);
    let mut active = Vec::with_capacity(definitions.len());
    for ((definition, state), timer_seed) in definitions.into_iter().zip(states).zip(timer_seeds) {
        let timer = match (definition.schedule(), timer_seed) {
            (Some(schedule), Some((existing, initial_next_due_at))) => {
                let schedule_revision = existing
                    .as_ref()
                    .map_or(state.revision, |timer| timer.schedule_revision);
                let input = TimerDefinitionInput {
                    id: definition.id.clone(),
                    schedule_revision,
                    schedule: schedule.clone(),
                    enabled: definition.enabled,
                    initial_next_due_at,
                };
                let state = router
                    .initialize_global_timer(&input, registration_now)
                    .await?;
                Some(ActiveTimer {
                    armed_due_at_ms: state.next_due_at.filter(|due| *due >= registration_now),
                    state,
                    catch_up: *definition.catch_up(),
                    blocked: false,
                })
            }
            (None, None) => None,
            _ => {
                return Err(Error::new(
                    "internal",
                    "global rule timer preflight did not match its definition",
                ));
            }
        };
        active.push(ActiveRule {
            definition,
            state,
            timer,
        });
    }
    Ok(active)
}

pub(crate) async fn run_global_rules(
    router: Router,
    mut rules: Vec<ActiveRule>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if rules.is_empty() {
        return Ok(());
    }

    // Bus messages are wake hints; durable Store pages remain authoritative.
    // Register before the first high-water read so no event can fall between them.
    let mut events = router.subscribe_rule_events();
    let mut progress = router.progress_receiver();
    loop {
        if *shutdown.borrow() || !router.rule_firings_open() {
            return Ok(());
        }
        let high_water = router.rule_high_water().await?;

        for rule in &mut rules {
            if rule.state.current_cursor > high_water {
                return Err(Error::new(
                    "source_integrity",
                    "rule cursor is above the event high-water mark",
                ));
            }
            if rule.state.current_cursor == high_water {
                continue;
            }
            if rule.state.paused_reason.is_some() && rule.state.enabled {
                continue;
            }
            if !rule.state.enabled || !rule.definition.enabled {
                // One silent skipped range; cursor-only updates never feed the Bus.
                rule.state = router
                    .advance_rule_cursor(&rule.state, high_water, RuleCursorOutcome::Disabled)
                    .await?;
                continue;
            }

            while rule.state.current_cursor < high_water {
                if *shutdown.borrow() || !router.rule_firings_open() {
                    return Ok(());
                }
                let page = router
                    .read_rule_events(rule.state.current_cursor, high_water, RULE_EVENT_PAGE_SIZE)
                    .await?;
                if page.is_empty() {
                    return Err(Error::new(
                        "history_gap",
                        "rule cursor is behind unavailable event history",
                    ));
                }

                let mut no_match_through = None;
                for event in page {
                    if event.seq <= rule.state.current_cursor {
                        continue;
                    }
                    if event.kind != "turn.completed" {
                        no_match_through = Some(event.seq);
                        continue;
                    }

                    let stop = match definition::evaluate(&rule.definition, &event) {
                        Ok(Some(stop)) => stop,
                        Ok(None) => {
                            no_match_through = Some(event.seq);
                            continue;
                        }
                        Err(error) => {
                            flush_no_match(&router, rule, &mut no_match_through).await?;
                            rule.state = router
                                .advance_rule_cursor(
                                    &rule.state,
                                    event.seq,
                                    RuleCursorOutcome::Rejected { code: error.code },
                                )
                                .await?;
                            continue;
                        }
                    };

                    flush_no_match(&router, rule, &mut no_match_through).await?;
                    if !router.rule_firings_open() {
                        return Ok(());
                    }
                    let request = stop_command(stop);
                    let prepared = match router.prepare_owner_submission(request).await {
                        Ok(prepared) => prepared,
                        Err(_) if !router.rule_firings_open() => return Ok(()),
                        Err(error) if deterministic_rejection(&error.code) => {
                            rule.state = router
                                .advance_rule_cursor(
                                    &rule.state,
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
                        // Shutdown won the shared Store-lock ordering. Leave the
                        // cursor at this event; a later open daemon can process it.
                        return Ok(());
                    };
                    rule.state = admission.state;
                    // Router schedules only a new child after the commit lock drops.
                }
                flush_no_match(&router, rule, &mut no_match_through).await?;
            }
        }

        if !reconcile_due_timers(&router, &mut rules, now_ms()).await? {
            return Ok(());
        }

        let timer_wait = next_timer_wait(&rules, now_ms());
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
        TimerSchedule::Once { at } if *at == registration_now => Err(Error::new(
            "invalid",
            "once timer has no slot strictly after registration",
        )),
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

async fn flush_no_match(
    router: &Router,
    rule: &mut ActiveRule,
    through: &mut Option<i64>,
) -> Result<()> {
    if let Some(through) = through.take()
        && through > rule.state.current_cursor
    {
        rule.state = router
            .advance_rule_cursor(&rule.state, through, RuleCursorOutcome::NoMatch)
            .await?;
    }
    Ok(())
}

fn load_global_rules(
    data_root: &Path,
) -> Result<(Vec<definition::RuleDefinition>, Vec<RuleDefinitionInput>)> {
    let directory = data_root.join("rules");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok((Vec::new(), Vec::new())),
        Err(_) => return Err(Error::new("unavailable", "cannot enumerate global rules")),
    };

    let mut paths = Vec::<PathBuf>::new();
    for entry in entries {
        let entry =
            entry.map_err(|_| Error::new("unavailable", "cannot read global rule entry"))?;
        let file_type = entry
            .file_type()
            .map_err(|_| Error::new("unavailable", "cannot inspect global rule entry"))?;
        let path = entry.path();
        if file_type.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "toml")
        {
            paths.push(path);
        }
    }
    paths.sort();

    let mut ids = HashSet::new();
    let mut definitions = Vec::new();
    let mut inputs = Vec::new();
    for path in paths {
        let file = File::open(&path)
            .map_err(|_| Error::new("unavailable", "cannot open global rule file"))?;
        for definition in definition::load(file)? {
            if !ids.insert(definition.id.clone()) {
                return Err(Error::new(
                    "invalid",
                    "global rule files contain duplicate IDs",
                ));
            }
            let canonical_value = serde_json::to_value(&definition)
                .map_err(|_| Error::new("invalid", "rule definition cannot be encoded"))?;
            let canonical_definition = serde_json::to_string(&canonical_value)
                .map_err(|_| Error::new("invalid", "rule definition cannot be canonicalized"))?;
            let definition_digest = definition.semantic_digest()?;
            inputs.push(RuleDefinitionInput {
                id: definition.id.clone(),
                canonical_definition,
                definition_digest,
                enabled: definition.enabled,
            });
            definitions.push(definition);
        }
    }
    Ok((definitions, inputs))
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
    fn new_once_timer_has_only_a_due_slot_strictly_after_registration() {
        let past = TimerSchedule::Once { at: 99 };
        assert_eq!(initial_next_due_at(&past, 100).unwrap(), None);

        let future = TimerSchedule::Once { at: 101 };
        assert_eq!(initial_next_due_at(&future, 100).unwrap(), Some(101));

        let at_boundary = TimerSchedule::Once { at: 100 };
        assert_eq!(
            initial_next_due_at(&at_boundary, 100).unwrap_err().code,
            "invalid"
        );
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
