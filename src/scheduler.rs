//! Persistent, latest-only scheduler for configured and manager-owned CheckRuns.
//!
//! The scheduler never invokes a model or selects arbitrary Store methods. It
//! turns one due registry entry into the closed `ScheduleAction::CheckRun`
//! request and delegates admission, target checks, request receipts and state
//! persistence to Store-owned transactions. Manager calendars share this one
//! loop with the legacy interval registry.

use crate::{
    error::{Error, Result},
    model,
    store::Store,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};
use tokio::{sync::watch, time::Instant};

pub(crate) mod calendar;
pub(crate) use calendar::{CalendarOccurrence, CronSettings};

pub(crate) const MAX_SCHEDULES: usize = 64;
const MAX_SCHEDULE_ID_BYTES: usize = 64;
const CLOCK_RECHECK: Duration = Duration::from_secs(60);

/// Validate the cron settings stored on a manager-owned automation entry.
pub(crate) fn validate_cron_settings(settings: &CronSettings) -> Result<()> {
    calendar::validate_settings(settings)
}

/// A validated, operator-authored schedule. The action is a tagged closed
/// enum so config cannot name a Store method or submit arbitrary JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    pub schedule_id: String,
    #[serde(default)]
    pub enabled: bool,
    /// Unix epoch milliseconds. This is both the one-shot due time and the
    /// anchor for an interval schedule.
    pub anchor_ms: i64,
    /// `None` means one-shot. An interval is always fixed and wall-clock
    /// anchored; cron and calendar periods are intentionally unsupported.
    #[serde(default)]
    pub period_ms: Option<i64>,
    pub action: ScheduleAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScheduleAction {
    /// Repeat an existing, immutable candidate check for the pinned Attempt
    /// and Task revision. Profile identity is pinned by exact revision.
    CheckRun {
        attempt_id: String,
        expected_task_revision: i64,
        candidate_ref: String,
        profile_id: String,
        profile_revision: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DueSlot {
    pub slot: i64,
    pub due_at_ms: i64,
}

/// Validate the bounded schedule registry before Store/host startup.
pub(crate) fn validate_schedules(schedules: &[ScheduleConfig]) -> Result<()> {
    if schedules.len() > MAX_SCHEDULES {
        return Err(Error::invalid(format!(
            "at most {MAX_SCHEDULES} schedules are supported"
        )));
    }
    let mut ids = BTreeSet::new();
    for schedule in schedules {
        if schedule.schedule_id.is_empty()
            || schedule.schedule_id.len() > MAX_SCHEDULE_ID_BYTES
            || !schedule.schedule_id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_".contains(&byte)
            })
            || !ids.insert(schedule.schedule_id.as_str())
        {
            return Err(Error::invalid(
                "schedule IDs must be unique lowercase identifiers of at most 64 bytes",
            ));
        }
        if schedule.anchor_ms < 0 || schedule.period_ms.is_some_and(|period| period <= 0) {
            return Err(Error::invalid(
                "schedule anchor must be nonnegative and interval period must be positive",
            ));
        }
        match &schedule.action {
            ScheduleAction::CheckRun {
                attempt_id,
                expected_task_revision,
                candidate_ref,
                profile_id,
                profile_revision,
            } => {
                if uuid::Uuid::parse_str(attempt_id).is_err()
                    || *expected_task_revision < 1
                    || [candidate_ref, profile_id, profile_revision]
                        .iter()
                        .any(|value| value.trim().is_empty() || value.contains('\0'))
                {
                    return Err(Error::invalid(
                        "check_run schedules require an Attempt UUID, positive Task revision, candidate and exact profile revision",
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Return the latest due logical slot, never one of the intermediate missed
/// interval slots. `last_considered` suppresses re-admission of terminal or
/// structurally rejected slots. A drained schedule deliberately leaves that
/// cursor unchanged so resume can consider only the then-latest slot.
pub(crate) fn latest_due_slot(
    schedule: &ScheduleConfig,
    now_ms: i64,
    last_considered: Option<i64>,
) -> Result<Option<DueSlot>> {
    if !schedule.enabled || now_ms < schedule.anchor_ms {
        return Ok(None);
    }
    let (slot, due_at_ms) = match schedule.period_ms {
        None => (0, schedule.anchor_ms),
        Some(period) => {
            let elapsed = now_ms
                .checked_sub(schedule.anchor_ms)
                .ok_or_else(|| Error::invalid("schedule elapsed time overflow"))?;
            let slot = elapsed / period;
            let due_at_ms = schedule
                .anchor_ms
                .checked_add(
                    slot.checked_mul(period)
                        .ok_or_else(|| Error::invalid("schedule due time overflow"))?,
                )
                .ok_or_else(|| Error::invalid("schedule due time overflow"))?;
            (slot, due_at_ms)
        }
    };
    if last_considered.is_some_and(|last| slot <= last) {
        return Ok(None);
    }
    Ok(Some(DueSlot { slot, due_at_ms }))
}

pub(crate) fn next_due_at_ms(schedule: &ScheduleConfig, now_ms: i64) -> Result<Option<i64>> {
    if !schedule.enabled {
        return Ok(None);
    }
    if now_ms < schedule.anchor_ms {
        return Ok(Some(schedule.anchor_ms));
    }
    match schedule.period_ms {
        None => Ok(None),
        Some(period) => {
            let elapsed = now_ms
                .checked_sub(schedule.anchor_ms)
                .ok_or_else(|| Error::invalid("schedule elapsed time overflow"))?;
            let next_slot = (elapsed / period)
                .checked_add(1)
                .ok_or_else(|| Error::invalid("schedule slot overflow"))?;
            let offset = next_slot
                .checked_mul(period)
                .ok_or_else(|| Error::invalid("schedule due time overflow"))?;
            Ok(Some(schedule.anchor_ms.checked_add(offset).ok_or_else(
                || Error::invalid("schedule due time overflow"),
            )?))
        }
    }
}

/// Run the configured registry. The Store hook performs each eligibility
/// check and Operation receipt in its own single transaction. Store changes
/// wake the wait so admission-mode changes and completed checks are observed.
pub(crate) async fn run(store: Store, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let mut changed = store.subscribe_schedule_changes();
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        // Reconcile retained CheckRun evidence before each catch-up pass. This
        // is readback only; normal supervisors remain responsible for launch.
        store.reconcile_checks_once().await?;
        let now = model::now_ms()?;
        let schedules = store.schedule_configs();
        let mut next_due = store
            .reconcile_automation_cron_once(MAX_SCHEDULES, now)
            .await?;
        if let Some(due) = store.reconcile_goals_once(now).await? {
            next_due = Some(next_due.map_or(due, |old: i64| old.min(due)));
        }
        for schedule in schedules {
            if let Some(due) = store.consider_scheduled(schedule.clone(), now).await? {
                next_due = Some(next_due.map_or(due, |old: i64| old.min(due)));
            }
        }
        let delay = match next_due {
            Some(due) => Duration::from_millis(due.saturating_sub(model::now_ms()?).max(1) as u64)
                .min(CLOCK_RECHECK),
            None => CLOCK_RECHECK,
        };
        let deadline = Instant::now() + delay;
        tokio::select! {
            changed_result = changed.changed() => {
                if changed_result.is_err() {
                    return Err(Error::new("STORE_CLOSED", "scheduler change watch closed"));
                }
            }
            stop_result = stopping.changed() => {
                if stop_result.is_err() || *stopping.borrow() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep_until(deadline) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interval_schedule(anchor_ms: i64, period_ms: i64) -> ScheduleConfig {
        ScheduleConfig {
            schedule_id: "nightly-check".into(),
            enabled: true,
            anchor_ms,
            period_ms: Some(period_ms),
            action: ScheduleAction::CheckRun {
                attempt_id: "00000000-0000-4000-8000-000000000001".into(),
                expected_task_revision: 3,
                candidate_ref: "candidate-1".into(),
                profile_id: "strict".into(),
                profile_revision: "r1".into(),
            },
        }
    }

    #[test]
    fn interval_catch_up_returns_only_the_latest_due_slot() {
        let schedule = interval_schedule(1_000, 1_000);
        assert_eq!(
            latest_due_slot(&schedule, 9_750, None).unwrap(),
            Some(DueSlot {
                slot: 8,
                due_at_ms: 9_000,
            })
        );
        assert_eq!(latest_due_slot(&schedule, 9_750, Some(8)).unwrap(), None);
        assert_eq!(latest_due_slot(&schedule, 999, None).unwrap(), None);
        assert_eq!(next_due_at_ms(&schedule, 9_750).unwrap(), Some(10_000));
    }

    #[test]
    fn one_shot_has_one_slot_and_disabled_schedules_have_no_due_time() {
        let mut schedule = interval_schedule(5_000, 1_000);
        schedule.period_ms = None;
        assert_eq!(
            latest_due_slot(&schedule, 6_000, None)
                .unwrap()
                .unwrap()
                .slot,
            0
        );
        assert_eq!(latest_due_slot(&schedule, 6_000, Some(0)).unwrap(), None);
        assert_eq!(next_due_at_ms(&schedule, 6_000).unwrap(), None);
        schedule.enabled = false;
        assert_eq!(latest_due_slot(&schedule, 9_000, None).unwrap(), None);
        assert_eq!(next_due_at_ms(&schedule, 9_000).unwrap(), None);
    }

    #[test]
    fn validation_rejects_duplicate_ids_and_nonpositive_periods() {
        let mut schedule = interval_schedule(1_000, 0);
        assert_eq!(
            validate_schedules(&[schedule.clone()]).unwrap_err().code,
            "INVALID_PARAMS"
        );
        schedule.period_ms = None;
        assert!(validate_schedules(&[schedule.clone(), schedule]).is_err());
    }
}
