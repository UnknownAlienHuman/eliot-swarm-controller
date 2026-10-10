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
const STORE_BUSY_RETRY: Duration = Duration::from_secs(1);

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
pub(crate) async fn run(store: Store, stopping: watch::Receiver<bool>) -> Result<()> {
    run_with_busy_hook(store, stopping, || {}).await
}

#[cfg(test)]
async fn run_with_busy_observer(
    store: Store,
    stopping: watch::Receiver<bool>,
    observed: tokio::sync::oneshot::Sender<()>,
    release_retry_status: std::sync::mpsc::Receiver<()>,
) -> Result<()> {
    let mut observed = Some(observed);
    let mut release_retry_status = Some(release_retry_status);
    run_with_busy_hook(store, stopping, move || {
        if let Some(sender) = observed.take() {
            let _ = sender.send(());
            if let Some(release) = release_retry_status.take() {
                let _ = release.recv();
            }
        }
    })
    .await
}

async fn run_with_busy_hook<F>(
    store: Store,
    mut stopping: watch::Receiver<bool>,
    mut on_busy: F,
) -> Result<()>
where
    F: FnMut() + Send + 'static,
{
    let mut changed = store.subscribe_schedule_changes();
    let mut busy_failures = 0_u32;
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        let pass = async {
            let next_due = reconcile_pass(&store).await?;
            if busy_failures > 0 {
                store
                    .record_legacy_worker_status("scheduler", "running", 0, None, None)
                    .await?;
            }
            Ok::<_, Error>(next_due)
        }
        .await;
        let next_due = match pass {
            Ok(next_due) => next_due,
            Err(error) if error.code == "STORE_BUSY" => {
                on_busy();
                busy_failures = busy_failures.saturating_add(1).min(32);
                let retry = STORE_BUSY_RETRY
                    .saturating_mul(1_u32 << busy_failures.saturating_sub(1).min(6))
                    .min(CLOCK_RECHECK);
                // Only SQLite's typed lock contention is retryable. A failed
                // pass grants no effect or cursor authority; the next pass
                // starts with retained evidence readback. Other Store and
                // transaction failures still reach the host supervisor.
                if let Err(status_error) = store
                    .record_legacy_worker_status(
                        "scheduler",
                        "retry_wait",
                        busy_failures,
                        Some(error.code),
                        Some(retry.as_millis() as u64),
                    )
                    .await
                {
                    if status_error.code != "STORE_BUSY" {
                        return Err(status_error);
                    }
                    eprintln!("scheduler retry status not persisted: STORE_BUSY");
                }
                tokio::select! {
                    stop_result = stopping.changed() => {
                        if stop_result.is_err() || *stopping.borrow() {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep(retry) => {}
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        busy_failures = 0;
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

async fn reconcile_pass(store: &Store) -> Result<Option<i64>> {
    // Reconcile retained CheckRun evidence before each catch-up pass. This
    // is readback only; normal supervisors remain responsible for launch.
    store.reconcile_checks_once().await?;
    let now = model::now_ms()?;
    let mut next_due = store
        .reconcile_automation_cron_once(MAX_SCHEDULES, now)
        .await?;
    if let Some(due) = store.reconcile_goals_once(now).await? {
        next_due = Some(next_due.map_or(due, |old: i64| old.min(due)));
    }
    for schedule in store.schedule_configs() {
        if let Some(due) = store.consider_scheduled(schedule, now).await? {
            next_due = Some(next_due.map_or(due, |old: i64| old.min(due)));
        }
    }
    Ok(next_due)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        model::{self, Principal},
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
    };
    use rusqlite::Connection;
    use serde_json::{Value, json};
    use std::{fs, path::PathBuf, sync::Arc};
    use tokio::sync::watch;

    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("scheduler-busy-{}", model::new_id()));
            fs::create_dir(&path).expect("create unique scheduler test directory");
            Self(path)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct Fixture {
        directory: ScratchDir,
        owner: StoreOwner,
        operator: Principal,
    }

    impl Fixture {
        async fn new() -> Self {
            let directory = ScratchDir::new();
            let root = DataRoot::acquire(&directory.0).expect("acquire fixture data root");
            let credential = bootstrap_credential(&root.path).expect("bootstrap fixture operator");
            let owner = StoreOwner::start(root, Arc::new(Config::default()), credential.clone())
                .await
                .expect("start real Store fixture");
            let operator = owner
                .store
                .authenticate(credential)
                .await
                .expect("authenticate fixture operator");
            Self {
                directory,
                owner,
                operator,
            }
        }

        async fn status(&self) -> Value {
            self.owner
                .store
                .call(self.operator.clone(), "host.status".to_owned(), json!({}))
                .await
                .expect("read host health through the real Store status path")
        }

        fn scheduler_health(status: &Value) -> &Value {
            &status["host_lifecycle"]["optional_workers"]["scheduler"]
        }

        async fn wait_for_scheduler_state(&self, expected: &str, timeout: Duration) -> Value {
            tokio::time::timeout(timeout, async {
                loop {
                    let status = self.status().await;
                    if Self::scheduler_health(&status)["state"].as_str() == Some(expected) {
                        return status;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("scheduler health reached the expected state")
        }

        async fn close(self) {
            let Self {
                directory,
                owner,
                operator: _,
            } = self;
            owner.close().await.expect("close fixture StoreOwner");
            drop(directory);
        }
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_busy_is_paced_health_readback_recovers_and_observes_stop() {
        let fixture = Fixture::new().await;
        let database_path = fixture.directory.0.join("swarm.db");
        let blocker = Connection::open(&database_path).expect("open independent SQLite writer");
        blocker
            .execute_batch("BEGIN IMMEDIATE;")
            .expect("hold the independent SQLite writer lock");

        let (stop, stopping) = watch::channel(false);
        let (busy_observed, observe_busy) = tokio::sync::oneshot::channel();
        let (release_retry_status, wait_for_release) = std::sync::mpsc::channel();
        let scheduler = tokio::spawn(run_with_busy_observer(
            fixture.owner.store.clone(),
            stopping,
            busy_observed,
            wait_for_release,
        ));
        tokio::time::timeout(Duration::from_secs(10), observe_busy)
            .await
            .expect("scheduler reached its typed STORE_BUSY branch")
            .expect("scheduler sent the bounded contention observation");
        // The observer fires after typed STORE_BUSY and gates the next health
        // write. Keep the lock through the independent read, then release both
        // the SQLite lock and test-only gate so retry_wait can be persisted.

        // host.status uses the independent Store status-reader thread; it
        // remains responsive while the writer is waiting on SQLite contention.
        let during_contention = tokio::time::timeout(Duration::from_secs(2), fixture.status())
            .await
            .expect("status reader remains responsive during writer contention");
        assert_eq!(
            during_contention["kernel_host"]["admission"].as_str(),
            Some("open")
        );
        assert!(
            !scheduler.is_finished(),
            "scheduler exited instead of retaining typed STORE_BUSY for retry"
        );
        blocker
            .execute_batch("COMMIT;")
            .expect("release the independent SQLite writer lock");
        release_retry_status
            .send(())
            .expect("release the scheduler before its retry health write");

        let retry_wait = fixture
            .wait_for_scheduler_state("retry_wait", Duration::from_secs(4))
            .await;
        assert_eq!(
            Fixture::scheduler_health(&retry_wait)["last_error_code"].as_str(),
            Some("STORE_BUSY")
        );
        assert_eq!(
            Fixture::scheduler_health(&retry_wait)["consecutive_failures"].as_u64(),
            Some(1)
        );
        let retry_observed_at = Instant::now();
        let retry_after = Fixture::scheduler_health(&retry_wait)["retry_after_ms"]
            .as_i64()
            .expect("retry receipt includes a bounded retry deadline");
        let remaining = retry_after.saturating_sub(model::now_ms().expect("read current time"));
        let maximum_retry_ms = i64::try_from(STORE_BUSY_RETRY.as_millis())
            .expect("configured retry interval fits in i64 milliseconds");
        assert!(
            (0..=maximum_retry_ms).contains(&remaining),
            "retry deadline is outside the configured paced interval: {remaining} ms"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        let still_waiting = fixture.status().await;
        assert_eq!(
            Fixture::scheduler_health(&still_waiting)["state"].as_str(),
            Some("retry_wait"),
            "scheduler retried before its paced backoff"
        );
        let running = fixture
            .wait_for_scheduler_state("running", Duration::from_secs(4))
            .await;
        assert!(
            retry_observed_at.elapsed() >= Duration::from_millis(700),
            "scheduler did not honor the one-second retry pace"
        );
        assert_eq!(
            Fixture::scheduler_health(&running)["consecutive_failures"].as_u64(),
            Some(0),
            "a successful reconciliation clears the retry counter"
        );
        assert!(
            !scheduler.is_finished(),
            "scheduler exited after recovering from typed STORE_BUSY"
        );

        stop.send(true)
            .expect("scheduler is still subscribed to stop");
        tokio::time::timeout(Duration::from_secs(2), scheduler)
            .await
            .expect("scheduler observes stop during its normal wait")
            .expect("scheduler task joins")
            .expect("scheduler exits cleanly after stop");
        fixture.close().await;
    }

    #[tokio::test]
    async fn non_busy_store_closed_error_remains_hard() {
        let fixture = Fixture::new().await;
        let Fixture {
            directory,
            owner,
            operator: _,
        } = fixture;
        let store = owner.store.clone();
        owner
            .close()
            .await
            .expect("close Store before scheduler starts");

        let (_stop, stopping) = watch::channel(false);
        let error = tokio::time::timeout(Duration::from_secs(2), run(store, stopping))
            .await
            .expect("closed Store fails promptly")
            .expect_err("non-BUSY Store failures remain fatal to this scheduler run");
        assert_eq!(error.code, "STORE_CLOSED");
        drop(directory);
    }
}
