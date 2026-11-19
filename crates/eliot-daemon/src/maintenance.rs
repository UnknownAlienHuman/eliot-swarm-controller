//! Periodic physical-retention maintenance. Each hourly wake reloads the
//! daemon retention settings, then runs bounded retention epochs through
//! the one Store writer until an epoch drains: an epoch that exhausts its
//! budgets while still deleting rows continues cooperatively instead of
//! leaving eligible backlog until the next hour. Pruning is silent and
//! never replays an effect; only counts are logged.

use crate::{common::now_ms, config, logging};
use eliot_core::{Result, SecretPolicy};
use eliot_store::{RetentionReport, Store};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, watch};

/// Hourly cleanup is plenty for day-granularity retention windows.
pub(crate) const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3600);
/// One retention epoch loops bounded passes until a pass deletes nothing
/// or a budget runs out.
pub(crate) const RETENTION_TICK_ROW_BUDGET: i64 = 10_000;
pub(crate) const RETENTION_TICK_TIME_BUDGET: Duration = Duration::from_secs(30);

pub(crate) async fn run(
    store: Arc<Mutex<Store>>,
    data_root: PathBuf,
    policy: Arc<SecretPolicy>,
    initial: config::DaemonConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut config = initial;
    let mut ticker = tokio::time::interval(MAINTENANCE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick: maintenance runs on the interval, not at
    // startup.
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
        }
        if *shutdown.borrow() {
            return Ok(());
        }
        match config::load_daemon_config(&data_root) {
            Ok(reloaded) => config = reloaded,
            Err(error) => {
                logging::record(
                    &policy,
                    "daemon.retention_config_rejected",
                    &[],
                    Some(&error),
                );
            }
        }
        // Retention catch-up: while an epoch spends a budget, run another
        // instead of leaving eligible backlog until the next hourly wake.
        // Only retention repeats here; the config reload above stays hourly.
        // Epoch failures are already logged inside; the next hourly wake
        // retries.
        run_epochs(&store, &config.retention, &policy, now_ms(), || {
            shutdown.has_changed().is_err() || *shutdown.borrow()
        })
        .await
        .ok();
        // Unbounded-growth visibility, at most one line per hourly pass:
        // while every retention window is unset and the store passes the
        // documented size, say so. Retention semantics never change here;
        // the exact sizes stay in `daemon.status`.
        if unbounded_store_over_threshold(
            &config.retention,
            &data_root,
            config::RETENTION_UNBOUNDED_SIZE_ADVISORY_BYTES,
        ) {
            logging::record(&policy, "daemon.retention_unbounded_store_large", &[], None);
        }
    }
}

/// True when every retention window is unset and the main+WAL store files
/// pass `threshold_bytes`. Missing files read as zero bytes. Retention
/// semantics never change here: this only decides whether the unbounded
/// advisory is due.
pub(crate) fn unbounded_store_over_threshold(
    retention: &config::RetentionConfig,
    data_root: &Path,
    threshold_bytes: u64,
) -> bool {
    if retention.events_days.is_some()
        || retention.commands_days.is_some()
        || retention.mail_days.is_some()
    {
        return false;
    }
    let (db_bytes, wal_bytes) = store_file_sizes(data_root);
    db_bytes.saturating_add(wal_bytes) > threshold_bytes
}

/// Main+WAL store file sizes for status and retention visibility. Missing
/// files read as zero bytes; sizes are informational and never gate reads.
pub(crate) fn store_file_sizes(data_root: &Path) -> (u64, u64) {
    let db_path = data_root.join("eliot.db");
    let db_bytes = std::fs::metadata(&db_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let wal_bytes = std::fs::metadata(db_path.with_extension("db-wal"))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    (db_bytes, wal_bytes)
}

/// Owner-triggered pass (`maintenance.run`): reload the daemon retention
/// settings, then run the same bounded epochs as the hourly tick. A rejected
/// reload fails the call so the owner fixes the config instead of pruning
/// under stale windows.
pub(crate) async fn run_now(
    store: &Mutex<Store>,
    data_root: &Path,
    policy: &Arc<SecretPolicy>,
    now: i64,
) -> Result<RetentionReport> {
    let config = config::load_daemon_config(data_root)?;
    run_epochs(store, &config.retention, policy, now, || false).await
}

/// Run bounded retention epochs until an epoch drains or a budget runs out:
/// the shared body of the hourly tick and the owner-triggered pass. Totals
/// sum every epoch; `pruned_through` is the last epoch's watermark.
/// `stop_requested` is checked at the top of every epoch; the tick wires it
/// to daemon shutdown, the RPC path never stops early.
pub(crate) async fn run_epochs(
    store: &Mutex<Store>,
    retention: &config::RetentionConfig,
    policy: &Arc<SecretPolicy>,
    now: i64,
    mut stop_requested: impl FnMut() -> bool,
) -> Result<RetentionReport> {
    let mut total = RetentionReport {
        events_deleted: 0,
        pruned_through: 0,
        commands_deleted: 0,
        writes_deleted: 0,
        mail_deleted: 0,
        digests_deleted: 0,
    };
    loop {
        // Shutdown is checked at the top of every epoch, so a signal
        // arriving during the inter-epoch yield below still stops the
        // wake before another deletion epoch runs.
        if stop_requested() {
            return Ok(total);
        }
        let epoch_start = std::time::Instant::now();
        let report = match run_once(store, retention, now).await {
            Ok(report) => report,
            Err(error) => {
                logging::record(policy, "daemon.retention_failed", &[], Some(&error));
                return Err(error);
            }
        };
        total.events_deleted += report.events_deleted;
        total.pruned_through = report.pruned_through;
        total.commands_deleted += report.commands_deleted;
        total.writes_deleted += report.writes_deleted;
        total.mail_deleted += report.mail_deleted;
        total.digests_deleted += report.digests_deleted;
        let rows = report.events_deleted
            + report.commands_deleted
            + report.writes_deleted
            + report.mail_deleted
            + report.digests_deleted;
        if rows > 0 {
            logging::record_counts(
                policy,
                "daemon.retention_pruned",
                logging::Counts {
                    events: report.events_deleted.max(0) as u64,
                    pruned_through: report.pruned_through.max(0) as u64,
                    commands: report.commands_deleted.max(0) as u64,
                    writes: report.writes_deleted.max(0) as u64,
                    mail: report.mail_deleted.max(0) as u64,
                    digests: report.digests_deleted.max(0) as u64,
                },
                None,
            );
        }
        // Zero rows always means drained, even if lock waits pushed
        // elapsed time past the budget. Otherwise a spent budget means
        // more retention may be pending; a final empty epoch is harmless.
        let exhausted = rows >= RETENTION_TICK_ROW_BUDGET
            || epoch_start.elapsed() >= RETENTION_TICK_TIME_BUDGET;
        if rows == 0 || !exhausted {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(total)
}

/// Run one retention epoch now: bounded passes under the tick row and
/// time budgets.
pub(crate) async fn run_once(
    store: &Mutex<Store>,
    retention: &config::RetentionConfig,
    now: i64,
) -> Result<RetentionReport> {
    run_once_with_budgets(
        store,
        retention,
        now,
        RETENTION_TICK_ROW_BUDGET,
        RETENTION_TICK_TIME_BUDGET,
    )
    .await
}

/// Run bounded passes until a pass deletes nothing or the tick budgets run
/// out. At least one pass always runs. `now` stays fixed so every pass
/// shares the same cutoffs. The Store lock is taken per bounded pass and
/// released between passes, so one epoch never holds the lock for the whole
/// time budget.
pub(crate) async fn run_once_with_budgets(
    store: &Mutex<Store>,
    retention: &config::RetentionConfig,
    now: i64,
    row_budget: i64,
    time_budget: Duration,
) -> Result<RetentionReport> {
    let start = std::time::Instant::now();
    let mut total = RetentionReport {
        events_deleted: 0,
        pruned_through: 0,
        commands_deleted: 0,
        writes_deleted: 0,
        mail_deleted: 0,
        digests_deleted: 0,
    };
    loop {
        let pass = {
            let mut guard = store.lock().await;
            guard.prune_retention(
                retention.events_days,
                retention.commands_days,
                retention.mail_days,
                now,
            )?
        };
        total.events_deleted += pass.events_deleted;
        total.pruned_through = pass.pruned_through;
        total.commands_deleted += pass.commands_deleted;
        total.writes_deleted += pass.writes_deleted;
        total.mail_deleted += pass.mail_deleted;
        total.digests_deleted += pass.digests_deleted;
        let pass_rows = pass.events_deleted
            + pass.commands_deleted
            + pass.writes_deleted
            + pass.mail_deleted
            + pass.digests_deleted;
        if pass_rows == 0 {
            break;
        }
        let total_rows = total.events_deleted
            + total.commands_deleted
            + total.writes_deleted
            + total.mail_deleted
            + total.digests_deleted;
        if total_rows >= row_budget || start.elapsed() >= time_budget {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(total)
}
