//! Periodic physical-retention maintenance. Each configured wake reloads
//! daemon retention settings and runs bounded epochs through the one Store
//! writer, releasing its mutex between small batches. Pruning is silent and
//! never replays an effect; row and space-reclaim outcomes are logged.

use crate::{common::now_ms, config, daemon::workers::WorkerReporter, logging};
use eliot_core::{Result, SecretPolicy};
use eliot_store::{RetentionReport, Store};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, watch};

/// One retention epoch loops bounded passes until a pass deletes nothing
/// or a budget runs out.
pub(crate) const RETENTION_TICK_ROW_BUDGET: i64 = 10_000;
pub(crate) const RETENTION_TICK_TIME_BUDGET: Duration = Duration::from_secs(30);

/// Known provider resets and due probes are serviced within one second.
pub(crate) const PROVIDER_HOLD_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) async fn run(
    store: Arc<Mutex<Store>>,
    data_root: PathBuf,
    policy: Arc<SecretPolicy>,
    initial: config::DaemonConfig,
    router: crate::router::Router,
    shutdown: watch::Receiver<bool>,
    reporter: WorkerReporter,
) -> Result<()> {
    router.recover_ended_provider_probes(&shutdown).await?;
    reporter.running("maintenance");
    let reactions_enabled = initial.attention.provider_refusal.enabled;
    tokio::try_join!(
        run_retention(
            store,
            data_root,
            Arc::clone(&policy),
            initial,
            shutdown.clone()
        ),
        run_provider_maintenance(router, policy, shutdown, reactions_enabled),
    )?;
    Ok(())
}

async fn run_provider_maintenance(
    router: crate::router::Router,
    policy: Arc<SecretPolicy>,
    mut shutdown: watch::Receiver<bool>,
    reactions_enabled: bool,
) -> Result<()> {
    let mut provider_ticker = tokio::time::interval(PROVIDER_HOLD_INTERVAL);
    provider_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut provider_probe_tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = provider_ticker.tick() => {
                let available = eliot_store::PROVIDER_PROBE_BATCH_LIMIT
                    .saturating_sub(provider_probe_tasks.len());
                let available = if reactions_enabled { available } else { 0 };
                match router.maintain_provider_holds(available).await {
                    Ok(work) => {
                        for work in work {
                            let probe_router = router.clone();
                            let probe_shutdown = shutdown.clone();
                            provider_probe_tasks.spawn(async move {
                                probe_router.finish_provider_probe(work, probe_shutdown).await
                            });
                        }
                    }
                    Err(error) => {
                        logging::record(&policy, "daemon.provider_hold_maintenance_failed", &[], Some(&error));
                    }
                }
                if reactions_enabled
                    && let Err(error) = router.notify_provider_attention().await
                {
                    logging::record(&policy, "daemon.provider_notice_failed", &[], Some(&error));
                }
                continue;
            }
            joined = provider_probe_tasks.join_next(), if !provider_probe_tasks.is_empty() => {
                match joined {
                    Some(Ok(Err(error))) => {
                        logging::record(&policy, "daemon.provider_probe_cleanup_failed", &[], Some(&error));
                    }
                    Some(Err(_)) => {
                        logging::record(&policy, "daemon.provider_probe_cleanup_task_failed", &[], None);
                    }
                    _ => {}
                }
                continue;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    while let Some(joined) = provider_probe_tasks.join_next().await {
                        if let Ok(Err(error)) = joined {
                            logging::record(&policy, "daemon.provider_probe_cleanup_failed", &[], Some(&error));
                        }
                    }
                    return Ok(());
                }
                continue;
            }
        }
    }
}

async fn run_retention(
    store: Arc<Mutex<Store>>,
    data_root: PathBuf,
    policy: Arc<SecretPolicy>,
    initial: config::DaemonConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut config = initial;
    let mut interval_seconds = config.retention.maintenance_interval_seconds();
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_seconds));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Tokio's first tick is immediate: do one bounded startup pass, then keep
    // the configured cadence for subsequent passes.
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
            Ok(reloaded) => {
                let next_interval_seconds = reloaded.retention.maintenance_interval_seconds();
                config = reloaded;
                if next_interval_seconds != interval_seconds {
                    let mut next_ticker =
                        tokio::time::interval(Duration::from_secs(next_interval_seconds));
                    next_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    next_ticker.tick().await;
                    ticker = next_ticker;
                    interval_seconds = next_interval_seconds;
                }
            }
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
        // instead of leaving eligible backlog until the next configured wake.
        // Epoch failures are already logged inside; the next wake retries.
        run_epochs(
            &store,
            &data_root,
            &config.retention,
            &policy,
            now_ms(),
            || shutdown.has_changed().is_err() || *shutdown.borrow(),
        )
        .await
        .ok();
        // Keep the legacy unbounded-growth notice for any future policy that
        // disables both event and process retention; current defaults are finite.
        if unbounded_store_over_threshold(
            &config.retention,
            &data_root,
            config::RETENTION_UNBOUNDED_SIZE_ADVISORY_BYTES,
        ) {
            logging::record(&policy, "daemon.retention_unbounded_store_large", &[], None);
        }
    }
}

/// True when no retention window is active and the main+WAL store files
/// pass `threshold_bytes`. Missing files read as zero bytes. Retention
/// semantics never change here: this only decides whether the unbounded
/// advisory is due.
pub(crate) fn unbounded_store_over_threshold(
    retention: &config::RetentionConfig,
    data_root: &Path,
    threshold_bytes: u64,
) -> bool {
    if retention.events_retention_seconds() > 0
        || retention.processes_retention_seconds() > 0
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
/// settings, then run one bounded epoch, so the call returns inside the
/// client deadline; a larger backlog continues on the configured tick. A
/// rejected reload fails the call so the owner fixes the config instead of
/// pruning under stale windows.
pub(crate) async fn run_now(
    store: &Mutex<Store>,
    data_root: &Path,
    policy: &Arc<SecretPolicy>,
    now: i64,
) -> Result<RetentionReport> {
    let config = config::load_daemon_config(data_root)?;
    let mut epochs = 0;
    run_epochs(
        store,
        data_root,
        &config.retention,
        policy,
        now,
        move || {
            epochs += 1;
            epochs > 1
        },
    )
    .await
}

/// Run bounded retention epochs until an epoch drains or a budget runs out:
/// the shared body of the periodic tick and the owner-triggered pass. Totals
/// sum every epoch; `pruned_through` is the last epoch's watermark.
/// `stop_requested` is checked at the top of every epoch; the tick wires it
/// to daemon shutdown, the RPC path stops after its first epoch.
async fn run_epochs(
    store: &Mutex<Store>,
    data_root: &Path,
    retention: &config::RetentionConfig,
    policy: &Arc<SecretPolicy>,
    now: i64,
    mut stop_requested: impl FnMut() -> bool,
) -> Result<RetentionReport> {
    let mut total = RetentionReport::default();
    loop {
        // Shutdown is checked at the top of every epoch, so a signal
        // arriving during the inter-epoch yield below still stops the
        // wake before another deletion epoch runs.
        if stop_requested() {
            return Ok(total);
        }
        let epoch_start = std::time::Instant::now();
        let mut report = match run_once(store, retention, now).await {
            Ok(report) => report,
            Err(error) => {
                logging::record(policy, "daemon.retention_failed", &[], Some(&error));
                return Err(error);
            }
        };
        let rows = report.rows_deleted();
        if rows > 0 || report.pages_reclaimed > 0 {
            let path = data_root.join("eliot.db");
            let result =
                tokio::task::spawn_blocking(move || Store::checkpoint_retention(&path)).await;
            match result {
                Ok(Ok((frames, checkpointed, busy))) => {
                    report.wal_frames = frames;
                    report.checkpointed_frames = checkpointed;
                    report.checkpoint_busy = busy;
                }
                Ok(Err(error)) => {
                    logging::record(
                        policy,
                        "daemon.retention_checkpoint_failed",
                        &[],
                        Some(&error),
                    );
                }
                Err(_) => {
                    logging::record(policy, "daemon.retention_checkpoint_failed", &[], None);
                }
            }
        }
        total.accumulate(report);
        if rows > 0 || report.pages_reclaimed > 0 || report.checkpoint_busy {
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
                    processes: report.processes_deleted.max(0) as u64,
                    pages_reclaimed: report.pages_reclaimed.max(0) as u64,
                    freelist_pages: report.freelist_pages.max(0) as u64,
                    auto_vacuum_mode: report.auto_vacuum_mode.max(0) as u64,
                    wal_frames: report.wal_frames.max(0) as u64,
                    checkpointed_frames: report.checkpointed_frames.max(0) as u64,
                    checkpoint_busy: if report.checkpoint_busy { 1 } else { 0 },
                },
                None,
            );
        }
        let exhausted = report.work_count() >= RETENTION_TICK_ROW_BUDGET
            || epoch_start.elapsed() >= RETENTION_TICK_TIME_BUDGET;
        if !exhausted || (rows == 0 && report.pages_reclaimed == 0 && !report.process_scan_more) {
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
    let mut total = RetentionReport::default();
    loop {
        let pass = {
            let mut guard = store.lock().await;
            guard.prune_retention(
                retention.events_retention_seconds(),
                retention.processes_retention_seconds(),
                retention.commands_days,
                retention.mail_days,
                now,
            )?
        };
        total.accumulate(pass);
        if pass.rows_deleted() == 0 && pass.pages_reclaimed == 0 && !pass.process_scan_more {
            break;
        }
        if total.work_count() >= row_budget || start.elapsed() >= time_budget {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(total)
}
