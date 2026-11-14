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
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{
        NativeEvent, SourceBatch, SubmitCommand,
        api::{MailAckRequest, MailInboxRequest, MailLimits, MailMode, MailSendRequest},
    };
    use serde_json::json;

    async fn seed_settled_launches(
        store: &Mutex<Store>,
        count: usize,
        admitted_at: i64,
    ) -> Vec<String> {
        let mut guard = store.lock().await;
        let mut ids = Vec::with_capacity(count);
        for index in 0..count {
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({
                "project": format!("retention-tick-project-{index}"),
                "workspace": format!("retention-tick-workspace-{index}"),
                "harness": "codex",
                "profile": "fixture",
                "profile_snapshot": {},
            });
            launch.expected_generation = Some(0);
            let id = launch.id.clone();
            guard.submit("owner", launch, admitted_at).unwrap();
            guard.dispatch(&id, admitted_at).unwrap();
            guard
                .settle(&id, eliot_core::CommandState::Applied, None, admitted_at)
                .unwrap();
            ids.push(id);
        }
        ids
    }

    fn commands_retention() -> config::RetentionConfig {
        config::RetentionConfig {
            events_days: None,
            commands_days: Some(30),
            mail_days: None,
        }
    }

    #[tokio::test]
    async fn tick_drains_roots_across_passes() {
        let store = Mutex::new(Store::open(std::path::Path::new(":memory:")).unwrap());
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        let ids = seed_settled_launches(&store, 60, admitted_at).await;
        let report = run_once_with_budgets(
            &store,
            &commands_retention(),
            pruned_at,
            i64::MAX,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        assert_eq!(report.commands_deleted, 60);
        let guard = store.lock().await;
        for id in &ids {
            assert!(guard.get_command(id).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn tick_stops_at_row_budget() {
        let store = Mutex::new(Store::open(std::path::Path::new(":memory:")).unwrap());
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        let ids = seed_settled_launches(&store, 120, admitted_at).await;
        let report = run_once_with_budgets(
            &store,
            &commands_retention(),
            pruned_at,
            75,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        // Whole bounded passes: 50 + 50 reaches the 75-row budget.
        assert_eq!(report.commands_deleted, 100);
        let guard = store.lock().await;
        let remaining = ids
            .iter()
            .filter(|id| guard.get_command(id).unwrap().is_some())
            .count();
        assert_eq!(remaining, 20);
    }

    #[tokio::test]
    async fn tick_zero_time_budget_runs_one_pass() {
        let store = Mutex::new(Store::open(std::path::Path::new(":memory:")).unwrap());
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        let ids = seed_settled_launches(&store, 120, admitted_at).await;
        let report = run_once_with_budgets(
            &store,
            &commands_retention(),
            pruned_at,
            i64::MAX,
            Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(report.commands_deleted, 50);
        let guard = store.lock().await;
        let remaining = ids
            .iter()
            .filter(|id| guard.get_command(id).unwrap().is_some())
            .count();
        assert_eq!(remaining, 70);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tick_releases_store_lock_between_passes() {
        let store = Arc::new(Mutex::new(
            Store::open(std::path::Path::new(":memory:")).unwrap(),
        ));
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        let ids = seed_settled_launches(&store, 510, admitted_at).await;
        // Roots prune in id order within one created_at, so the smallest id
        // goes in the first pass and the largest only in the last data pass:
        // smallest-gone with largest-present is proof of a mid-tick lock
        // acquisition, with no timing assertions.
        let first = ids.iter().min().unwrap().clone();
        let last = ids.iter().max().unwrap().clone();
        let retention = commands_retention();
        let tick = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                run_once_with_budgets(
                    &store,
                    &retention,
                    pruned_at,
                    i64::MAX,
                    Duration::from_secs(60),
                )
                .await
            }
        });
        // Pure spin, no yield: the tick runs on another worker, and yielding
        // here would queue this observer behind the tick's own inter-pass
        // yield, so every probe would land while the tick holds the lock.
        // The tick's yields keep each free window long; the probe holds the
        // lock only for two reads.
        let mut saw_mid_tick = false;
        while !tick.is_finished() {
            if let Ok(guard) = store.try_lock() {
                let first_gone = guard.get_command(&first).unwrap().is_none();
                let last_present = guard.get_command(&last).unwrap().is_some();
                saw_mid_tick |= first_gone && last_present;
            }
        }
        let report = tick.await.unwrap().unwrap();
        assert_eq!(report.commands_deleted, 510);
        assert!(
            saw_mid_tick,
            "tick never released the Store lock between passes"
        );
    }

    #[tokio::test]
    async fn maintenance_prunes_and_replay_reports_history_gap() {
        let root =
            std::env::temp_dir().join(format!("eliot-retention-maintenance-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("config.toml"),
            "[daemon]\nretention.events_days = 30\nretention.commands_days = 30\n",
        )
        .unwrap();
        let config = config::load_daemon_config(&root).unwrap();
        let store = Mutex::new(Store::open(std::path::Path::new(":memory:")).unwrap());
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        let captured = {
            let mut store = store.lock().await;
            for index in 0..3 {
                let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
                launch.args = json!({
                    "project": format!("retention-project-{index}"),
                    "workspace": format!("retention-workspace-{index}"),
                    "harness": "codex",
                    "profile": "fixture",
                    "profile_snapshot": {},
                });
                launch.expected_generation = Some(0);
                store.submit("owner", launch, admitted_at).unwrap();
            }
            let value = json!({"id": "gap-rule", "enabled": true});
            let state = store
                .ensure_rule_state(
                    "owner",
                    &eliot_store::RuleDefinitionInput {
                        id: "gap-rule".to_owned(),
                        canonical_definition: serde_json::to_string(&value).unwrap(),
                        definition_digest: eliot_core::canonical_json_sha256(&value).unwrap(),
                        enabled: true,
                    },
                    admitted_at,
                )
                .unwrap();
            state.current_cursor
        };

        let report = run_once(&store, &config.retention, pruned_at)
            .await
            .unwrap();
        assert_eq!(report.events_deleted, captured);
        assert_eq!(report.pruned_through, captured);
        // The unstarted launches stay accepted: open work is never pruned.
        assert_eq!(report.commands_deleted, 0);

        let mut store = store.lock().await;
        let error = store.read_events(0, 100, None).unwrap_err();
        assert_eq!(error.code, "history_gap");
        let state = store.get_rule_state("owner", "gap-rule").unwrap().unwrap();
        let error = store
            .admit_rule_catch_up(
                "owner",
                &eliot_core::api::RuleCatchUpRequest::new(
                    ulid::Ulid::new().to_string(),
                    "gap-rule",
                    0,
                    Some(captured),
                ),
                &state,
                captured,
                &[],
                now_ms(),
            )
            .unwrap_err();
        assert_eq!(error.code, "history_gap");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn maintenance_prunes_acked_mail_and_reports_history_gap() {
        let root =
            std::env::temp_dir().join(format!("eliot-mail-maintenance-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("config.toml"),
            "[daemon]\nretention.mail_days = 30\n",
        )
        .unwrap();
        let config = config::load_daemon_config(&root).unwrap();
        assert_eq!(config.retention.mail_days, Some(30));
        let store = Mutex::new(Store::open(std::path::Path::new(":memory:")).unwrap());
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        let (principal, acked, unacked) = {
            let mut store = store.lock().await;
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({
                "project": "mail-retention-project",
                "workspace": "mail-retention-workspace",
                "harness": "codex",
                "profile": "fixture",
                "profile_snapshot": {},
            });
            launch.expected_generation = Some(0);
            let agent = store
                .submit("owner", launch.clone(), admitted_at)
                .unwrap()
                .command
                .target
                .unwrap();
            store.dispatch(&launch.id, admitted_at).unwrap();
            let mut acked = Vec::new();
            for _ in 0..3 {
                let sent = store
                    .mail_send(
                        "owner",
                        &MailSendRequest {
                            id: ulid::Ulid::new().to_string(),
                            to: vec![agent.clone()],
                            mode: MailMode::Queue,
                            text: "old".to_owned(),
                            thread: None,
                            reply_to_delivery: None,
                        },
                        MailLimits::default(),
                        admitted_at,
                    )
                    .unwrap()
                    .value;
                acked.push(sent.deliveries[0].delivery_id.clone());
            }
            let unacked = store
                .mail_send(
                    "owner",
                    &MailSendRequest {
                        id: ulid::Ulid::new().to_string(),
                        to: vec![agent.clone()],
                        mode: MailMode::Queue,
                        text: "keep".to_owned(),
                        thread: None,
                        reply_to_delivery: None,
                    },
                    MailLimits::default(),
                    admitted_at,
                )
                .unwrap()
                .value
                .deliveries[0]
                .delivery_id
                .clone();
            store
                .mail_ack(
                    &format!("agent:{agent}:1"),
                    &MailAckRequest {
                        id: ulid::Ulid::new().to_string(),
                        agent: None,
                        deliveries: acked.clone(),
                    },
                    admitted_at,
                )
                .unwrap();
            (format!("agent:{agent}:1"), acked, unacked)
        };

        let report = run_once(&store, &config.retention, pruned_at)
            .await
            .unwrap();
        assert_eq!(report.mail_deleted, 3);
        assert_eq!(report.events_deleted, 0);
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(report.writes_deleted, 0);

        let store = store.lock().await;
        let inbox = store
            .mail_inbox(
                &principal,
                &MailInboxRequest {
                    agent: None,
                    limit: None,
                    after: None,
                },
                pruned_at,
            )
            .unwrap();
        assert_eq!(inbox.deliveries.len(), 1);
        assert_eq!(inbox.deliveries[0].delivery_id, unacked);
        for delivery in &acked {
            let error = store
                .mail_read(
                    &principal,
                    &eliot_core::api::MailReadRequest {
                        agent: None,
                        delivery_id: delivery.clone(),
                    },
                    pruned_at,
                )
                .unwrap_err();
            assert_eq!(error.code, "history_gap");
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn tick_counts_mail_rows_against_the_row_budget() {
        let store = Mutex::new(Store::open(std::path::Path::new(":memory:")).unwrap());
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        seed_settled_launches(&store, 60, admitted_at).await;
        {
            let mut store = store.lock().await;
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({
                "project": "mail-budget-project",
                "workspace": "mail-budget-workspace",
                "harness": "codex",
                "profile": "fixture",
                "profile_snapshot": {},
            });
            launch.expected_generation = Some(0);
            let agent = store
                .submit("owner", launch.clone(), admitted_at)
                .unwrap()
                .command
                .target
                .unwrap();
            store.dispatch(&launch.id, admitted_at).unwrap();
            let principal = format!("agent:{agent}:1");
            for _ in 0..60 {
                let sent = store
                    .mail_send(
                        "owner",
                        &MailSendRequest {
                            id: ulid::Ulid::new().to_string(),
                            to: vec![agent.clone()],
                            mode: MailMode::Queue,
                            text: "old".to_owned(),
                            thread: None,
                            reply_to_delivery: None,
                        },
                        MailLimits::default(),
                        admitted_at,
                    )
                    .unwrap()
                    .value;
                store
                    .mail_ack(
                        &principal,
                        &MailAckRequest {
                            id: ulid::Ulid::new().to_string(),
                            agent: None,
                            deliveries: vec![sent.deliveries[0].delivery_id.clone()],
                        },
                        admitted_at,
                    )
                    .unwrap();
            }
        }
        // The first whole pass deletes 50 command roots and all 60 acked
        // rows: 110 counted rows reach the 75-row budget, so the tick stops
        // with 10 command roots left. Without mail in the budget the tick
        // would run a second pass and delete all 60 commands.
        let retention = config::RetentionConfig {
            events_days: None,
            commands_days: Some(30),
            mail_days: Some(30),
        };
        let report =
            run_once_with_budgets(&store, &retention, pruned_at, 75, Duration::from_secs(60))
                .await
                .unwrap();
        assert_eq!(report.commands_deleted, 50);
        assert_eq!(report.mail_deleted, 60);
    }

    #[tokio::test]
    async fn maintenance_without_windows_deletes_nothing() {
        let store = Mutex::new(Store::open(std::path::Path::new(":memory:")).unwrap());
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        let launch_id = {
            let mut store = store.lock().await;
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({
                "project": "retention-disabled-project",
                "workspace": "retention-disabled-workspace",
                "harness": "codex",
                "profile": "fixture",
                "profile_snapshot": {},
            });
            launch.expected_generation = Some(0);
            let launch_id = launch.id.clone();
            let agent = store
                .submit("owner", launch, admitted_at)
                .unwrap()
                .command
                .target
                .unwrap();
            store.dispatch(&launch_id, admitted_at).unwrap();
            crate::test_support::install_fixture_sources(
                &mut store,
                &agent,
                &launch_id,
                1,
                admitted_at,
            );
            store
                .put_batch(
                    SourceBatch {
                        batch_id: "disabled-ready".to_owned(),
                        agent_id: agent,
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "session.ready".to_owned(),
                            native_id: "disabled-ready".to_owned(),
                            data: json!({"command_id": launch_id}),
                        }],
                        cursor: None,
                    },
                    admitted_at,
                )
                .unwrap();
            store
                .settle(
                    &launch_id,
                    eliot_core::CommandState::Applied,
                    None,
                    admitted_at,
                )
                .unwrap();
            launch_id
        };

        let disabled = config::RetentionConfig::default();
        let report = run_once(&store, &disabled, pruned_at).await.unwrap();
        assert_eq!(report.events_deleted, 0);
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(report.writes_deleted, 0);
        assert!(
            store
                .lock()
                .await
                .get_command(&launch_id)
                .unwrap()
                .is_some()
        );

        // The same row prunes once its window is configured.
        let enabled = config::RetentionConfig {
            events_days: None,
            commands_days: Some(30),
            mail_days: None,
        };
        let report = run_once(&store, &enabled, pruned_at).await.unwrap();
        assert_eq!(report.commands_deleted, 1);
        assert!(
            store
                .lock()
                .await
                .get_command(&launch_id)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn run_now_reloads_config_and_reports_bounded_totals() {
        let root = std::env::temp_dir().join(format!("eliot-run-now-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("config.toml"),
            "[daemon]\nretention.commands_days = 30\n",
        )
        .unwrap();
        let store = Mutex::new(Store::open(std::path::Path::new(":memory:")).unwrap());
        let admitted_at = now_ms();
        let pruned_at = admitted_at + 31 * 24 * 60 * 60 * 1_000;
        let settled = seed_settled_launches(&store, 3, admitted_at).await;
        let open = {
            let mut guard = store.lock().await;
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({
                "project": "run-now-project",
                "workspace": "run-now-workspace",
                "harness": "codex",
                "profile": "fixture",
                "profile_snapshot": {},
            });
            launch.expected_generation = Some(0);
            let id = launch.id.clone();
            guard.submit("owner", launch, admitted_at).unwrap();
            id
        };
        let policy = Arc::new(SecretPolicy::new(["owner-credential"]));
        let report = run_now(&store, &root, &policy, pruned_at).await.unwrap();
        assert_eq!(report.commands_deleted, 3);
        assert_eq!(report.events_deleted, 0);
        {
            let guard = store.lock().await;
            for id in &settled {
                assert!(guard.get_command(id).unwrap().is_none());
            }
            // Open work is kept by the same pass.
            assert!(guard.get_command(&open).unwrap().is_some());
        }
        // A drained pass reports zeros and the same watermark.
        let drained = run_now(&store, &root, &policy, pruned_at).await.unwrap();
        assert_eq!(drained.commands_deleted, 0);
        assert_eq!(drained.events_deleted, 0);
        assert_eq!(drained.pruned_through, report.pruned_through);
        // A rejected settings reload fails instead of pruning under stale
        // windows.
        std::fs::write(
            root.join("config.toml"),
            "[daemon]\nretention.commands_days = 0\n",
        )
        .unwrap();
        assert!(run_now(&store, &root, &policy, pruned_at).await.is_err());
        let _ = std::fs::remove_dir_all(&root);
    }
}
