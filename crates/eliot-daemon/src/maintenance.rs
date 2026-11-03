//! Periodic physical-retention maintenance. Each tick reloads the daemon
//! retention settings and runs one bounded Store cleanup pass through the one
//! Store writer. Pruning is silent and never replays an effect; only counts
//! are logged.

use crate::{common::now_ms, config, logging};
use eliot_core::{Result, SecretPolicy};
use eliot_store::{RetentionReport, Store};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, watch};

/// Hourly cleanup is plenty for day-granularity retention windows.
pub(crate) const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3600);

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
        match run_once(&store, &config.retention, now_ms()).await {
            Ok(report)
                if report.events_deleted + report.commands_deleted + report.writes_deleted > 0 =>
            {
                let events = report.events_deleted.to_string();
                let pruned_through = report.pruned_through.to_string();
                let commands = report.commands_deleted.to_string();
                let writes = report.writes_deleted.to_string();
                logging::record(
                    &policy,
                    "daemon.retention_pruned",
                    &[
                        ("events", events.as_str()),
                        ("pruned_through", pruned_through.as_str()),
                        ("commands", commands.as_str()),
                        ("writes", writes.as_str()),
                    ],
                    None,
                );
            }
            Ok(_) => {}
            Err(error) => {
                logging::record(&policy, "daemon.retention_failed", &[], Some(&error));
            }
        }
    }
}

/// Run one bounded retention pass now. This is the maintenance tick body; the
/// loop above only schedules it.
pub(crate) async fn run_once(
    store: &Mutex<Store>,
    retention: &config::RetentionConfig,
    now: i64,
) -> Result<RetentionReport> {
    store
        .lock()
        .await
        .prune_retention(retention.events_days, retention.commands_days, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::SubmitCommand;
    use serde_json::json;

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
}
