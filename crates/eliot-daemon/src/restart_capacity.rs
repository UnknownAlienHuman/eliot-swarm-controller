//! Restart capacity recovery. A generation that a daemon replacement left
//! `lost` or `stopped` keeps its reservation until a later daemon verifies,
//! outside the Store lock, that its custody closed.
//!
//! Neither startup nor rechecks ever hold the whole unresolved fleet: one
//! pass reads one Store page at a time with a cursor and reconciles each page
//! in bounded batches, so resident memory stays proportional to one batch.

use crate::{
    bus::Bus,
    common::now_ms,
    logging,
    platform::{RestartCustody, restart_custody},
    router::Router,
};
use eliot_core::{Error, Result, SecretPolicy};
use eliot_store::{RetainedGeneration, Store};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

const RECHECK: Duration = Duration::from_secs(2);

/// Generations reconciled per batch. Below the Store page size so one page
/// fans out to several batches; the fleet is never materialized.
pub(crate) const RECONCILE_BATCH_SIZE: usize = 32;

/// Outcome of one bounded pass over the retained fleet.
#[derive(Debug, Default)]
pub(crate) struct PassReport {
    /// Retained generations observed across every page.
    pub processed: usize,
    /// Batches reconciled.
    pub batches: usize,
    /// Largest batch held at once; never exceeds [`RECONCILE_BATCH_SIZE`].
    pub max_batch: usize,
    /// Generations still unresolved at the end of the pass.
    pub retained: usize,
}

/// First startup pass: page through every retained generation with a cursor
/// and reconcile each page in bounded batches. Call before IPC bind, so that
/// no generation started by this daemon can be among them.
pub(crate) async fn reconcile_first_pass(context: &Reconcile) -> Result<PassReport> {
    reconcile_pass(context, true).await
}

async fn reconcile_pass(context: &Reconcile, first_pass: bool) -> Result<PassReport> {
    let mut report = PassReport::default();
    let mut after: Option<String> = None;
    loop {
        let page = {
            let store = context.store.lock().await;
            store.restart_retained_generations(after.as_deref())?
        };
        if page.is_empty() {
            break;
        }
        after = page.last().map(|generation| generation.agent.clone());
        report.processed += page.len();
        for batch in page.chunks(RECONCILE_BATCH_SIZE) {
            report.batches += 1;
            report.max_batch = report.max_batch.max(batch.len());
            report.retained += reconcile(context, batch.to_vec(), first_pass).await.len();
        }
    }
    Ok(report)
}

/// Release each generation in one bounded batch whose custody is verified
/// closed and return the rest. A failure for one generation keeps it retained
/// and is logged; it never stops startup or the other generations. The first
/// pass also records why each remaining reservation stays.
pub(crate) async fn reconcile(
    context: &Reconcile,
    pending: Vec<RetainedGeneration>,
    first_pass: bool,
) -> Vec<RetainedGeneration> {
    let mut remaining = Vec::new();
    for generation in pending {
        let custody = restart_custody(&context.namespace, &generation).unwrap_or_else(|error| {
            context.log("daemon.restart_custody_probe_failed", &generation, &error);
            RestartCustody::Retained {
                missing: "custody_probe_failed",
            }
        });
        let now = now_ms();
        let committed = match custody {
            RestartCustody::Closed {
                evidence,
                orphan_possible,
            } => context
                .store
                .lock()
                .await
                .release_restart_closed_generation(
                    &generation.agent,
                    generation.generation,
                    evidence,
                    orphan_possible,
                    now,
                ),
            RestartCustody::Retained { missing } if first_pass => {
                context.store.lock().await.record_restart_capacity_retained(
                    &generation.agent,
                    generation.generation,
                    missing,
                    now,
                )
            }
            RestartCustody::Retained { .. } => Ok(Vec::new()),
        };
        let released = matches!(custody, RestartCustody::Closed { .. });
        match committed {
            Ok(events) => {
                context.bus.publish(&events);
                context.router.note_events(&events);
                if !released {
                    remaining.push(generation);
                }
            }
            Err(error) => {
                context.log("daemon.restart_capacity_failed", &generation, &error);
                remaining.push(generation);
            }
        }
    }
    remaining
}

/// Recheck retained generations until a full bounded pass retains none; Unix
/// plugin groups do not die with the daemon and may exit after it restarts.
/// Each pass re-pages the Store with a cursor instead of holding the fleet,
/// so later passes also converge generations lost after startup. A failed
/// page read is logged and retried on the next tick; it never ends the loop.
pub(crate) async fn recheck(context: Reconcile) {
    loop {
        tokio::time::sleep(RECHECK).await;
        match reconcile_pass(&context, false).await {
            Ok(report) if report.retained == 0 => break,
            Ok(_) => {}
            Err(error) => logging::record(
                &context.policy,
                "daemon.restart_capacity_recheck_failed",
                &[],
                Some(&error),
            ),
        }
    }
}

pub(crate) struct Reconcile {
    pub store: Arc<Mutex<Store>>,
    pub bus: Bus,
    pub router: Router,
    pub policy: Arc<SecretPolicy>,
    pub namespace: String,
}

impl Reconcile {
    fn log(&self, event: &str, generation: &RetainedGeneration, error: &Error) {
        let generation_text = generation.generation.to_string();
        logging::record(
            &self.policy,
            event,
            &[
                ("agent", generation.agent.as_str()),
                ("generation", generation_text.as_str()),
            ],
            Some(error),
        );
    }
}
#[cfg(test)]
mod bounded_recovery_tests {
    use super::*;
    use eliot_core::{CommandState, NativeEvent, SourceBatch, SubmitCommand};
    use serde_json::json;
    use std::collections::BTreeMap;

    /// One stopped generation that keeps its capacity reservation, mirroring
    /// the Store stopped-capacity fixture through public API only.
    fn stopped_capacity_agent(store: &mut Store, project: &str, now: i64) -> String {
        let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        launch.args = json!({
            "project": project,
            "harness": "codex",
            "profile": "restart-page",
            "workspace": format!("synthetic-{project}"),
            "profile_snapshot": {"capacity_scopes": [{"scope": "profile:restart-page", "max_live": null}]},
        });
        let launch_id = launch.id.clone();
        let agent = store
            .submit("owner", launch, now)
            .expect("admit launch")
            .command
            .target
            .expect("launch Agent");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        let plan: BTreeMap<String, String> = ["session", "turn", "message", "tool", "request"]
            .into_iter()
            .map(|family| (family.to_owned(), "stream".to_owned()))
            .collect();
        store
            .install_plugin_sources(&agent, 1, &launch_id, &plan, now)
            .expect("install source plan");
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:ready"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        kind: "session.ready".to_owned(),
                        native_id: format!("{project}:ready"),
                        data: json!({
                            "command_id": launch_id,
                            "native_scope": format!("capacity-scope:{project}"),
                            "native_ref": format!("capacity-ref:{project}"),
                        }),
                        ts: None,
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle launch");
        store
            .ensure_process_source(&agent, 1, now + 1)
            .expect("install OS process source");
        let stop_id = ulid::Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: stop_id.clone(),
                    target: Some(agent.clone()),
                    selector: None,
                    verb: "stop".to_owned(),
                    args: json!({"force": true}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now + 2,
            )
            .expect("admit forced stop");
        store.dispatch(&stop_id, now + 2).expect("dispatch stop");
        store
            .settle(&stop_id, CommandState::Applied, None, now + 3)
            .expect("forced stop applied");
        agent
    }

    #[tokio::test]
    async fn first_pass_reconciles_more_than_one_page_in_bounded_batches() {
        const RETAINED_AGENTS: usize = 128 + 24;
        let mut store =
            eliot_store::Store::open(std::path::Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(ulid::Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        for index in 0..RETAINED_AGENTS {
            stopped_capacity_agent(&mut store, &format!("restart-page-{index}"), now);
        }
        let head = store
            .restart_retained_generations(None)
            .expect("read retained fleet");
        assert_eq!(
            head.len(),
            128,
            "one Store page holds the fleet head, proving more than one page exists"
        );

        let store = Arc::new(Mutex::new(store));
        let namespace = store
            .lock()
            .await
            .custody_namespace()
            .expect("read custody namespace");
        let root = std::env::temp_dir().join(format!("eliot-restart-page-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).expect("create router root");
        let router = Router::new(
            root,
            "restart-page-fixture".to_owned(),
            Arc::clone(&store),
            Bus::new(16),
        );
        let context = Reconcile {
            store: Arc::clone(&store),
            bus: Bus::new(16),
            router,
            policy: Arc::new(SecretPolicy::new(Vec::<String>::new())),
            namespace,
        };
        let report = reconcile_first_pass(&context)
            .await
            .expect("bounded first pass");

        assert_eq!(
            report.processed, RETAINED_AGENTS,
            "every retained generation is reconciled exactly once"
        );
        assert!(
            report.batches > 1,
            "more than one page fans out to several batches"
        );
        assert!(
            report.max_batch <= RECONCILE_BATCH_SIZE,
            "no batch exceeds the bound: max {} > {RECONCILE_BATCH_SIZE}",
            report.max_batch,
        );
        let reread = {
            let store = context.store.lock().await;
            let mut count = 0;
            let mut after: Option<String> = None;
            loop {
                let page = store
                    .restart_retained_generations(after.as_deref())
                    .expect("re-read retained fleet");
                if page.is_empty() {
                    break;
                }
                after = page.last().map(|generation| generation.agent.clone());
                count += page.len();
            }
            count
        };
        assert_eq!(
            reread, report.retained,
            "the Store still holds exactly the unresolved remainder"
        );
    }
}
