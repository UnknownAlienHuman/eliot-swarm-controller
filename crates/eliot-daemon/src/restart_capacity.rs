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
        let custody = restart_custody(&generation).unwrap_or_else(|error| {
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
            Ok(report) => {
                if report.processed > report.retained
                    && let Err(error) = context.router.recover_restart_idle_agents().await
                {
                    logging::record(
                        &context.policy,
                        "daemon.restart_agent_recovery_failed",
                        &[],
                        Some(&error),
                    );
                }
                if report.retained == 0 {
                    break;
                }
            }
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
