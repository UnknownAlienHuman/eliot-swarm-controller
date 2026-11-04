//! Restart capacity recovery. A generation that a daemon replacement left
//! `lost` or `stopped` keeps its reservation until a later daemon verifies,
//! outside the Store lock, that its custody closed.

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

/// Read every retained generation. Call before IPC bind, so that no
/// generation started by this daemon can be among them.
pub(crate) async fn retained(store: &Mutex<Store>) -> Result<(String, Vec<RetainedGeneration>)> {
    let store = store.lock().await;
    let namespace = store.custody_namespace()?;
    let mut retained = Vec::new();
    loop {
        let after = retained
            .last()
            .map(|generation: &RetainedGeneration| generation.agent.clone());
        let page = store.restart_retained_generations(after.as_deref())?;
        if page.is_empty() {
            break;
        }
        retained.extend(page);
    }
    Ok((namespace, retained))
}

/// Release each generation whose custody is verified closed and return the
/// rest. A failure for one generation keeps it retained and is logged; it
/// never stops startup or the other generations. The first pass also records
/// why each remaining reservation stays.
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

/// Recheck retained generations until each one is released; Unix plugin
/// groups do not die with the daemon and may exit after it restarts.
pub(crate) async fn recheck(context: Reconcile, mut pending: Vec<RetainedGeneration>) {
    while !pending.is_empty() {
        tokio::time::sleep(RECHECK).await;
        pending = reconcile(&context, pending, false).await;
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
