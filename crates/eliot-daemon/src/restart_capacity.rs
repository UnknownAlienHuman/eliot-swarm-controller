//! Restart capacity recovery. A generation that a daemon replacement left
//! `lost` keeps its reservation until a later daemon verifies, outside the
//! Store lock, that its custody closed.

use crate::{
    bus::Bus,
    common::now_ms,
    logging,
    platform::{RestartCustody, restart_custody},
    router::Router,
};
use eliot_core::{Result, SecretPolicy};
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

/// Release each generation whose custody is verified closed. The first pass
/// records why each remaining reservation stays; returns those generations.
pub(crate) async fn reconcile(
    store: &Mutex<Store>,
    bus: &Bus,
    router: &Router,
    namespace: &str,
    pending: Vec<RetainedGeneration>,
    first_pass: bool,
) -> Result<Vec<RetainedGeneration>> {
    let mut remaining = Vec::new();
    for generation in pending {
        let custody = restart_custody(namespace, &generation).unwrap_or(RestartCustody::Retained {
            missing: "custody_probe_failed",
        });
        let events = match custody {
            RestartCustody::Closed {
                evidence,
                orphan_possible,
            } => store.lock().await.release_restart_closed_generation(
                &generation.agent,
                generation.generation,
                evidence,
                orphan_possible,
                now_ms(),
            )?,
            RestartCustody::Retained { missing } => {
                let events = if first_pass {
                    store.lock().await.record_restart_capacity_retained(
                        &generation.agent,
                        generation.generation,
                        missing,
                        now_ms(),
                    )?
                } else {
                    Vec::new()
                };
                remaining.push(generation);
                events
            }
        };
        bus.publish(&events);
        router.note_events(&events);
    }
    Ok(remaining)
}

/// Recheck retained generations until each one is released; Unix plugin
/// groups do not die with the daemon and may exit after it restarts.
pub(crate) async fn recheck(
    store: Arc<Mutex<Store>>,
    bus: Bus,
    router: Router,
    namespace: String,
    mut pending: Vec<RetainedGeneration>,
    policy: Arc<SecretPolicy>,
) {
    while !pending.is_empty() {
        tokio::time::sleep(RECHECK).await;
        match reconcile(
            &store,
            &bus,
            &router,
            &namespace,
            std::mem::take(&mut pending),
            false,
        )
        .await
        {
            Ok(remaining) => pending = remaining,
            Err(error) => {
                logging::record(&policy, "daemon.restart_capacity_failed", &[], Some(&error));
                return;
            }
        }
    }
}
