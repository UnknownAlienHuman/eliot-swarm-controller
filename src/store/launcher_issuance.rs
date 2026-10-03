//! Bounded host-side Participant issuance for retained launch manifests.
//!
//! Store scope is prepared and committed on the database owner. Credential
//! artifacts and the ordinary Store registration run between those phases;
//! every request retains the same launch-scoped idempotency key.

use super::{launcher, launcher_participant};
use crate::{
    error::{Error, Result},
    model, participant_credentials,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

const MAX_ISSUANCE_PAGE: i64 = 16;
const MAX_ISSUANCE_PER_TICK: usize = 2;
const MAX_TRACKED_BACKOFFS: usize = 256;
const INITIAL_BACKOFF: Duration = Duration::from_secs(2);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

static LAUNCH_ISSUANCE_TICK: OnceLock<Mutex<()>> = OnceLock::new();
static LAUNCH_ISSUANCE_CURSOR: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static LAUNCH_ISSUANCE_BACKOFF: OnceLock<Mutex<HashMap<String, Backoff>>> = OnceLock::new();

struct Backoff {
    delay: Duration,
    next_attempt: Instant,
    touched: Instant,
}

fn tick_lock() -> &'static Mutex<()> {
    LAUNCH_ISSUANCE_TICK.get_or_init(|| Mutex::new(()))
}

fn backoff_map() -> &'static Mutex<HashMap<String, Backoff>> {
    LAUNCH_ISSUANCE_BACKOFF.get_or_init(|| Mutex::new(HashMap::new()))
}

fn issuance_cursor() -> &'static Mutex<Option<String>> {
    LAUNCH_ISSUANCE_CURSOR.get_or_init(|| Mutex::new(None))
}

async fn retry_due(operation_id: &str) -> bool {
    let now = Instant::now();
    let mut entries = backoff_map().lock().await;
    match entries.get_mut(operation_id) {
        Some(entry) => {
            entry.touched = now;
            entry.next_attempt <= now
        }
        None => true,
    }
}

async fn defer(operation_id: &str) {
    let now = Instant::now();
    let mut entries = backoff_map().lock().await;
    let delay = entries
        .get(operation_id)
        .map(|entry| entry.delay.saturating_mul(2).min(MAX_BACKOFF))
        .unwrap_or(INITIAL_BACKOFF);
    let oldest = if !entries.contains_key(operation_id) && entries.len() >= MAX_TRACKED_BACKOFFS {
        entries
            .iter()
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(key, _)| key.clone())
    } else {
        None
    };
    if let Some(oldest) = oldest {
        entries.remove(&oldest);
    }
    entries.insert(
        operation_id.to_owned(),
        Backoff {
            delay,
            next_attempt: now + delay,
            touched: now,
        },
    );
}

async fn clear_backoff(operation_id: &str) {
    backoff_map().lock().await.remove(operation_id);
}

fn pending_launch_issuance_page(
    db: &Connection,
    after_operation_id: Option<&str>,
) -> Result<Vec<String>> {
    let mut statement = db.prepare(
        "SELECT operation_id FROM operations \
         WHERE method='swarm.launch' AND state='queued' \
           AND json_extract(effective_request_json,'$.launch_manifest.state') \
             ='awaiting_participant_credential' \
           AND (?1 IS NULL OR operation_id>?1) \
         ORDER BY operation_id LIMIT ?2",
    )?;
    statement
        .query_map(
            rusqlite::params![after_operation_id, MAX_ISSUANCE_PAGE],
            |row| row.get::<_, String>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn pending_launch_issuance(
    db: &Connection,
    after_operation_id: Option<&str>,
) -> Result<Vec<String>> {
    let page = pending_launch_issuance_page(db, after_operation_id)?;
    if page.is_empty() && after_operation_id.is_some() {
        pending_launch_issuance_page(db, None)
    } else {
        Ok(page)
    }
}

fn safe_gap(error: &Error) -> &'static str {
    match error.code.as_str() {
        "LAUNCH_ISSUANCE_UNKNOWN" => "participant_registration_effect_unknown_readback_only",
        "LAUNCH_ISSUANCE_REJECTED" => "participant_registration_has_terminal_rejection",
        "LAUNCH_ISSUANCE_PENDING" => "participant_registration_receipt_pending",
        "STALE_LAUNCH" | "WORKSPACE_LEASE_STALE" | "BINDING_NOT_READY" | "FORBIDDEN" => {
            "launch_scope_revalidation_failed"
        }
        "STORE_CLOSED" | "STORE_ERROR" => "participant_issuance_store_unavailable",
        _ => "participant_issuance_deferred",
    }
}

/// Drive at most two queued assignment credential issuances per call. A
/// process-local lock coalesces overlapping host ticks; per-launch backoff is
/// capped and retried indefinitely, so it is pacing rather than an attempt
/// quota. No launch is dispatched by this worker.
impl super::Store {
    pub(crate) async fn reconcile_launch_issuance_once(&self) -> Result<Value> {
        let Ok(_tick) = tick_lock().try_lock() else {
            return Ok(json!({
                "coalesced":true,
                "selected":0,
                "committed":0,
                "outcome_unknown":0,
                "deferred":0,
                "gaps":["participant_issuance_tick_already_running"],
            }));
        };

        let after = issuance_cursor().lock().await.clone();
        let ids = match self
            .run(move |db| pending_launch_issuance(db, after.as_deref()))
            .await
        {
            Ok(ids) => ids,
            Err(_) => {
                return Ok(json!({
                    "coalesced":false,
                    "selected":0,
                    "attempted":0,
                    "committed":0,
                    "outcome_unknown":0,
                    "deferred":0,
                    "gaps":["participant_issuance_selector_unavailable"],
                }));
            }
        };
        if let Some(last) = ids.last() {
            *issuance_cursor().lock().await = Some(last.clone());
        } else {
            *issuance_cursor().lock().await = None;
        }
        let selected = ids.len();
        let mut attempted = 0usize;
        let mut committed = 0usize;
        let mut unknown = 0usize;
        let mut deferred = 0usize;
        let mut gaps = Vec::new();

        for operation_id in ids {
            if attempted >= MAX_ISSUANCE_PER_TICK {
                break;
            }
            if !retry_due(&operation_id).await {
                deferred += 1;
                continue;
            }
            attempted += 1;

            let prepare_id = operation_id.clone();
            let prepare_config = self.config.clone();
            let prepared = self
                .run(move |db| {
                    let tx =
                        db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    let actor = launcher::launch_actor(&tx, &prepare_id)?;
                    let request = launcher_participant::prepare_launch_issuance(
                        &tx,
                        &actor,
                        &prepare_id,
                        &prepare_config,
                    )?;
                    tx.commit()?;
                    Ok((actor, request))
                })
                .await;
            let (actor, request) = match prepared {
                Ok(value) => value,
                Err(error) => {
                    let gap = safe_gap(&error);
                    if gap == "participant_registration_effect_unknown_readback_only" {
                        unknown += 1;
                    } else {
                        deferred += 1;
                    }
                    if !gaps.contains(&gap) {
                        gaps.push(gap);
                    }
                    defer(&operation_id).await;
                    continue;
                }
            };

            let issued =
                participant_credentials::issue_for_launch(self, actor, &self.config, request).await;
            let issued = match issued {
                Ok(issued) => issued,
                Err(error) => {
                    let gap = safe_gap(&error);
                    if gap == "participant_registration_effect_unknown_readback_only" {
                        unknown += 1;
                    } else {
                        deferred += 1;
                    }
                    if !gaps.contains(&gap) {
                        gaps.push(gap);
                    }
                    defer(&operation_id).await;
                    continue;
                }
            };

            let commit_id = operation_id.clone();
            let commit_config = self.config.clone();
            let result = self
                .run(move |db| {
                    let tx =
                        db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    let actor = launcher::launch_actor(&tx, &commit_id)?;
                    let result = launcher_participant::commit_launch_issuance(
                        &tx,
                        &actor,
                        &commit_id,
                        &commit_config,
                        &issued,
                        model::now_ms()?,
                    )?;
                    tx.commit()?;
                    Ok(result)
                })
                .await;
            match result {
                Ok(_) => {
                    committed += 1;
                    clear_backoff(&operation_id).await;
                }
                Err(error) => {
                    let gap = safe_gap(&error);
                    if gap == "participant_registration_effect_unknown_readback_only" {
                        unknown += 1;
                    } else {
                        deferred += 1;
                    }
                    if !gaps.contains(&gap) {
                        gaps.push(gap);
                    }
                    defer(&operation_id).await;
                }
            }
        }

        if committed > 0 {
            self.changed
                .send_modify(|value| *value = value.wrapping_add(1));
        }
        Ok(json!({
            "coalesced":false,
            "selected":selected,
            "attempted":attempted,
            "committed":committed,
            "outcome_unknown":unknown,
            "deferred":deferred,
            "gaps":gaps,
        }))
    }
}
