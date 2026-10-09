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
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

const MAX_ISSUANCE_PAGE: i64 = 16;
const MAX_ISSUANCE_PER_TICK: usize = 2;
const MAX_TRACKED_BACKOFFS: usize = 256;
const INITIAL_BACKOFF: Duration = Duration::from_secs(2);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

pub(super) struct IssuanceRuntime {
    tick: Mutex<()>,
    state: Mutex<IssuanceRuntimeState>,
}

struct IssuanceRuntimeState {
    cursor: Option<String>,
    backoffs: HashMap<String, Backoff>,
}

struct Backoff {
    delay: Duration,
    next_attempt: Instant,
    touched: Instant,
}

struct IssuanceSelection {
    operation_ids: Vec<String>,
    deferred: usize,
}

impl IssuanceRuntime {
    pub(super) fn new() -> Self {
        Self {
            tick: Mutex::new(()),
            state: Mutex::new(IssuanceRuntimeState {
                cursor: None,
                backoffs: HashMap::new(),
            }),
        }
    }

    async fn cursor(&self) -> Option<String> {
        self.state.lock().await.cursor.clone()
    }

    async fn reset_cursor(&self) {
        self.state.lock().await.cursor = None;
    }

    async fn select_attempts(&self, discovered: &[String]) -> IssuanceSelection {
        let now = Instant::now();
        let mut state = self.state.lock().await;
        let mut due_retries = state
            .backoffs
            .iter()
            .filter(|(_, entry)| entry.next_attempt <= now)
            .map(|(operation_id, entry)| (entry.next_attempt, operation_id.clone()))
            .collect::<Vec<_>>();
        due_retries.sort_by(|(left_at, left_id), (right_at, right_id)| {
            left_at.cmp(right_at).then_with(|| left_id.cmp(right_id))
        });

        // When both lanes have work, reserve one of the two effect slots for each.
        let fresh_budget = if due_retries.is_empty() {
            MAX_ISSUANCE_PER_TICK
        } else {
            1
        };
        let mut fresh = Vec::with_capacity(fresh_budget);
        let mut deferred = 0usize;
        for operation_id in discovered {
            if fresh.len() >= fresh_budget {
                break;
            }
            state.cursor = Some(operation_id.clone());
            match state.backoffs.get(operation_id) {
                Some(entry) => {
                    if entry.next_attempt > now {
                        deferred += 1;
                    }
                }
                None => fresh.push(operation_id.clone()),
            }
        }

        let retry_budget = MAX_ISSUANCE_PER_TICK.saturating_sub(fresh.len());
        let mut operation_ids = Vec::with_capacity(MAX_ISSUANCE_PER_TICK);
        for (_, operation_id) in due_retries.into_iter().take(retry_budget) {
            if let Some(entry) = state.backoffs.get_mut(&operation_id) {
                entry.touched = now;
            }
            operation_ids.push(operation_id);
        }
        operation_ids.extend(fresh);
        IssuanceSelection {
            operation_ids,
            deferred,
        }
    }

    async fn defer(&self, operation_id: &str) {
        let now = Instant::now();
        let mut state = self.state.lock().await;
        let delay = state
            .backoffs
            .get(operation_id)
            .map(|entry| entry.delay.saturating_mul(2).min(MAX_BACKOFF))
            .unwrap_or(INITIAL_BACKOFF);
        let oldest = if !state.backoffs.contains_key(operation_id)
            && state.backoffs.len() >= MAX_TRACKED_BACKOFFS
        {
            state
                .backoffs
                .iter()
                .min_by_key(|(_, entry)| entry.touched)
                .map(|(key, _)| key.clone())
        } else {
            None
        };
        if let Some(oldest) = oldest {
            state.backoffs.remove(&oldest);
        }
        state.backoffs.insert(
            operation_id.to_owned(),
            Backoff {
                delay,
                next_attempt: now + delay,
                touched: now,
            },
        );
    }

    async fn clear_backoff(&self, operation_id: &str) {
        self.state.lock().await.backoffs.remove(operation_id);
    }
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

/// Drive at most two queued assignment credential issuances per call. One
/// Store-local runtime coalesces overlapping host ticks and owns transient
/// cursor/backoff state; separate Store instances never share scheduling state.
/// Discovery and due retries are selected independently. When both lanes have
/// work, each receives one of the two effect slots; backoff is pacing rather
/// than an attempt quota. No launch is dispatched by this worker.
impl super::Store {
    pub(crate) async fn reconcile_launch_issuance_once(&self) -> Result<Value> {
        let Ok(_tick) = self.launch_issuance.tick.try_lock() else {
            return Ok(json!({
                "coalesced":true,
                "selected":0,
                "committed":0,
                "outcome_unknown":0,
                "deferred":0,
                "gaps":["participant_issuance_tick_already_running"],
            }));
        };

        let after = self.launch_issuance.cursor().await;
        let ids = self
            .run(move |db| pending_launch_issuance(db, after.as_deref()))
            .await?;
        if ids.is_empty() {
            self.launch_issuance.reset_cursor().await;
        }
        let selection = self.launch_issuance.select_attempts(&ids).await;
        let selected = selection.operation_ids.len();
        let mut attempted = 0usize;
        let mut committed = 0usize;
        let mut unknown = 0usize;
        let mut deferred = selection.deferred;
        let mut gaps = Vec::new();

        for operation_id in selection.operation_ids {
            attempted += 1;

            let prepare_id = operation_id.clone();
            let prepare_config = self.config.clone();
            let prepared = self
                .run(move |db| {
                    let tx =
                        db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    let effective_request_json: Option<String> = tx
                        .query_row(
                            "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch' AND state='queued' \
                             AND json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_participant_credential'",
                            [&prepare_id],
                            |row| row.get(0),
                        )
                        .optional()?;
                    let Some(effective_request_json) = effective_request_json else {
                        tx.commit()?;
                        return Ok(None);
                    };
                    let actor = launcher::launch_actor(&tx, &prepare_id)?;
                    let request = launcher_participant::prepare_launch_issuance(
                        &tx,
                        &actor,
                        &prepare_id,
                        &prepare_config,
                    )?;
                    tx.commit()?;
                    Ok(Some((actor, request, effective_request_json)))
                })
                .await;
            let (actor, request, effective_request_json) = match prepared {
                Ok(Some(value)) => value,
                Ok(None) => {
                    self.launch_issuance.clear_backoff(&operation_id).await;
                    continue;
                }
                Err(error) => {
                    let gap = safe_gap(&error);
                    if let Err(secondary) = self
                        .record_participant_issuance_failure(
                            operation_id.clone(),
                            None,
                            IssuanceFailureStage::Preparation,
                            &error,
                        )
                        .await
                    {
                        return Err(error.with_secondary_error(secondary));
                    }
                    if gap == "participant_registration_effect_unknown_readback_only" {
                        unknown += 1;
                    } else {
                        deferred += 1;
                    }
                    if !gaps.contains(&gap) {
                        gaps.push(gap);
                    }
                    self.launch_issuance.defer(&operation_id).await;
                    continue;
                }
            };

            let issued =
                participant_credentials::issue_for_launch(self, actor, &self.config, request).await;
            let issued = match issued {
                Ok(issued) => issued,
                Err(error) => {
                    let gap = safe_gap(&error);
                    if let Err(secondary) = self
                        .record_participant_issuance_failure(
                            operation_id.clone(),
                            Some(effective_request_json.clone()),
                            IssuanceFailureStage::CredentialIssue,
                            &error,
                        )
                        .await
                    {
                        return Err(error.with_secondary_error(secondary));
                    }
                    if gap == "participant_registration_effect_unknown_readback_only" {
                        unknown += 1;
                    } else {
                        deferred += 1;
                    }
                    if !gaps.contains(&gap) {
                        gaps.push(gap);
                    }
                    self.launch_issuance.defer(&operation_id).await;
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
                    self.launch_issuance.clear_backoff(&operation_id).await;
                }
                Err(error) => {
                    let gap = safe_gap(&error);
                    if let Err(secondary) = self
                        .record_participant_issuance_failure(
                            operation_id.clone(),
                            Some(effective_request_json.clone()),
                            IssuanceFailureStage::Commit,
                            &error,
                        )
                        .await
                    {
                        return Err(error.with_secondary_error(secondary));
                    }
                    if gap == "participant_registration_effect_unknown_readback_only" {
                        unknown += 1;
                    } else {
                        deferred += 1;
                    }
                    if !gaps.contains(&gap) {
                        gaps.push(gap);
                    }
                    self.launch_issuance.defer(&operation_id).await;
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

    async fn record_participant_issuance_failure(
        &self,
        operation_id: String,
        expected_effective_request_json: Option<String>,
        stage: IssuanceFailureStage,
        error: &Error,
    ) -> Result<bool> {
        let expected_effective_request_json = match expected_effective_request_json {
            Some(snapshot) => Some(snapshot),
            None => {
                let snapshot_id = operation_id.clone();
                self.run(move |db| {
                    let retained: Option<(String, String)> = db
                        .query_row(
                            "SELECT state,effective_request_json FROM operations \
                             WHERE operation_id=?1 AND method='swarm.launch'",
                            [&snapshot_id],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .optional()?;
                    Ok(retained.and_then(|(state, effective)| {
                        (state == "queued"
                            && serde_json::from_str::<Value>(&effective).is_ok_and(|value| {
                                value["launch_manifest"]["state"]
                                    == "awaiting_participant_credential"
                            }))
                        .then_some(effective)
                    }))
                })
                .await?
            }
        };
        let Some(expected_effective_request_json) = expected_effective_request_json else {
            return Ok(false);
        };
        let now = model::now_ms()?;
        let failure = participant_issuance_failure(error, stage, now);
        let changed = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let mut effective: Value = serde_json::from_str(&expected_effective_request_json)?;
                if effective["launch_manifest"]["state"] != "awaiting_participant_credential" {
                    tx.commit()?;
                    return Ok(false);
                }
                effective["launch_manifest"]["participant_issuance_latest_failure"] = failure;
                let changed = tx.execute(
                    "UPDATE operations SET effective_request_json=?3,updated_at_ms=?4 \
                     WHERE operation_id=?1 AND method='swarm.launch' AND state='queued' \
                       AND effective_request_json=?2 \
                       AND json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_participant_credential'",
                    rusqlite::params![
                        operation_id,
                        expected_effective_request_json,
                        model::canonical(&effective)?,
                        now,
                    ],
                )?;
                tx.commit()?;
                Ok(changed == 1)
            })
            .await?;
        if changed {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(changed)
    }
}

#[derive(Clone, Copy)]
enum IssuanceFailureStage {
    Preparation,
    CredentialIssue,
    Commit,
}

impl IssuanceFailureStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Preparation => "participant_issuance_prepare",
            Self::CredentialIssue => "participant_credential_issue",
            Self::Commit => "participant_issuance_commit",
        }
    }
}

fn participant_issuance_failure(error: &Error, stage: IssuanceFailureStage, now: i64) -> Value {
    let category = match error.code.as_str() {
        "LAUNCH_ISSUANCE_REJECTED" => "participant_registration_rejected",
        "CONFIG_ERROR" | "IO_ERROR" | "PRIVATE_ARTIFACT_PATH" | "PRIVATE_ARTIFACT_REFERENCE" => {
            "scoped_artifact_unavailable"
        }
        _ => "participant_issuance_incomplete",
    };
    json!({
        "schema_version":1,
        "code":safe_participant_issuance_error_code(&error.code),
        "stage":stage.as_str(),
        "recorded_at_ms":now,
        "category":category,
    })
}

fn safe_participant_issuance_error_code(code: &str) -> String {
    if !code.is_empty()
        && code.len() <= 64
        && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        code.to_owned()
    } else {
        "PARTICIPANT_ISSUANCE_FAILED".to_owned()
    }
}

#[cfg(test)]
#[path = "participant_issuance_failure_tests.rs"]
mod participant_issuance_failure_tests;
