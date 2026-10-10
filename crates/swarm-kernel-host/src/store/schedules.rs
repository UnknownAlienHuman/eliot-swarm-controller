//! Durable, transaction-coupled schedule admission using the existing meta
//! and Operation records. There is no separate scheduler database/table.

use super::automation_reconcile::{QuarantineEvidence, SubjectErrorDisposition};
use super::{Store, meta, mutate_in_transaction_with_check_plan, set_meta};
use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, INTERNAL_SCHEDULER_CLIENT_ID, Principal, Role},
    scheduler::{self, ScheduleAction, ScheduleConfig},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::sync::watch;

const REGISTRY_KEY: &str = "schedule_registry:v1";
const REGISTRY_VERSION: u32 = 1;
const MAX_RETAINED_IDENTITIES: usize = scheduler::MAX_SCHEDULES;
const ACTIVE_OPERATION_STATES: &[&str] =
    &["queued", "sending", "native_accepted", "outcome_unknown"];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    schema_version: u32,
    schedules: BTreeMap<String, ScheduleState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleState {
    schema_version: u32,
    definition_sha256: String,
    #[serde(default)]
    last_observed_due_slot: Option<i64>,
    #[serde(default)]
    last_considered_slot: Option<i64>,
    #[serde(default)]
    last_admitted_slot: Option<i64>,
    #[serde(default)]
    last_operation: Option<LastOperation>,
    #[serde(default)]
    last_failure: Option<Value>,
    #[serde(default)]
    failure_input_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LastOperation {
    slot: i64,
    request_id: String,
    receipt_operation_id: String,
    work_operation_id: String,
    outcome: String,
}

#[derive(Debug, Clone)]
struct ExistingReceipt {
    method: String,
    original_request: String,
}

fn empty_registry() -> Registry {
    Registry {
        schema_version: REGISTRY_VERSION,
        schedules: BTreeMap::new(),
    }
}

fn registry(db: &Connection) -> Result<Registry> {
    let Some(value) = meta(db, REGISTRY_KEY)? else {
        return Ok(empty_registry());
    };
    let state: Registry = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "SCHEDULE_STATE_INVALID",
            "stored schedule registry is malformed",
        )
    })?;
    if state.schema_version != REGISTRY_VERSION || state.schedules.len() > MAX_RETAINED_IDENTITIES {
        return Err(Error::new(
            "SCHEDULE_STATE_VERSION",
            "stored schedule registry version or bound is unsupported",
        ));
    }
    Ok(state)
}

fn save_registry(db: &Connection, registry: &Registry) -> Result<()> {
    set_meta(db, REGISTRY_KEY, &json!(registry))
}

/// Digest only execution-defining fields. Toggling `enabled` is a pause/resume
/// switch and does not create a new schedule identity.
pub(super) fn definition_digest(schedule: &ScheduleConfig) -> Result<String> {
    let definition = json!({
        "schema_version": REGISTRY_VERSION,
        "schedule_id": schedule.schedule_id,
        "anchor_ms": schedule.anchor_ms,
        "period_ms": schedule.period_ms,
        "action": schedule.action,
    });
    Ok(model::digest(model::canonical(&definition)?.as_bytes()))
}

pub(super) fn scheduler_source_evidence(schedule: &ScheduleConfig) -> Result<QuarantineEvidence> {
    Ok(QuarantineEvidence {
        subject_identity: format!("schedule_id:{}", schedule.schedule_id),
        source_pointer: Some(format!("config/schedules/{}", schedule.schedule_id)),
        source_digest: Some(definition_digest(schedule)?),
    })
}

pub(super) fn classify_scheduler_error(error: &Error) -> Option<SubjectErrorDisposition> {
    if !error.secondary_codes.is_empty() {
        return None;
    }
    match error.code.as_str() {
        "SCHEDULE_REGISTRY_FULL" => Some(SubjectErrorDisposition::Pending {
            code: error.code.clone(),
            reason: "the bounded schedule registry is full; retain the due slot for a later wake"
                .to_owned(),
        }),
        _ => None,
    }
}

fn request_id(schedule_id: &str, slot: i64) -> Result<String> {
    let identity = json!({"schedule_id":schedule_id,"slot":slot});
    Ok(model::digest(model::canonical(&identity)?.as_bytes()))
}

fn operation_for_request(db: &Connection, request_id: &str) -> Result<Option<ExistingReceipt>> {
    Ok(db
        .query_row(
            "SELECT method,original_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![INTERNAL_SCHEDULER_CLIENT_ID, request_id],
            |row| {
                Ok(ExistingReceipt {
                    method: row.get(0)?,
                    original_request: row.get(1)?,
                })
            },
        )
        .optional()?)
}

fn result_by_request(db: &Connection, request_id: &str) -> Result<(String, Value)> {
    let (operation_id, effective): (String, String) = db.query_row(
        "SELECT operation_id,effective_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
        params![INTERNAL_SCHEDULER_CLIENT_ID, request_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let effective: Value = serde_json::from_str(&effective)?;
    Ok((operation_id, effective["receipt"].clone()))
}

fn input_facts(
    db: &Connection,
    schedule: &ScheduleConfig,
    config: &Config,
) -> Result<(Value, Option<Error>)> {
    let ScheduleAction::CheckRun {
        attempt_id,
        expected_task_revision,
        candidate_ref,
        profile_id,
        profile_revision,
    } = &schedule.action;

    let attempt = super::tasks::get_attempt(db, attempt_id);
    let (attempt_facts, task_facts, attempt_error) = match attempt {
        Ok(attempt) => {
            let task_id = attempt["task_id"].as_str().unwrap_or("");
            let task = super::tasks::get_task(db, task_id);
            match task {
                Ok(task) => {
                    let facts = json!({
                        "attempt_id":attempt["attempt_id"],
                        "task_id":attempt["task_id"],
                        "task_revision":attempt["task_revision"],
                        "released_at_ms":attempt["released_at_ms"],
                        "attempt_state":attempt["state"],
                    });
                    let task_facts = json!({
                        "task_id":task["task_id"],
                        "revision":task["revision"],
                        "state":task["state"],
                    });
                    let error = if attempt["released_at_ms"] != Value::Null
                        || attempt["task_revision"] != *expected_task_revision
                        || task["revision"] != *expected_task_revision
                        || task["state"] != "open"
                    {
                        Some(Error::new(
                            "SCHEDULE_TARGET_STALE",
                            "scheduled Attempt is not current at its pinned Task revision",
                        ))
                    } else {
                        None
                    };
                    (facts, task_facts, error)
                }
                Err(error) => (
                    json!({"error":error.code}),
                    Value::Null,
                    Some(Error::new(
                        "SCHEDULE_TARGET_UNAVAILABLE",
                        "scheduled Attempt Task is unavailable",
                    )),
                ),
            }
        }
        Err(error) => (
            json!({"error":error.code}),
            Value::Null,
            Some(Error::new(
                "SCHEDULE_TARGET_UNAVAILABLE",
                "scheduled Attempt is unavailable",
            )),
        ),
    };

    let profile = config
        .checks
        .profiles
        .iter()
        .find(|profile| {
            profile.profile_id == *profile_id && profile.profile_revision == *profile_revision
        })
        .map(|profile| json!(profile))
        .unwrap_or(Value::Null);
    let profile_error = if !config.checks.enabled {
        Some(Error::new(
            "CHECKS_DISABLED",
            "scheduled CheckRun is blocked because checks are disabled",
        ))
    } else {
        config.checks.profile(profile_id, profile_revision).err()
    };

    let candidate = super::results::get(db, candidate_ref);
    let (candidate_facts, candidate_error) = match candidate {
        Ok(candidate) => {
            let facts = json!({
                "artifact_id":candidate.artifact_id,
                "kind":candidate.kind,
                "content_digest":candidate.content_digest,
                "metadata_attempt_id":candidate.metadata["attempt_id"],
                "metadata_task_revision":candidate.metadata["task_revision"],
            });
            let error = if candidate.kind != "source_snapshot"
                || candidate.metadata["attempt_id"] != attempt_id.as_str()
                || candidate.metadata["task_revision"] != *expected_task_revision
            {
                Some(Error::new(
                    "SCHEDULE_TARGET_STALE",
                    "scheduled source snapshot does not match the pinned Attempt and Task revision",
                ))
            } else {
                None
            };
            (facts, error)
        }
        Err(error) => (
            json!({"error":error.code}),
            Some(Error::new(
                "SCHEDULE_TARGET_UNAVAILABLE",
                "scheduled source snapshot is unavailable",
            )),
        ),
    };

    let facts = json!({
        "definition_sha256":definition_digest(schedule)?,
        "attempt":attempt_facts,
        "task":task_facts,
        "candidate":candidate_facts,
        "checks_enabled":config.checks.enabled,
        "profile":profile,
    });
    Ok((facts, attempt_error.or(profile_error).or(candidate_error)))
}

fn request(schedule: &ScheduleConfig, slot: i64) -> Result<Value> {
    let ScheduleAction::CheckRun {
        attempt_id,
        candidate_ref,
        profile_id,
        profile_revision,
        ..
    } = &schedule.action;
    Ok(json!({
        "client_request_id":request_id(&schedule.schedule_id, slot)?,
        "attempt_id":attempt_id,
        "candidate_ref":candidate_ref,
        "profile_id":profile_id,
        "profile_revision":profile_revision,
    }))
}

/// Snapshot only when a scheduled slot can create a new CheckRun. The caller
/// resolves source bytes after this read and revalidates the result in the final
/// IMMEDIATE transaction below.
pub(super) fn check_plan_request(
    db: &Connection,
    schedule: &ScheduleConfig,
    config: &Config,
    now_ms: i64,
) -> Result<Option<Value>> {
    if !schedule.enabled {
        return Ok(None);
    }
    let digest = definition_digest(schedule)?;
    let registry = registry(db)?;
    let state = registry.schedules.get(&schedule.schedule_id);
    if state.is_some_and(|state| state.definition_sha256 != digest) {
        return Ok(None);
    }
    let last_admitted = state.and_then(|state| state.last_admitted_slot);
    let Some(due) = scheduler::latest_due_slot(schedule, now_ms, last_admitted)? else {
        return Ok(None);
    };
    if meta(db, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled" {
        return Ok(None);
    }
    if let Some(last) = state.and_then(|state| state.last_operation.as_ref())
        && operation_state(db, &last.work_operation_id)?
            .is_some_and(|state| ACTIVE_OPERATION_STATES.contains(&state.as_str()))
    {
        return Ok(None);
    }
    let params = request(schedule, due.slot)?;
    let request_id = model::text(&params, "client_request_id")?;
    if operation_for_request(db, request_id)?.is_some() {
        return Ok(None);
    }
    let (facts, error) = input_facts(db, schedule, config)?;
    if error.is_some() {
        return Ok(None);
    }
    let fingerprint = model::digest(model::canonical(&facts)?.as_bytes());
    if state.and_then(|state| state.failure_input_sha256.as_deref()) == Some(&fingerprint) {
        return Ok(None);
    }
    Ok(Some(params))
}

fn record_failure(state: &mut ScheduleState, slot: i64, fingerprint: &str, error: &Error) {
    state.last_considered_slot = Some(slot);
    state.failure_input_sha256 = Some(fingerprint.to_owned());
    state.last_failure = Some(json!({
        "slot":slot,
        "code":error.code,
        "message":error.message,
        "input_sha256":fingerprint,
    }));
}

fn update_operation_record(
    db: &Connection,
    state: &mut ScheduleState,
    slot: i64,
    request_id: &str,
    outcome: &str,
    value: Option<&Value>,
) -> Result<()> {
    let (receipt_operation_id, _) = result_by_request(db, request_id)?;
    let work_operation_id = value
        .and_then(|value| value.get("operation_id"))
        .and_then(Value::as_str)
        .unwrap_or(&receipt_operation_id)
        .to_owned();
    state.last_considered_slot = Some(slot);
    state.last_admitted_slot = Some(slot);
    state.last_operation = Some(LastOperation {
        slot,
        request_id: request_id.to_owned(),
        receipt_operation_id,
        work_operation_id,
        outcome: outcome.to_owned(),
    });
    Ok(())
}

fn consider(
    db: &mut Connection,
    schedule: &ScheduleConfig,
    config: &Config,
    now_ms: i64,
    check_plan: Option<super::checks::CheckPlanResolution>,
) -> Result<(Option<i64>, bool, bool)> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let digest = definition_digest(schedule)?;
    let mut registry = registry(&tx)?;
    if !registry.schedules.contains_key(&schedule.schedule_id)
        && registry.schedules.len() >= MAX_RETAINED_IDENTITIES
    {
        tx.rollback()?;
        return Err(Error::new(
            "SCHEDULE_REGISTRY_FULL",
            "the bounded schedule registry has no free identity slots; disable and reuse an existing schedule ID",
        ));
    }
    let state = registry
        .schedules
        .entry(schedule.schedule_id.clone())
        .or_insert_with(|| ScheduleState {
            schema_version: REGISTRY_VERSION,
            definition_sha256: digest.clone(),
            last_observed_due_slot: None,
            last_considered_slot: None,
            last_admitted_slot: None,
            last_operation: None,
            last_failure: None,
            failure_input_sha256: None,
        });
    if state.schema_version != REGISTRY_VERSION {
        return Err(Error::new(
            "SCHEDULE_STATE_VERSION",
            "schedule state version is unsupported",
        ));
    }

    let definition_changed = state.definition_sha256 != digest;
    let due = scheduler::latest_due_slot(schedule, now_ms, state.last_admitted_slot)?;
    let mut wake_check_worker = false;
    if definition_changed {
        let fingerprint = model::digest(
            model::canonical(&json!({
                "stored_definition_sha256":state.definition_sha256,
                "configured_definition_sha256":digest,
            }))?
            .as_bytes(),
        );
        if state.failure_input_sha256.as_deref() != Some(&fingerprint) {
            let slot = due
                .map(|due| due.slot)
                .unwrap_or_else(|| state.last_observed_due_slot.unwrap_or(0));
            record_failure(
                state,
                slot,
                &fingerprint,
                &Error::new(
                    "SCHEDULE_CONFIG_CHANGED",
                    "schedule definition changed under a retained identity; use a new schedule_id",
                ),
            );
        }
        if let Some(due) = due {
            state.last_observed_due_slot = Some(due.slot);
        }
        save_registry(&tx, &registry)?;
        tx.commit()?;
        return Ok((scheduler::next_due_at_ms(schedule, now_ms)?, false, false));
    }

    if let Some(due) = due {
        let mode = meta(&tx, "execution_mode")?.unwrap_or(Value::Null);
        if mode["new_work"] != "enabled" {
            state.last_observed_due_slot = Some(due.slot);
            save_registry(&tx, &registry)?;
            tx.commit()?;
            return Ok((scheduler::next_due_at_ms(schedule, now_ms)?, false, false));
        }

        if let Some(last) = state.last_operation.as_ref()
            && let Some(operation_state) = operation_state(&tx, &last.work_operation_id)?
            && ACTIVE_OPERATION_STATES.contains(&operation_state.as_str())
        {
            state.last_observed_due_slot = Some(due.slot);
            save_registry(&tx, &registry)?;
            tx.commit()?;
            return Ok((scheduler::next_due_at_ms(schedule, now_ms)?, false, false));
        }

        let (facts, preflight_error) = input_facts(&tx, schedule, config)?;
        let fingerprint = model::digest(model::canonical(&facts)?.as_bytes());
        if state.failure_input_sha256.as_deref() == Some(&fingerprint) {
            state.last_observed_due_slot = Some(due.slot);
            save_registry(&tx, &registry)?;
            tx.commit()?;
            return Ok((scheduler::next_due_at_ms(schedule, now_ms)?, false, false));
        }
        if let Some(error) = preflight_error {
            state.last_failure = None;
            record_failure(state, due.slot, &fingerprint, &error);
            state.last_observed_due_slot = Some(due.slot);
            save_registry(&tx, &registry)?;
            tx.commit()?;
            return Ok((scheduler::next_due_at_ms(schedule, now_ms)?, false, false));
        }
        state.last_failure = None;
        state.failure_input_sha256 = None;

        let params = request(schedule, due.slot)?;
        model::validate_mutation("check.run", &params)?;
        let request_id = model::text(&params, "client_request_id")?.to_owned();
        let canonical_request = model::canonical(&params)?;
        if let Some(old) = operation_for_request(&tx, &request_id)?
            && (old.method != "check.run" || old.original_request != canonical_request)
        {
            let error = Error::new(
                "REQUEST_ID_CONFLICT",
                "schedule/slot identity already has a receipt for different request bytes",
            );
            record_failure(state, due.slot, &fingerprint, &error);
            state.last_observed_due_slot = Some(due.slot);
            save_registry(&tx, &registry)?;
            tx.commit()?;
            return Ok((scheduler::next_due_at_ms(schedule, now_ms)?, false, false));
        }

        let principal = Principal {
            client_id: INTERNAL_SCHEDULER_CLIENT_ID.to_owned(),
            link_id: format!("schedule:{}", schedule.schedule_id),
            role: Role::Scheduler,
        };
        if operation_for_request(&tx, &request_id)?.is_none() {
            let Some(check_plan) = check_plan.as_ref() else {
                tx.rollback()?;
                return Ok((Some(now_ms.saturating_add(250)), false, true));
            };
            let current_plan = super::checks::plan_inputs(&tx, &principal, &params, config)?;
            if check_plan.context_fingerprint != current_plan.context_fingerprint {
                tx.rollback()?;
                return Ok((Some(now_ms.saturating_add(250)), false, true));
            }
            if let Err(error) = &check_plan.result {
                // Suppression is keyed to the same DB-owned inputs that
                // check_plan_request recomputes. A stable off-transaction
                // resolver error must not be retried on every scheduler tick.
                record_failure(state, due.slot, &fingerprint, error);
                state.last_observed_due_slot = Some(due.slot);
                save_registry(&tx, &registry)?;
                tx.commit()?;
                return Ok((scheduler::next_due_at_ms(schedule, now_ms)?, false, false));
            }
        }
        let receipt = mutate_in_transaction_with_check_plan(
            &tx,
            &principal,
            "check.run",
            &params,
            config,
            now_ms,
            check_plan.as_ref(),
        )?;
        match receipt {
            Ok(value) => {
                update_operation_record(
                    &tx,
                    state,
                    due.slot,
                    &request_id,
                    if value["coalesced"] == true {
                        "coalesced"
                    } else {
                        "admitted"
                    },
                    Some(&value),
                )?;
                state.last_failure = None;
                state.failure_input_sha256 = None;
                wake_check_worker = value["coalesced"] != true && value["cached"] != true;
            }
            Err(error) => {
                let (operation_id, _) = result_by_request(&tx, &request_id)?;
                state.last_considered_slot = Some(due.slot);
                state.last_admitted_slot = Some(due.slot);
                state.last_operation = Some(LastOperation {
                    slot: due.slot,
                    request_id: request_id.clone(),
                    receipt_operation_id: operation_id.clone(),
                    work_operation_id: operation_id,
                    outcome: "rejected".to_owned(),
                });
                record_failure(state, due.slot, &fingerprint, &error);
            }
        }
        state.last_observed_due_slot = Some(due.slot);
    }

    save_registry(&tx, &registry)?;
    tx.commit()?;
    Ok((
        scheduler::next_due_at_ms(schedule, now_ms)?,
        wake_check_worker,
        false,
    ))
}

fn operation_state(db: &Connection, operation_id: &str) -> Result<Option<String>> {
    Ok(db
        .query_row(
            "SELECT state FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .optional()?)
}

fn last_operation(db: &Connection, state: &ScheduleState) -> Result<Option<Value>> {
    let Some(last) = state.last_operation.as_ref() else {
        return Ok(None);
    };
    Ok(Some(json!({
        "slot":last.slot,
        "request_id":last.request_id,
        "receipt_operation_id":last.receipt_operation_id,
        "work_operation_id":last.work_operation_id,
        "state":operation_state(db,&last.work_operation_id)?,
        "outcome":last.outcome,
    })))
}

/// Read-only schedule projection used by `host.status`.
pub(crate) fn status(db: &Connection, schedules: &[ScheduleConfig], now_ms: i64) -> Result<Value> {
    let registry = registry(db)?;
    let mut items = Vec::with_capacity(schedules.len());
    for schedule in schedules {
        let digest = definition_digest(schedule)?;
        let state = registry.schedules.get(&schedule.schedule_id);
        let last_admitted = state.and_then(|state| state.last_admitted_slot);
        let overdue = scheduler::latest_due_slot(schedule, now_ms, last_admitted)?;
        let next_due_ms = match overdue {
            Some(due) => Some(due.due_at_ms),
            None => scheduler::next_due_at_ms(schedule, now_ms)?,
        };
        let last_operation = state
            .map(|state| last_operation(db, state))
            .transpose()?
            .flatten();
        let identity_changed = state.is_some_and(|state| state.definition_sha256 != digest);
        items.push(json!({
            "schedule_id":schedule.schedule_id,
            "enabled":schedule.enabled,
            "action":"check_run",
            "anchor_ms":schedule.anchor_ms,
            "period_ms":schedule.period_ms,
            "next_due_ms":next_due_ms,
            "last_considered_slot":state.and_then(|state|state.last_considered_slot),
            "last_observed_due_slot":state.and_then(|state|state.last_observed_due_slot),
            "last_operation":last_operation,
            "last_failure":state.and_then(|state|state.last_failure.clone()),
            "identity_changed":identity_changed,
        }));
    }
    Ok(json!({"registry_version":REGISTRY_VERSION,"items":items}))
}

impl Store {
    pub(crate) fn schedule_configs(&self) -> Vec<ScheduleConfig> {
        self.config.schedules.clone()
    }

    pub(crate) fn subscribe_schedule_changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// Admit at most the latest due slot for one configured schedule. Target
    /// relevance, the standard `check.run` receipt, and the registry cursor
    /// share one SQLite IMMEDIATE transaction.
    pub(crate) async fn consider_scheduled(
        &self,
        schedule: ScheduleConfig,
        now_ms: i64,
    ) -> Result<Option<i64>> {
        let config = self.config.clone();
        let schedule_for_read = schedule.clone();
        let config_for_read = config.clone();
        let request = self
            .run(move |db| check_plan_request(db, &schedule_for_read, &config_for_read, now_ms))
            .await?;
        let mut resolution = if let Some(request) = request {
            Some(
                self.resolve_check_plan(
                    Principal {
                        client_id: INTERNAL_SCHEDULER_CLIENT_ID.to_owned(),
                        link_id: format!("schedule:{}", schedule.schedule_id),
                        role: Role::Scheduler,
                    },
                    request,
                )
                .await?,
            )
        } else {
            None
        };

        // A changed final-TX snapshot gets one fresh resolve. Persistent churn
        // leaves the same due slot pending and sleeps briefly before retrying.
        for _ in 0..2 {
            let schedule_for_tx = schedule.clone();
            let config_for_tx = config.clone();
            let prepared = resolution.clone();
            let (next_due, wake_worker, stale_plan) = self
                .run(move |db| consider(db, &schedule_for_tx, &config_for_tx, now_ms, prepared))
                .await?;
            if wake_worker {
                self.changed
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
            }
            if !stale_plan {
                return Ok(next_due);
            }

            let schedule_for_read = schedule.clone();
            let config_for_read = config.clone();
            let request = self
                .run(move |db| check_plan_request(db, &schedule_for_read, &config_for_read, now_ms))
                .await?;
            resolution = if let Some(request) = request {
                Some(
                    self.resolve_check_plan(
                        Principal {
                            client_id: INTERNAL_SCHEDULER_CLIENT_ID.to_owned(),
                            link_id: format!("schedule:{}", schedule.schedule_id),
                            role: Role::Scheduler,
                        },
                        request,
                    )
                    .await?,
                )
            } else {
                None
            };
        }
        Ok(Some(now_ms.saturating_add(250)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        artifacts::ArtifactFiles,
        checks::model::{CheckProfile, Parser},
        checks::source::{SourceFile, SourceManifest},
        platform::{DataRoot, bootstrap_credential},
        store::{StoreOwner, schedules},
    };
    use rusqlite::{TransactionBehavior, params};
    use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

    async fn fixture() -> (PathBuf, StoreOwner, Arc<Config>, ScheduleConfig, i64) {
        let directory =
            std::env::temp_dir().join(format!("swarm-schedule-test-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let directory = root.path.clone();
        let credential = bootstrap_credential(&root.path).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = directory.clone();
        config.checks.enabled = true;
        config.checks.profiles = vec![CheckProfile {
            profile_id: "strict".into(),
            profile_revision: "v1".into(),
            executable: std::env::current_exe().unwrap(),
            args: Vec::new(),
            parser: Parser::ExitCode,
            resource: "checks".into(),
            environment: BTreeMap::new(),
            inherit_env: Vec::new(),
            expected_targets: Vec::new(),
            fingerprint_env: Vec::new(),
            reproducible: false,
            versioned_inputs: BTreeMap::new(),
        }];
        let config = Arc::new(config);
        let owner = StoreOwner::start(root, config.clone(), credential.clone())
            .await
            .unwrap();
        let operator = owner.store.authenticate(credential).await.unwrap();
        let task = owner
            .store
            .call(
                operator.clone(),
                "task.create".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "project_id":"schedule-fixture",
                    "spec":{
                        "objective":"Scheduled check fixture",
                        "phase":"verification",
                        "owner_policy_id":"owner-policy-v1",
                        "requirements":[{"id":"R1","statement":"Check the pinned candidate"}],
                    },
                }),
            )
            .await
            .unwrap();
        let task_id = task["task_id"].as_str().unwrap().to_owned();
        let claim = owner
            .store
            .call(
                operator,
                "task.claim".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "task_id":task_id,
                    "expected_revision":1,
                }),
            )
            .await
            .unwrap();
        let attempt_id = claim["attempt_id"].as_str().unwrap().to_owned();
        let candidate_ref = format!("source-{}", model::digest(task_id.as_bytes()));
        let content = b"scheduled fixture source";
        let metadata = json!({
            "task_id":task_id,
            "attempt_id":attempt_id,
            "task_revision":1,
            "commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "tree":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "file_count":1,
            "coverage":"complete"
        });
        let manifest = SourceManifest {
            version: 1,
            commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            tree: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            files: vec![SourceFile {
                path: "fixture.txt".into(),
                mode: "100644".into(),
                object_id: "cccccccccccccccccccccccccccccccccccccccc".into(),
                byte_length: content.len() as u64,
                sha256: model::digest(content),
            }],
        };
        let (record, bytes) = ArtifactFiles::document(
            "source_snapshot",
            &candidate_ref,
            &json!(manifest),
            metadata.clone(),
        )
        .unwrap();
        let source_dir = directory.join("sources").join(&candidate_ref);
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(source_dir.join("fixture.txt"), content).unwrap();
        ArtifactFiles::new(&directory)
            .unwrap()
            .publish(&record, &bytes)
            .unwrap();
        owner
            .store
            .run({
                let record = record.clone();
                move |db| {
                    db.execute(
                        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                        params![record.artifact_id,record.relative_path,record.kind,record.byte_length as i64,record.content_digest,model::now_ms()?,model::canonical(&metadata)?],
                    )?;
                    Ok(())
                }
            })
            .await
            .unwrap();

        let now = model::now_ms().unwrap();
        let schedule = ScheduleConfig {
            schedule_id: "nightly-check".into(),
            enabled: true,
            anchor_ms: now.saturating_sub(9_500),
            period_ms: Some(1_000),
            action: ScheduleAction::CheckRun {
                attempt_id,
                expected_task_revision: 1,
                candidate_ref,
                profile_id: "strict".into(),
                profile_revision: "v1".into(),
            },
        };
        (directory, owner, config, schedule, now)
    }

    #[test]
    fn slot_request_identity_is_deterministic_and_scoped_by_schedule() {
        let first = request_id("check-one", 17).unwrap();
        assert_eq!(first, request_id("check-one", 17).unwrap());
        assert_ne!(first, request_id("check-one", 18).unwrap());
        assert_ne!(first, request_id("check-two", 17).unwrap());
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn registry_action_is_closed_and_does_not_accept_arbitrary_methods() {
        let parsed = serde_json::from_value::<ScheduleConfig>(json!({
            "schedule_id":"check-one",
            "enabled":true,
            "anchor_ms":1,
            "action":{"kind":"agent_send","method":"agent.send"},
        }));
        assert!(parsed.is_err());
    }

    #[tokio::test]
    async fn admission_is_atomic_latest_only_and_status_is_read_only() {
        let (directory, owner, _, schedule, now) = fixture().await;
        let latest = (now - schedule.anchor_ms) / schedule.period_ms.unwrap();
        owner
            .store
            .run(|db| super::super::set_meta(db, "execution_mode", &json!({"new_work":"disabled"})))
            .await
            .unwrap();
        owner
            .store
            .consider_scheduled(schedule.clone(), now)
            .await
            .unwrap();
        let during_drain: i64 = owner
            .store
            .run(|db| {
                db.query_row(
                    "SELECT count(*) FROM operations WHERE caller_id=?1",
                    [INTERNAL_SCHEDULER_CLIENT_ID],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap();
        assert_eq!(during_drain, 0);

        owner
            .store
            .run(|db| super::super::set_meta(db, "execution_mode", &json!({"new_work":"enabled"})))
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            owner.store.consider_scheduled(schedule.clone(), now),
            owner.store.consider_scheduled(schedule.clone(), now),
        );
        first.unwrap();
        second.unwrap();
        let (status, operation_count, check_count, recorded_slot) = owner
            .store
            .run({
                let schedule = schedule.clone();
                move |db| {
                    let operation_count = db.query_row(
                        "SELECT count(*) FROM operations WHERE caller_id=?1",
                        [INTERNAL_SCHEDULER_CLIENT_ID],
                        |row| row.get::<_, i64>(0),
                    )?;
                    let check_count =
                        db.query_row("SELECT count(*) FROM check_runs", [], |row| {
                            row.get::<_, i64>(0)
                        })?;
                    let status = schedules::status(db, &[schedule], now)?;
                    let recorded_slot =
                        status["items"][0]["last_considered_slot"].as_i64().unwrap();
                    Ok((status, operation_count, check_count, recorded_slot))
                }
            })
            .await
            .unwrap();
        assert_eq!(operation_count, 1, "same slot must retain one receipt");
        assert_eq!(check_count, 1, "one slot starts at most one CheckRun");
        assert_eq!(recorded_slot, latest, "catch-up admits only latest slot");
        assert_eq!(status["items"][0]["last_operation"]["state"], "queued");
        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn stale_resolved_plan_keeps_due_slot_pending_without_receipt() {
        let (directory, owner, config, schedule, now) = fixture().await;
        owner
            .store
            .run(|db| super::super::set_meta(db, "execution_mode", &json!({"new_work":"enabled"})))
            .await
            .unwrap();
        let scheduler = Principal {
            client_id: INTERNAL_SCHEDULER_CLIENT_ID.to_owned(),
            link_id: format!("schedule:{}", schedule.schedule_id),
            role: Role::Scheduler,
        };
        let request = owner
            .store
            .run({
                let schedule = schedule.clone();
                let config = config.clone();
                move |db| check_plan_request(db, &schedule, &config, now)
            })
            .await
            .unwrap()
            .expect("fixture has one due valid source candidate");
        let plan = owner
            .store
            .resolve_check_plan(scheduler, request)
            .await
            .unwrap();
        // The candidate/profile/Attempt remain admissible to input_facts, but
        // the claim-time baseline trust snapshot changes after off-TX source
        // resolution. The final transaction must detect that resolved plan as
        // stale and preserve the due slot for a fresh resolve.
        let attempt_id = match &schedule.action {
            ScheduleAction::CheckRun { attempt_id, .. } => attempt_id.clone(),
        };
        owner
            .store
            .run(move |db| {
                db.execute(
                    "UPDATE attempts SET task_snapshot_json=json_set(task_snapshot_json,'$.baseline_candidate.reason','changed_after_resolution') WHERE attempt_id=?1",
                    [&attempt_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let (next_due, wake_worker, stale) = owner
            .store
            .run({
                let schedule = schedule.clone();
                let config = config.clone();
                move |db| consider(db, &schedule, &config, now, Some(plan))
            })
            .await
            .unwrap();
        assert!(stale);
        assert!(!wake_worker);
        assert_eq!(next_due, Some(now.saturating_add(250)));
        let (operations, checks, registry) = owner
            .store
            .run(|db| {
                let operations = db.query_row(
                    "SELECT count(*) FROM operations WHERE caller_id=?1",
                    [INTERNAL_SCHEDULER_CLIENT_ID],
                    |row| row.get::<_, i64>(0),
                )?;
                let checks = db.query_row("SELECT count(*) FROM check_runs", [], |row| {
                    row.get::<_, i64>(0)
                })?;
                Ok((operations, checks, registry(db)?))
            })
            .await
            .unwrap();
        assert_eq!(operations, 0);
        assert_eq!(checks, 0);
        assert!(!registry.schedules.contains_key(&schedule.schedule_id));
        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn unresolved_scheduled_operation_blocks_later_slots_after_readback() {
        let (directory, owner, _, schedule, now) = fixture().await;
        owner
            .store
            .run(|db| super::super::set_meta(db, "execution_mode", &json!({"new_work":"enabled"})))
            .await
            .unwrap();
        owner
            .store
            .consider_scheduled(schedule.clone(), now)
            .await
            .unwrap();

        let work_operation_id = owner
            .store
            .run(|db| {
                let state = registry(db)?
                    .schedules
                    .get("nightly-check")
                    .cloned()
                    .ok_or_else(|| Error::new("TEST_STATE", "schedule state was not stored"))?;
                let operation_id = state
                    .last_operation
                    .as_ref()
                    .ok_or_else(|| Error::new("TEST_STATE", "slot has no receipt"))?
                    .work_operation_id
                    .clone();
                db.execute(
                    "UPDATE operations SET state='outcome_unknown',settled_at_ms=NULL WHERE operation_id=?1",
                    [&operation_id],
                )?;
                Ok(operation_id)
            })
            .await
            .unwrap();

        // Readback observes the unresolved receipt, then a later slot and a
        // repeated scheduler wake must both preserve it without another run.
        let first_readback = owner
            .store
            .run({
                let schedule = schedule.clone();
                move |db| schedules::status(db, &[schedule], now)
            })
            .await
            .unwrap();
        assert_eq!(
            first_readback["items"][0]["last_operation"]["state"],
            "outcome_unknown"
        );

        let period = schedule.period_ms.unwrap();
        let later = now + period * 4;
        owner
            .store
            .consider_scheduled(schedule.clone(), later)
            .await
            .unwrap();
        owner
            .store
            .consider_scheduled(schedule.clone(), later)
            .await
            .unwrap();

        let (status, operation_count, check_count, stored_work_id, last_observed_due_slot) = owner
            .store
            .run({
                let schedule = schedule.clone();
                move |db| {
                    let operation_count = db.query_row(
                        "SELECT count(*) FROM operations WHERE caller_id=?1",
                        [INTERNAL_SCHEDULER_CLIENT_ID],
                        |row| row.get::<_, i64>(0),
                    )?;
                    let check_count =
                        db.query_row("SELECT count(*) FROM check_runs", [], |row| {
                            row.get::<_, i64>(0)
                        })?;
                    let status = schedules::status(db, &[schedule], later)?;
                    let state = registry(db)?
                        .schedules
                        .get("nightly-check")
                        .cloned()
                        .ok_or_else(|| Error::new("TEST_STATE", "schedule state disappeared"))?;
                    Ok((
                        status,
                        operation_count,
                        check_count,
                        state.last_operation.unwrap().work_operation_id,
                        state.last_observed_due_slot,
                    ))
                }
            })
            .await
            .unwrap();
        assert_eq!(
            operation_count, 1,
            "unknown work must not create a later receipt"
        );
        assert_eq!(
            check_count, 1,
            "unknown work must not start a later CheckRun"
        );
        assert_eq!(stored_work_id, work_operation_id);
        assert_eq!(
            status["items"][0]["last_operation"]["state"],
            "outcome_unknown"
        );
        assert_eq!(
            last_observed_due_slot,
            Some((later - schedule.anchor_ms) / period),
            "the later due slot remains observed but unadmitted"
        );
        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn nested_rejection_commits_with_schedule_state_but_structural_error_rolls_back() {
        let (directory, owner, config, mut schedule, now) = fixture().await;
        let ScheduleAction::CheckRun { attempt_id, .. } = &mut schedule.action;
        *attempt_id = "missing-attempt".into();
        let slot = 17;
        let params = request(&schedule, slot).unwrap();
        let client_request_id = model::text(&params, "client_request_id")
            .unwrap()
            .to_owned();
        let schedule_for_commit = schedule.clone();
        let config_for_commit = config.clone();
        let (receipt_operation_id, failure_code, projected) = owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let principal = Principal {
                    client_id: INTERNAL_SCHEDULER_CLIENT_ID.into(),
                    link_id: format!("schedule:{}", schedule_for_commit.schedule_id),
                    role: Role::Scheduler,
                };
                let apply = super::super::mutate_in_transaction(
                    &tx,
                    &principal,
                    "check.run",
                    &params,
                    &config_for_commit,
                    now,
                )?
                .expect_err("missing Attempt is an application rejection");
                assert_eq!(apply.code, "NOT_FOUND");
                let (operation_id, receipt) = result_by_request(&tx, &client_request_id)?;
                assert_eq!(receipt["ok"], false);
                assert_eq!(receipt["error"]["code"], apply.code);
                let operation_state: String = tx.query_row(
                    "SELECT state FROM operations WHERE operation_id=?1",
                    [&operation_id],
                    |row| row.get(0),
                )?;
                assert_eq!(operation_state, "rejected");

                let digest = definition_digest(&schedule_for_commit)?;
                let mut state = ScheduleState {
                    schema_version: REGISTRY_VERSION,
                    definition_sha256: digest.clone(),
                    last_observed_due_slot: Some(slot),
                    last_considered_slot: None,
                    last_admitted_slot: None,
                    last_operation: None,
                    last_failure: None,
                    failure_input_sha256: None,
                };
                update_operation_record(
                    &tx,
                    &mut state,
                    slot,
                    &client_request_id,
                    "rejected",
                    None,
                )?;
                let fingerprint = model::digest(b"preflight-input");
                record_failure(&mut state, slot, &fingerprint, &apply);
                let mut registry = empty_registry();
                registry
                    .schedules
                    .insert(schedule_for_commit.schedule_id.clone(), state);
                save_registry(&tx, &registry)?;
                tx.commit()?;

                let status = schedules::status(db, &[schedule_for_commit], now)?;
                Ok((operation_id, apply.code, status))
            })
            .await
            .unwrap();
        assert_eq!(failure_code, "NOT_FOUND");
        assert_eq!(
            projected["items"][0]["last_operation"]["receipt_operation_id"],
            receipt_operation_id
        );
        assert_eq!(projected["items"][0]["last_operation"]["state"], "rejected");
        assert_eq!(
            projected["items"][0]["last_operation"]["outcome"],
            "rejected"
        );
        assert_eq!(projected["items"][0]["last_failure"]["code"], "NOT_FOUND");

        let structural_id = request_id("structural-probe", slot).unwrap();
        let malformed = json!({
            "client_request_id":structural_id,
            "attempt_id":"missing-attempt",
            "candidate_ref":"candidate",
            "profile_id":"strict"
        });
        let structural_schedule_id = "structural-probe".to_owned();
        let structural_config = config.clone();
        owner
            .store
            .run(move |db| {
                let count_before: i64 = db.query_row(
                    "SELECT count(*) FROM operations WHERE caller_id=?1",
                    [INTERNAL_SCHEDULER_CLIENT_ID],
                    |row| row.get(0),
                )?;
                {
                    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let mut staged = registry(&tx)?;
                    staged.schedules.insert(
                        structural_schedule_id.clone(),
                        ScheduleState {
                            schema_version: REGISTRY_VERSION,
                            definition_sha256: model::digest(b"uncommitted schedule"),
                            last_observed_due_slot: Some(slot),
                            last_considered_slot: None,
                            last_admitted_slot: None,
                            last_operation: None,
                            last_failure: None,
                            failure_input_sha256: None,
                        },
                    );
                    save_registry(&tx, &staged)?;
                    let principal = Principal {
                        client_id: INTERNAL_SCHEDULER_CLIENT_ID.into(),
                        link_id: "schedule:structural-probe".into(),
                        role: Role::Scheduler,
                    };
                    assert!(
                        super::super::mutate_in_transaction(
                            &tx,
                            &principal,
                            "check.run",
                            &malformed,
                            &structural_config,
                            now,
                        )
                        .is_err()
                    );
                    // Same rollback boundary used by `consider`'s `?` on the
                    // helper's outer structural/DB error.
                }
                let count_after: i64 = db.query_row(
                    "SELECT count(*) FROM operations WHERE caller_id=?1",
                    [INTERNAL_SCHEDULER_CLIENT_ID],
                    |row| row.get(0),
                )?;
                assert_eq!(count_after, count_before);
                assert!(operation_for_request(db, &structural_id)?.is_none());
                assert!(!registry(db)?.schedules.contains_key("structural-probe"));
                Ok(())
            })
            .await
            .unwrap();

        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
