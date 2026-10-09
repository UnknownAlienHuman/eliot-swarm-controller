//! Bounded, database-only reconciliation for durable workspace lease authority.
//!
//! This module never inspects or removes a worktree. Attempt release is an
//! owner attestation, not proof that a native process stopped, so a lease is
//! released only when its exact terminal Attempt is released and no linked
//! operation, producer, or native binding remains active or unresolved, or its
//! unbound launch closed with positive evidence that execution never began.

use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const MAX_LIFECYCLE_SWEEP: usize = 32;
const MAX_PRODUCER_EVIDENCE_BYTES: usize = 64 * 1024;
const MAX_PRODUCERS: usize = 256;
const SCAN_CURSOR_KEY: &str = "workspace:lifecycle:cursor:v1";
const TERMINAL_ATTEMPT_STATES: &[&str] = &["accepted", "failed", "cancelled", "superseded"];

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkspaceLeaseSweep {
    pub examined: usize,
    pub stale: usize,
    pub released: usize,
    pub raced: usize,
}

#[derive(Debug)]
struct LeaseCandidate {
    lease_id: String,
    registration_id: String,
    registration_generation: i64,
    project_id: String,
    task_id: String,
    task_revision: i64,
    operation_id: String,
    owner_client_id: String,
    attempt_id: Option<String>,
    generation: i64,
    binding_digest: String,
    state: String,
    clean_state_json: String,
    plan_digest: String,
    baseline_commit: String,
    updated_at_ms: i64,
}

/// A progress marker only; it cannot authorize a lease transition.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScanCursor {
    updated_at_ms: i64,
    lease_id: String,
}

#[derive(Debug)]
struct AttemptFacts {
    attempt_id: String,
    task_id: String,
    task_revision: i64,
    owner_id: String,
    start_operation_id: Option<String>,
    state: String,
    released_at_ms: Option<i64>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    producers_json: String,
    producers_oversized: bool,
}

#[derive(Debug, Clone, Copy)]
enum Transition {
    Stale {
        reason: &'static str,
        native_effect: &'static str,
    },
    Released {
        reason: &'static str,
    },
}

/// Reconcile at most `limit` actionable leases in deterministic oldest-first
/// order. Live rows with a superseded Task/registration or an invalid Attempt
/// are invalidated. Closed unbound launches and released terminal Attempts
/// re-enter only with positive effect evidence, so safe scopes leave the fence.
/// The result contains aggregate counts only.
pub(crate) fn reconcile(
    tx: &Transaction<'_>,
    now: i64,
    limit: usize,
) -> Result<WorkspaceLeaseSweep> {
    if limit == 0 || limit > MAX_LIFECYCLE_SWEEP {
        return Err(Error::invalid(
            "workspace lifecycle sweep limit must be between 1 and 32",
        ));
    }

    let cursor = super::meta(tx, SCAN_CURSOR_KEY)?
        .and_then(|value| serde_json::from_value::<ScanCursor>(value).ok())
        .filter(|cursor| {
            cursor.updated_at_ms >= 0
                && !cursor.lease_id.is_empty()
                && cursor.lease_id.len() <= 256
                && !cursor.lease_id.chars().any(char::is_control)
        });
    let mut candidates = candidates(tx, limit, cursor.as_ref())?;
    if candidates.is_empty() && cursor.is_some() {
        candidates = self::candidates(tx, limit, None)?;
    }
    if let Some(last) = candidates.last() {
        super::set_meta(
            tx,
            SCAN_CURSOR_KEY,
            &json!(ScanCursor {
                updated_at_ms: last.updated_at_ms,
                lease_id: last.lease_id.clone(),
            }),
        )?;
    }
    let mut sweep = WorkspaceLeaseSweep::default();
    for lease in candidates {
        sweep.examined += 1;
        let Some(transition) = transition_for(tx, &lease)? else {
            continue;
        };
        let state = match transition {
            Transition::Stale { .. } => "stale",
            Transition::Released { .. } => "released",
        };
        let clean_state_json = lifecycle_facts(&lease, transition, now)?;
        let changed = tx.execute(
            "UPDATE workspace_leases
             SET state=?1,clean_state_json=?2,updated_at_ms=MAX(updated_at_ms,?3)
             WHERE lease_id=?4 AND registration_id=?5 AND registration_generation=?6
               AND project_id=?7 AND task_id=?8 AND task_revision=?9 AND operation_id=?10
               AND owner_client_id=?11 AND attempt_id IS ?12 AND generation=?13
               AND binding_digest=?14 AND state=?15 AND plan_digest=?16 AND baseline_commit=?17",
            params![
                state,
                clean_state_json,
                now,
                lease.lease_id,
                lease.registration_id,
                lease.registration_generation,
                lease.project_id,
                lease.task_id,
                lease.task_revision,
                lease.operation_id,
                lease.owner_client_id,
                lease.attempt_id,
                lease.generation,
                lease.binding_digest,
                lease.state,
                lease.plan_digest,
                lease.baseline_commit,
            ],
        )?;
        if changed != 1 {
            sweep.raced += 1;
        } else if state == "released" {
            sweep.released += 1;
        } else {
            sweep.stale += 1;
        }
    }
    Ok(sweep)
}

fn candidates(
    tx: &Transaction<'_>,
    limit: usize,
    cursor: Option<&ScanCursor>,
) -> Result<Vec<LeaseCandidate>> {
    let mut statement = tx.prepare(
        "SELECT l.lease_id,l.registration_id,l.registration_generation,l.project_id,
                l.task_id,l.task_revision,l.operation_id,l.owner_client_id,l.attempt_id,
                l.generation,l.binding_digest,l.state,l.clean_state_json,l.plan_digest,l.baseline_commit,l.updated_at_ms
         FROM workspace_leases l
         WHERE (
           (l.state IN ('preparing','held','outcome_unknown')
             AND (
               NOT EXISTS (
                 SELECT 1 FROM workspace_registrations r
                 WHERE r.registration_id=l.registration_id AND r.project_id=l.project_id
                   AND r.state='active' AND r.generation=l.registration_generation
               )
               OR NOT EXISTS (
                 SELECT 1 FROM tasks t
                 WHERE t.task_id=l.task_id AND t.project_id=l.project_id
                   AND t.revision=l.task_revision AND t.state='open'
               )
               OR (l.attempt_id IS NOT NULL AND NOT EXISTS (
                 SELECT 1 FROM attempts a
                 WHERE a.attempt_id=l.attempt_id AND a.task_id=l.task_id
                   AND a.task_revision=l.task_revision AND a.owner_id=l.owner_client_id
                   AND a.released_at_ms IS NULL
                   AND a.state NOT IN ('accepted','failed','cancelled','superseded')
               ))
             ))
           OR (l.attempt_id IS NULL AND l.state IN ('held','stale') AND EXISTS (
             SELECT 1 FROM operations o WHERE o.operation_id=l.operation_id
               AND o.method='swarm.launch' AND o.state='settled'
               AND o.attempt_id IS NULL AND o.binding_id IS NULL
               AND o.binding_generation IS NULL
               AND CASE WHEN json_valid(o.result_json) THEN
                 json_extract(o.result_json,'$.native_effect')='not_attempted'
                 ELSE 0 END
           ))
           OR (l.state='stale' AND l.attempt_id IS NOT NULL
             AND COALESCE(
               json_extract(l.clean_state_json,'$.prior_lease_state'),
               json_extract(l.clean_state_json,'$.lease_lifecycle.latest.prior_lease_state'),
               json_extract(l.clean_state_json,'$.lease_lifecycle.prior_lease_state')
             ) IN ('held','preparing')
             AND EXISTS (
             SELECT 1 FROM attempts a
             WHERE a.attempt_id=l.attempt_id AND a.task_id=l.task_id
               AND a.task_revision=l.task_revision AND a.owner_id=l.owner_client_id
               AND a.released_at_ms IS NOT NULL
               AND a.state IN ('accepted','failed','cancelled','superseded')
           ))
         )
         AND (?2 IS NULL OR l.updated_at_ms>?2 OR (l.updated_at_ms=?2 AND l.lease_id>?3))
         ORDER BY l.updated_at_ms,l.lease_id LIMIT ?1",
    )?;
    let rows = statement.query_map(
        params![
            limit as i64,
            cursor.map(|cursor| cursor.updated_at_ms),
            cursor.map(|cursor| cursor.lease_id.as_str())
        ],
        |row| {
            Ok(LeaseCandidate {
                lease_id: row.get(0)?,
                registration_id: row.get(1)?,
                registration_generation: row.get(2)?,
                project_id: row.get(3)?,
                task_id: row.get(4)?,
                task_revision: row.get(5)?,
                operation_id: row.get(6)?,
                owner_client_id: row.get(7)?,
                attempt_id: row.get(8)?,
                generation: row.get(9)?,
                binding_digest: row.get(10)?,
                state: row.get(11)?,
                clean_state_json: row.get(12)?,
                plan_digest: row.get(13)?,
                baseline_commit: row.get(14)?,
                updated_at_ms: row.get(15)?,
            })
        },
    )?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn transition_for(tx: &Transaction<'_>, lease: &LeaseCandidate) -> Result<Option<Transition>> {
    let invalid_scope = if !registration_is_current(tx, lease)? {
        Some("registration_superseded")
    } else if !task_is_current(tx, lease)? {
        Some("task_superseded")
    } else {
        None
    };

    let Some(attempt_id) = lease.attempt_id.as_deref() else {
        if closed_launch_has_no_native_effect(tx, lease)? {
            return Ok(Some(Transition::Released {
                reason: "unbound_launch_closed_before_native_effect",
            }));
        }
        if let Some(reason) = invalid_scope {
            return Ok(Some(Transition::Stale {
                reason,
                native_effect: effect_status_for_lease(tx, lease)?,
            }));
        }
        return Ok(None);
    };
    let Some(attempt) = attempt_facts(tx, attempt_id)? else {
        return Ok(Some(Transition::Stale {
            reason: "attempt_missing",
            native_effect: effect_status_for_lease(tx, lease)?,
        }));
    };
    if attempt.attempt_id != attempt_id
        || attempt.task_id != lease.task_id
        || attempt.task_revision != lease.task_revision
        || attempt.owner_id != lease.owner_client_id
    {
        return Ok(Some(Transition::Stale {
            reason: "attempt_binding_changed",
            native_effect: effect_status_for_lease(tx, lease)?,
        }));
    }

    let terminal = TERMINAL_ATTEMPT_STATES.contains(&attempt.state.as_str());
    let released = attempt.released_at_ms.is_some();
    if terminal && released {
        let unresolved_native = workspace_resource_use(tx, lease, &attempt)?.unresolved();
        let held_origin =
            lease.state == "held" || (lease.state == "stale" && stale_origin_was_held(lease));
        if !unresolved_native && held_origin {
            return Ok(Some(Transition::Released {
                reason: "exact_attempt_released_no_active_native_records",
            }));
        }
        if !unresolved_native && closed_launch_has_no_native_effect(tx, lease)? {
            return Ok(Some(Transition::Released {
                reason: "exact_attempt_released_after_workspace_admission_rejection",
            }));
        }
        if unresolved_native {
            return Ok(Some(Transition::Stale {
                reason: "attempt_native_work_unresolved",
                native_effect: "active_or_unknown",
            }));
        }
        return Ok(Some(Transition::Stale {
            reason: if held_origin {
                "attempt_ended_during_workspace_effect"
            } else {
                "stale_lease_origin_unverified"
            },
            native_effect: "possible_or_unknown",
        }));
    }

    let reason = if terminal {
        "terminal_attempt_not_released"
    } else if released {
        "attempt_release_state_invalid"
    } else if let Some(reason) = invalid_scope {
        reason
    } else {
        return Ok(None);
    };
    Ok(Some(Transition::Stale {
        reason,
        native_effect: effect_status_for_lease(tx, lease)?,
    }))
}

struct UnadmittedLaunch {
    caller_id: String,
    client_request_id: String,
    task_id: Option<String>,
    state: String,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    original: String,
    effective: String,
    result: String,
}

/// A terminal launch without an Attempt may release its scope fence, but only
/// from the exact original intent and positive workspace/no-execution proof.
/// The prepared worktree and its clean-state evidence remain intact.
fn closed_launch_has_no_native_effect(
    tx: &Transaction<'_>,
    lease: &LeaseCandidate,
) -> Result<bool> {
    let Some(launch) = tx.query_row(
        "SELECT client_request_id,task_id,state,attempt_id,binding_id,binding_generation,
                CASE WHEN length(CAST(original_request_json AS BLOB))<=1048576 THEN original_request_json ELSE '' END,
                CASE WHEN length(CAST(effective_request_json AS BLOB))<=1048576 THEN effective_request_json ELSE '' END,
                CASE WHEN length(CAST(result_json AS BLOB))<=1048576 THEN result_json ELSE '' END,caller_id
         FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [&lease.operation_id],
        |row| Ok(UnadmittedLaunch {
            client_request_id: row.get(0)?, task_id: row.get(1)?, state: row.get(2)?,
            attempt_id: row.get(3)?, binding_id: row.get(4)?, binding_generation: row.get(5)?,
            original: row.get(6)?, effective: row.get(7)?, result: row.get(8)?,
            caller_id: row.get(9)?,
        }),
    ).optional()? else { return Ok(false) };
    if launch.state != "settled"
        || launch.task_id.as_deref() != Some(&lease.task_id)
        || launch.attempt_id != lease.attempt_id
        || launch.binding_id.is_some()
        || launch.binding_generation.is_some()
    {
        return Ok(false);
    }
    let (Ok(original), Ok(effective), Ok(result), Ok(clean)) = (
        serde_json::from_str::<Value>(&launch.original),
        serde_json::from_str::<Value>(&launch.effective),
        serde_json::from_str::<Value>(&launch.result),
        serde_json::from_str::<Value>(&lease.clean_state_json),
    ) else {
        return Ok(false);
    };
    let Ok(request) = crate::launcher::LaunchRequest::parse(&original) else {
        return Ok(false);
    };
    let manifest = &effective["launch_manifest"];
    let expected_attempt = json!(&lease.attempt_id);
    let actor = &manifest["actor"];
    let owner = match actor["kind"].as_str() {
        Some("direct") => actor["client_id"].as_str(),
        Some("work_dispatch") => actor["effective_manager_id"].as_str(),
        _ => None,
    };
    if request.client_request_id != launch.client_request_id
        || request.plan_digest != lease.plan_digest
        || request.preview.task_id != lease.task_id
        || request.preview.expected_task_revision != lease.task_revision
        || manifest["plan_digest"] != lease.plan_digest
        || result["plan_digest"] != lease.plan_digest
        || manifest["task"]["task_id"] != lease.task_id
        || result["task_id"] != lease.task_id
        || manifest["task"]["project_id"] != lease.project_id
        || manifest["task"]["observed_revision"] != lease.task_revision
        || result["task_revision"] != lease.task_revision
        || manifest["task"]["attempt_id"] != expected_attempt
        || result["attempt_id"] != expected_attempt
        || actor["client_id"] != launch.caller_id
        || owner != Some(lease.owner_client_id.as_str())
        || manifest["runtime"]["native_effect"] != "not_attempted"
        || result["native_effect"] != "not_attempted"
        || !matches!(manifest["state"].as_str(), Some("blocked" | "stale"))
        || result["launch_state"] != manifest["state"]
        || result["operation_id"] != lease.operation_id
        || result["state"] != "settled"
        || result["failure"] != manifest["failure"]
        || !matches!(
            manifest["failure"]["code"].as_str(),
            Some(
                "workspace_admission_rejected"
                    | "route_admission_unavailable"
                    | "workspace_stale_before_effect"
            )
        )
    {
        return Ok(false);
    }
    let held_proof = (lease.state == "held" || stale_origin_was_held(lease))
        && clean["status"] == "verified_clean"
        && clean["source_head_rechecked"] == true
        && clean["tracked_and_untracked"] == true
        && clean["source_head_before"] == clean["source_head_after"]
        && clean["source_head_before"]
            .as_str()
            .is_some_and(crate::forge::valid_object_id)
        && clean["worktree_head"] == lease.baseline_commit
        && crate::forge::valid_object_id(&lease.baseline_commit);
    let rejected_preparation = lease.state == "stale"
        && clean["prior_lease_state"] == "preparing"
        && clean["status"] == "admission_rejected"
        && clean["reason_code"] == "WORKSPACE_GIT_PATH_TOO_LONG"
        && clean["native_effect_status"] == "not_attempted"
        && manifest["failure"]["code"] == "workspace_admission_rejected";
    if !held_proof && !rejected_preparation {
        return Ok(false);
    }
    let linked_work: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM operations o
           LEFT JOIN bindings b ON b.binding_id=o.binding_id AND b.generation=o.binding_generation
           WHERE (o.prerequisite_operation_id=?4 OR CASE WHEN json_valid(o.effective_request_json) THEN
             json_extract(o.effective_request_json,'$.workspace_lease.lease_id')=?1
             AND json_extract(o.effective_request_json,'$.workspace_lease.generation')=?2
             AND json_extract(o.effective_request_json,'$.workspace_lease.binding_digest')=?3 ELSE 0 END)
             AND (o.state IN ('queued','sending','native_accepted','outcome_unknown')
                  OR (b.binding_id IS NOT NULL AND b.released_at_ms IS NULL)
                  OR NOT json_valid(o.effective_request_json)))
         OR EXISTS(SELECT 1 FROM owned_service_starts
           WHERE lease_id=?1 AND lease_generation=?2
             AND state IN ('reserved','outcome_unknown','service_observed'))",
        params![lease.lease_id, lease.generation, lease.binding_digest, lease.operation_id],
        |row| row.get(0),
    )?;
    Ok(!linked_work)
}

fn registration_is_current(tx: &Transaction<'_>, lease: &LeaseCandidate) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM workspace_registrations
           WHERE registration_id=?1 AND project_id=?2 AND state='active' AND generation=?3
         )",
        params![
            lease.registration_id,
            lease.project_id,
            lease.registration_generation
        ],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn task_is_current(tx: &Transaction<'_>, lease: &LeaseCandidate) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM tasks
           WHERE task_id=?1 AND project_id=?2 AND revision=?3 AND state='open'
         )",
        params![lease.task_id, lease.project_id, lease.task_revision],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn attempt_facts(tx: &Transaction<'_>, attempt_id: &str) -> Result<Option<AttemptFacts>> {
    tx.query_row(
        "SELECT attempt_id,task_id,task_revision,owner_id,start_operation_id,state,
                released_at_ms,binding_id,binding_generation,
                CASE WHEN length(CAST(producers_json AS BLOB))<=?2
                     THEN producers_json ELSE '[]' END,
                length(CAST(producers_json AS BLOB))>?2
         FROM attempts WHERE attempt_id=?1",
        params![attempt_id, MAX_PRODUCER_EVIDENCE_BYTES as i64],
        |row| {
            Ok(AttemptFacts {
                attempt_id: row.get(0)?,
                task_id: row.get(1)?,
                task_revision: row.get(2)?,
                owner_id: row.get(3)?,
                start_operation_id: row.get(4)?,
                state: row.get(5)?,
                released_at_ms: row.get(6)?,
                binding_id: row.get(7)?,
                binding_generation: row.get(8)?,
                producers_json: row.get(9)?,
                producers_oversized: row.get(10)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

#[derive(Debug)]
enum ResourceHold {
    LeaseOperation,
    AttemptStartOperation,
    ExactAttemptOperation,
    ExactBindingGeneration,
    ExactOwnedServiceStart,
    CheckResource,
    ExactProducer,
    EvidenceGap,
}

struct WorkspaceResourceUse {
    holds: Vec<ResourceHold>,
}

impl WorkspaceResourceUse {
    fn unresolved(&self) -> bool {
        !self.holds.is_empty()
    }
}

fn workspace_resource_use(
    tx: &Transaction<'_>,
    lease: &LeaseCandidate,
    attempt: &AttemptFacts,
) -> Result<WorkspaceResourceUse> {
    let mut holds = Vec::new();
    for (operation_id, hold) in [
        (
            Some(lease.operation_id.as_str()),
            ResourceHold::LeaseOperation,
        ),
        (
            attempt.start_operation_id.as_deref(),
            ResourceHold::AttemptStartOperation,
        ),
    ] {
        let unresolved: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations WHERE operation_id=?1 AND state IN ('queued','sending','native_accepted','outcome_unknown'))",
            [operation_id], |row| row.get(0))?;
        if unresolved {
            holds.push(hold);
        }
    }
    let linked_operation: bool = tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM operations INDEXED BY workspace_attempt_operation_state
           WHERE attempt_id=?1
             AND state IN ('queued','sending','native_accepted','outcome_unknown')
             AND ((?2 IS NOT NULL AND binding_id=?2 AND binding_generation=?3)
                  OR CASE WHEN json_valid(effective_request_json) THEN
                    json_extract(effective_request_json,'$.workspace_lease.lease_id')=?4
                    AND json_extract(effective_request_json,'$.workspace_lease.generation')=?5
                    AND json_extract(effective_request_json,'$.workspace_lease.binding_digest')=?6
                  ELSE 0 END)
         )",
        params![
            attempt.attempt_id,
            attempt.binding_id,
            attempt.binding_generation,
            lease.lease_id,
            lease.generation,
            lease.binding_digest,
        ],
        |row| row.get(0),
    )?;
    if linked_operation {
        holds.push(ResourceHold::ExactAttemptOperation);
    }
    let damaged_link: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM operations INDEXED BY workspace_attempt_operation_state WHERE attempt_id=?1 AND state IN ('queued','sending','native_accepted','outcome_unknown') AND NOT json_valid(effective_request_json))",
        [&attempt.attempt_id], |row| row.get(0))?;
    if damaged_link || attempt.binding_id.is_some() != attempt.binding_generation.is_some() {
        holds.push(ResourceHold::EvidenceGap);
    }
    let held_check_resource: bool = tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM check_runs
           WHERE attempt_id=?1 AND resource_claimed_at_ms IS NOT NULL
             AND resource_released_at_ms IS NULL
         )",
        [attempt.attempt_id.as_str()],
        |row| row.get(0),
    )?;
    if held_check_resource {
        holds.push(ResourceHold::CheckResource);
    }
    let active_binding: bool = tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM bindings
           WHERE binding_id=?1 AND generation=?2 AND released_at_ms IS NULL
         )",
        params![attempt.binding_id, attempt.binding_generation,],
        |row| row.get(0),
    )?;
    if active_binding {
        holds.push(ResourceHold::ExactBindingGeneration);
    }
    // A reserved service has not crossed the Store's one-shot start boundary;
    // its queued launch/open Operations fence it until cancellation CASes the
    // reservation to failed_no_effect. Unknown and observed starts are an
    // independent process fence even if their Operations have become terminal.
    let owned_service_live: bool = tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM owned_service_starts
           WHERE state IN ('outcome_unknown','service_observed')
             AND (
               (lease_id=?1 AND lease_generation=?2)
               OR (attempt_id=?3 AND binding_id IS ?4 AND binding_generation IS ?5)
             )
         )",
        params![
            lease.lease_id,
            lease.generation,
            attempt.attempt_id,
            attempt.binding_id.as_deref(),
            attempt.binding_generation,
        ],
        |row| row.get(0),
    )?;
    if owned_service_live {
        holds.push(ResourceHold::ExactOwnedServiceStart);
    }
    if producers_are_unresolved(&attempt.producers_json, attempt.producers_oversized) {
        holds.push(ResourceHold::ExactProducer);
    }
    Ok(WorkspaceResourceUse { holds })
}

fn producers_are_unresolved(raw: &str, oversized: bool) -> bool {
    if oversized || raw.len() > MAX_PRODUCER_EVIDENCE_BYTES {
        return true;
    }
    let Ok(producers) = serde_json::from_str::<Value>(raw) else {
        return true;
    };
    let Some(items) = producers.as_array() else {
        return true;
    };
    if items.len() > MAX_PRODUCERS {
        return true;
    }
    items.iter().any(|item| {
        !matches!(
            item.get("disposition").and_then(Value::as_str),
            Some("completed" | "failed" | "cancelled")
        )
    })
}

fn effect_status_for_lease(tx: &Transaction<'_>, lease: &LeaseCandidate) -> Result<&'static str> {
    match lease.state.as_str() {
        "preparing" | "outcome_unknown" => Ok("possible_or_unknown"),
        "held" | "stale" => {
            if lease.state == "stale" && !stale_origin_was_held(lease) {
                return Ok("possible_or_unknown");
            }
            let Some(attempt_id) = lease.attempt_id.as_deref() else {
                return Ok("possible_or_unknown");
            };
            let Some(attempt) = attempt_facts(tx, attempt_id)? else {
                return Ok("possible_or_unknown");
            };
            if attempt.task_id != lease.task_id
                || attempt.task_revision != lease.task_revision
                || attempt.owner_id != lease.owner_client_id
            {
                return Ok("possible_or_unknown");
            }
            if workspace_resource_use(tx, lease, &attempt)?.unresolved() {
                Ok("active_or_unknown")
            } else {
                Ok("not_reconciled")
            }
        }
        _ => Ok("not_reconciled"),
    }
}

fn stale_origin_was_held(lease: &LeaseCandidate) -> bool {
    let Ok(facts) = serde_json::from_str::<Value>(&lease.clean_state_json) else {
        return false;
    };
    facts["prior_lease_state"] == "held"
        || facts["lease_lifecycle"]["latest"]["prior_lease_state"] == "held"
        || facts["lease_lifecycle"]["prior_lease_state"] == "held"
}

fn lifecycle_facts(lease: &LeaseCandidate, transition: Transition, now: i64) -> Result<String> {
    let prior: Value = serde_json::from_str(&lease.clean_state_json)?;
    let original_lease_state = if lease.state == "stale" {
        prior["prior_lease_state"]
            .as_str()
            .or_else(|| prior["lease_lifecycle"]["latest"]["prior_lease_state"].as_str())
            .or_else(|| prior["lease_lifecycle"]["prior_lease_state"].as_str())
            .unwrap_or("unknown")
            .to_owned()
    } else {
        lease.state.clone()
    };
    let (state, reason, native_effect) = match transition {
        Transition::Stale {
            reason,
            native_effect,
        } => ("stale", reason, native_effect),
        Transition::Released { reason } => {
            ("released", reason, "no_active_or_unknown_native_records")
        }
    };
    let event = json!({
        "state": state,
        "reason_code": reason,
        "prior_lease_state": original_lease_state,
        "at_ms": now,
        "native_effect_status": native_effect,
        "runtime_observation": "not_performed",
        "native_process_stop_action": "none",
        "filesystem_cleanup": "not_attempted",
    });
    let mut facts = if prior.is_object() {
        prior
    } else {
        json!({"prior_clean_state":prior})
    };
    let object = facts.as_object_mut().ok_or_else(|| {
        Error::new(
            "WORKSPACE_LIFECYCLE",
            "clean-state evidence is not an object",
        )
    })?;
    object.insert(
        "prior_lease_state".to_owned(),
        json!(original_lease_state.clone()),
    );
    let previous = object
        .remove("lease_lifecycle")
        .map(|history| history.get("latest").cloned().unwrap_or(history));
    let lifecycle = match previous {
        Some(previous) => json!({"previous":previous,"latest":event}),
        None => event,
    };
    object.insert("lease_lifecycle".to_owned(), lifecycle);
    model::canonical(&facts)
}
