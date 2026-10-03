//! Bounded, database-only reconciliation for durable workspace lease authority.
//!
//! This module never inspects or removes a worktree. Attempt release is an
//! owner attestation, not proof that a native process stopped, so a lease is
//! released only when its exact terminal Attempt is released and no linked
//! operation, producer, or native binding remains active or unresolved.

use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::{Value, json};

const MAX_LIFECYCLE_SWEEP: usize = 32;
const MAX_PRODUCER_EVIDENCE_BYTES: usize = 64 * 1024;
const MAX_PRODUCERS: usize = 256;
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
/// are invalidated. Stale rows re-enter only after their exact Attempt is
/// terminal and released, so safe scopes can eventually leave the fence.
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

    let candidates = candidates(tx, limit)?;
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
               AND binding_digest=?14 AND state=?15",
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

fn candidates(tx: &Transaction<'_>, limit: usize) -> Result<Vec<LeaseCandidate>> {
    let mut statement = tx.prepare(
        "SELECT l.lease_id,l.registration_id,l.registration_generation,l.project_id,
                l.task_id,l.task_revision,l.operation_id,l.owner_client_id,l.attempt_id,
                l.generation,l.binding_digest,l.state,l.clean_state_json
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
           OR (l.state='stale' AND l.attempt_id IS NOT NULL
             AND COALESCE(
               json_extract(l.clean_state_json,'$.prior_lease_state'),
               json_extract(l.clean_state_json,'$.lease_lifecycle.latest.prior_lease_state'),
               json_extract(l.clean_state_json,'$.lease_lifecycle.prior_lease_state')
             )='held'
             AND EXISTS (
             SELECT 1 FROM attempts a
             WHERE a.attempt_id=l.attempt_id AND a.task_id=l.task_id
               AND a.task_revision=l.task_revision AND a.owner_id=l.owner_client_id
               AND a.released_at_ms IS NOT NULL
               AND a.state IN ('accepted','failed','cancelled','superseded')
           ))
         )
         ORDER BY l.updated_at_ms,l.lease_id LIMIT ?1",
    )?;
    let rows = statement.query_map([limit as i64], |row| {
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
        })
    })?;
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
        let unresolved_native = has_unresolved_native_work(tx, lease, &attempt)?;
        let held_origin =
            lease.state == "held" || (lease.state == "stale" && stale_origin_was_held(lease));
        if !unresolved_native && held_origin {
            return Ok(Some(Transition::Released {
                reason: "exact_attempt_released_no_active_native_records",
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

fn has_unresolved_native_work(
    tx: &Transaction<'_>,
    lease: &LeaseCandidate,
    attempt: &AttemptFacts,
) -> Result<bool> {
    let linked_operation: bool = tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM operations
           WHERE operation_id=?1 AND state IN ('queued','sending','native_accepted','outcome_unknown')
           UNION ALL
           SELECT 1 FROM operations
           WHERE operation_id=?2 AND state IN ('queued','sending','native_accepted','outcome_unknown')
           UNION ALL
           SELECT 1 FROM operations INDEXED BY workspace_attempt_operation_state
           WHERE attempt_id=?3 AND state IN ('queued','sending','native_accepted','outcome_unknown')
           UNION ALL
           SELECT 1 FROM operations INDEXED BY workspace_task_operation_state
           WHERE task_id=?4 AND state IN ('queued','sending','native_accepted','outcome_unknown')
           UNION ALL
           SELECT 1 FROM operations INDEXED BY unresolved_target_operations
           WHERE binding_id=?5 AND binding_generation=?6
             AND state IN ('queued','sending','native_accepted','outcome_unknown')
         )",
        params![
            lease.operation_id,
            attempt.start_operation_id,
            attempt.attempt_id,
            lease.task_id,
            attempt.binding_id,
            attempt.binding_generation
        ],
        |row| row.get(0),
    )?;
    let held_check_resource: bool = tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM check_runs
           WHERE attempt_id=?1 AND resource_claimed_at_ms IS NOT NULL
             AND resource_released_at_ms IS NULL
         )",
        [attempt.attempt_id.as_str()],
        |row| row.get(0),
    )?;
    let active_binding: bool = tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM bindings
           WHERE binding_id=?1 AND generation=?2 AND released_at_ms IS NULL
           UNION ALL
           SELECT 1 FROM operations o INDEXED BY workspace_task_operation_state
           JOIN bindings b ON b.binding_id=o.binding_id AND b.generation=o.binding_generation
           WHERE o.task_id=?3 AND o.state IN ('queued','sending','native_accepted','outcome_unknown')
             AND b.released_at_ms IS NULL
           UNION ALL
           SELECT 1 FROM operations o INDEXED BY workspace_attempt_operation_state
           JOIN bindings b ON b.binding_id=o.binding_id AND b.generation=o.binding_generation
           WHERE o.attempt_id=?4 AND o.state IN ('queued','sending','native_accepted','outcome_unknown')
             AND b.released_at_ms IS NULL
           UNION ALL
           SELECT 1 FROM operations o
           JOIN bindings b ON b.binding_id=o.binding_id AND b.generation=o.binding_generation
           WHERE o.operation_id=?5 AND o.state IN ('queued','sending','native_accepted','outcome_unknown')
             AND b.released_at_ms IS NULL
           UNION ALL
           SELECT 1 FROM operations o
           JOIN bindings b ON b.binding_id=o.binding_id AND b.generation=o.binding_generation
           WHERE o.operation_id=?6 AND o.state IN ('queued','sending','native_accepted','outcome_unknown')
             AND b.released_at_ms IS NULL
         )",
        params![
            attempt.binding_id,
            attempt.binding_generation,
            lease.task_id,
            attempt.attempt_id,
            lease.operation_id,
            attempt.start_operation_id,
        ],
        |row| row.get(0),
    )?;
    let unresolved_producer =
        producers_are_unresolved(&attempt.producers_json, attempt.producers_oversized);
    Ok(linked_operation || held_check_resource || active_binding || unresolved_producer)
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
            if has_unresolved_native_work(tx, lease, &attempt)? {
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
