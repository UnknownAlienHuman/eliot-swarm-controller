//! Durable registration and lease authority for manager-owned worktrees.
//!
//! This module performs only bounded SQLite work. Filesystem and Git effects
//! are implemented by `crate::workspace` after the reservation transaction.

use super::{meta, require_local_operator};
use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role, TaskSpec},
    workspace::{
        self, LeaseAuthorityRef, LeaseEvidence, LeaseReservation, WorkspaceLeasePlan,
        WorkspaceRegistration,
    },
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::path::{Component, Path, PathBuf, Prefix};

const MAX_PENDING_LEASES: usize = 32;
const MAX_ACTIVE_SCOPE_ROWS: i64 = 257;

#[derive(Debug, Clone)]
pub(crate) struct WorkspaceLeaseTicket {
    pub registration: WorkspaceRegistration,
    pub reservation: LeaseReservation,
    pub plan: WorkspaceLeasePlan,
    pub state: String,
}

#[derive(Debug)]
struct LeaseRow {
    lease_id: String,
    registration_id: String,
    registration_generation: i64,
    project_id: String,
    task_id: String,
    task_revision: i64,
    operation_id: String,
    plan_digest: String,
    owner_client_id: String,
    attempt_id: Option<String>,
    allowed_paths: Vec<String>,
    allowed_symbols: Vec<String>,
    baseline_commit: String,
    branch_ref: String,
    worktree_handle: String,
    workspace_path: PathBuf,
    clean_state: Value,
    generation: i64,
    binding_digest: String,
    state: String,
}

/// Synchronize operator-configured registrations after bootstrap authority is
/// available. This reads resolved Config values only; it performs no FS work.
pub(crate) fn sync_configured_registrations(
    tx: &Transaction<'_>,
    local_operator_client_id: &str,
    config: &Config,
    now: i64,
) -> Result<()> {
    require_local_operator(tx, local_operator_client_id)?;
    let mut existing = std::collections::BTreeMap::new();
    {
        let mut statement = tx.prepare(
            "SELECT registration_id,project_id,registration_digest,generation,state
             FROM workspace_registrations ORDER BY project_id",
        )?;
        for row in statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })? {
            let (id, project, digest, generation, state) = row?;
            existing.insert(project, (id, digest, generation, state));
        }
    }

    for project_id in config.workspace.projects.keys() {
        let old = existing.get(project_id);
        let registration_id = old.map(|row| row.0.clone()).unwrap_or_else(model::new_id);
        let generation = match old {
            Some((_, _, generation, state)) => {
                // Re-activation is a new authority generation even when the
                // paths happen to be unchanged.
                let probe = WorkspaceRegistration::from_config(
                    project_id,
                    &config.workspace,
                    &config.forge,
                    registration_id.clone(),
                    *generation,
                )?;
                if state == "active"
                    && old.is_some_and(|(_, digest, _, _)| digest == &probe.registration_digest)
                {
                    *generation
                } else {
                    generation
                        .checked_add(1)
                        .ok_or_else(|| Error::new("WORKSPACE_GENERATION", "generation overflow"))?
                }
            }
            None => 1,
        };
        let registration = WorkspaceRegistration::from_config(
            project_id,
            &config.workspace,
            &config.forge,
            registration_id,
            generation,
        )?;
        let allowed_roots = registration
            .allowed_roots
            .iter()
            .map(|path| path_text(path.as_path()))
            .collect::<Result<Vec<_>>>()?;
        tx.execute(
            "INSERT INTO workspace_registrations
                (registration_id,project_id,trusted_repository,repository_path,
                 allowed_roots_json,registration_digest,generation,authorized_by,state,
                 created_at_ms,updated_at_ms)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'active',?9,?9)
             ON CONFLICT(project_id) DO UPDATE SET
                registration_id=excluded.registration_id,
                trusted_repository=excluded.trusted_repository,
                repository_path=excluded.repository_path,
                allowed_roots_json=excluded.allowed_roots_json,
                registration_digest=excluded.registration_digest,
                generation=excluded.generation,
                authorized_by=excluded.authorized_by,
                state='active',updated_at_ms=excluded.updated_at_ms",
            params![
                registration.registration_id,
                registration.project_id,
                registration.trusted_repository,
                path_text(&registration.repository_path)?,
                model::canonical(&json!(allowed_roots))?,
                registration.registration_digest,
                registration.generation,
                local_operator_client_id,
                now,
            ],
        )?;
        // A configuration generation change invalidates prior authority but
        // never deletes a worktree or treats it as safe to reuse.
        tx.execute(
            "UPDATE workspace_leases SET state='stale',
                 clean_state_json=json_set(?3,'$.prior_lease_state',state),updated_at_ms=?4
             WHERE registration_id=?1 AND registration_generation<>?2
               AND state IN ('preparing','held','outcome_unknown')",
            params![
                registration.registration_id,
                registration.generation,
                model::canonical(&json!({
                    "status":"registration_changed",
                    "reason_code":"registration_changed",
                    "native_effect_status":"possible_or_unknown",
                    "runtime_observation":"not_performed",
                    "filesystem_cleanup":"not_attempted",
                }))?,
                now,
            ],
        )?;
    }

    for (project_id, (registration_id, digest, generation, state)) in existing {
        if config.workspace.projects.contains_key(&project_id) || state == "revoked" {
            continue;
        }
        let next = generation
            .checked_add(1)
            .ok_or_else(|| Error::new("WORKSPACE_GENERATION", "generation overflow"))?;
        tx.execute(
            "UPDATE workspace_registrations SET state='revoked',generation=?2,
                registration_digest=?3,authorized_by=?4,updated_at_ms=?5
             WHERE registration_id=?1 AND generation=?6 AND state='active'",
            params![
                registration_id,
                next,
                digest,
                local_operator_client_id,
                now,
                generation
            ],
        )?;
        tx.execute(
            "UPDATE workspace_leases SET state='stale',
                 clean_state_json=json_set(?3,'$.prior_lease_state',state),updated_at_ms=?4
             WHERE registration_id=?1 AND registration_generation<>?2
               AND state IN ('preparing','held','outcome_unknown')",
            params![
                registration_id,
                next,
                model::canonical(&json!({
                    "status":"registration_revoked",
                    "reason_code":"registration_revoked",
                    "native_effect_status":"possible_or_unknown",
                    "runtime_observation":"not_performed",
                    "filesystem_cleanup":"not_attempted",
                }))?,
                now,
            ],
        )?;
    }
    Ok(())
}

type WorkspaceRegistrationQueryRow = (String, String, String, i64, String, String, String, String);

/// Rebuild the active registration from resolved config and persisted
/// generation. The exact digest comparison detects config/database drift.
pub(crate) fn get_registration(
    db: &Connection,
    project_id: &str,
    config: &Config,
) -> Result<WorkspaceRegistration> {
    let row: Option<WorkspaceRegistrationQueryRow> = db
        .query_row(
            "SELECT registration_id,registration_digest,state,generation,project_id,
                    trusted_repository,repository_path,allowed_roots_json
             FROM workspace_registrations WHERE project_id=?1",
            [project_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()?;
    let (
        registration_id,
        digest,
        state,
        generation,
        stored_project,
        trusted_repository,
        repository_path,
        allowed_roots_json,
    ) = row.ok_or_else(|| {
        Error::new(
            "WORKSPACE_UNREGISTERED",
            "project has no workspace registration",
        )
    })?;
    if state != "active" || generation <= 0 || stored_project != project_id {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_REVOKED",
            "workspace registration is not active",
        ));
    }
    let registration = WorkspaceRegistration::from_config(
        project_id,
        &config.workspace,
        &config.forge,
        registration_id,
        generation,
    )?;
    if registration.registration_digest != digest {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_CHANGED",
            "configured workspace facts differ from the active registration",
        ));
    }
    if registration.trusted_repository != trusted_repository
        || path_text(&registration.repository_path)? != repository_path
        || model::canonical(&json!(
            registration
                .allowed_roots
                .iter()
                .map(|path| path_text(path))
                .collect::<Result<Vec<_>>>()?
        ))? != allowed_roots_json
    {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_CHANGED",
            "persisted repository facts differ from configured authority",
        ));
    }
    Ok(registration)
}

/// Reserve a lease for the typed launcher actor. A WorkDispatch caller remains
/// the Operation caller while its current Manager owns the lease.
pub(crate) fn reserve_lease_for_launch(
    tx: &Transaction<'_>,
    actor: &super::launcher::LaunchActor,
    plan: &WorkspaceLeasePlan,
    now: i64,
) -> Result<LeaseReservation> {
    plan.validate()?;
    authorize_launch_actor(tx, actor, plan, plan.attempt_id.as_deref())?;
    reserve_lease_inner(
        tx,
        actor.technical_requester_id(),
        actor.effective_manager_id(),
        plan,
        now,
    )
}

fn reserve_lease_inner(
    tx: &Transaction<'_>,
    technical_requester_id: &str,
    effective_manager_id: &str,
    plan: &WorkspaceLeasePlan,
    now: i64,
) -> Result<LeaseReservation> {
    verify_launch_operation(
        tx,
        plan,
        technical_requester_id,
        effective_manager_id,
        false,
    )?;
    verify_task_authority(tx, plan)?;
    reject_scope_conflicts(tx, plan)?;

    let registration = registration_row(tx, &plan.project_id)?;
    if registration.state != "active" {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_REVOKED",
            "workspace registration is not active",
        ));
    }
    let root = registration
        .allowed_roots
        .first()
        .ok_or_else(|| Error::new("WORKSPACE_UNREGISTERED", "registered roots are empty"))?;
    let lease_id = model::new_id();
    let worktree_handle = format!("wt-{lease_id}");
    let branch_ref = format!("refs/heads/codex/swarm/{lease_id}");
    let workspace_path = root.join(&worktree_handle);
    let workspace_path_text = path_text(&workspace_path)?;
    let generation: i64 = tx.query_row(
        "SELECT COALESCE(MAX(generation),0)+1 FROM workspace_leases WHERE registration_id=?1",
        [&registration.registration_id],
        |row| row.get(0),
    )?;
    if generation <= 0 {
        return Err(Error::new("WORKSPACE_GENERATION", "generation overflow"));
    }
    let intent_digest = lease_intent_digest(
        &lease_id,
        &registration,
        generation,
        plan,
        &branch_ref,
        &worktree_handle,
        &workspace_path_text,
    )?;
    let baseline_commit = plan.expected_baseline_commit.as_deref().unwrap_or("");
    tx.execute(
        "INSERT INTO workspace_leases
            (lease_id,registration_id,registration_generation,project_id,task_id,
             task_revision,operation_id,plan_digest,owner_client_id,attempt_id,
             allowed_paths_json,allowed_symbols_json,baseline_commit,branch_ref,
             worktree_handle,workspace_path,clean_state_json,generation,binding_digest,
             state,created_at_ms,updated_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,
                '{\"status\":\"pending\"}',?17,?18,'preparing',?19,?19)",
        params![
            lease_id,
            registration.registration_id,
            registration.generation,
            plan.project_id,
            plan.task_id,
            plan.task_revision,
            plan.operation_id,
            plan.plan_digest,
            plan.owner_client_id,
            plan.attempt_id,
            model::canonical(&json!(plan.allowed_paths))?,
            model::canonical(&json!(plan.allowed_symbols))?,
            baseline_commit,
            branch_ref,
            worktree_handle,
            workspace_path_text,
            generation,
            intent_digest,
            now,
        ],
    )?;
    Ok(LeaseReservation::from_store(
        lease_id,
        registration.registration_id,
        registration.generation,
        generation,
        intent_digest,
        plan,
        plan.expected_baseline_commit.clone(),
        branch_ref,
        worktree_handle,
        workspace_path,
    ))
}

/// Commit verified host evidence for the same typed launch authority used to
/// reserve the lease. No Principal is reconstructed for WorkDispatch.
pub(crate) fn commit_lease_for_launch(
    tx: &Transaction<'_>,
    actor: &super::launcher::LaunchActor,
    registration: &WorkspaceRegistration,
    reservation: &LeaseReservation,
    plan: &WorkspaceLeasePlan,
    evidence: &LeaseEvidence,
    now: i64,
) -> Result<LeaseAuthorityRef> {
    plan.validate()?;
    authorize_launch_actor(tx, actor, plan, plan.attempt_id.as_deref())?;
    verify_launch_operation(
        tx,
        plan,
        actor.technical_requester_id(),
        actor.effective_manager_id(),
        false,
    )?;
    persist_verified_evidence(
        tx,
        actor.effective_manager_id(),
        registration,
        reservation,
        plan,
        evidence,
        "preparing",
        now,
    )
}

/// Reconcile an unknown host effect only under the same exact current actor,
/// Task and lease identity that authorized the original reservation.
pub(crate) fn reconcile_lease_for_launch(
    tx: &Transaction<'_>,
    actor: &super::launcher::LaunchActor,
    registration: &WorkspaceRegistration,
    reservation: &LeaseReservation,
    plan: &WorkspaceLeasePlan,
    evidence: &LeaseEvidence,
    now: i64,
) -> Result<LeaseAuthorityRef> {
    plan.validate()?;
    authorize_launch_actor(tx, actor, plan, plan.attempt_id.as_deref())?;
    verify_launch_operation(
        tx,
        plan,
        actor.technical_requester_id(),
        actor.effective_manager_id(),
        true,
    )?;
    persist_verified_evidence(
        tx,
        actor.effective_manager_id(),
        registration,
        reservation,
        plan,
        evidence,
        "outcome_unknown",
        now,
    )
}

// These are separate authority inputs from Store, the caller, the admitted
// plan, and verified filesystem evidence. Keeping them explicit makes the
// compare-and-swap proof visible at the commit boundary.
#[allow(clippy::too_many_arguments)]
fn persist_verified_evidence(
    tx: &Transaction<'_>,
    effective_manager_id: &str,
    registration: &WorkspaceRegistration,
    reservation: &LeaseReservation,
    plan: &WorkspaceLeasePlan,
    evidence: &LeaseEvidence,
    expected_state: &str,
    now: i64,
) -> Result<LeaseAuthorityRef> {
    plan.validate()?;
    if effective_manager_id != plan.owner_client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "workspace lease owner differs from the effective manager",
        ));
    }
    workspace::evidence_matches_reservation(evidence, reservation, plan, registration)?;
    let current = registration_row(tx, &plan.project_id)?;
    if current.state != "active"
        || current.registration_id != registration.registration_id
        || current.generation != registration.generation
        || current.registration_digest != registration.registration_digest
    {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_CHANGED",
            "workspace registration changed during lease preparation",
        ));
    }
    let row = lease_row(tx, &reservation.lease_id)?;
    if row.state != expected_state
        || row.binding_digest != reservation.intent_digest
        || row.generation != reservation.generation
        || row.registration_generation != registration.generation
        || row.workspace_path != *reservation.workspace_path()
        || row.project_id != plan.project_id
        || row.task_id != plan.task_id
        || row.task_revision != plan.task_revision
        || row.operation_id != plan.operation_id
        || row.plan_digest != plan.plan_digest
        || row.owner_client_id != plan.owner_client_id
        || row.attempt_id != plan.attempt_id
        || row.allowed_paths != plan.allowed_paths
        || row.allowed_symbols != plan.allowed_symbols
        || row.branch_ref != reservation.branch_ref
        || row.worktree_handle != reservation.worktree_handle
    {
        return Err(Error::conflict(
            "workspace lease reservation changed before evidence commit",
        ));
    }
    verify_task_authority(tx, plan)?;
    let binding_digest = workspace::final_binding_digest(evidence)?;
    let changed = tx.execute(
        "UPDATE workspace_leases SET baseline_commit=?4,clean_state_json=?5,
             binding_digest=?6,state='held',updated_at_ms=?7
         WHERE lease_id=?1 AND generation=?2 AND binding_digest=?3 AND state=?8",
        params![
            reservation.lease_id,
            reservation.generation,
            reservation.intent_digest,
            evidence.baseline_commit,
            model::canonical(&evidence.clean_state)?,
            binding_digest,
            now,
            expected_state,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict("workspace lease evidence CAS failed"));
    }
    lease_authority(&lease_row(tx, &reservation.lease_id)?)
}

/// Recheck held authority immediately before claim/open. This is a SQL-only
/// compare of Task, operation, current registration, scope and exact Attempt.
pub(crate) fn assert_held_for_claim(
    tx: &Transaction<'_>,
    reference: &LeaseAuthorityRef,
    plan: &WorkspaceLeasePlan,
) -> Result<()> {
    plan.validate()?;
    let row = lease_row(tx, &reference.lease_id)?;
    let attempt_matches = match (plan.attempt_id.as_deref(), row.attempt_id.as_deref()) {
        (Some(expected), Some(actual)) => expected == actual,
        (None, None) | (None, Some(_)) => true,
        (Some(_), None) => false,
    };
    if row.state != "held"
        || row.generation != reference.generation
        || row.binding_digest != reference.binding_digest
        || row.registration_id != reference.registration_id
        || row.registration_generation != reference.registration_generation
        || row.project_id != plan.project_id
        || row.task_id != plan.task_id
        || row.task_revision != plan.task_revision
        || row.operation_id != plan.operation_id
        || row.plan_digest != plan.plan_digest
        || row.owner_client_id != plan.owner_client_id
        || row.allowed_paths != plan.allowed_paths
        || row.allowed_symbols != plan.allowed_symbols
        || plan
            .expected_baseline_commit
            .as_deref()
            .is_some_and(|expected| !row.baseline_commit.eq_ignore_ascii_case(expected))
        || !attempt_matches
    {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "held workspace lease does not match this launch plan",
        ));
    }
    verify_authority_ref(reference, &row)?;
    verify_current_registration_generation(tx, &row)?;
    verify_task_snapshot(tx, plan)?;
    verify_task_authority_with_pinned_attempt(tx, plan, row.attempt_id.as_deref())?;
    Ok(())
}

/// Pin the exact post-claim Attempt without converting a WorkDispatch actor
/// into a Principal. Its technical parent Operation and effective owner are
/// revalidated against the retained context before the lease CAS.
pub(crate) fn pin_lease_attempt_for_launch(
    tx: &Transaction<'_>,
    actor: &super::launcher::LaunchActor,
    reference: &LeaseAuthorityRef,
    plan: &WorkspaceLeasePlan,
    attempt_id: &str,
    now: i64,
) -> Result<LeaseAuthorityRef> {
    plan.validate()?;
    if actor.effective_manager_id() != plan.owner_client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "workspace lease owner differs from the effective manager",
        ));
    }
    actor.require_claimed_launch_attempt(
        tx,
        &plan.operation_id,
        &plan.task_id,
        plan.task_revision,
        attempt_id,
    )?;
    verify_operation_caller(tx, &plan.operation_id, actor.technical_requester_id())?;
    pin_lease_attempt_inner(
        tx,
        actor.effective_manager_id(),
        reference,
        plan,
        attempt_id,
        now,
    )
}

fn pin_lease_attempt_inner(
    tx: &Transaction<'_>,
    effective_manager_id: &str,
    reference: &LeaseAuthorityRef,
    plan: &WorkspaceLeasePlan,
    attempt_id: &str,
    now: i64,
) -> Result<LeaseAuthorityRef> {
    plan.validate()?;
    if reference.attempt_id.is_some() || plan.attempt_id.is_some() {
        return Err(Error::new(
            "WORKSPACE_ATTEMPT_PINNED",
            "workspace lease already has an Attempt binding",
        ));
    }
    let row = lease_row(tx, &reference.lease_id)?;
    if row.state != "held"
        || row.generation != reference.generation
        || row.binding_digest != reference.binding_digest
        || row.attempt_id.is_some()
        || row.task_id != plan.task_id
        || row.task_revision != plan.task_revision
        || row.project_id != plan.project_id
        || row.operation_id != plan.operation_id
        || row.plan_digest != plan.plan_digest
        || row.owner_client_id != effective_manager_id
    {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "held workspace lease changed before Attempt pin",
        ));
    }
    verify_authority_ref(reference, &row)?;
    verify_current_registration_generation(tx, &row)?;
    verify_task_snapshot(tx, plan)?;
    let attempt: (String, i64, String, String, Option<i64>) = tx.query_row(
        "SELECT task_id,task_revision,owner_id,state,released_at_ms FROM attempts
         WHERE attempt_id=?1",
        [attempt_id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    if attempt.0 != plan.task_id
        || attempt.1 != plan.task_revision
        || attempt.2 != plan.owner_client_id
        || attempt.2 != effective_manager_id
        || attempt.4.is_some()
        || !matches!(
            attempt.3.as_str(),
            "reserved" | "running" | "needs_correction"
        )
    {
        return Err(Error::new(
            "WORKSPACE_ATTEMPT_MISMATCH",
            "Attempt is not the exact live Task owner reservation",
        ));
    }
    verify_task_authority_with_pinned_attempt(tx, plan, Some(attempt_id))?;
    let mut digest = model::canonical(&json!({
        "prior_binding_digest":row.binding_digest,
        "lease_id":row.lease_id,
        "generation":row.generation,
        "plan_digest":row.plan_digest,
        "attempt_id":attempt_id,
        "owner_client_id":row.owner_client_id,
    }))?;
    let binding_digest = model::digest(digest.as_bytes());
    digest.clear();
    let changed = tx.execute(
        "UPDATE workspace_leases SET attempt_id=?4,binding_digest=?5,updated_at_ms=?6
         WHERE lease_id=?1 AND generation=?2 AND binding_digest=?3
           AND state='held' AND attempt_id IS NULL",
        params![
            row.lease_id,
            row.generation,
            row.binding_digest,
            attempt_id,
            binding_digest,
            now,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict("workspace Attempt pin CAS failed"));
    }
    lease_authority(&lease_row(tx, &row.lease_id)?)
}

/// Path-free exact read projection for launcher manifest finalization.
pub(crate) fn get_lease_view(db: &Connection, reference: &LeaseAuthorityRef) -> Result<Value> {
    let row = lease_row(db, &reference.lease_id)?;
    verify_authority_ref(reference, &row)?;
    if row.state != "held" {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "workspace lease is not held",
        ));
    }
    verify_current_registration_generation(db, &row)?;
    Ok(json!({
        "lease_id":row.lease_id,
        "registration_id":row.registration_id,
        "registration_generation":row.registration_generation,
        "project_id":row.project_id,
        "task_id":row.task_id,
        "task_revision":row.task_revision,
        "operation_id":row.operation_id,
        "plan_digest":row.plan_digest,
        "owner_client_id":row.owner_client_id,
        "attempt_id":row.attempt_id,
        "generation":row.generation,
        "allowed_paths":row.allowed_paths,
        "allowed_symbols":row.allowed_symbols,
        "baseline_commit":row.baseline_commit,
        "branch_ref":row.branch_ref,
        "worktree_handle":row.worktree_handle,
        "clean_state":row.clean_state,
        "binding_digest":row.binding_digest,
        "state":row.state,
        "local_path_included":false,
    }))
}

/// Bind a Participant source capture to the exact host workspace retained by
/// the live launch lease. The lease path is deliberately absent from public
/// receipts, but it remains a durable Store fact for this local pre-effect
/// check. Managers and Operators use the existing general capture path.
pub(crate) fn participant_source_workspace(
    db: &Connection,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    repository: &Path,
) -> Result<PathBuf> {
    let lease_ids = {
        let mut statement = db.prepare(
            "SELECT lease_id FROM workspace_leases
             WHERE state='held' AND project_id=(SELECT project_id FROM tasks WHERE task_id=?1)
               AND task_id=?1 AND task_revision=?2 AND attempt_id=?3
             ORDER BY generation,lease_id LIMIT 2",
        )?;
        statement
            .query_map(params![task_id, task_revision, attempt_id], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let lease_id = match lease_ids.as_slice() {
        [lease_id] => lease_id,
        [] => {
            return Err(Error::new(
                "SOURCE_WORKSPACE_UNAVAILABLE",
                "Participant source capture requires one held launch workspace lease",
            ));
        }
        _ => {
            return Err(Error::new(
                "SOURCE_WORKSPACE_AMBIGUOUS",
                "Participant source capture has more than one held workspace lease",
            ));
        }
    };
    let row = lease_row(db, lease_id)?;
    if row.task_id != task_id
        || row.task_revision != task_revision
        || row.attempt_id.as_deref() != Some(attempt_id)
        || row.state != "held"
        || !row.workspace_path.is_absolute()
    {
        return Err(Error::new(
            "SOURCE_WORKSPACE_STALE",
            "held workspace lease no longer matches the Participant Task and Attempt",
        ));
    }
    verify_current_registration_generation(db, &row)?;
    let authority = lease_authority(&row)?;
    if held_lease_for_operation(db, &row.operation_id)?.as_ref() != Some(&authority) {
        return Err(Error::new(
            "SOURCE_WORKSPACE_STALE",
            "held workspace lease is not the exact current launch authority",
        ));
    }

    let (method, operation_task, operation_attempt, effective_raw): (
        String,
        Option<String>,
        Option<String>,
        String,
    ) = db.query_row(
        "SELECT method,task_id,attempt_id,effective_request_json
         FROM operations WHERE operation_id=?1",
        [row.operation_id.as_str()],
        |record| {
            Ok((
                record.get(0)?,
                record.get(1)?,
                record.get(2)?,
                record.get(3)?,
            ))
        },
    )?;
    if method != "swarm.launch"
        || operation_task.as_deref() != Some(task_id)
        || operation_attempt.as_deref() != Some(attempt_id)
    {
        return Err(Error::new(
            "SOURCE_WORKSPACE_STALE",
            "workspace lease is not retained by the exact launch Operation",
        ));
    }
    let effective: Value = serde_json::from_str(&effective_raw)?;
    let manifest = effective.get("launch_manifest").ok_or_else(|| {
        Error::new(
            "SOURCE_WORKSPACE_STALE",
            "launch Operation has no retained launch manifest",
        )
    })?;
    if manifest["task"]["task_id"] != task_id
        || manifest["task"]["observed_revision"] != task_revision
        || manifest["task"]["attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "SOURCE_WORKSPACE_STALE",
            "launch manifest does not retain the exact source Task and Attempt",
        ));
    }
    let manifest_authority: LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone()).map_err(|_| {
            Error::new(
                "SOURCE_WORKSPACE_STALE",
                "launch manifest workspace authority is unavailable",
            )
        })?;
    if manifest_authority != authority {
        return Err(Error::new(
            "SOURCE_WORKSPACE_STALE",
            "launch manifest workspace authority differs from the held lease",
        ));
    }

    if lexical_source_path(repository)? != lexical_source_path(&row.workspace_path)? {
        return Err(Error::new(
            "SOURCE_WORKSPACE_MISMATCH",
            "Participant source repository is outside the exact held workspace",
        ));
    }
    Ok(row.workspace_path)
}

fn lexical_source_path(path: &Path) -> Result<String> {
    if !path.is_absolute() {
        return Err(Error::new(
            "SOURCE_WORKSPACE_MISMATCH",
            "Participant source repository must be absolute",
        ));
    }
    let mut normalized = String::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => match prefix.kind() {
                Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                    normalized.push(char::from(drive));
                    normalized.push(':');
                }
                Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                    let server = server.to_str().ok_or_else(|| {
                        Error::new("SOURCE_WORKSPACE_MISMATCH", "source path is not UTF-8")
                    })?;
                    let share = share.to_str().ok_or_else(|| {
                        Error::new("SOURCE_WORKSPACE_MISMATCH", "source path is not UTF-8")
                    })?;
                    normalized.push_str("//");
                    normalized.push_str(server);
                    normalized.push('/');
                    normalized.push_str(share);
                }
                Prefix::DeviceNS(_) | Prefix::Verbatim(_) => {
                    return Err(Error::new(
                        "SOURCE_WORKSPACE_MISMATCH",
                        "unsupported device namespace in Participant source repository",
                    ));
                }
            },
            Component::RootDir => {
                if !normalized.ends_with('/') {
                    normalized.push('/');
                }
            }
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(Error::new(
                    "SOURCE_WORKSPACE_MISMATCH",
                    "Participant source repository may not contain parent traversal",
                ));
            }
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    Error::new("SOURCE_WORKSPACE_MISMATCH", "source path is not UTF-8")
                })?;
                if !normalized.is_empty() && !normalized.ends_with('/') {
                    normalized.push('/');
                }
                normalized.push_str(part);
            }
        }
    }
    if normalized.is_empty() || normalized.chars().any(char::is_control) {
        return Err(Error::new(
            "SOURCE_WORKSPACE_MISMATCH",
            "Participant source repository path is invalid",
        ));
    }
    Ok(if cfg!(windows) {
        normalized.to_ascii_lowercase()
    } else {
        normalized
    })
}

/// Recover durable authority after a lost commit or pin response. The caller
/// correlates by exact launch Operation; no local path is returned.
pub(crate) fn held_lease_for_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<LeaseAuthorityRef>> {
    let ids = {
        let mut statement = db.prepare(
            "SELECT lease_id FROM workspace_leases
             WHERE operation_id=?1 AND state='held' ORDER BY generation,lease_id LIMIT 2",
        )?;
        statement
            .query_map([operation_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    if ids.len() > 1 {
        return Err(Error::new(
            "WORKSPACE_LEASE_AMBIGUOUS",
            "launch Operation has more than one held workspace lease",
        ));
    }
    let Some(lease_id) = ids.first() else {
        return Ok(None);
    };
    let row = lease_row(db, lease_id)?;
    verify_current_registration_generation(db, &row)?;
    Ok(Some(lease_authority(&row)?))
}

/// Retain the host path and precise scope for recovery of preparing/unknown
/// leases. Configured repository/root identity is rebuilt outside SQL.
pub(crate) fn pending_leases(
    db: &Connection,
    config: &Config,
    limit: usize,
) -> Result<Vec<WorkspaceLeaseTicket>> {
    if limit == 0 || limit > MAX_PENDING_LEASES {
        return Err(Error::invalid(
            "pending lease limit is outside the bounded range",
        ));
    }
    let ids = {
        let mut statement = db.prepare(
            "SELECT lease_id FROM workspace_leases
             WHERE state IN ('preparing','outcome_unknown')
             ORDER BY updated_at_ms,lease_id LIMIT ?1",
        )?;
        statement
            .query_map([limit as i64], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut tickets = Vec::with_capacity(ids.len());
    for lease_id in ids {
        let row = lease_row(db, &lease_id)?;
        if !config.workspace.projects.contains_key(&row.project_id)
            || !config.forge.projects.contains_key(&row.project_id)
        {
            continue;
        }
        let current = registration_row(db, &row.project_id)?;
        if current.state != "active"
            || current.registration_id != row.registration_id
            || current.generation != row.registration_generation
        {
            continue;
        }
        let registration = get_registration(db, &row.project_id, config)?;
        if registration.registration_id != row.registration_id
            || registration.generation != row.registration_generation
        {
            continue;
        }
        tickets.push(ticket_from_row(row, registration)?);
    }
    Ok(tickets)
}

/// Exact-operation recovery avoids a bounded global page hiding one lease
/// behind unrelated revoked projects or an older backlog.
pub(crate) fn pending_lease_for_operation(
    db: &Connection,
    config: &Config,
    operation_id: &str,
) -> Result<Option<WorkspaceLeaseTicket>> {
    let ids = {
        let mut statement = db.prepare(
            "SELECT lease_id FROM workspace_leases
             WHERE operation_id=?1 AND state IN ('preparing','outcome_unknown')
             ORDER BY generation,lease_id LIMIT 2",
        )?;
        statement
            .query_map([operation_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    if ids.len() > 1 {
        return Err(Error::new(
            "WORKSPACE_LEASE_AMBIGUOUS",
            "launch Operation has more than one preparing workspace lease",
        ));
    }
    let Some(lease_id) = ids.first() else {
        return Ok(None);
    };
    let row = lease_row(db, lease_id)?;
    if !config.workspace.projects.contains_key(&row.project_id)
        || !config.forge.projects.contains_key(&row.project_id)
    {
        return Ok(None);
    }
    let current = registration_row(db, &row.project_id)?;
    if current.state != "active"
        || current.registration_id != row.registration_id
        || current.generation != row.registration_generation
    {
        return Ok(None);
    }
    let registration = get_registration(db, &row.project_id, config)?;
    if registration.registration_id != row.registration_id
        || registration.generation != row.registration_generation
    {
        return Ok(None);
    }
    Ok(Some(ticket_from_row(row, registration)?))
}

fn ticket_from_row(
    row: LeaseRow,
    registration: WorkspaceRegistration,
) -> Result<WorkspaceLeaseTicket> {
    let plan = plan_from_row(&row)?;
    let reservation = LeaseReservation::from_store(
        row.lease_id.clone(),
        row.registration_id.clone(),
        row.registration_generation,
        row.generation,
        row.binding_digest.clone(),
        &plan,
        (!row.baseline_commit.is_empty()).then(|| row.baseline_commit.clone()),
        row.branch_ref.clone(),
        row.worktree_handle.clone(),
        row.workspace_path.clone(),
    );
    Ok(WorkspaceLeaseTicket {
        registration,
        reservation,
        plan,
        state: row.state,
    })
}

/// A host error does not authorize cleanup or replay. Mark only this exact
/// preparing lease stale and preserve the path/branch as an external artifact.
#[expect(dead_code, reason = "workspace lifecycle rejection wiring is pending")]
pub(crate) fn mark_lease_stale(
    tx: &Transaction<'_>,
    lease_id: &str,
    generation: i64,
    intent_digest: &str,
    reason_code: &str,
    now: i64,
) -> Result<bool> {
    if reason_code.is_empty()
        || reason_code.len() > 64
        || !reason_code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(Error::invalid(
            "reason_code must be a bounded lowercase code",
        ));
    }
    let changed = tx.execute(
        "UPDATE workspace_leases SET state='stale',
             clean_state_json=json_set(?4,'$.prior_lease_state',state),updated_at_ms=?5
         WHERE lease_id=?1 AND generation=?2 AND binding_digest=?3
           AND state IN ('preparing','outcome_unknown')",
        params![
            lease_id,
            generation,
            intent_digest,
            model::canonical(&json!({
                "status":"preparation_failed",
                "reason_code":reason_code,
                "native_effect_status":"possible_or_unknown",
                "runtime_observation":"not_performed",
                "filesystem_cleanup":"not_attempted",
            }))?,
            now,
        ],
    )?;
    Ok(changed == 1)
}

/// Restart moves interrupted host preparation to unknown without deleting or
/// re-running external worktree effects.
pub(crate) fn mark_preparing_unknown(tx: &Transaction<'_>, now: i64) -> Result<usize> {
    Ok(tx.execute(
        "UPDATE workspace_leases SET state='outcome_unknown',updated_at_ms=?1
         WHERE state='preparing'",
        [now],
    )?)
}

/// Release only the authority row; cleanup remains a separately verified
/// host action and is intentionally absent from this Store primitive.
#[expect(dead_code, reason = "workspace lifecycle cleanup wiring is pending")]
pub(crate) fn release_lease(
    tx: &Transaction<'_>,
    principal: &Principal,
    reference: &LeaseAuthorityRef,
    now: i64,
) -> Result<()> {
    require_manager_owner(tx, principal, &reference.owner_client_id)?;
    let row = lease_row(tx, &reference.lease_id)?;
    verify_authority_ref(reference, &row)?;
    if row.state != "held" {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "workspace lease is not held",
        ));
    }
    if let Some(attempt_id) = row.attempt_id.as_deref() {
        let attempt: Option<(String, Option<i64>)> = tx
            .query_row(
                "SELECT task_id,released_at_ms FROM attempts WHERE attempt_id=?1",
                [attempt_id],
                |db_row| Ok((db_row.get(0)?, db_row.get(1)?)),
            )
            .optional()?;
        if !attempt.is_some_and(|(task_id, released)| task_id == row.task_id && released.is_some())
        {
            return Err(Error::new(
                "WORKSPACE_ATTEMPT_ACTIVE",
                "workspace lease cannot be released while its Attempt is active",
            ));
        }
    } else {
        let active_attempt: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL)",
            [&row.task_id],
            |db_row| db_row.get(0),
        )?;
        if active_attempt {
            return Err(Error::new(
                "WORKSPACE_ATTEMPT_UNPINNED",
                "current Task Attempt must be pinned before lease release",
            ));
        }
    }
    let changed = tx.execute(
        "UPDATE workspace_leases SET state='released',updated_at_ms=?4
         WHERE lease_id=?1 AND generation=?2 AND binding_digest=?3 AND state='held'",
        params![
            reference.lease_id,
            reference.generation,
            reference.binding_digest,
            now
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict("workspace lease release CAS failed"));
    }
    Ok(())
}

fn verify_task_snapshot(db: &Connection, plan: &WorkspaceLeasePlan) -> Result<TaskSpec> {
    let task: (String, i64, String, String) = db.query_row(
        "SELECT project_id,revision,state,spec_json FROM tasks WHERE task_id=?1",
        [&plan.task_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    if task.0 != plan.project_id || task.1 != plan.task_revision || task.2 != "open" {
        return Err(Error::new(
            "STALE_REVISION",
            "workspace Task is not open at the exact planned revision",
        ));
    }
    let spec: TaskSpec = serde_json::from_str(&task.3).map_err(|_| {
        Error::new(
            "TASK_SPEC_INVALID",
            "workspace Task has no valid typed specification",
        )
    })?;
    spec.validate()?;
    crate::policy::accepted_edition(spec.owner_policy_id.as_deref())?;
    let task_paths = spec
        .scope
        .as_ref()
        .map(|scope| scope.initial_paths.as_slice())
        .unwrap_or_default();
    if task_paths != plan.allowed_paths.as_slice() {
        return Err(Error::new(
            "WORKSPACE_SCOPE_MISMATCH",
            "lease scope differs from the exact current Task scope",
        ));
    }
    Ok(spec)
}

fn verify_task_authority(tx: &Transaction<'_>, plan: &WorkspaceLeasePlan) -> Result<()> {
    let spec = verify_task_snapshot(tx, plan)?;
    if plan.attempt_id.is_none() {
        let new_work = meta(tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] == "enabled";
        if !new_work {
            return Err(Error::new(
                "ADMISSION_DISABLED",
                "new work is disabled for this Task",
            ));
        }
        for dependency in &spec.dependencies {
            super::acceptance::resolve_dependency(tx, dependency)?;
        }
    }
    verify_task_authority_with_pinned_attempt(tx, plan, plan.attempt_id.as_deref())
}

fn verify_task_authority_with_pinned_attempt(
    tx: &Transaction<'_>,
    plan: &WorkspaceLeasePlan,
    attempt_id: Option<&str>,
) -> Result<()> {
    let mut statement = tx.prepare(
        "SELECT attempt_id,task_revision,owner_id,state,released_at_ms
         FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL LIMIT 2",
    )?;
    let current = statement
        .query_map([&plan.task_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if current.len() > 1 {
        return Err(Error::new(
            "TASK_OWNERSHIP_UNKNOWN",
            "more than one unreleased Attempt exists for the Task",
        ));
    }
    match (attempt_id, current.first()) {
        (None, None) => Ok(()),
        (Some(expected), Some((actual, revision, owner, state, released)))
            if expected == actual
                && *revision == plan.task_revision
                && owner == &plan.owner_client_id
                && released.is_none()
                && matches!(state.as_str(), "reserved" | "running" | "needs_correction") =>
        {
            Ok(())
        }
        _ => Err(Error::new(
            "WORKSPACE_ATTEMPT_MISMATCH",
            "current Attempt does not match the launch lease owner and revision",
        )),
    }
}

fn verify_launch_operation(
    db: &Connection,
    plan: &WorkspaceLeasePlan,
    technical_requester_id: &str,
    effective_manager_id: &str,
    allow_unknown: bool,
) -> Result<()> {
    let row: Option<(String, String, Option<String>, String, String)> = db
        .query_row(
            "SELECT caller_id,method,task_id,state,effective_request_json
             FROM operations WHERE operation_id=?1",
            [&plan.operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let (caller, method, task_id, state, effective) =
        row.ok_or_else(|| Error::new("STALE_LAUNCH", "launch Operation is unavailable"))?;
    let manifest: Value = serde_json::from_str(&effective)?;
    let manifest = &manifest["launch_manifest"];
    let state_matches = (state == "queued" && manifest["state"] == "pending_workspace")
        || (allow_unknown && state == "outcome_unknown" && manifest["state"] == "outcome_unknown");
    if caller != technical_requester_id
        || effective_manager_id != plan.owner_client_id
        || method != "swarm.launch"
        || task_id.as_deref() != Some(plan.task_id.as_str())
        || !state_matches
        || manifest["plan_digest"] != plan.plan_digest
        || manifest["task"]["task_id"] != plan.task_id
        || manifest["task"]["project_id"] != plan.project_id
        || manifest["task"]["observed_revision"] != plan.task_revision
        || manifest["task"]["attempt_id"]
            != plan
                .attempt_id
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null)
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch Operation does not retain this exact manager-owned plan",
        ));
    }
    Ok(())
}

fn verify_operation_caller(
    db: &Connection,
    operation_id: &str,
    technical_requester_id: &str,
) -> Result<()> {
    let caller: Option<String> = db
        .query_row(
            "SELECT caller_id FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .optional()?;
    if caller.as_deref() != Some(technical_requester_id) {
        return Err(Error::new(
            "FORBIDDEN",
            "launch Operation caller differs from its technical requester",
        ));
    }
    Ok(())
}

fn reject_scope_conflicts(db: &Connection, plan: &WorkspaceLeasePlan) -> Result<()> {
    let mut statement = db.prepare(
        "SELECT task_id,allowed_paths_json,allowed_symbols_json FROM workspace_leases
         WHERE project_id=?1
           AND state IN ('preparing','held','outcome_unknown','stale')
         ORDER BY lease_id LIMIT ?2",
    )?;
    let rows = statement
        .query_map(params![plan.project_id, MAX_ACTIVE_SCOPE_ROWS], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if rows.len() == MAX_ACTIVE_SCOPE_ROWS as usize {
        return Err(Error::new(
            "WORKSPACE_SCOPE_COVERAGE_UNKNOWN",
            "active workspace scope scan reached its bounded limit",
        ));
    }
    for (task_id, paths_json, symbols_json) in rows {
        let paths: Vec<String> = serde_json::from_str(&paths_json)?;
        let symbols: Vec<String> = serde_json::from_str(&symbols_json)?;
        if scopes_overlap(&plan.allowed_paths, &plan.allowed_symbols, &paths, &symbols) {
            return Err(Error::new(
                "WORKSPACE_SCOPE_CONFLICT",
                format!("active Task {task_id} holds an overlapping registered scope"),
            ));
        }
    }
    Ok(())
}

fn scopes_overlap(
    a_paths: &[String],
    a_symbols: &[String],
    b_paths: &[String],
    b_symbols: &[String],
) -> bool {
    a_paths.iter().any(|a| {
        b_paths.iter().any(|b| {
            a == b
                || a.as_str()
                    .strip_prefix(b.as_str())
                    .is_some_and(|tail| tail.starts_with('/'))
                || b.as_str()
                    .strip_prefix(a.as_str())
                    .is_some_and(|tail| tail.starts_with('/'))
        })
    }) || a_symbols.iter().any(|symbol| b_symbols.contains(symbol))
}

fn require_manager_owner(tx: &Transaction<'_>, principal: &Principal, owner: &str) -> Result<()> {
    if principal.client_id != owner {
        return Err(Error::new(
            "FORBIDDEN",
            "workspace lease requires the exact launch owner",
        ));
    }
    match principal.role {
        Role::Manager => Ok(()),
        Role::Operator => require_local_operator(tx, &principal.client_id),
        _ => Err(Error::new(
            "FORBIDDEN",
            "workspace lease requires a Manager or the local Operator",
        )),
    }
}

fn authorize_launch_actor(
    tx: &Transaction<'_>,
    actor: &super::launcher::LaunchActor,
    plan: &WorkspaceLeasePlan,
    attempt_id: Option<&str>,
) -> Result<()> {
    plan.validate()?;
    if actor.effective_manager_id() != plan.owner_client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "workspace lease owner differs from the effective manager",
        ));
    }
    actor.require_action_object(
        tx,
        "swarm.launch",
        &plan.task_id,
        plan.task_revision,
        attempt_id,
    )
}

#[derive(Debug)]
struct RegistrationRow {
    registration_id: String,
    allowed_roots: Vec<PathBuf>,
    registration_digest: String,
    generation: i64,
    state: String,
}

fn registration_row(db: &Connection, project_id: &str) -> Result<RegistrationRow> {
    let raw: Option<(String, String, String, i64, String)> = db
        .query_row(
            "SELECT registration_id,allowed_roots_json,registration_digest,generation,state
             FROM workspace_registrations WHERE project_id=?1",
            [project_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let (registration_id, roots, digest, generation, state) = raw.ok_or_else(|| {
        Error::new(
            "WORKSPACE_UNREGISTERED",
            "project has no workspace registration",
        )
    })?;
    let roots: Vec<String> = serde_json::from_str(&roots)?;
    if generation <= 0 || roots.is_empty() {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_INVALID",
            "persisted registration facts are invalid",
        ));
    }
    Ok(RegistrationRow {
        registration_id,
        allowed_roots: roots.into_iter().map(PathBuf::from).collect(),
        registration_digest: digest,
        generation,
        state,
    })
}

fn lease_row(db: &Connection, lease_id: &str) -> Result<LeaseRow> {
    type LeaseQueryRow = (
        String,
        String,
        i64,
        String,
        String,
        i64,
        String,
        String,
        String,
        Option<String>,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
        String,
        String,
    );
    let raw: Option<LeaseQueryRow> = db
        .query_row(
            "SELECT lease_id,registration_id,registration_generation,project_id,task_id,
                task_revision,operation_id,plan_digest,owner_client_id,attempt_id,
                allowed_paths_json,allowed_symbols_json,baseline_commit,branch_ref,
                worktree_handle,workspace_path,clean_state_json,generation,binding_digest,state
         FROM workspace_leases WHERE lease_id=?1",
            [lease_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                    row.get(16)?,
                    row.get(17)?,
                    row.get(18)?,
                    row.get(19)?,
                ))
            },
        )
        .optional()?;
    let (
        lease_id,
        registration_id,
        registration_generation,
        project_id,
        task_id,
        task_revision,
        operation_id,
        plan_digest,
        owner_client_id,
        attempt_id,
        allowed_paths,
        allowed_symbols,
        baseline_commit,
        branch_ref,
        worktree_handle,
        workspace_path,
        clean_state,
        generation,
        binding_digest,
        state,
    ) =
        raw.ok_or_else(|| Error::new("WORKSPACE_LEASE_MISSING", "workspace lease is unavailable"))?;
    Ok(LeaseRow {
        lease_id,
        registration_id,
        registration_generation,
        project_id,
        task_id,
        task_revision,
        operation_id,
        plan_digest,
        owner_client_id,
        attempt_id,
        allowed_paths: serde_json::from_str(&allowed_paths)?,
        allowed_symbols: serde_json::from_str(&allowed_symbols)?,
        baseline_commit,
        branch_ref,
        worktree_handle,
        workspace_path: PathBuf::from(workspace_path),
        clean_state: serde_json::from_str(&clean_state)?,
        generation,
        binding_digest,
        state,
    })
}

fn lease_intent_digest(
    lease_id: &str,
    registration: &RegistrationRow,
    generation: i64,
    plan: &WorkspaceLeasePlan,
    branch_ref: &str,
    worktree_handle: &str,
    workspace_path: &str,
) -> Result<String> {
    let value = json!({
        "version":1,
        "lease_id":lease_id,
        "registration_id":registration.registration_id,
        "registration_generation":registration.generation,
        "registration_digest":registration.registration_digest,
        "generation":generation,
        "project_id":plan.project_id,
        "task_id":plan.task_id,
        "task_revision":plan.task_revision,
        "operation_id":plan.operation_id,
        "plan_digest":plan.plan_digest,
        "owner_client_id":plan.owner_client_id,
        "attempt_id":plan.attempt_id,
        "allowed_paths":plan.allowed_paths,
        "allowed_symbols":plan.allowed_symbols,
        "expected_baseline_commit":plan.expected_baseline_commit,
        "branch_ref":branch_ref,
        "worktree_handle":worktree_handle,
        "workspace_path":workspace_path,
    });
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(&value)?.as_bytes())
    ))
}

fn plan_from_row(row: &LeaseRow) -> Result<WorkspaceLeasePlan> {
    Ok(WorkspaceLeasePlan {
        project_id: row.project_id.clone(),
        task_id: row.task_id.clone(),
        task_revision: row.task_revision,
        operation_id: row.operation_id.clone(),
        plan_digest: row.plan_digest.clone(),
        owner_client_id: row.owner_client_id.clone(),
        attempt_id: row.attempt_id.clone(),
        allowed_paths: row.allowed_paths.clone(),
        allowed_symbols: row.allowed_symbols.clone(),
        expected_baseline_commit: (!row.baseline_commit.is_empty())
            .then(|| row.baseline_commit.clone()),
    })
}

fn lease_authority(row: &LeaseRow) -> Result<LeaseAuthorityRef> {
    if row.state != "held" || !crate::forge::valid_object_id(&row.baseline_commit) {
        return Err(Error::new(
            "WORKSPACE_LEASE_INVALID",
            "only a held lease with a verified full baseline has authority",
        ));
    }
    Ok(LeaseAuthorityRef {
        lease_id: row.lease_id.clone(),
        registration_id: row.registration_id.clone(),
        registration_generation: row.registration_generation,
        project_id: row.project_id.clone(),
        task_id: row.task_id.clone(),
        task_revision: row.task_revision,
        operation_id: row.operation_id.clone(),
        plan_digest: row.plan_digest.clone(),
        owner_client_id: row.owner_client_id.clone(),
        attempt_id: row.attempt_id.clone(),
        generation: row.generation,
        baseline_commit: row.baseline_commit.clone(),
        branch_ref: row.branch_ref.clone(),
        worktree_handle: row.worktree_handle.clone(),
        binding_digest: row.binding_digest.clone(),
        state: row.state.clone(),
    })
}

fn verify_authority_ref(reference: &LeaseAuthorityRef, row: &LeaseRow) -> Result<()> {
    let current = lease_authority(row)?;
    if current != *reference {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "workspace lease reference does not match persisted authority",
        ));
    }
    Ok(())
}

fn verify_current_registration_generation(db: &Connection, row: &LeaseRow) -> Result<()> {
    let current = registration_row(db, &row.project_id)?;
    if current.state != "active"
        || current.registration_id != row.registration_id
        || current.generation != row.registration_generation
    {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_CHANGED",
            "workspace lease no longer matches the active registration generation",
        ));
    }
    Ok(())
}

fn path_text(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .ok_or_else(|| Error::new("WORKSPACE_PATH", "workspace path is not UTF-8"))?;
    if !path.is_absolute() || value.chars().any(char::is_control) {
        return Err(Error::new(
            "WORKSPACE_PATH",
            "workspace path is not a safe absolute path",
        ));
    }
    Ok(value.to_owned())
}
