//! Revisioned manager-owned automation configuration stored in authenticated,
//! schema-versioned per-record `meta` entries. No migration is required.

use super::{
    automation_dispatch, automation_publication, automation_work_dispatch, gm, operations, page,
    review_disposition,
};
use crate::{
    automation::{
        actions::AutomationStep,
        authorization,
        config::{self, AutomationEntry, ConfigRequest},
    },
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, Transaction, params};
use serde_json::{Value, json};

const MAX_IMPACT_OPERATIONS: i64 = 100;

#[derive(Debug)]
struct PlannedChange {
    before: Option<AutomationEntry>,
    after: AutomationEntry,
    include_existing: bool,
    new_coverage: bool,
    changed: bool,
}

pub(super) fn get(db: &Connection, p: &Principal, value: &Value) -> Result<Value> {
    require_manager(p)?;
    model::validate_automation_config_read("automation.config.get", value)?;
    let project = model::text(value, "project_id")?;
    let owner_manager_id = read_owner_manager_id(db, p, value)?;
    let (limit, after) = page(value)?;
    let entries = scoped_entries(db, &owner_manager_id, project)?;
    let start =
        usize::try_from(after).map_err(|_| Error::invalid("after exceeds platform range"))?;
    if start > entries.len() {
        return Err(Error::invalid("after exceeds the scoped automation list"));
    }
    let end = entries.len().min(start.saturating_add(limit as usize));
    let items = entries[start..end]
        .iter()
        .map(entry_projection)
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "project_id":project,
        "owner_manager_id":owner_manager_id,
        "items":items,
        "after":after,
        "next_after":if end < entries.len() { Some(end) } else { None },
        "count":entries.len()
    }))
}

pub(super) fn preview(db: &Connection, p: &Principal, value: &Value) -> Result<Value> {
    require_manager(p)?;
    let request = config::parse_request(value, false)?;
    let digest = config::changes_digest(&p.client_id, &request)?;
    let conflicts = revision_conflicts(db, p, &request)?;
    if !conflicts.is_empty() {
        return Ok(json!({
            "valid":false,
            "project_id":request.project_id,
            "plan_sha256":digest,
            "conflicts":conflicts
        }));
    }
    let planned = build_plan(db, p, &request, model::now_ms()?)?;
    let cut = observation_cut(db)?;
    let changes = planned
        .iter()
        .map(|change| {
            Ok(json!({
                "automation_id":change.after.automation_id,
                "expected_revision":change.before.as_ref().map_or(0,|entry|entry.revision),
                "new_revision":change.after.revision,
                "changed":change.changed,
                "include_existing":change.include_existing,
                "before":change.before.as_ref().map(entry_projection).transpose()?,
                "after":entry_projection(&change.after)?,
                "activation_cut_preview":if change.new_coverage { Some(cut) } else { None }
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "valid":true,
        "project_id":request.project_id,
        "plan_sha256":digest,
        "changes":changes
    }))
}

pub(super) fn apply(
    tx: &Transaction<'_>,
    p: &Principal,
    value: &Value,
    operation_id: &str,
    launcher_config: &crate::config::Config,
    now_ms: i64,
) -> Result<Value> {
    require_manager(p)?;
    let request = config::parse_request(value, true)?;
    let digest = config::changes_digest(&p.client_id, &request)?;
    let conflicts = revision_conflicts(tx, p, &request)?;
    if !conflicts.is_empty() {
        return Ok(json!({
            "operation_id":operation_id,
            "applied":false,
            "reason":"stale_revision",
            "plan_sha256":digest,
            "conflicts":conflicts
        }));
    }
    if let Some(preview_digest) = &request.preview_digest
        && preview_digest != &digest
    {
        return Ok(json!({
            "operation_id":operation_id,
            "applied":false,
            "reason":"preview_digest_mismatch",
            "supplied_preview_sha256":preview_digest,
            "current_plan_sha256":digest
        }));
    }
    let planned = build_plan(tx, p, &request, now_ms)?;
    let cut = observation_cut(tx)?;
    let mut entries = Vec::with_capacity(planned.len());
    let mut dispatch = Vec::new();
    let mut work_dispatch = Vec::new();
    let mut disabled_or_narrowed = Vec::new();
    for change in &planned {
        if change.changed {
            let key = config::entry_key(
                &p.client_id,
                &request.project_id,
                &change.after.automation_id,
            )?;
            config::write_record(tx, &key, &change.after.value()?)?;
            automation_dispatch::configure_activation(
                tx,
                change.before.as_ref(),
                &change.after,
                change.include_existing,
                cut,
                now_ms,
            )?;
            review_disposition::configure_activation(
                tx,
                change.before.as_ref(),
                &change.after,
                change.include_existing,
                cut,
                now_ms,
            )?;
            automation_work_dispatch::configure_activation(
                tx,
                change.before.as_ref(),
                &change.after,
                change.include_existing,
                now_ms,
            )?;
            automation_publication::configure_activation(
                tx,
                change.before.as_ref(),
                &change.after,
                change.include_existing,
                cut,
                now_ms,
            )?;
            let removed_or_narrowed_dispatch = change.before.as_ref().is_some_and(|before| {
                (before.enabled && !change.after.enabled)
                    || (before.steps.contains(&AutomationStep::ReviewDispatch)
                        && !change.after.steps.contains(&AutomationStep::ReviewDispatch))
                    || (before.steps.contains(&AutomationStep::WorkDispatch)
                        && !change.after.steps.contains(&AutomationStep::WorkDispatch))
                    || (before.steps.contains(&AutomationStep::Publication)
                        && !change.after.steps.contains(&AutomationStep::Publication))
                    || (before.work_dispatch_ready() && !change.after.work_dispatch_ready())
                    || (before.publication_ready() && !change.after.publication_ready())
                    || (before.publication != change.after.publication)
                    || (before.review.profile != change.after.review.profile)
                    || (before.scope.work_pool_id != change.after.scope.work_pool_id)
            });
            if removed_or_narrowed_dispatch {
                disabled_or_narrowed.push(change.after.automation_id.as_str());
            }
            entries.push(entry_projection(&change.after)?);
        } else {
            entries.push(entry_projection(&change.after)?);
        }
    }
    // Activate every changed entry before any include-existing consumer pass.
    // Disable-only edits do not depend on source reconciliation or a launch
    // admission callback.
    let include_existing_review = planned.iter().any(|change| {
        change.changed
            && change.include_existing
            && change.after.enabled
            && change.after.steps.contains(&AutomationStep::ReviewDispatch)
    });
    if include_existing_review {
        let intake = automation_dispatch::reconcile_source_intake(tx, 64, now_ms)?;
        for change in &planned {
            if change.changed
                && change.include_existing
                && change.after.enabled
                && change.after.steps.contains(&AutomationStep::ReviewDispatch)
            {
                let budget = 16usize.saturating_sub(dispatch.len());
                if budget > 0 {
                    dispatch.push(automation_dispatch::reconcile_entry(
                        tx,
                        &change.after,
                        budget,
                        intake,
                        now_ms,
                    )?);
                }
            }
        }
    }
    for change in &planned {
        if change.changed
            && change.include_existing
            && change.after.enabled
            && change.after.steps.contains(&AutomationStep::WorkDispatch)
        {
            let budget = 16usize.saturating_sub(work_dispatch.len());
            if budget > 0 {
                work_dispatch.push(automation_work_dispatch::reconcile_entry(
                    tx,
                    &change.after,
                    change.after.work_dispatch.as_ref(),
                    budget,
                    now_ms,
                    |tx, prepared| {
                        super::launcher::admit_work_dispatch(tx, prepared, launcher_config, now_ms)
                    },
                )?);
            }
        }
    }
    let impacted = disabled_or_narrowed
        .iter()
        .map(|automation_id| {
            operation_impacts(tx, &p.client_id, &request.project_id, automation_id)
                .map(|impact| json!({"automation_id":automation_id,"work":impact}))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "operation_id":operation_id,
        "applied":true,
        "owner_manager_id":p.client_id,
        "project_id":request.project_id,
        "plan_sha256":digest,
        "activation_cut":cut,
        "entries":entries,
        "dispatch":dispatch,
        "work_dispatch":work_dispatch,
        "affected_work":impacted
    }))
}

pub(super) fn explain(db: &Connection, p: &Principal, value: &Value) -> Result<Value> {
    require_manager(p)?;
    model::validate_automation_config_read("automation.config.explain", value)?;
    let project = model::text(value, "project_id")?;
    let automation_id = model::text(value, "automation_id")?;
    let owner_manager_id = read_owner_manager_id(db, p, value)?;
    let entry = config::load_entry(db, &owner_manager_id, project, automation_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", "automation entry was not found in this scope"))?;
    let state = automation_dispatch::dispatch_state(db, &entry)?;
    let work_dispatch = automation_work_dispatch::dispatch_state(db, &entry)?;
    let disposition = review_disposition::disposition_state(db, &entry)?;
    let publication = automation_publication::state(db, &entry)?;
    let work = operation_impacts(db, &owner_manager_id, project, automation_id)?;
    let operation_history =
        linked_operation_history(db, &owner_manager_id, project, automation_id)?;
    Ok(json!({
        "owner_manager_id":owner_manager_id,
        "entry":entry_projection(&entry)?,
        "dispatch":state,
        "work_dispatch":work_dispatch,
        "review_disposition":disposition,
        "publication":publication,
        "linked_operations":work,
        "linked_operation_history":operation_history
    }))
}

fn linked_operation_history(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<Value> {
    let mut links = authorization::entry_operation_links(
        db,
        owner,
        project,
        automation_id,
        "",
        MAX_IMPACT_OPERATIONS as usize + 1,
    )?;
    let truncated = links.len() > MAX_IMPACT_OPERATIONS as usize;
    links.truncate(MAX_IMPACT_OPERATIONS as usize);
    let items = links
        .iter()
        .map(|link| {
            let operation = operations::get_operation(db, &link.operation_id)?;
            Ok(json!({
                "operation_id":link.operation_id,
                "action":link.action,
                "state":operation["state"],
                "automation_revision":link.automation_revision,
                "cause":link.cause
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({"items":items,"truncated":truncated}))
}

fn require_manager(p: &Principal) -> Result<()> {
    if p.role != Role::Manager {
        return Err(Error::new(
            "FORBIDDEN",
            "automation configuration is scoped to the authenticated manager",
        ));
    }
    Ok(())
}

fn read_owner_manager_id(db: &Connection, p: &Principal, value: &Value) -> Result<String> {
    let requested = value
        .get("owner_manager_id")
        .map(|_| model::text(value, "owner_manager_id"))
        .transpose()?
        .unwrap_or(&p.client_id);
    if requested == p.client_id.as_str() {
        return Ok(p.client_id.clone());
    }

    // Cross-owner recovery is read-only and available only to the live,
    // registered current GM. Never construct or substitute another Principal:
    // the requested owner selects a provenance-preserving metadata scope only.
    authorization::require_registered_manager(db, &p.client_id)?;
    let is_current_gm = gm::record(db)?.is_some_and(|designation| {
        designation["client_id"] == p.client_id
            && designation["epoch"].as_i64().is_some_and(|epoch| epoch > 0)
    });
    if !is_current_gm {
        return Err(Error::new(
            "FORBIDDEN",
            "only the current enabled Manager may inspect another owner's automation state",
        ));
    }
    Ok(requested.to_owned())
}

fn scoped_entries(db: &Connection, owner: &str, project: &str) -> Result<Vec<AutomationEntry>> {
    let prefix = config::entry_prefix(owner, project)?;
    let pattern = format!("{prefix}%");
    let mut statement =
        db.prepare("SELECT key FROM meta WHERE key LIKE ?1 ORDER BY key LIMIT ?2")?;
    let keys = statement
        .query_map(
            params![pattern, (config::MAX_AUTOMATIONS_PER_SCOPE + 1) as i64],
            |row| row.get::<_, String>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if keys.len() > config::MAX_AUTOMATIONS_PER_SCOPE {
        return Err(Error::new(
            "AUTOMATION_SCOPE_LIMIT",
            "stored automation entry count exceeds the scoped bound",
        ));
    }
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let automation_id = key.strip_prefix(&prefix).ok_or_else(|| {
            Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                "automation metadata key is invalid",
            )
        })?;
        let entry = config::load_entry(db, owner, project, automation_id)?.ok_or_else(|| {
            Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                "listed automation entry disappeared",
            )
        })?;
        entries.push(entry);
    }
    Ok(entries)
}

fn entry_projection(entry: &AutomationEntry) -> Result<Value> {
    let mut value = entry.value()?;
    value["steps"] = json!(
        entry
            .steps
            .iter()
            .map(|step| step.as_str())
            .collect::<Vec<_>>()
    );
    value["capability_gaps"] = json!(entry.capability_gaps());
    Ok(value)
}

fn revision_conflicts(
    db: &Connection,
    p: &Principal,
    request: &ConfigRequest,
) -> Result<Vec<Value>> {
    let mut conflicts = Vec::new();
    for change in &request.changes {
        let current =
            config::load_entry(db, &p.client_id, &request.project_id, &change.automation_id)?;
        let current_revision = current.as_ref().map_or(0, |entry| entry.revision);
        if current_revision != change.expected_revision {
            conflicts.push(json!({
                "automation_id":change.automation_id,
                "expected_revision":change.expected_revision,
                "current_revision":current_revision,
                "current":current.as_ref().map(entry_projection).transpose()?
            }));
        }
        if current.is_none() && change.expected_revision != 0 {
            continue;
        }
    }
    Ok(conflicts)
}

fn build_plan(
    db: &Connection,
    p: &Principal,
    request: &ConfigRequest,
    now_ms: i64,
) -> Result<Vec<PlannedChange>> {
    let mut planned = Vec::with_capacity(request.changes.len());
    let mut scoped_count = scoped_entries(db, &p.client_id, &request.project_id)?.len();
    for change in &request.changes {
        let before =
            config::load_entry(db, &p.client_id, &request.project_id, &change.automation_id)?;
        let creating = before.is_none();
        if creating {
            scoped_count += 1;
            if scoped_count > config::MAX_AUTOMATIONS_PER_SCOPE {
                return Err(Error::new(
                    "AUTOMATION_SCOPE_LIMIT",
                    "manager automation count exceeds the per-project bound",
                ));
            }
        }
        let base = before.clone().unwrap_or_else(|| {
            AutomationEntry::new(
                &p.client_id,
                &request.project_id,
                &change.automation_id,
                now_ms,
            )
        });
        let mut after = config::apply_patch(&base, &change.patch, now_ms)?;
        if creating {
            after.revision = 1;
            after.created_at_ms = now_ms;
            after.updated_at_ms = now_ms;
        }
        config::validate_entry(&after)?;
        let added_steps = after.steps.iter().any(|step| {
            before
                .as_ref()
                .is_none_or(|prior| !prior.steps.contains(step))
        });
        let enabled_now = after.enabled && before.as_ref().is_none_or(|prior| !prior.enabled);
        let new_work_dispatch_coverage = after.work_dispatch_ready()
            && before
                .as_ref()
                .is_none_or(|prior| !prior.work_dispatch_ready());
        let new_review_dispatch_coverage = after.review_dispatch_ready()
            && before
                .as_ref()
                .is_none_or(|prior| !prior.review_dispatch_ready());
        let new_publication_coverage = after.publication_ready()
            && before.as_ref().is_none_or(|prior| {
                !prior.publication_ready() || prior.publication != after.publication
            });
        let new_coverage = after.enabled
            && (enabled_now
                || added_steps
                || new_work_dispatch_coverage
                || new_review_dispatch_coverage
                || new_publication_coverage);
        if change.include_existing && !new_coverage {
            return Err(Error::invalid(
                "include_existing is meaningful only when enabling an entry or adding step coverage",
            ));
        }
        if change.include_existing && !after.enabled {
            return Err(Error::invalid(
                "include_existing requires the resulting entry to be enabled",
            ));
        }
        let changed = creating || before.as_ref().is_some_and(|prior| prior != &after);
        planned.push(PlannedChange {
            before,
            after,
            include_existing: change.include_existing,
            new_coverage,
            changed,
        });
    }
    Ok(planned)
}

fn observation_cut(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations",
        [],
        |row| row.get(0),
    )?)
}

fn operation_impacts(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<Value> {
    let prefix = config::entry_operation_prefix(owner, project, automation_id)?;
    let pattern = format!("{prefix}%");
    let mut statement = db.prepare(
        "SELECT substr(link.key,length(?1)+1),op.state FROM meta AS link \
         JOIN operations AS op ON op.operation_id=substr(link.key,length(?1)+1) \
         WHERE link.key LIKE ?2 AND op.state IN ('queued','sending','native_accepted','outcome_unknown') \
         ORDER BY op.created_at_ms,op.operation_id LIMIT ?3",
    )?;
    let rows = statement
        .query_map(params![prefix, pattern, MAX_IMPACT_OPERATIONS + 1], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let truncated = rows.len() > MAX_IMPACT_OPERATIONS as usize;
    let mut unstarted = Vec::new();
    let mut in_flight = Vec::new();
    let mut uncertain = Vec::new();
    for (operation_id, state) in rows.into_iter().take(MAX_IMPACT_OPERATIONS as usize) {
        let link = authorization::operation_link(db, &operation_id)?.ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "operation impact has no valid on-behalf link",
            )
        })?;
        if link.effective_manager_id != owner
            || link.project_id != project
            || link.automation_id != automation_id
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "operation impact scope mismatch",
            ));
        }
        let reference = json!({
            "operation_id":operation_id,
            "state":state,
            "cause":link.cause
        });
        match state.as_str() {
            "queued" => unstarted.push(reference),
            "sending" | "native_accepted" => in_flight.push(reference),
            "outcome_unknown" => uncertain.push(reference),
            _ => {}
        }
    }
    Ok(json!({
        "unstarted":unstarted,
        "in_flight":in_flight,
        "uncertain":uncertain,
        "truncated":truncated
    }))
}
