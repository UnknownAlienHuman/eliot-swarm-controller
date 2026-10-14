//! CheckRun transactions and one host scheduler, using the existing nine tables.
use super::{Store, acceptance, current_principal, meta, operations, results, set_meta, tasks};
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    automation::authorization,
    checks::inputs::ProbeCleanupOwner,
    checks::{
        inputs,
        model::{CaptureRequest, CheckRequest},
        source, standalone_host,
        worker::{self, CancelRequest, Completion, Work},
    },
    config::Config,
    error::{Error, Result},
    hooks::git::GitCleanupOwner,
    model::{self, Principal, Role},
    workspace::LeaseAuthorityRef,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Child,
    time::Duration,
};
use tokio::sync::watch;

pub(super) struct CheckPlanInputs {
    pub context_fingerprint: String,
    pub project_id: String,
    pub profile: crate::checks::model::CheckProfile,
    pub candidate: ArtifactRecord,
    pub baseline: Option<ArtifactRecord>,
    pub baseline_reason: Option<String>,
}

#[derive(Clone)]
pub(super) struct PreparedCheckPlan {
    pub context_fingerprint: String,
    pub project_id: String,
    pub resolved_inputs: Value,
    pub scope_plan: Value,
    pub input_fingerprint: String,
    pub reproducible: bool,
}

#[derive(Clone)]
pub(super) struct PreparedCacheHit {
    pub check_id: String,
    pub operation_id: String,
    pub candidate_ref: String,
    pub source_acceptance: Value,
    pub candidate: ArtifactRecord,
    pub result: ArtifactRecord,
    pub outputs: Vec<ArtifactRecord>,
    pub coverage: Value,
    pub process: Value,
}

struct CacheOriginRow {
    operation_id: String,
    candidate_ref: String,
    cache_key: String,
    spec_json: String,
    coverage_json: String,
    result_ref: Option<String>,
    process_identity_json: Option<String>,
}

#[derive(Clone)]
pub(super) struct CheckPlanResolution {
    pub context_fingerprint: String,
    pub result: Result<PreparedCheckPlan>,
    pub cache_hit: Option<PreparedCacheHit>,
}

struct CheckAdmission<'a> {
    resolution: Option<&'a CheckPlanResolution>,
    cron_context: Option<&'a crate::automation::authorization::CronExecutionContext>,
    executor: Option<crate::checks::model::ExecutorPin>,
}

/// Capture DB-owned inputs for the filesystem resolver. The context hash is
/// checked again in the admission transaction, after artifact verification.
pub(super) fn plan_inputs(
    db: &Connection,
    p: &Principal,
    v: &Value,
    config: &Config,
) -> Result<CheckPlanInputs> {
    let input = CheckRequest::parse(v)?;
    let a = attempt(db, p, &input.attempt_id)?;
    plan_inputs_for_attempt(db, &input, &a, config)
}

fn plan_inputs_for_attempt(
    db: &Connection,
    input: &CheckRequest,
    a: &Value,
    config: &Config,
) -> Result<CheckPlanInputs> {
    let task = tasks::get_task(db, model::text(a, "task_id")?)?;
    let profile = config
        .checks
        .profile(&input.profile_id, &input.profile_revision)?;
    let candidate = results::get(db, &input.candidate_ref)?;
    if candidate.kind != "source_snapshot"
        || candidate.metadata["task_id"] != task["task_id"]
        || candidate.metadata["attempt_id"] != input.attempt_id
        || candidate.metadata["task_revision"] != a["task_revision"]
    {
        return Err(Error::new(
            "CHECK_SOURCE_REQUIRED",
            "capture the exact source for this Attempt before requesting a machine check",
        ));
    }

    let frozen = a["task_snapshot"]["baseline_candidate"].clone();
    let frozen_ref = a["task_snapshot"]["spec"]["baseline_candidate_ref"]
        .as_str()
        .map(str::to_owned);
    let current = acceptance::freeze_baseline_candidate(
        db,
        model::text(&task, "project_id")?,
        frozen_ref.as_deref(),
    )?;
    let baseline_is_current = frozen["status"] == "verified"
        && current["status"] == "verified"
        && model::canonical(&frozen)? == model::canonical(&current)?;
    let (baseline, baseline_reason) = if baseline_is_current {
        match results::get(db, model::text(&frozen, "candidate_ref")?) {
            Ok(record) if record.kind == "source_snapshot" => (Some(record), None),
            Ok(_) => (
                None,
                Some("baseline_not_complete_source_snapshot".to_owned()),
            ),
            Err(error) if error.code == "NOT_FOUND" => {
                (None, Some("baseline_artifact_unregistered".to_owned()))
            }
            Err(error) => return Err(error),
        }
    } else {
        let reason = if frozen["status"] == "verified" {
            "baseline_acceptance_changed_since_claim"
        } else {
            frozen["reason"]
                .as_str()
                .unwrap_or("baseline_not_verified_at_claim")
        };
        (None, Some(reason.to_owned()))
    };

    // This is a freshness token for a separate admission context, not the
    // reusable cache key. Use the redacted profile projection so secret or
    // opaque inherited environment values never enter retained Store data.
    let profile_sha256 = inputs::profile_identity_sha256(&profile)?;
    let context = json!({
        "version":1,
        "task_id":task["task_id"],
        "project_id":task["project_id"],
        "task_revision":task["revision"],
        "task_state":task["state"],
        "attempt_id":a["attempt_id"],
        "attempt_revision":a["task_revision"],
        "attempt_state":a["state"],
        "attempt_released_at_ms":a["released_at_ms"],
        "frozen_baseline":frozen,
        "current_baseline":current,
        "profile_sha256":profile_sha256,
        "candidate":{
            "artifact_id":candidate.artifact_id,
            "kind":candidate.kind,
            "content_digest":candidate.content_digest,
            "byte_length":candidate.byte_length,
            "metadata":candidate.metadata,
        },
        "baseline_artifact":baseline.as_ref().map(|record| json!({
            "artifact_id":record.artifact_id,
            "kind":record.kind,
            "content_digest":record.content_digest,
            "byte_length":record.byte_length,
            "metadata":record.metadata,
        })),
        "baseline_reason":baseline_reason,
    });
    let context_fingerprint = model::digest(model::canonical(&context)?.as_bytes());
    Ok(CheckPlanInputs {
        context_fingerprint,
        project_id: model::text(&task, "project_id")?.to_owned(),
        profile,
        candidate,
        baseline,
        baseline_reason,
    })
}

impl Store {
    /// Shared external/scheduled preflight. It snapshots trusted DB facts,
    /// verifies exact source bytes off the DB thread and resolves the immutable
    /// plan before the final admission transaction.
    pub(super) async fn resolve_check_plan(
        &self,
        principal: Principal,
        params: Value,
    ) -> Result<CheckPlanResolution> {
        let p = principal.clone();
        let v = params.clone();
        let config = self.config.clone();
        let snapshot = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                plan_inputs(db, &p, &v, &config)
            })
            .await;
        self.resolve_check_plan_snapshot(snapshot).await
    }

    pub(super) async fn resolve_cron_check_plan(
        &self,
        context: crate::automation::authorization::CronExecutionContext,
    ) -> Result<CheckPlanResolution> {
        let params = context.request_params()?;
        let config = self.config.clone();
        let snapshot = self
            .run(move |db| {
                context.require_current_check_target(db)?;
                let input = CheckRequest::parse(&params)?;
                let attempt = tasks::get_attempt(db, &input.attempt_id)?;
                plan_inputs_for_attempt(db, &input, &attempt, &config)
            })
            .await;
        self.resolve_check_plan_snapshot(snapshot).await
    }

    async fn resolve_check_plan_snapshot(
        &self,
        snapshot: Result<CheckPlanInputs>,
    ) -> Result<CheckPlanResolution> {
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Ok(CheckPlanResolution {
                    context_fingerprint: String::new(),
                    result: Err(error),
                    cache_hit: None,
                });
            }
        };
        let CheckPlanInputs {
            context_fingerprint,
            project_id,
            profile,
            candidate: candidate_record,
            baseline: baseline_record,
            baseline_reason,
        } = snapshot;
        let pending_context = context_fingerprint.clone();
        let pending_resolution = move || CheckPlanResolution {
            context_fingerprint: pending_context.clone(),
            result: Err(Error::new(
                "CHECK_PROBE_CLEANUP_PENDING",
                "an input probe remains in Store custody for a conflicting or unidentified check target",
            )),
            cache_hit: None,
        };
        // The resource is known before reading candidate bytes; the exact
        // workspace scope is checked again after source verification.
        let incoming_resource = inputs::ProbeCleanupScope::resource_for_profile(&profile);
        let Some(probe_resolution) = self
            .check_child_custody
            .reserve_probe_resolution(incoming_resource.as_deref())
            .await
        else {
            return Ok(pending_resolution());
        };
        let has_baseline_record = baseline_record.is_some();
        let data_dir = self.data_dir.clone();
        let verified_sources = self
            .file_io(move |files| {
                let candidate = source::verified_content(&files, &data_dir, &candidate_record)?;
                let baseline = match baseline_record.as_ref() {
                    Some(record) => source::verified_content(&files, &data_dir, record).ok(),
                    None => None,
                };
                Ok((candidate, baseline))
            })
            .await;
        let (candidate, baseline) = match verified_sources {
            Ok(sources) => sources,
            Err(error) => {
                return Ok(CheckPlanResolution {
                    context_fingerprint: context_fingerprint.clone(),
                    result: Err(error),
                    cache_hit: None,
                });
            }
        };
        let scope = inputs::ProbeCleanupScope::for_candidate(&profile, &candidate);
        if !self
            .check_child_custody
            .pin_probe_resolution_scope(&probe_resolution, scope.as_ref())
            .await
        {
            return Ok(pending_resolution());
        }
        let baseline_reason = baseline_reason.or_else(|| {
            (has_baseline_record && baseline.is_none())
                .then(|| "baseline_source_unverified".to_owned())
        });
        let resolved_context = context_fingerprint.clone();
        let resolved_scope = scope.clone();
        let cleanup_scope = resolved_scope.clone();
        let probe_custody = self.check_child_custody.clone();
        let blocking_resolution = probe_resolution.clone();
        let result = self
            .file_io(move |_files| {
                // Cancellation of the awaiting request must not make this
                // resource available while its blocking probe is still running.
                let _resolution = blocking_resolution;
                let (resolved, cleanup_owner) =
                    inputs::resolve_with_cleanup(&profile, &candidate, baseline.as_ref());
                // Install custody before returning to the cancellable async
                // caller. A pending owner never lives only in an await result.
                let install_conflict = if let Some(mut owner) = cleanup_owner {
                    owner.set_scope(cleanup_scope);
                    probe_custody.retain_probe_cleanup(owner)
                } else {
                    false
                };
                let prepared = resolved.and_then(|mut resolved| {
                    if let Some(reason) = baseline_reason {
                        resolved.add_widening_reason(&reason)?;
                    }
                    let scope_plan = serde_json::to_value(&resolved.scope_plan)?;
                    Ok(PreparedCheckPlan {
                        context_fingerprint: resolved_context,
                        project_id,
                        resolved_inputs: resolved.resolved_inputs,
                        scope_plan,
                        input_fingerprint: resolved.input_fingerprint,
                        reproducible: profile.reproducible,
                    })
                });
                Ok((prepared, install_conflict))
            })
            .await;
        let (prepared, install_conflict) = match result {
            Ok(result) => result,
            Err(error) => (Err(error), false),
        };
        if install_conflict {
            return Ok(pending_resolution());
        }
        if prepared.is_ok()
            && self
                .check_child_custody
                .probe_cleanup_conflicts(resolved_scope.as_ref())
                .await
        {
            return Ok(pending_resolution());
        }
        let (result, cache_hit) = match prepared {
            Ok(prepared) if prepared.resolved_inputs["cache_reusable"] == true => {
                let project_id = prepared.project_id.clone();
                let lookup = prepared.clone();
                let candidate = self
                    .run(move |db| completed_cache_candidate(db, &project_id, &lookup))
                    .await?;
                let cache_hit = if let Some(candidate) = candidate {
                    let expected = prepared.clone();
                    let data_dir = self.data_dir.clone();
                    self.file_io(move |files| {
                        verify_cache_candidate(&files, &data_dir, &expected, candidate)
                    })
                    .await
                    .unwrap_or(None)
                } else {
                    None
                };
                (Ok(prepared), cache_hit)
            }
            Ok(prepared) => (Ok(prepared), None),
            Err(error) => (Err(error), None),
        };
        if result.is_ok()
            && self
                .check_child_custody
                .probe_cleanup_conflicts(resolved_scope.as_ref())
                .await
        {
            return Ok(pending_resolution());
        }
        Ok(CheckPlanResolution {
            context_fingerprint,
            result,
            cache_hit,
        })
    }

    pub(super) async fn check_run(&self, principal: Principal, params: Value) -> Result<Value> {
        model::validate_mutation("check.run", &params)?;
        let p = principal.clone();
        let v = params.clone();
        let existing = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                p.require_writer()?;
                let request_id = model::text(&v, "client_request_id")?;
                let raw: Option<(String, String, String)> = db
                    .query_row(
                        "SELECT method,original_request_json,effective_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
                        params![p.client_id, request_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?;
                let Some((old_method, original, effective)) = raw else {
                    return Ok(None);
                };
                if old_method != "check.run" || original != model::canonical(&v)? {
                    return Err(Error::new(
                        "REQUEST_ID_CONFLICT",
                        "request ID was used with a different method or payload",
                    ));
                }
                let effective: Value = serde_json::from_str(&effective)?;
                super::receipt_result(&effective["receipt"]).map(Some)
            })
            .await?;
        if let Some(existing) = existing {
            return Ok(existing);
        }

        let resolution = self
            .resolve_check_plan(principal.clone(), params.clone())
            .await?;
        let p = principal.clone();
        let v = params.clone();
        let config = self.config.clone();
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let p = current_principal(&tx, p)?;
                let result = super::mutate_in_transaction_with_check_plan(
                    &tx,
                    &p,
                    "check.run",
                    &v,
                    &config,
                    model::now_ms()?,
                    Some(&resolution),
                )?;
                tx.commit()?;
                result
            })
            .await;
        if result.as_ref().is_ok_and(|value| value["cached"] != true) {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        result
    }
    /// Async acknowledgement gate for one quarantined CheckRun. A possibly
    /// live owned child rejects with uncertainty before any admission, so
    /// acknowledgement can never authorize against an unobserved process
    /// family. Genuine terminal evidence stays with the reap and finish
    /// gates and is never inferred here; the synchronous admission below
    /// keeps the run nonterminal and the quarantine evidence immutable.
    pub(super) async fn acknowledge_held_check(
        &self,
        principal: Principal,
        params: Value,
    ) -> Result<Value> {
        model::validate_mutation("check.acknowledge_held", &params)?;
        let check = model::text(&params, "check_id")?.to_owned();
        {
            let owners = self.check_child_custody.lock().await;
            if owners.get(&check).is_some_and(|owner| owner.exit.is_none()) {
                return Err(Error::new(
                    "CHECK_HELD_LIVE_UNCERTAIN",
                    "an owned CheckRun child may still be live; acknowledgement waits for exact reap evidence",
                ));
            }
        }
        let p = principal.clone();
        let v = params.clone();
        let config = self.config.clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let p = current_principal(&tx, p)?;
            let result = super::mutate_in_transaction_with_check_plan(
                &tx,
                &p,
                "check.acknowledge_held",
                &v,
                &config,
                model::now_ms()?,
                None,
            )?;
            tx.commit()?;
            result
        })
        .await
    }
}

pub(super) fn valid_process_receipt(receipt: &Value, spec: &Value) -> bool {
    if receipt["token"] != spec["token"]
        || receipt["token"].as_str().is_none_or(str::is_empty)
        || receipt["control_version"] != 2
        || receipt["ready_at_ms"]
            .as_i64()
            .is_none_or(|value| value <= 0)
    {
        return false;
    }
    let process = &receipt["process"];
    if !process.is_object() || process["purpose"] != "check" {
        return false;
    }
    #[cfg(windows)]
    {
        process["scope"] == "windows_job"
            && process["pid"].as_u64().is_some_and(|value| value > 0)
            && process["creation_filetime"]
                .as_u64()
                .is_some_and(|value| value > 0)
            && process["job_name"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
            && process["disposition_source"] == "job_accounting"
    }
    #[cfg(target_os = "linux")]
    {
        process["scope"] == "linux_process_group"
            && process["pid"].as_u64().is_some_and(|value| value > 0)
            && process["pgid"].as_u64().is_some_and(|value| value > 0)
            && process["start_ticks"]
                .as_str()
                // `/proc/<pid>/stat` is parsed and retained as a decimal
                // string by the process-group identity code. Keep that exact
                // wire type here; requiring a JSON number rejects every real
                // Linux process receipt and silently disables cache reuse.
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|value| value > 0)
            && process["boot_id"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
            && process["disposition_source"] == "proc_group_members"
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        false
    }
}

/// Return one direct, completed process result whose complete input identity is
/// reusable. This is a hint only: its exact rows and acceptance proof are
/// rechecked in the final admission transaction after file verification.
fn completed_cache_candidate(
    db: &Connection,
    project_id: &str,
    plan: &PreparedCheckPlan,
) -> Result<Option<PreparedCacheHit>> {
    if plan.resolved_inputs["cache_reusable"] != true {
        return Ok(None);
    }
    let rows = {
        let mut statement = db.prepare(
            "SELECT check_id,operation_id,candidate_ref,spec_json,coverage_json,result_ref,process_identity_json FROM check_runs WHERE cache_key=?1 AND cached_from_check_id IS NULL AND state='passed' AND exit_code=0 AND resource_released_at_ms IS NOT NULL ORDER BY finished_at_ms DESC,check_id",
        )?;
        statement
            .query_map([&plan.input_fingerprint], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for (check_id, operation_id, candidate_ref, spec_raw, coverage_raw, result_ref, process_raw) in
        rows
    {
        let Ok(spec) = serde_json::from_str::<Value>(&spec_raw) else {
            continue;
        };
        let Ok(coverage) = serde_json::from_str::<Value>(&coverage_raw) else {
            continue;
        };
        let Ok(profile) =
            serde_json::from_value::<crate::checks::model::CheckProfile>(spec["profile"].clone())
        else {
            continue;
        };
        let Ok(expected_targets) =
            serde_json::from_value::<Vec<String>>(plan.resolved_inputs["expected_targets"].clone())
        else {
            continue;
        };
        let profile_identity = match inputs::profile_identity(&profile) {
            Ok(identity) => identity,
            Err(_) => continue,
        };
        if spec["cache_policy"] != "reusable"
            || spec["reproducible"] != true
            || spec["cache_key"] != plan.input_fingerprint
            || spec["input_fingerprint"] != plan.input_fingerprint
            || spec["resolved_inputs"] != plan.resolved_inputs
            || spec["scope_plan"] != plan.scope_plan
            || !spec["cache_source_acceptance"].is_null()
            || !spec["cached_from_check_id"].is_null()
            || profile_identity != plan.resolved_inputs["profile_identity"]
            || worker::validate_passed_coverage(
                &profile.parser,
                &expected_targets,
                &spec["scope_plan"],
                &coverage,
            )
            .is_err()
        {
            continue;
        }
        let Some(process_raw) = process_raw else {
            continue;
        };
        let Ok(process) = serde_json::from_str::<Value>(&process_raw) else {
            continue;
        };
        if !valid_process_receipt(&process, &spec) {
            continue;
        }
        let operation = match operations::get_operation(db, &operation_id) {
            Ok(operation) => operation,
            Err(error) if error.code == "NOT_FOUND" => continue,
            Err(error) => return Err(error),
        };
        if operation["method"] != "check.run"
            || operation["state"] != "settled"
            || operation["result"]["outcome"] != "applied"
            || operation["result"]["check_id"] != check_id
            || operation["result"]["state"] != "passed"
            || operation["result"]["exit_code"] != 0
            || operation["result"]["source_checkout_verified"] != true
            || operation["result"]["result_ref"] != result_ref
            || !operation["result"]["cached"].is_null()
            || !operation["result"]["cached_from_check_id"].is_null()
        {
            continue;
        }
        let Some(output_refs) = operation["result"]["output_refs"].as_array() else {
            continue;
        };
        if output_refs.len() != 2 || spec["candidate"]["artifact_id"] != candidate_ref {
            continue;
        }
        let candidate = match results::get(db, &candidate_ref) {
            Ok(record)
                if record.kind == "source_snapshot"
                    && record.metadata["coverage"] == "complete" =>
            {
                record
            }
            _ => continue,
        };
        let accepted = acceptance::freeze_baseline_candidate(db, project_id, Some(&candidate_ref))?;
        if accepted["status"] != "verified"
            || accepted["content_sha256"] != candidate.content_digest
        {
            continue;
        }
        let result = match results::get(db, &result_ref) {
            Ok(record)
                if record.kind == "check_result" && record.metadata["check_id"] == check_id =>
            {
                record
            }
            _ => continue,
        };
        let mut outputs = Vec::with_capacity(output_refs.len());
        let mut valid_outputs = true;
        for reference in output_refs {
            let Some(reference) = reference.as_str() else {
                valid_outputs = false;
                break;
            };
            let record = match results::get(db, reference) {
                Ok(record)
                    if record.kind == "check_output" && record.metadata["check_id"] == check_id =>
                {
                    record
                }
                _ => {
                    valid_outputs = false;
                    break;
                }
            };
            outputs.push(record);
        }
        let streams = outputs
            .iter()
            .filter_map(|record| record.metadata["stream"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if !valid_outputs
            || streams.len() != 2
            || !streams.contains("stdout")
            || !streams.contains("stderr")
        {
            continue;
        }
        return Ok(Some(PreparedCacheHit {
            check_id,
            operation_id,
            candidate_ref,
            source_acceptance: accepted,
            candidate,
            result,
            outputs,
            coverage,
            process,
        }));
    }
    Ok(None)
}

fn verify_cache_candidate(
    files: &ArtifactFiles,
    data_dir: &std::path::Path,
    plan: &PreparedCheckPlan,
    hit: PreparedCacheHit,
) -> Result<Option<PreparedCacheHit>> {
    let verified_source = match source::verified_content(files, data_dir, &hit.candidate) {
        Ok(source) => source,
        Err(_) => return Ok(None),
    };
    if verified_source.content_sha256 != plan.resolved_inputs["candidate_content_sha256"] {
        return Ok(None);
    }
    let report_bytes = match files.document_bytes(&hit.result) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };
    let report: Value = match serde_json::from_slice(&report_bytes) {
        Ok(report) => report,
        Err(_) => return Ok(None),
    };
    let parser: crate::checks::model::Parser =
        match serde_json::from_value(plan.resolved_inputs["profile_identity"]["parser"].clone()) {
            Ok(parser) => parser,
            Err(_) => return Ok(None),
        };
    let expected_targets: Vec<String> =
        match serde_json::from_value(plan.resolved_inputs["expected_targets"].clone()) {
            Ok(targets) => targets,
            Err(_) => return Ok(None),
        };
    // Worker reports carry only the safe profile projection (declared
    // non-secret environment values as digests and opaque names only).
    let profile_sha256 = model::digest(model::canonical(&report["profile"])?.as_bytes());
    let output_refs = hit
        .outputs
        .iter()
        .map(|output| output.artifact_id.clone())
        .collect::<Vec<_>>();
    let reported_outputs = report["outputs"].as_array().cloned().unwrap_or_default();
    let reported_refs = reported_outputs
        .iter()
        .map(|output| output["artifact_ref"].clone())
        .collect::<Vec<_>>();
    if report["version"] != 1
        || report["check_id"] != hit.check_id
        || report["operation_id"] != hit.operation_id
        || report["candidate_ref"] != hit.candidate_ref
        || report["candidate_sha256"] != hit.candidate.content_digest
        || report["input_fingerprint"] != plan.input_fingerprint
        || report["resolved_inputs"] != plan.resolved_inputs
        || report["profile"] != plan.resolved_inputs["profile_identity"]
        || report["resolved_inputs"]["profile_sha256"] != profile_sha256
        || report["profile"]["profile_id"] != plan.resolved_inputs["profile_id"]
        || report["profile"]["profile_revision"] != plan.resolved_inputs["profile_revision"]
        || report["scope_plan"] != plan.scope_plan
        || report["cache_reusable"] != true
        || report["profile"]["reproducible"] != true
        || report["state"] != "passed"
        || report["exit_code"] != 0
        || report["resource_released"] != true
        || report["source_checkout_verified"] != true
        || !report["error"].is_null()
        || (!report["cancellation"].is_null()
            && (report["cancellation"]["disposition"] != "completed_before_termination"
                || report["cancellation"]["termination_requests"] != 0
                || !report["cancellation"]["last_error"].is_null()))
        || report["process"] != hit.process["process"]
        || report["coverage"] != hit.coverage
        || worker::validate_passed_coverage(
            &parser,
            &expected_targets,
            &report["scope_plan"],
            &hit.coverage,
        )
        .is_err()
        || reported_refs != output_refs.iter().map(|id| json!(id)).collect::<Vec<_>>()
    {
        return Ok(None);
    }
    for (output, reference) in reported_outputs.iter().zip(&hit.outputs) {
        if output["stream"] != reference.metadata["stream"]
            || output["sha256"] != reference.content_digest
            || output["length"] != reference.byte_length
            || files.verify(reference).is_err()
        {
            return Ok(None);
        }
    }
    Ok(Some(hit))
}

fn cache_hit_is_current(
    tx: &Transaction<'_>,
    hit: &PreparedCacheHit,
    plan: &PreparedCheckPlan,
) -> Result<bool> {
    let row = tx
        .query_row(
            "SELECT operation_id,candidate_ref,cache_key,spec_json,coverage_json,result_ref,process_identity_json FROM check_runs WHERE check_id=?1 AND cached_from_check_id IS NULL AND state='passed' AND exit_code=0 AND resource_released_at_ms IS NOT NULL",
            [&hit.check_id],
            |row| {
                Ok(CacheOriginRow {
                    operation_id: row.get(0)?,
                    candidate_ref: row.get(1)?,
                    cache_key: row.get(2)?,
                    spec_json: row.get(3)?,
                    coverage_json: row.get(4)?,
                    result_ref: row.get(5)?,
                    process_identity_json: row.get(6)?,
                })
            },
        )
        .optional()?;
    let Some(row) = row else {
        return Ok(false);
    };
    let (Ok(spec), Ok(coverage)) = (
        serde_json::from_str::<Value>(&row.spec_json),
        serde_json::from_str::<Value>(&row.coverage_json),
    ) else {
        return Ok(false);
    };
    let profile: crate::checks::model::CheckProfile =
        match serde_json::from_value(spec["profile"].clone()) {
            Ok(profile) => profile,
            Err(_) => return Ok(false),
        };
    let expected_targets: Vec<String> =
        match serde_json::from_value(plan.resolved_inputs["expected_targets"].clone()) {
            Ok(targets) => targets,
            Err(_) => return Ok(false),
        };
    if row.operation_id != hit.operation_id
        || row.candidate_ref != hit.candidate_ref
        || row.cache_key != plan.input_fingerprint
        || row.result_ref.as_deref() != Some(hit.result.artifact_id.as_str())
        || spec["cache_policy"] != "reusable"
        || spec["cache_key"] != plan.input_fingerprint
        || spec["input_fingerprint"] != plan.input_fingerprint
        || spec["resolved_inputs"] != plan.resolved_inputs
        || spec["scope_plan"] != plan.scope_plan
        || !spec["cache_source_acceptance"].is_null()
        || !spec["cached_from_check_id"].is_null()
        || inputs::profile_identity(&profile).ok().as_ref()
            != Some(&plan.resolved_inputs["profile_identity"])
        || worker::validate_passed_coverage(
            &profile.parser,
            &expected_targets,
            &spec["scope_plan"],
            &coverage,
        )
        .is_err()
        || !valid_process_receipt(&hit.process, &spec)
        || coverage != hit.coverage
        || row
            .process_identity_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .as_ref()
            != Some(&hit.process)
    {
        return Ok(false);
    }
    let operation = operations::get_operation(tx, &hit.operation_id)?;
    if operation["method"] != "check.run"
        || operation["state"] != "settled"
        || operation["result"]["outcome"] != "applied"
        || operation["result"]["check_id"] != hit.check_id
        || operation["result"]["state"] != "passed"
        || operation["result"]["exit_code"] != 0
        || operation["result"]["source_checkout_verified"] != true
        || operation["result"]["result_ref"] != hit.result.artifact_id
        || !operation["result"]["cached"].is_null()
        || !operation["result"]["cached_from_check_id"].is_null()
    {
        return Ok(false);
    }
    let Some(output_refs) = operation["result"]["output_refs"].as_array() else {
        return Ok(false);
    };
    if output_refs
        != &hit
            .outputs
            .iter()
            .map(|record| json!(record.artifact_id))
            .collect::<Vec<_>>()
    {
        return Ok(false);
    }
    let candidate = results::get(tx, &hit.candidate_ref)?;
    let result = results::get(tx, &hit.result.artifact_id)?;
    if model::canonical(&json!(candidate))? != model::canonical(&json!(hit.candidate))?
        || model::canonical(&json!(result))? != model::canonical(&json!(hit.result))?
    {
        return Ok(false);
    }
    for expected in &hit.outputs {
        let actual = results::get(tx, &expected.artifact_id)?;
        if model::canonical(&json!(actual))? != model::canonical(&json!(expected))? {
            return Ok(false);
        }
    }
    let accepted =
        acceptance::freeze_baseline_candidate(tx, &plan.project_id, Some(&hit.candidate_ref))?;
    Ok(accepted["status"] == "verified"
        && model::canonical(&accepted)? == model::canonical(&hit.source_acceptance)?)
}

fn attempt(db: &Connection, p: &Principal, id: &str) -> Result<Value> {
    let a = tasks::get_attempt(db, id)?;
    if p.role == Role::Participant {
        let task_id = model::text(&a, "task_id")?;
        let revision = a["task_revision"]
            .as_i64()
            .ok_or_else(|| Error::invalid("Attempt has no task revision"))?;
        // Source capture is a scoped Participant submission primitive. The
        // coordination guard proves the live registration, exact Task
        // revision, Attempt, and non-review participation basis before any
        // filesystem capture is admitted.
        super::coordination::authorize_task_submission(db, p, task_id, revision, id)?;
    } else {
        p.require_writer()?;
        super::gm::require_attempt_control(db, p, &a)?;
    }
    let t = tasks::get_task(db, model::text(&a, "task_id")?)?;
    if !a["released_at_ms"].is_null() || t["state"] != "open" || t["revision"] != a["task_revision"]
    {
        return Err(Error::new(
            "STALE_ATTEMPT",
            "source/check requires current unreleased open Task ownership",
        ));
    }
    Ok(a)
}
fn artifact(db: &Connection, r: &ArtifactRecord) -> Result<()> {
    let length = i64::try_from(r.byte_length)
        .map_err(|_| Error::invalid("artifact length exceeds SQLite range"))?;
    db.execute("INSERT OR IGNORE INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,metadata_json,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![r.artifact_id,r.relative_path,r.kind,length,r.content_digest,model::canonical(&r.metadata)?,model::now_ms()?])?;
    let old = results::get(db, &r.artifact_id)?;
    if old.content_digest != r.content_digest
        || old.byte_length != r.byte_length
        || old.relative_path != r.relative_path
        || old.metadata != r.metadata
        || old.kind != r.kind
    {
        return Err(Error::conflict(
            "artifact identity already holds other bytes",
        ));
    }
    Ok(())
}
fn settle(db: &Connection, id: &str, result: &Value) -> Result<()> {
    let now = model::now_ms()?;
    let encoded = model::canonical(result)?;
    db.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1",params![id,encoded,now])?;
    db.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:checks',?1,?1,'check.completed',?2,?3)",params![id,encoded,now])?;
    Ok(())
}

fn source_admission_json(
    id: &str,
    p: &Principal,
    a: &Value,
    input: &CaptureRequest,
    lease_authority: Option<&LeaseAuthorityRef>,
) -> Result<Value> {
    Ok(json!({
        "schema_version":1,
        "operation_id":id,
        "method":"source.capture",
        "caller_id":p.client_id,
        "role":serde_json::to_value(&p.role)?,
        "task_id":a["task_id"],
        "task_revision":a["task_revision"],
        "attempt_id":input.attempt_id,
        "repository":serde_json::to_value(&input.repository)?,
        "commit":input.commit,
        "lease_authority":lease_authority.map(serde_json::to_value).transpose()?.unwrap_or(Value::Null),
    }))
}

/// Validate only the immutable source admission sealed before native file I/O.
/// This deliberately performs no current-principal, Task, Attempt or lease
/// lookup so already-captured bytes remain recordable after release.
fn validate_source_admission(
    op: &Value,
    effective: &Value,
    id: &str,
    p: &Principal,
    input: &CaptureRequest,
) -> Result<Option<LeaseAuthorityRef>> {
    let admission = effective.get("source_admission").ok_or_else(|| {
        Error::new(
            "SOURCE_ADMISSION_MISSING",
            "source capture has no retained admission origin",
        )
    })?;
    if admission["schema_version"] != 1
        || admission["operation_id"] != id
        || admission["method"] != "source.capture"
        || admission["caller_id"] != p.client_id
        || admission["caller_id"] != op["caller_id"]
        || admission["task_id"] != op["task_id"]
        || admission["attempt_id"] != op["attempt_id"]
        || admission["task_revision"] != input.expected_revision
        || admission["repository"] != serde_json::to_value(&input.repository)?
        || admission["commit"] != input.commit
    {
        return Err(Error::new(
            "SOURCE_ADMISSION_MISMATCH",
            "source capture admission no longer names the original request",
        ));
    }
    let identity = effective.get("identity").ok_or_else(|| {
        Error::new(
            "SOURCE_ADMISSION_MISSING",
            "source capture has no retained Task identity",
        )
    })?;
    if identity["task_id"] != op["task_id"]
        || identity["attempt_id"] != op["attempt_id"]
        || identity["task_revision"] != input.expected_revision
    {
        return Err(Error::new(
            "SOURCE_ADMISSION_MISMATCH",
            "source capture identity no longer names the original Task Attempt",
        ));
    }
    let admitted_role: Role = serde_json::from_value(admission["role"].clone())?;
    if !matches!(
        admitted_role,
        Role::Participant | Role::Manager | Role::Operator
    ) {
        return Err(Error::new(
            "SOURCE_ADMISSION_MISMATCH",
            "source capture admission retained a role without capture authority",
        ));
    }
    if admitted_role == Role::Participant {
        if admission["lease_authority"].is_null() {
            return Err(Error::new(
                "SOURCE_ADMISSION_MISSING",
                "Participant source capture has no retained launch authority",
            ));
        }
        let task_id = model::text(op, "task_id")?;
        let authority: LeaseAuthorityRef =
            serde_json::from_value(admission["lease_authority"].clone())?;
        if authority.state != "held"
            || authority.task_id != task_id
            || authority.task_revision != input.expected_revision
            || authority.attempt_id.as_deref() != Some(input.attempt_id.as_str())
        {
            return Err(Error::new(
                "SOURCE_ADMISSION_MISMATCH",
                "Participant source capture retained an invalid launch authority",
            ));
        }
        Ok(Some(authority))
    } else {
        if !admission["lease_authority"].is_null() {
            return Err(Error::new(
                "SOURCE_ADMISSION_MISMATCH",
                "non-Participant source capture retained a launch authority",
            ));
        }
        Ok(None)
    }
}

fn validate_source_artifact(
    op: &Value,
    input: &CaptureRequest,
    record: &ArtifactRecord,
) -> Result<()> {
    let operation_id = model::text(op, "operation_id")?;
    if record.artifact_id != format!("source-{}", model::digest(operation_id.as_bytes())) {
        return Err(Error::new(
            "SOURCE_CAPTURE_METADATA",
            "capture result is bound to a different source Operation",
        ));
    }
    if record.kind != "source_snapshot"
        || record.metadata["task_id"] != op["task_id"]
        || record.metadata["attempt_id"] != input.attempt_id
        || record.metadata["task_revision"] != input.expected_revision
    {
        return Err(Error::new(
            "STALE_REVISION",
            "capture result no longer names the admitted Task Attempt",
        ));
    }
    let commit = record.metadata["commit"].as_str().ok_or_else(|| {
        Error::new(
            "SOURCE_CAPTURE_METADATA",
            "capture result has no immutable commit identity",
        )
    })?;
    if !commit.eq_ignore_ascii_case(&input.commit) {
        return Err(Error::new(
            "SOURCE_CAPTURE_METADATA",
            "capture result commit differs from the admitted source",
        ));
    }
    let tree = record.metadata["tree"].as_str().ok_or_else(|| {
        Error::new(
            "SOURCE_CAPTURE_METADATA",
            "capture result has no immutable tree identity",
        )
    })?;
    if !matches!(tree.len(), 40 | 64) || !tree.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::new(
            "SOURCE_CAPTURE_METADATA",
            "capture result tree identity is invalid",
        ));
    }
    if record.metadata["coverage"] != "complete" || record.metadata["file_count"].as_u64().is_none()
    {
        return Err(Error::new(
            "SOURCE_CAPTURE_METADATA",
            "capture result is not a complete immutable source snapshot",
        ));
    }
    Ok(())
}

pub(super) fn reserve_source(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    config: &Config,
) -> Result<Value> {
    let input = CaptureRequest::parse(v)?;
    let a = attempt(tx, p, &input.attempt_id)?;
    if a["task_revision"] != input.expected_revision {
        return Err(Error::new(
            "STALE_REVISION",
            "source capture revision changed",
        ));
    }
    let participant_workspace = if p.role == Role::Participant {
        Some(super::workspace::participant_source_workspace(
            tx,
            model::text(&a, "task_id")?,
            input.expected_revision,
            &input.attempt_id,
            &input.repository,
        )?)
    } else {
        None
    };
    let admission = source_admission_json(
        id,
        p,
        &a,
        &input,
        participant_workspace
            .as_ref()
            .map(|workspace| &workspace.authority),
    )?;
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",params![id,a["task_id"].as_str(),input.attempt_id,model::canonical(&json!({"capture":input,"git_executable":config.checks.git_executable,"identity":{"task_id":a["task_id"],"attempt_id":a["attempt_id"],"task_revision":a["task_revision"]},"source_admission":admission}))?])?;
    Ok(json!({"operation_id":id,"state":"queued","admission":"durable_local"}))
}

type BeginSourceCapture = (CaptureRequest, PathBuf, Value, Option<PathBuf>);

fn begin_source(db: &mut Connection, p: Principal, id: &str) -> Result<Option<BeginSourceCapture>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let p = current_principal(&tx, p)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "source.capture" || op["caller_id"] != p.client_id {
        return Err(Error::new("FORBIDDEN", "capture belongs to another caller"));
    }
    if !matches!(op["state"].as_str(), Some("queued" | "outcome_unknown")) {
        return Ok(None);
    }
    let raw: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let v: Value = serde_json::from_str(&raw)?;
    let input: CaptureRequest = serde_json::from_value(v["capture"].clone())?;
    let a = attempt(&tx, &p, &input.attempt_id)?;
    if op["task_id"] != a["task_id"]
        || op["attempt_id"] != input.attempt_id
        || a["task_revision"] != input.expected_revision
    {
        return Err(Error::new(
            "STALE_REVISION",
            "source capture Operation no longer names the exact Task Attempt",
        ));
    }
    let sealed_lease = if v.get("source_admission").is_some() {
        let sealed = validate_source_admission(&op, &v, id, &p, &input)?;
        if v["source_admission"]["role"] != serde_json::to_value(&p.role)? {
            return Err(Error::new(
                "SOURCE_ADMISSION_MISMATCH",
                "source capture admission role differs from the current start principal",
            ));
        }
        sealed
    } else {
        let participant_workspace = if p.role == Role::Participant {
            Some(super::workspace::participant_source_workspace(
                &tx,
                model::text(&a, "task_id")?,
                input.expected_revision,
                &input.attempt_id,
                &input.repository,
            )?)
        } else {
            None
        };
        let admission = source_admission_json(
            id,
            &p,
            &a,
            &input,
            participant_workspace
                .as_ref()
                .map(|workspace| &workspace.authority),
        )?;
        tx.execute(
            "UPDATE operations SET effective_request_json=?2 WHERE operation_id=?1",
            params![
                id,
                model::canonical(&json!({
                    "capture":input,
                    "git_executable":v["git_executable"],
                    "identity":v["identity"],
                    "source_admission":admission,
                }))?
            ],
        )?;
        participant_workspace.map(|workspace| workspace.authority)
    };
    let authorized_repository = if p.role == Role::Participant {
        let admitted = super::workspace::participant_source_workspace(
            &tx,
            model::text(&a, "task_id")?,
            input.expected_revision,
            &input.attempt_id,
            &input.repository,
        )?;
        if sealed_lease.as_ref() != Some(&admitted.authority) {
            return Err(Error::new(
                "SOURCE_WORKSPACE_STALE",
                "held workspace lease differs from the sealed source admission",
            ));
        }
        Some(admitted.path)
    } else {
        if sealed_lease.is_some() {
            return Err(Error::new(
                "SOURCE_ADMISSION_MISMATCH",
                "non-Participant source capture retained a launch authority",
            ));
        }
        None
    };
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1",params![id,model::now_ms()?])?;
    tx.commit()?;
    Ok(Some((
        input,
        serde_json::from_value(v["git_executable"].clone())?,
        v["identity"].clone(),
        authorized_repository,
    )))
}
fn finish_source(
    db: &mut Connection,
    p: Principal,
    id: &str,
    outcome: Result<ArtifactRecord>,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let op = operations::get_operation(&tx, id)?;
    if op["state"] != "sending" || op["method"] != "source.capture" {
        return Err(Error::conflict("capture is not executing"));
    }
    let raw: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |row| row.get(0),
    )?;
    let effective: Value = serde_json::from_str(&raw)?;
    let input = CaptureRequest::parse(&effective["capture"])?;
    let _sealed_lease = validate_source_admission(&op, &effective, id, &p, &input)?;
    let result = outcome.and_then(|r| {
        validate_source_artifact(&op, &input, &r)?;
        artifact(&tx, &r)?;
        Ok(json!({"operation_id":id,"outcome":"applied","candidate_ref":r.artifact_id,"commit":r.metadata["commit"],"tree":r.metadata["tree"],"file_count":r.metadata["file_count"],"task_accepted":false}))
    });
    let result = match result {
        Ok(v) => v,
        Err(e) => json!({"operation_id":id,"outcome":"failed","error":e}),
    };
    settle(&tx, id, &result)?;
    tx.commit()?;
    Ok(())
}
pub(super) fn reserve(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    config: &Config,
    resolution: Option<&CheckPlanResolution>,
) -> Result<(Value, bool)> {
    let input = CheckRequest::parse(v)?;
    let a = attempt(tx, p, &input.attempt_id)?;
    let facts = plan_inputs(tx, p, v, config)?;
    reserve_with_facts(
        tx,
        input,
        a,
        facts,
        id,
        CheckAdmission {
            resolution,
            cron_context: None,
            executor: config.checks.executor.clone(),
        },
    )
}

pub(super) fn reserve_cron(
    tx: &Transaction<'_>,
    context: &crate::automation::authorization::CronExecutionContext,
    id: &str,
    config: &Config,
    resolution: Option<&CheckPlanResolution>,
) -> Result<(Value, bool)> {
    let request = context.request_params()?;
    model::validate_mutation("check.run", &request)?;
    context.require_current_check_target(tx)?;
    let input = CheckRequest::parse(&request)?;
    let a = tasks::get_attempt(tx, &input.attempt_id)?;
    let facts = plan_inputs_for_attempt(tx, &input, &a, config)?;
    reserve_with_facts(
        tx,
        input,
        a,
        facts,
        id,
        CheckAdmission {
            resolution,
            cron_context: Some(context),
            executor: config.checks.executor.clone(),
        },
    )
}

fn reserve_with_facts(
    tx: &Transaction<'_>,
    input: CheckRequest,
    a: Value,
    facts: CheckPlanInputs,
    id: &str,
    admission: CheckAdmission<'_>,
) -> Result<(Value, bool)> {
    let resolution = admission.resolution.ok_or_else(|| {
        Error::new(
            "CHECK_PLAN_REQUIRED",
            "CheckRunner inputs must be resolved before admission",
        )
    })?;
    if resolution.context_fingerprint != facts.context_fingerprint {
        return Err(Error::new(
            "CHECK_PLAN_STALE",
            "CheckRunner source, baseline acceptance, Attempt or profile changed after plan resolution",
        ));
    }
    let prepared = resolution.result.as_ref().map_err(Clone::clone)?;
    if prepared.input_fingerprint.is_empty()
        || prepared.resolved_inputs["input_fingerprint"] != prepared.input_fingerprint
    {
        return Err(Error::new(
            "CHECK_PLAN_INVALID",
            "resolved CheckRunner fingerprint is missing or inconsistent",
        ));
    }
    let profile = facts.profile;
    let candidate = facts.candidate;
    let key = prepared.input_fingerprint.clone();
    let existing:Option<(String,String)>=tx.query_row("SELECT check_id,operation_id FROM check_runs WHERE attempt_id=?1 AND cache_key=?2 AND (state IN ('queued','running','reconciling') OR (resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL))",params![input.attempt_id,key],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((check, op)) = existing {
        if admission.cron_context.is_some() {
            tx.execute(
                "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=json_object('coalesced_check_id',?4) WHERE operation_id=?1",
                params![id, a["task_id"].as_str(), input.attempt_id, check],
            )?;
        }
        return Ok((
            json!({"operation_id":op,"check_id":check,"coalesced":true}),
            false,
        ));
    }
    let check = model::new_id();
    let token = model::new_id();
    let mut spec = json!({
        "profile_id":profile.profile_id,
        "profile_revision":profile.profile_revision,
        "profile":profile,
        "candidate":candidate,
        "baseline":facts.baseline,
        "baseline_acceptance":a["task_snapshot"]["baseline_candidate"],
        "token":token,
        "task_revision":a["task_revision"],
        "cache_key":key,
        "cache_policy":if prepared.reproducible && prepared.resolved_inputs["cache_reusable"] == true {"reusable"} else {"disabled_unverified_inputs"},
        "reproducible":prepared.reproducible,
        "plan_context_sha256":prepared.context_fingerprint,
        "resolved_inputs":prepared.resolved_inputs,
        "scope_plan":prepared.scope_plan,
        "input_fingerprint":prepared.input_fingerprint,
    });
    if let Some(executor) = admission.executor {
        spec["executor"] = serde_json::to_value(executor)?;
    }
    let resource = format!("check-target:{}", profile.resource.to_lowercase());
    let current_cache_hit = match resolution.cache_hit.as_ref() {
        Some(hit) if cache_hit_is_current(tx, hit, prepared)? => Some(hit),
        _ => None,
    };
    if let Some(hit) = current_cache_hit {
        // Reuse only the original process check. The new row owns no process,
        // exit code, claimed resource or resource-release timestamp.
        artifact(tx, &hit.result)?;
        for output in &hit.outputs {
            artifact(tx, output)?;
        }
        let check = model::new_id();
        spec["cache_source_acceptance"] = hit.source_acceptance.clone();
        tx.execute(
            "INSERT INTO check_runs(check_id,operation_id,attempt_id,candidate_ref,cache_key,cached_from_check_id,resource_key,spec_json,state,coverage_json,result_ref,finished_at_ms,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'passed',?9,?10,?11,?11)",
            params![
                check,
                id,
                input.attempt_id,
                input.candidate_ref,
                key,
                hit.check_id,
                resource,
                model::canonical(&spec)?,
                model::canonical(&hit.coverage)?,
                hit.result.artifact_id,
                model::now_ms()?,
            ],
        )?;
        let owner = model::text(&a, "owner_id")?;
        tx.execute(
            "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=json_object('check_id',?4) WHERE operation_id=?1",
            params![id, a["task_id"].as_str(), input.attempt_id, check],
        )?;
        return Ok((
            json!({
                "operation_id":id,
                "outcome":"applied",
                "check_id":check,
                "state":"passed",
                "exit_code":null,
                "result_ref":hit.result.artifact_id,
                "recipient":owner,
                "source_checkout_verified":true,
                "output_refs":hit.outputs.iter().map(|record| &record.artifact_id).collect::<Vec<_>>(),
                "cached":true,
                "cached_from_check_id":hit.check_id,
                "task_accepted":false
            }),
            false,
        ));
    }
    tx.execute("INSERT INTO check_runs(check_id,operation_id,attempt_id,candidate_ref,cache_key,resource_key,spec_json,state,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,'queued',?8)",params![check,id,input.attempt_id,input.candidate_ref,key,resource,model::canonical(&spec)?,model::now_ms()?])?;
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=json_object('check_id',?4) WHERE operation_id=?1",params![id,a["task_id"].as_str(),input.attempt_id,check])?;
    Ok((
        json!({"operation_id":id,"check_id":check,"state":"queued","admission":"durable_local"}),
        true,
    ))
}
pub(super) fn cancel(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    model::fields(v, &["client_request_id", "check_id", "reason"])?;
    let check = model::text(v, "check_id")?;
    let reason = model::text(v, "reason")?;
    let c = describe(tx, &json!({"check_id":check}))?;
    let a = tasks::get_attempt(tx, model::text(&c, "attempt_id")?)?;
    super::gm::require_attempt_control(tx, p, &a)?;
    if !matches!(
        c["state"].as_str(),
        Some("queued" | "running" | "reconciling")
    ) {
        return Ok(
            json!({"operation_id":id,"check_id":check,"cancellation_requested":false,"state":c["state"],"disposition":"already_terminal"}),
        );
    }
    if let Some(previous) = c.get("cancel_request").filter(|v| !v.is_null()) {
        return Ok(
            json!({"operation_id":id,"check_id":check,"cancellation_operation_id":previous["operation_id"],"cancellation_requested":true,"coalesced":true,"process_killed":false}),
        );
    }
    // An older independently running binary cannot be hot-upgraded into a new protocol.
    if !c["process"].is_null() && c["process"]["control_version"] != 2 {
        return Err(Error::new(
            "CHECK_CANCEL_UNSUPPORTED",
            "this running worker predates active cancellation; its normal result is still collected",
        ));
    }
    let request = CancelRequest {
        operation_id: id.into(),
        reason: reason.into(),
    };
    tx.execute("UPDATE check_runs SET spec_json=json_set(spec_json,'$.cancel_requested',?2,'$.cancel_request',json(?3)) WHERE check_id=?1",
        params![check,reason,model::canonical(&json!(request))?])?;
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
        params![id, a["task_id"].as_str(), a["attempt_id"].as_str()],
    )?;
    Ok(
        json!({"operation_id":id,"check_id":check,"cancellation_operation_id":id,"cancellation_requested":true,"admission":"durable_request","process_killed":false}),
    )
}
/// Load and shape-validate the retained quarantine record without parsing
/// the damaged spec. Returns None when this CheckRun was never quarantined.
fn load_quarantine_record(
    db: &Connection,
    check_id: &str,
) -> Result<Option<PendingQuarantineEvidence>> {
    let Some(value) = meta(db, &quarantine_key(check_id))? else {
        return Ok(None);
    };
    if model::canonical(&value)?.len() > MAX_PENDING_QUARANTINE_BYTES {
        return Err(Error::new(
            "CHECK_PENDING_QUARANTINE_CORRUPT",
            "retained pending quarantine evidence exceeds its bounded size",
        ));
    }
    let record: PendingQuarantineEvidence = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "CHECK_PENDING_QUARANTINE_CORRUPT",
            "retained pending quarantine evidence cannot be decoded",
        )
    })?;
    validate_pending_quarantine(&record, check_id)?;
    Ok(Some(record))
}
/// Prove the retained quarantine record still names this exact CheckRun
/// lineage, comparing only digests and plain identifiers. A changed source
/// stays a hard identity conflict; it never resumes under old quarantine.
fn read_quarantine_for_held(
    tx: &Transaction<'_>,
    check_id: &str,
    operation_id: &str,
    attempt_id: &str,
    spec_json: &str,
) -> Result<PendingQuarantineEvidence> {
    let Some(record) = load_quarantine_record(tx, check_id)? else {
        return Err(Error::new(
            "CHECK_HELD_NOT_QUARANTINED",
            "CheckRun has no retained quarantine evidence",
        ));
    };
    let current_identity = read_subject_identity(tx, check_id)?;
    if record.identity != current_identity
        || record.identity.check_id != check_id
        || record.identity.operation_id != operation_id
        || record.identity.attempt_id != attempt_id
        || record.identity.check_spec_sha256 != model::digest(spec_json.as_bytes())
    {
        return Err(Error::new(
            "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT",
            "retained quarantine identity no longer matches the CheckRun source",
        ));
    }
    Ok(record)
}
fn write_held_acknowledged(
    tx: &Transaction<'_>,
    held: &HeldRecord,
    acknowledged_by: &str,
    acknowledged_at_ms: i64,
    operation_id: &str,
) -> Result<()> {
    let mut next = held.clone();
    next.acknowledged = true;
    next.acknowledged_by = Some(acknowledged_by.to_owned());
    next.acknowledged_at_ms = Some(acknowledged_at_ms);
    next.ack_operation_id = Some(operation_id.to_owned());
    validate_held_record(&next, &next.check_id)?;
    set_meta(tx, &held_key(&next.check_id), &json!(&next))?;
    Ok(())
}
/// Explicit exact acknowledgement of a quarantined CheckRun into the held
/// inventory. The run stays nonterminal: no repair, release, terminal state
/// or worker is produced, and the quarantine evidence itself is never
/// rewritten. Only the held inventory record flips to acknowledged under
/// the exact retained source digest.
pub(super) fn acknowledge_held(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
) -> Result<Value> {
    model::fields(
        v,
        &["client_request_id", "check_id", "expected_source_digest"],
    )?;
    let check = model::text(v, "check_id")?;
    let expected = model::text(v, "expected_source_digest")?;
    if !valid_held_digest(expected) {
        return Err(Error::invalid("expected_source_digest must be SHA-256 hex"));
    }
    let row: Option<(String, String, String, String)> = tx
        .query_row(
            "SELECT state, operation_id, attempt_id, spec_json FROM check_runs WHERE check_id=?1",
            [check],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((state, operation_id, attempt_id, spec_json)) = row else {
        return Err(Error::new("NOT_FOUND", "unknown CheckRun"));
    };
    let attempt = tasks::get_attempt(tx, &attempt_id)?;
    super::gm::require_attempt_control(tx, p, &attempt)?;
    if !matches!(state.as_str(), "queued" | "running" | "reconciling") {
        return Ok(
            json!({"operation_id":id,"check_id":check,"acknowledged":false,"state":state,"disposition":"already_terminal"}),
        );
    }
    let record = read_quarantine_for_held(tx, check, &operation_id, &attempt_id, &spec_json)?;
    let source_digest = model::digest(spec_json.as_bytes());
    if expected != source_digest {
        return Err(Error::new(
            "CHECK_HELD_DIGEST_MISMATCH",
            "expected source digest does not match the retained quarantine identity",
        ));
    }
    let now = model::now_ms()?;
    match load_held_record(tx, check)? {
        Some(held) => {
            validate_held_matches_quarantine(&held, &record)?;
            if held.acknowledged {
                return Ok(
                    json!({"operation_id":id,"check_id":check,"acknowledged":true,"coalesced":true,"held":true,"state":state,"source_digest":source_digest,"ack_operation_id":held.ack_operation_id}),
                );
            }
            write_held_acknowledged(tx, &held, &p.client_id, now, id)?;
        }
        None => {
            let held = HeldRecord {
                schema_version: HELD_SCHEMA,
                check_id: check.to_owned(),
                quarantine_code: record.code.clone(),
                quarantine_field: record.field.clone(),
                quarantine_reason: record.reason.clone(),
                source_digest: source_digest.clone(),
                acknowledged: false,
                acknowledged_by: None,
                acknowledged_at_ms: None,
                ack_operation_id: None,
            };
            validate_held_record(&held, check)?;
            set_meta(tx, &held_key(check), &json!(&held))?;
            write_held_acknowledged(tx, &held, &p.client_id, now, id)?;
        }
    }
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
        params![
            id,
            attempt["task_id"].as_str(),
            attempt["attempt_id"].as_str()
        ],
    )?;
    Ok(
        json!({"operation_id":id,"check_id":check,"acknowledged":true,"held":true,"state":state,"source_digest":source_digest}),
    )
}
pub(super) fn describe(db: &Connection, v: &Value) -> Result<Value> {
    model::fields(v, &["check_id"])?;
    let id = model::text(v, "check_id")?;
    let raw:Option<String>=db.query_row("SELECT json_object('check_id',check_id,'operation_id',operation_id,'attempt_id',attempt_id,'candidate_ref',candidate_ref,'cached_from',cached_from_check_id,'state',state,'resource_key',resource_key,'resource_claimed_at_ms',resource_claimed_at_ms,'resource_released_at_ms',resource_released_at_ms,'process',json(process_identity_json),'coverage',json(coverage_json),'result_ref',result_ref,'exit_code',exit_code,'profile_id',json_extract(spec_json,'$.profile_id'),'profile_revision',json_extract(spec_json,'$.profile_revision'),'cancel_request',json_extract(spec_json,'$.cancel_request'),'cancellation',json_extract(spec_json,'$.cancellation'),'process_diagnostic',json(json_extract(spec_json,'$.process_diagnostic.public')),'cleanup_pending',json(CASE WHEN json_type(spec_json,'$.cleanup_pending') IS NOT NULL THEN 'true' ELSE 'false' END),'resource_released',json(CASE json_extract(spec_json,'$.completion_resource_released') WHEN 1 THEN 'true' WHEN 0 THEN 'false' ELSE 'null' END),'process_facts',json(json_extract(spec_json,'$.process_facts')),'release_evidence',json(json_extract(spec_json,'$.release_evidence'))) FROM check_runs WHERE check_id=?1",[id],|r|r.get(0)).optional()?;
    Ok(serde_json::from_str(&raw.ok_or_else(|| {
        Error::new("NOT_FOUND", "unknown CheckRun")
    })?)?)
}
/// Bounded sanitized projection of a quarantined CheckRun. It reads only
/// plain columns plus the retained quarantine evidence and never parses the
/// damaged spec, so `check.get` succeeds exactly where `describe` cannot.
/// Returns None when this CheckRun was never quarantined.
pub(super) fn held_projection(
    db: &Connection,
    check_id: &str,
    operation_id: &str,
    attempt_id: &str,
) -> Result<Option<Value>> {
    let Some(record) = load_quarantine_record(db, check_id)? else {
        return Ok(None);
    };
    let current_identity = read_subject_identity(db, check_id)?;
    if record.identity != current_identity
        || current_identity.operation_id != operation_id
        || current_identity.attempt_id != attempt_id
    {
        return Err(Error::new(
            "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT",
            "retained quarantine identity no longer matches the CheckRun source",
        ));
    }
    type HeldProjectionRow = (
        String,
        Option<String>,
        String,
        Option<i64>,
        Option<i64>,
        String,
        Option<String>,
        Option<i64>,
    );
    let row: Option<HeldProjectionRow> = db
        .query_row(
            "SELECT state, cached_from_check_id, resource_key, resource_claimed_at_ms, resource_released_at_ms, coverage_json, result_ref, exit_code FROM check_runs WHERE check_id=?1",
            [check_id],
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
    let Some((
        state,
        cached_from,
        resource_key,
        resource_claimed_at_ms,
        resource_released_at_ms,
        coverage_json,
        result_ref,
        exit_code,
    )) = row
    else {
        return Err(Error::new("NOT_FOUND", "unknown CheckRun"));
    };
    let held = load_held_record(db, check_id)?;
    if let Some(held) = &held {
        validate_held_matches_quarantine(held, &record)?;
    }
    let coverage: Value = serde_json::from_str(&coverage_json)?;
    let resource_released = resource_released_at_ms
        .map(|_| Value::Bool(true))
        .unwrap_or(Value::Null);
    Ok(Some(json!({
        "check_id": check_id,
        "operation_id": operation_id,
        "attempt_id": attempt_id,
        "candidate_ref": current_identity.candidate_ref,
        "cached_from": cached_from,
        "state": state,
        "resource_key": resource_key,
        "resource_claimed_at_ms": resource_claimed_at_ms,
        "resource_released_at_ms": resource_released_at_ms,
        "coverage": coverage,
        "result_ref": result_ref,
        "exit_code": exit_code,
        "resource_released": resource_released,
        "custody": "unknown",
        "reconciliation": {
            "disposition": "quarantined",
            "code": record.code,
            "field": record.field,
            "reason": record.reason,
            "source_digest": record.identity.check_spec_sha256,
            "held": true,
            "acknowledged": held.as_ref().is_some_and(|record| record.acknowledged),
        },
    })))
}
fn record_process_diagnostic(db: &mut Connection, work: &Work, diagnostic: Value) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<(String, String, Option<String>, String)> = tx
        .query_row(
            "SELECT operation_id,spec_json,process_identity_json,state FROM check_runs WHERE check_id=?1",
            [&work.check_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((operation_id, spec_json, identity_json, state)) = row else {
        return Ok(false);
    };
    if !matches!(state.as_str(), "running" | "reconciling") {
        return Ok(false);
    }
    if operation_id != work.operation_id {
        return Err(Error::conflict(
            "process diagnostic CheckRun identity changed",
        ));
    }
    let Some(expected_worker) = work.expected_worker.as_ref() else {
        return Ok(false);
    };
    let Some(identity_json) = identity_json else {
        return Ok(false);
    };
    let identity: Value = serde_json::from_str(&identity_json)?;
    if &identity != expected_worker
        || identity["token"] != work.token
        || diagnostic["version"] != 1
        || diagnostic["check_id"] != work.check_id
        || diagnostic["operation_id"] != work.operation_id
        || diagnostic["token"] != work.token
        || diagnostic["process"] != identity["process"]
    {
        return Err(Error::conflict(
            "process diagnostic differs from the current CheckRun process owner",
        ));
    }
    let cause = diagnostic["public"]["cause"]
        .as_str()
        .ok_or_else(|| Error::invalid("process diagnostic cause is invalid"))?;
    let (expected_code, expected_message) = match cause {
        "observation_error" => (
            "CHECK_PROCESS_OBSERVATION_UNKNOWN",
            "the owned process group could not be observed as empty; the resource remains held",
        ),
        "control_read_error" => (
            "CHECK_CONTROL_READ_UNKNOWN",
            "the CheckRun cancellation receipt could not be read while its process group remains held",
        ),
        "drain_pending" => (
            "CHECK_PROCESS_DRAIN_PENDING",
            "the owned process group remains active after the drain grace period; the resource remains held",
        ),
        "output_capture_pending" => (
            "CHECK_OUTPUT_DRAIN_PENDING",
            "the owned process group is empty but output capture has not finished; the CheckRun remains unresolved",
        ),
        _ => return Err(Error::invalid("process diagnostic cause is invalid")),
    };
    let output_capture_pending_valid = cause != "output_capture_pending"
        || matches!(
            (
                diagnostic["public"]["stdout_pending"].as_bool(),
                diagnostic["public"]["stderr_pending"].as_bool()
            ),
            (Some(true), _) | (_, Some(true))
        );
    if diagnostic["public"]["code"] != expected_code
        || diagnostic["public"]["status"] != "unresolved"
        || diagnostic["public"]["message"] != expected_message
        || diagnostic["public"]["control_read_unknown"]
            .as_bool()
            .is_none()
        || diagnostic["public"]["elapsed_ms"].as_u64().is_none()
        || diagnostic["public"]["observed_at_ms"]
            .as_i64()
            .is_none_or(|value| value < 0)
        || !diagnostic["public"]["resolved_at_ms"].is_null()
        || !output_capture_pending_valid
    {
        return Err(Error::invalid("process diagnostic projection is invalid"));
    }
    let spec: Value = serde_json::from_str(&spec_json)?;
    if spec.get("token").and_then(Value::as_str) != Some(work.token.as_str()) {
        return Err(Error::conflict(
            "process diagnostic token differs from CheckRun",
        ));
    }
    if let Some(existing) = spec.get("process_diagnostic") {
        if existing["check_id"] != diagnostic["check_id"]
            || existing["operation_id"] != diagnostic["operation_id"]
            || existing["token"] != diagnostic["token"]
            || existing["process"] != diagnostic["process"]
        {
            return Err(Error::conflict(
                "retained CheckRun process diagnostic owner changed",
            ));
        }
        let rank = |code: &Value| match code.as_str() {
            Some("CHECK_PROCESS_DRAIN_PENDING") => 1,
            Some("CHECK_CONTROL_READ_UNKNOWN") => 2,
            Some("CHECK_PROCESS_OBSERVATION_UNKNOWN") => 3,
            Some("CHECK_OUTPUT_DRAIN_PENDING") => 4,
            _ => 0,
        };
        let old_rank = rank(&existing["public"]["code"]);
        let new_rank = rank(&diagnostic["public"]["code"]);
        let old_control = existing["public"]["control_read_unknown"] == true;
        let new_control = diagnostic["public"]["control_read_unknown"] == true;
        if new_rank < old_rank || (new_rank == old_rank && (!new_control || old_control)) {
            return Ok(false);
        }
    }
    let diagnostic_json = model::canonical(&diagnostic)?;
    let changed = tx.execute(
        "UPDATE check_runs SET spec_json=json_set(spec_json,'$.process_diagnostic',json(?2)) WHERE check_id=?1 AND state IN ('running','reconciling') AND process_identity_json=?3",
        params![work.check_id, diagnostic_json, identity_json],
    )?;
    if changed == 0 {
        return Ok(false);
    }
    tx.commit()?;
    Ok(true)
}

fn work(db: &Connection, id: &str, root: PathBuf) -> Result<Work> {
    match materialize_subject(db, id, root)? {
        SubjectMaterialization::Decoded(work) => Ok(*work),
        SubjectMaterialization::Damaged { field, reason } => Err(Error::new(
            "CHECK_SPEC_SUBJECT_CORRUPT",
            format!(
                "retained CheckRun spec is damaged at {field}: {}",
                reason.name()
            ),
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubjectDamageReason {
    MissingRequiredField,
    InvalidSubjectShape,
    InvalidJson,
}

impl SubjectDamageReason {
    fn name(self) -> &'static str {
        match self {
            Self::MissingRequiredField => "missing_required_field",
            Self::InvalidSubjectShape => "invalid_subject_shape",
            Self::InvalidJson => "invalid_json",
        }
    }
}

enum SubjectMaterialization {
    Decoded(Box<Work>),
    Damaged {
        field: &'static str,
        reason: SubjectDamageReason,
    },
}

struct WorkRow {
    operation_id: String,
    spec_json: String,
    process_identity_json: Option<String>,
}

fn read_work_row(db: &Connection, id: &str) -> Result<Option<WorkRow>> {
    Ok(db
        .query_row(
            "SELECT operation_id,spec_json,process_identity_json FROM check_runs WHERE check_id=?1",
            [id],
            |row| {
                Ok(WorkRow {
                    operation_id: row.get(0)?,
                    spec_json: row.get(1)?,
                    process_identity_json: row.get(2)?,
                })
            },
        )
        .optional()?)
}

fn decode_subject_fields(
    id: &str,
    operation_id: String,
    raw: String,
    identity: Option<String>,
    root: PathBuf,
) -> Result<SubjectMaterialization> {
    let spec: Value = match serde_json::from_str(&raw) {
        Ok(spec) => spec,
        Err(_) => {
            return Ok(SubjectMaterialization::Damaged {
                field: "spec",
                reason: SubjectDamageReason::InvalidJson,
            });
        }
    };
    let Some(fields) = spec.as_object() else {
        return Ok(SubjectMaterialization::Damaged {
            field: "spec",
            reason: SubjectDamageReason::InvalidSubjectShape,
        });
    };

    let token = match fields.get("token") {
        None => {
            return Ok(SubjectMaterialization::Damaged {
                field: "token",
                reason: SubjectDamageReason::MissingRequiredField,
            });
        }
        Some(Value::String(token)) if !token.trim().is_empty() => token.clone(),
        Some(_) => {
            return Ok(SubjectMaterialization::Damaged {
                field: "token",
                reason: SubjectDamageReason::InvalidSubjectShape,
            });
        }
    };
    let candidate = match fields.get("candidate") {
        None => {
            return Ok(SubjectMaterialization::Damaged {
                field: "candidate",
                reason: SubjectDamageReason::MissingRequiredField,
            });
        }
        Some(value) => match serde_json::from_value(value.clone()) {
            Ok(candidate) => candidate,
            Err(_) => {
                return Ok(SubjectMaterialization::Damaged {
                    field: "candidate",
                    reason: SubjectDamageReason::InvalidSubjectShape,
                });
            }
        },
    };
    let profile = match fields.get("profile") {
        None => {
            return Ok(SubjectMaterialization::Damaged {
                field: "profile",
                reason: SubjectDamageReason::MissingRequiredField,
            });
        }
        Some(value) => match serde_json::from_value(value.clone()) {
            Ok(profile) => profile,
            Err(_) => {
                return Ok(SubjectMaterialization::Damaged {
                    field: "profile",
                    reason: SubjectDamageReason::InvalidSubjectShape,
                });
            }
        },
    };
    let executor = match fields.get("executor").filter(|value| !value.is_null()) {
        None => None,
        Some(value) => match serde_json::from_value(value.clone()) {
            Ok(executor) => Some(executor),
            Err(_) => {
                return Ok(SubjectMaterialization::Damaged {
                    field: "executor",
                    reason: SubjectDamageReason::InvalidSubjectShape,
                });
            }
        },
    };
    let cancel_request = match fields
        .get("cancel_request")
        .filter(|value| !value.is_null())
    {
        None => None,
        Some(value) => match serde_json::from_value(value.clone()) {
            Ok(request) => Some(request),
            Err(_) => {
                return Ok(SubjectMaterialization::Damaged {
                    field: "cancel_request",
                    reason: SubjectDamageReason::InvalidSubjectShape,
                });
            }
        },
    };
    let expected_worker = identity.map(|raw| serde_json::from_str(&raw)).transpose()?;

    Ok(SubjectMaterialization::Decoded(Box::new(Work {
        check_id: id.into(),
        operation_id,
        token,
        preflight_error: fields
            .get("preflight_error")
            .filter(|value| !value.is_null())
            .cloned(),
        data_dir: root,
        candidate,
        profile,
        resolved_inputs: fields
            .get("resolved_inputs")
            .filter(|value| !value.is_null())
            .cloned(),
        scope_plan: fields
            .get("scope_plan")
            .filter(|value| !value.is_null())
            .cloned(),
        input_fingerprint: fields
            .get("input_fingerprint")
            .and_then(Value::as_str)
            .map(str::to_owned),
        executor,
        cancel_request,
        expected_worker,
        launch: fields
            .get("launch")
            .filter(|value| !value.is_null())
            .cloned(),
    })))
}

fn materialize_subject(db: &Connection, id: &str, root: PathBuf) -> Result<SubjectMaterialization> {
    let row = read_work_row(db, id)?.ok_or_else(|| {
        Error::new(
            "CHECK_PENDING_SUBJECT_MISSING",
            "selected CheckRun disappeared before materialization",
        )
    })?;
    decode_subject_fields(
        id,
        row.operation_id,
        row.spec_json,
        row.process_identity_json,
        root,
    )
}

fn next(db: &mut Connection, config: &Config, root: PathBuf) -> Result<Option<Work>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let enabled = config.checks.enabled
        && meta(&tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] == "enabled";
    let running: i64 = tx.query_row(
        "SELECT count(*) FROM check_runs WHERE state='running'",
        [],
        |r| r.get(0),
    )?;
    // The pending reader owns malformed specs and their exact quarantine.
    // Admission must remain total after that reader leaves the bytes intact.
    let row:Option<(String,String,String)>=tx.query_row("SELECT c.check_id,o.caller_id,c.attempt_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE c.state='queued' AND o.state='queued' AND CASE WHEN json_valid(c.spec_json) THEN (json_extract(c.spec_json,'$.cancel_requested') IS NOT NULL OR (?1 AND ?2 AND NOT EXISTS(SELECT 1 FROM check_runs active WHERE active.resource_key=c.resource_key AND active.resource_claimed_at_ms IS NOT NULL AND active.resource_released_at_ms IS NULL))) ELSE 0 END ORDER BY c.created_at_ms,c.check_id LIMIT 1",params![enabled,running < config.checks.max_running as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((id, caller, attempt_id)) = row else {
        return Ok(None);
    };
    let mut w = work(&tx, &id, root)?;
    let preflight = (|| -> Result<()> {
        let cancelled:bool=tx.query_row("SELECT json_extract(spec_json,'$.cancel_requested') IS NOT NULL FROM check_runs WHERE check_id=?1",[&id],|r|r.get(0))?;
        if cancelled {
            return Err(Error::new(
                "CHECK_CANCELLED",
                "queued check cancelled before command execution",
            ));
        }
        if caller == authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
            authorization::authorize_cron_check_run_start(&tx, &w.operation_id)?;
        } else {
            let p = current_principal(
                &tx,
                Principal {
                    client_id: caller,
                    link_id: String::new(),
                    role: Role::Manager,
                },
            )?;
            attempt(&tx, &p, &attempt_id)?;
        }
        let current = config
            .checks
            .profile(&w.profile.profile_id, &w.profile.profile_revision)?;
        if model::canonical(&json!(current))? != model::canonical(&json!(w.profile))? {
            return Err(Error::new(
                "CHECK_PROFILE_CHANGED",
                "queued profile revision contents changed",
            ));
        }
        Ok(())
    })();
    if let Err(e) = preflight {
        w.preflight_error = Some(json!(e));
        tx.execute("UPDATE check_runs SET spec_json=json_set(spec_json,'$.preflight_error',json(?2)) WHERE check_id=?1",params![id,model::canonical(&json!(e))?])?;
    }
    let now = model::now_ms()?;
    if w.preflight_error.is_none() {
        tx.execute("UPDATE check_runs SET state='running',resource_claimed_at_ms=?2,started_at_ms=?2 WHERE check_id=?1 AND state='queued'",params![id,now])?;
    }
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",params![w.operation_id,now])?;
    tx.commit()?;
    Ok(Some(w))
}
// An outer prepare error proves that no child was spawned. Retain that fact
// before publishing completion so an I/O failure only retries terminalization.
fn retain_pre_spawn_failure(db: &mut Connection, w: &Work, error: Value) -> Result<Work> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let retained = work(&tx, &w.check_id, w.data_dir.clone())?;
    if retained.operation_id != w.operation_id
        || retained.token != w.token
        || retained.launch.is_some()
        || retained.expected_worker.is_some()
        || retained
            .preflight_error
            .as_ref()
            .is_some_and(|previous| previous != &error)
    {
        return Err(Error::conflict(
            "pre-spawn failure differs from the retained CheckRun",
        ));
    }
    let changed = tx.execute(
        "UPDATE check_runs SET state='reconciling',spec_json=json_set(spec_json,'$.preflight_error',json(?2)) WHERE check_id=?1 AND state IN ('running','reconciling') AND resource_released_at_ms IS NULL",
        params![w.check_id, model::canonical(&error)?],
    )?;
    if changed != 1 {
        return Err(Error::conflict("pre-spawn failure has no active CheckRun"));
    }
    let retry = work(&tx, &w.check_id, w.data_dir.clone())?;
    tx.commit()?;
    Ok(retry)
}

/// Retain nullable identity values without allowing serde's missing-`Option`
/// default to make a truncated disposition look complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
struct PresentOption<T>(Option<T>);

/// Decode a nullable value only after serde has confirmed the map key exists.
/// The transparent wrapper otherwise makes an absent key deserialize as None.
fn deserialize_required_present_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<PresentOption<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(PresentOption)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingSubjectIdentity {
    check_id: String,
    operation_id: String,
    check_attempt_id: String,
    candidate_ref: String,
    cache_key: String,
    resource_key: String,
    check_created_at_ms: i64,
    check_spec_sha256: String,
    operation_caller_id: String,
    operation_client_request_id: String,
    operation_method: String,
    #[serde(deserialize_with = "deserialize_required_present_option")]
    operation_task_id: PresentOption<String>,
    #[serde(deserialize_with = "deserialize_required_present_option")]
    operation_attempt_id: PresentOption<String>,
    operation_original_request_sha256: String,
    operation_effective_request_sha256: String,
    #[serde(deserialize_with = "deserialize_required_present_option")]
    operation_binding_id: PresentOption<String>,
    #[serde(deserialize_with = "deserialize_required_present_option")]
    operation_binding_generation: PresentOption<i64>,
    attempt_id: String,
    attempt_task_id: String,
    attempt_task_revision: i64,
    attempt_task_snapshot_sha256: String,
    attempt_owner_id: String,
    attempt_start_owner: String,
    #[serde(deserialize_with = "deserialize_required_present_option")]
    attempt_start_operation_id: PresentOption<String>,
    #[serde(deserialize_with = "deserialize_required_present_option")]
    attempt_binding_id: PresentOption<String>,
    #[serde(deserialize_with = "deserialize_required_present_option")]
    attempt_binding_generation: PresentOption<i64>,
    task_id: String,
    task_project_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingQuarantineEvidence {
    schema_version: u32,
    identity: PendingSubjectIdentity,
    code: String,
    field: String,
    reason: String,
    first_seen_at_ms: i64,
    last_seen_at_ms: i64,
    occurrences: u32,
}

const MAX_PENDING_PAGE: usize = 16;
const MAX_PENDING_TEXT_BYTES: usize = 4096;
const MAX_PENDING_QUARANTINE_BYTES: usize = 16 * 1024;
const PENDING_CURSOR_KEY: &str = "checks:pending:v1:cursor";
const PENDING_QUARANTINE_PREFIX: &str = "checks:pending:v1:quarantine:";
const PENDING_QUARANTINE_SCHEMA: u32 = 1;
const PENDING_CYCLE_KEY: &str = "checks:pending:v1:cycle";
const PENDING_CYCLE_SCHEMA: u32 = 1;
const HELD_AUDIT_CURSOR_KEY: &str = "checks:held_audit:v1:cursor";
const MAX_PENDING_SUFFIX: usize = 12;
const MAX_PENDING_WRAP: usize = 4;
const PENDING_SUBJECT_SAVEPOINT: &str = "checks_pending_subject";

/// Durable CheckRun pending-cycle boundary for one bounded sweep.
///
/// `cut` names the lexical high-water of the cycle: identifiers admitted past
/// the cut stay out of the active page until the cycle turns over, so a
/// continuously replenished lexical tail cannot starve the failed older
/// subject. The cut is ordering metadata only, never execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingCycle {
    schema_version: u32,
    cut: String,
}

/// Closed canonical server identifier shape: lowercase hyphenated UUIDv4,
/// exactly as model::new_id mints every CheckRun identifier. Existence in
/// the current pending set is never required; shape is.
fn valid_pending_cursor_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
        return false;
    }
    if bytes[14] != b'4' {
        return false;
    }
    if !matches!(bytes[19], b'8' | b'9' | b'a' | b'b') {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        match index {
            8 | 13 | 18 | 23 | 14 | 19 => {}
            _ => {
                if !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase() {
                    return false;
                }
            }
        }
    }
    true
}

fn read_pending_cycle(db: &Connection) -> Result<Option<PendingCycle>> {
    let Some(value) = meta(db, PENDING_CYCLE_KEY)? else {
        return Ok(None);
    };
    let cycle: PendingCycle = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "CHECK_PENDING_CYCLE_CORRUPT",
            "retained pending cycle boundary cannot be decoded",
        )
    })?;
    if cycle.schema_version != PENDING_CYCLE_SCHEMA || !valid_pending_cursor_id(&cycle.cut) {
        return Err(Error::new(
            "CHECK_PENDING_CYCLE_CORRUPT",
            "retained pending cycle boundary is not a canonical CheckRun ordering identity",
        ));
    }
    Ok(Some(cycle))
}

fn max_pending_id(tx: &Transaction<'_>) -> Result<Option<String>> {
    let max: Option<String> = tx.query_row(
        "SELECT max(c.check_id) FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE (c.state IN ('running','reconciling') OR (c.state='queued' AND o.state IN ('sending','outcome_unknown')) OR (c.state='queued' AND o.state='queued' AND NOT json_valid(c.spec_json)))",
        [],
        |row| row.get(0),
    )?;
    Ok(max)
}

fn pending_subject_count(tx: &Transaction<'_>) -> Result<i64> {
    Ok(tx.query_row(
        "SELECT count(*) FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE (c.state IN ('running','reconciling') OR (c.state='queued' AND o.state IN ('sending','outcome_unknown')) OR (c.state='queued' AND o.state='queued' AND NOT json_valid(c.spec_json)))",
        [],
        |row| row.get(0),
    )?)
}

enum PendingSubjectOutcome {
    Applied(Box<Work>),
    AlreadyQuarantined(Box<PendingQuarantineEvidence>),
    Quarantined {
        identity: Box<PendingSubjectIdentity>,
        field: &'static str,
        reason: SubjectDamageReason,
    },
}

fn valid_pending_text(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_PENDING_TEXT_BYTES
        && !value.chars().any(char::is_control)
}

fn valid_pending_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_pending_binding(id: &Option<String>, generation: Option<i64>) -> bool {
    match (id.as_deref(), generation) {
        (None, None) => true,
        (Some(id), Some(generation)) => valid_pending_text(id) && generation > 0,
        _ => false,
    }
}

fn valid_pending_identity(identity: &PendingSubjectIdentity) -> bool {
    let has_valid_start_operation = identity
        .attempt_start_operation_id
        .0
        .as_ref()
        .is_none_or(|value| valid_pending_text(value));
    valid_pending_text(&identity.check_id)
        && valid_pending_text(&identity.operation_id)
        && valid_pending_text(&identity.check_attempt_id)
        && identity.check_attempt_id == identity.attempt_id
        && valid_pending_text(&identity.candidate_ref)
        && valid_pending_text(&identity.cache_key)
        && valid_pending_text(&identity.resource_key)
        && identity.check_created_at_ms >= 0
        && valid_pending_digest(&identity.check_spec_sha256)
        && valid_pending_text(&identity.operation_caller_id)
        && valid_pending_text(&identity.operation_client_request_id)
        && identity.operation_method == "check.run"
        && identity.operation_task_id.0.as_deref() == Some(identity.task_id.as_str())
        && identity.operation_attempt_id.0.as_deref() == Some(identity.attempt_id.as_str())
        && valid_pending_digest(&identity.operation_original_request_sha256)
        && valid_pending_digest(&identity.operation_effective_request_sha256)
        && valid_pending_binding(
            &identity.operation_binding_id.0,
            identity.operation_binding_generation.0,
        )
        && valid_pending_text(&identity.attempt_id)
        && valid_pending_text(&identity.attempt_task_id)
        && identity.attempt_task_id == identity.task_id
        && identity.attempt_task_revision > 0
        && valid_pending_digest(&identity.attempt_task_snapshot_sha256)
        && valid_pending_text(&identity.attempt_owner_id)
        && matches!(
            identity.attempt_start_owner.as_str(),
            "controller" | "native_manager"
        )
        && has_valid_start_operation
        && valid_pending_binding(
            &identity.attempt_binding_id.0,
            identity.attempt_binding_generation.0,
        )
        && valid_pending_text(&identity.task_id)
        && valid_pending_text(&identity.task_project_id)
}

fn read_pending_cursor(db: &Connection) -> Result<Option<String>> {
    let Some(value) = meta(db, PENDING_CURSOR_KEY)? else {
        return Ok(None);
    };
    let Some(cursor) = value.as_str() else {
        return Err(Error::new(
            "CHECK_PENDING_CURSOR_CORRUPT",
            "checks pending cursor is not a subject key",
        ));
    };
    if !valid_pending_text(cursor) || cursor.len() > 512 {
        return Err(Error::new(
            "CHECK_PENDING_CURSOR_CORRUPT",
            "checks pending cursor identity is invalid",
        ));
    }
    // The cursor is a closed ordering coordinate minted only by
    // model::new_id: canonical lowercase UUIDv4. Any other retained value
    // would silently skip the live set, so it is a hard integrity error and
    // no Work is processed and no metadata is written.
    if !valid_pending_cursor_id(cursor) {
        return Err(Error::new(
            "CHECK_PENDING_CURSOR_CORRUPT",
            "checks pending cursor is not a canonical CheckRun ordering identity",
        ));
    }
    Ok(Some(cursor.to_owned()))
}

fn pending_ids_after(
    tx: &Transaction<'_>,
    cursor: Option<&str>,
    cut: Option<&str>,
    limit: usize,
) -> Result<Vec<String>> {
    let mut statement = tx.prepare(
        "SELECT c.check_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE (c.state IN ('running','reconciling') OR (c.state='queued' AND o.state IN ('sending','outcome_unknown')) OR (c.state='queued' AND o.state='queued' AND NOT json_valid(c.spec_json))) AND (?1 IS NULL OR c.check_id>?1) AND (?2 IS NULL OR c.check_id<=?2) AND NOT EXISTS (SELECT 1 FROM meta AS held WHERE held.key='checks:held:v1:' || c.check_id) ORDER BY c.check_id LIMIT ?3",
    )?;
    let ids = statement
        .query_map(params![cursor, cut, limit as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}

fn pending_ids_through(tx: &Transaction<'_>, cursor: &str, limit: usize) -> Result<Vec<String>> {
    let mut statement = tx.prepare(
        "SELECT c.check_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE (c.state IN ('running','reconciling') OR (c.state='queued' AND o.state IN ('sending','outcome_unknown')) OR (c.state='queued' AND o.state='queued' AND NOT json_valid(c.spec_json))) AND c.check_id<=?1 AND NOT EXISTS (SELECT 1 FROM meta AS held WHERE held.key='checks:held:v1:' || c.check_id) ORDER BY c.check_id LIMIT ?2",
    )?;
    let ids = statement
        .query_map(params![cursor, limit as i64], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}

/// Bounded pending selection for one pass over the retained cycle.
///
/// Reads only: the captured cut is returned in memory and persisted only
/// together with a subject cursor after a stable subject disposition, so a
/// crash or failure before the first completed subject leaves both the prior
/// cursor and the prior cut untouched. Identifiers admitted past the cut stay
/// out of the active page until turnover. Turnover (clearing the cut and
/// capturing a fresh maximum) happens at most once per call and only when the
/// suffix and the wrap prefix are both empty while subjects remain, so
/// post-cut admissions alone can neither skip nor duplicate pre-cut subjects.
fn select_pending_page(
    tx: &Transaction<'_>,
    cursor: Option<&str>,
) -> Result<(Vec<String>, Option<PendingCycle>)> {
    let retained = read_pending_cycle(tx)?;
    let mut cycle = retained;
    if cycle.is_none()
        && let Some(max) = max_pending_id(tx)?
    {
        if !valid_pending_cursor_id(&max) {
            return Err(Error::new(
                "CHECK_PENDING_CYCLE_CORRUPT",
                "live pending CheckRun ordering identity is not canonical",
            ));
        }
        cycle = Some(PendingCycle {
            schema_version: PENDING_CYCLE_SCHEMA,
            cut: max,
        });
    }
    let Some(active) = cycle.clone() else {
        return Ok((Vec::new(), None));
    };
    let suffix_limit = if cursor.is_none() {
        MAX_PENDING_PAGE
    } else {
        MAX_PENDING_SUFFIX
    };
    // The wrap prefix is reserved on every pass: even while the suffix
    // stays full under continuous admissions, the failed older subject
    // before the cursor is revisited within a deterministic bounded number
    // of passes. Both ranges stay within the cut; the wrap range needs no
    // explicit cut filter because the cut always names the maximum pending
    // identifier, so every still-pending wrapped row is at or below it.
    let mut ids = pending_ids_after(tx, cursor, Some(active.cut.as_str()), suffix_limit)?;
    if ids.len() < MAX_PENDING_PAGE
        && let Some(cursor) = cursor
    {
        let wrap_limit = (MAX_PENDING_PAGE - ids.len()).max(MAX_PENDING_WRAP);
        ids.extend(pending_ids_through(tx, cursor, wrap_limit)?);
    }
    if ids.is_empty() && pending_subject_count(tx)? > 0 {
        tx.execute("DELETE FROM meta WHERE key=?1", [PENDING_CYCLE_KEY])?;
        if let Some(max) = max_pending_id(tx)? {
            if !valid_pending_cursor_id(&max) {
                return Err(Error::new(
                    "CHECK_PENDING_CYCLE_CORRUPT",
                    "live pending CheckRun ordering identity is not canonical",
                ));
            }
            let fresh = PendingCycle {
                schema_version: PENDING_CYCLE_SCHEMA,
                cut: max,
            };
            let mut fresh_ids =
                pending_ids_after(tx, cursor, Some(fresh.cut.as_str()), suffix_limit)?;
            if fresh_ids.len() < MAX_PENDING_PAGE
                && let Some(cursor) = cursor
            {
                let wrap_limit = (MAX_PENDING_PAGE - fresh_ids.len()).max(MAX_PENDING_WRAP);
                fresh_ids.extend(pending_ids_through(tx, cursor, wrap_limit)?);
            }
            return Ok((fresh_ids, Some(fresh)));
        }
        return Ok((Vec::new(), None));
    }
    Ok((ids, cycle))
}

fn read_subject_identity(db: &Connection, check_id: &str) -> Result<PendingSubjectIdentity> {
    let identity: Option<PendingSubjectIdentity> = db
        .query_row(
            "SELECT c.check_id,c.operation_id,c.attempt_id,c.candidate_ref,c.cache_key,c.resource_key,c.created_at_ms,c.spec_json,o.caller_id,o.client_request_id,o.method,o.original_request_json,o.effective_request_json,o.task_id,o.attempt_id,o.binding_id,o.binding_generation,a.attempt_id,a.task_id,a.task_revision,a.task_snapshot_json,a.owner_id,a.start_owner,a.start_operation_id,a.binding_id,a.binding_generation,t.task_id,t.project_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id JOIN attempts a ON a.attempt_id=c.attempt_id JOIN tasks t ON t.task_id=a.task_id WHERE c.check_id=?1",
            [check_id],
            |row| {
                let check_spec: String = row.get(7)?;
                let operation_request: String = row.get(11)?;
                let operation_effective_request: String = row.get(12)?;
                let attempt_snapshot: String = row.get(20)?;
                Ok(PendingSubjectIdentity {
                    check_id: row.get(0)?,
                    operation_id: row.get(1)?,
                    check_attempt_id: row.get(2)?,
                    candidate_ref: row.get(3)?,
                    cache_key: row.get(4)?,
                    resource_key: row.get(5)?,
                    check_created_at_ms: row.get(6)?,
                    check_spec_sha256: model::digest(check_spec.as_bytes()),
                    operation_caller_id: row.get(8)?,
                    operation_client_request_id: row.get(9)?,
                    operation_method: row.get(10)?,
                    operation_original_request_sha256: model::digest(operation_request.as_bytes()),
                    operation_effective_request_sha256: model::digest(
                        operation_effective_request.as_bytes(),
                    ),
                    operation_task_id: PresentOption(row.get(13)?),
                    operation_attempt_id: PresentOption(row.get(14)?),
                    operation_binding_id: PresentOption(row.get(15)?),
                    operation_binding_generation: PresentOption(row.get(16)?),
                    attempt_id: row.get(17)?,
                    attempt_task_id: row.get(18)?,
                    attempt_task_revision: row.get(19)?,
                    attempt_task_snapshot_sha256: model::digest(attempt_snapshot.as_bytes()),
                    attempt_owner_id: row.get(21)?,
                    attempt_start_owner: row.get(22)?,
                    attempt_start_operation_id: PresentOption(row.get(23)?),
                    attempt_binding_id: PresentOption(row.get(24)?),
                    attempt_binding_generation: PresentOption(row.get(25)?),
                    task_id: row.get(26)?,
                    task_project_id: row.get(27)?,
                })
            },
        )
        .optional()?;
    let identity = identity.ok_or_else(|| {
        Error::new(
            "CHECK_PENDING_SUBJECT_MISSING",
            "selected CheckRun has no complete Operation, Attempt, and Task lineage",
        )
    })?;
    if !valid_pending_identity(&identity) {
        return Err(Error::new(
            "CHECK_PENDING_LINEAGE_CORRUPT",
            "retained CheckRun, Operation, Attempt, or Task lineage is inconsistent",
        ));
    }
    Ok(identity)
}

fn quarantine_key(check_id: &str) -> String {
    format!(
        "{PENDING_QUARANTINE_PREFIX}{}",
        model::digest(check_id.as_bytes())
    )
}

/// Activation may ignore only an intact quarantine of this exact lineage.
/// A changed source still reaches the pending reader's normal conflict path.
pub(super) fn pending_source_is_quarantined(db: &Connection, check_id: &str) -> Result<bool> {
    let Some(record) = load_quarantine_record(db, check_id)? else {
        return Ok(false);
    };
    Ok(record.identity == read_subject_identity(db, check_id)?)
}
const HELD_PREFIX: &str = "checks:held:v1:";
const HELD_SCHEMA: u32 = 1;
fn held_key(check_id: &str) -> String {
    format!("{HELD_PREFIX}{check_id}")
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeldRecord {
    schema_version: u32,
    check_id: String,
    quarantine_code: String,
    quarantine_field: String,
    quarantine_reason: String,
    source_digest: String,
    acknowledged: bool,
    acknowledged_by: Option<String>,
    acknowledged_at_ms: Option<i64>,
    ack_operation_id: Option<String>,
}
fn valid_held_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn validate_held_record(record: &HeldRecord, check_id: &str) -> Result<()> {
    let acknowledged_context = record.acknowledged_by.is_some()
        && record.acknowledged_at_ms.is_some_and(|at| at >= 0)
        && record.ack_operation_id.is_some();
    if record.schema_version != HELD_SCHEMA
        || record.check_id != check_id
        || record.quarantine_code != "CHECK_SPEC_SUBJECT_CORRUPT"
        || !valid_pending_damage(&record.quarantine_field, &record.quarantine_reason)
        || !valid_held_digest(&record.source_digest)
        || record.acknowledged != acknowledged_context
    {
        return Err(Error::new(
            "CHECK_HELD_RECORD_CORRUPT",
            "retained held inventory record has an invalid identity or shape",
        ));
    }
    Ok(())
}

fn held_record_for_quarantine(quarantine: &PendingQuarantineEvidence) -> HeldRecord {
    HeldRecord {
        schema_version: HELD_SCHEMA,
        check_id: quarantine.identity.check_id.clone(),
        quarantine_code: quarantine.code.clone(),
        quarantine_field: quarantine.field.clone(),
        quarantine_reason: quarantine.reason.clone(),
        source_digest: quarantine.identity.check_spec_sha256.clone(),
        acknowledged: false,
        acknowledged_by: None,
        acknowledged_at_ms: None,
        ack_operation_id: None,
    }
}

fn load_held_record(db: &Connection, check_id: &str) -> Result<Option<HeldRecord>> {
    let Some(value) = meta(db, &held_key(check_id))? else {
        return Ok(None);
    };
    let record: HeldRecord = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "CHECK_HELD_RECORD_CORRUPT",
            "retained held inventory record cannot be decoded",
        )
    })?;
    validate_held_record(&record, check_id)?;
    Ok(Some(record))
}

fn validate_held_matches_quarantine(
    held: &HeldRecord,
    quarantine: &PendingQuarantineEvidence,
) -> Result<()> {
    if held.check_id != quarantine.identity.check_id
        || held.quarantine_code != quarantine.code
        || held.quarantine_field != quarantine.field
        || held.quarantine_reason != quarantine.reason
        || held.source_digest != quarantine.identity.check_spec_sha256
    {
        return Err(Error::new(
            "CHECK_HELD_RECORD_CORRUPT",
            "retained held inventory record differs from its quarantine evidence",
        ));
    }
    Ok(())
}

fn valid_pending_damage(field: &str, reason: &str) -> bool {
    matches!(
        (field, reason),
        ("token" | "candidate" | "profile", "missing_required_field")
            | (
                "spec" | "token" | "candidate" | "profile" | "executor" | "cancel_request",
                "invalid_subject_shape",
            )
            | ("spec", "invalid_json")
    )
}

fn validate_pending_quarantine(record: &PendingQuarantineEvidence, check_id: &str) -> Result<()> {
    if record.schema_version != PENDING_QUARANTINE_SCHEMA
        || record.identity.check_id != check_id
        || !valid_pending_identity(&record.identity)
        || record.code != "CHECK_SPEC_SUBJECT_CORRUPT"
        || !valid_pending_damage(&record.field, &record.reason)
        || record.first_seen_at_ms < 0
        || record.last_seen_at_ms < record.first_seen_at_ms
        || record.occurrences == 0
    {
        return Err(Error::new(
            "CHECK_PENDING_QUARANTINE_CORRUPT",
            "retained pending quarantine evidence has an invalid identity or shape",
        ));
    }
    Ok(())
}

fn read_pending_quarantine(
    tx: &Transaction<'_>,
    check_id: &str,
    identity: &PendingSubjectIdentity,
) -> Result<Option<PendingQuarantineEvidence>> {
    let Some(value) = meta(tx, &quarantine_key(check_id))? else {
        return Ok(None);
    };
    if model::canonical(&value)?.len() > MAX_PENDING_QUARANTINE_BYTES {
        return Err(Error::new(
            "CHECK_PENDING_QUARANTINE_CORRUPT",
            "retained pending quarantine evidence exceeds its bounded size",
        ));
    }
    let record: PendingQuarantineEvidence = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "CHECK_PENDING_QUARANTINE_CORRUPT",
            "retained pending quarantine evidence cannot be decoded",
        )
    })?;
    validate_pending_quarantine(&record, check_id)?;
    if &record.identity != identity {
        return Err(Error::new(
            "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT",
            "quarantine metadata no longer matches the exact retained lineage",
        ));
    }
    Ok(Some(record))
}

/// Bounded resumable audit of indexed held rows before the selector excludes
/// them. Each call validates at most MAX_PENDING_PAGE rows after the retained
/// audit cursor, wrapping to the lexical head only when the suffix is empty.
/// The cursor advances to the last actually validated row, so a 17+ held
/// prefix with an unchanged head eventually examines the changed tail and
/// hard-fails, without ever scanning a full cycle in one call. Quarantine
/// evidence itself is never rewritten; an empty held set stays read-only.
fn read_held_audit_cursor(db: &Connection) -> Result<Option<String>> {
    let Some(value) = meta(db, HELD_AUDIT_CURSOR_KEY)? else {
        return Ok(None);
    };
    let Some(cursor) = value.as_str() else {
        return Err(Error::new(
            "CHECK_HELD_AUDIT_CURSOR_CORRUPT",
            "held audit cursor is not a subject key",
        ));
    };
    if !valid_pending_text(cursor) || cursor.len() > 512 {
        return Err(Error::new(
            "CHECK_HELD_AUDIT_CURSOR_CORRUPT",
            "held audit cursor identity is invalid",
        ));
    };
    if !valid_pending_cursor_id(cursor) {
        return Err(Error::new(
            "CHECK_HELD_AUDIT_CURSOR_CORRUPT",
            "held audit cursor is not a canonical CheckRun ordering identity",
        ));
    };
    Ok(Some(cursor.to_owned()))
}
fn validate_bounded_held_quarantines(tx: &Transaction<'_>) -> Result<bool> {
    let cursor = read_held_audit_cursor(tx)?;
    let mut statement = tx.prepare(
        "SELECT c.check_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE (c.state IN ('running','reconciling') OR (c.state='queued' AND o.state IN ('sending','outcome_unknown')) OR (c.state='queued' AND o.state='queued' AND NOT json_valid(c.spec_json))) AND EXISTS (SELECT 1 FROM meta AS held WHERE held.key='checks:held:v1:' || c.check_id) AND (?1 IS NULL OR c.check_id>?1) ORDER BY c.check_id LIMIT ?2",
    )?;
    let mut batch = statement
        .query_map(params![cursor.as_deref(), MAX_PENDING_PAGE as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    if batch.is_empty() && cursor.is_some() {
        let mut wrap = tx.prepare(
            "SELECT c.check_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE (c.state IN ('running','reconciling') OR (c.state='queued' AND o.state IN ('sending','outcome_unknown')) OR (c.state='queued' AND o.state='queued' AND NOT json_valid(c.spec_json))) AND EXISTS (SELECT 1 FROM meta AS held WHERE held.key='checks:held:v1:' || c.check_id) ORDER BY c.check_id LIMIT ?1",
        )?;
        batch = wrap
            .query_map(params![MAX_PENDING_PAGE as i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(wrap);
        if batch.is_empty() {
            let deleted = tx.execute("DELETE FROM meta WHERE key=?1", [HELD_AUDIT_CURSOR_KEY])?;
            return Ok(deleted != 0);
        }
    }
    if batch.is_empty() {
        return Ok(false);
    }
    for check_id in &batch {
        let identity = read_subject_identity(tx, check_id)?;
        let quarantine = read_pending_quarantine(tx, check_id, &identity)?.ok_or_else(|| {
            Error::new(
                "CHECK_HELD_RECORD_CORRUPT",
                "held inventory record has no retained quarantine evidence",
            )
        })?;
        let held = load_held_record(tx, check_id)?.ok_or_else(|| {
            Error::new(
                "CHECK_HELD_RECORD_CORRUPT",
                "held inventory record disappeared during validation",
            )
        })?;
        validate_held_matches_quarantine(&held, &quarantine)?;
    }
    if let Some(last) = batch.last()
        && cursor.as_deref() != Some(last.as_str())
    {
        set_meta(tx, HELD_AUDIT_CURSOR_KEY, &json!(last))?;
        return Ok(true);
    }
    Ok(false)
}

fn decode_pending_row(
    identity: &PendingSubjectIdentity,
    row: WorkRow,
    root: PathBuf,
) -> Result<SubjectMaterialization> {
    if row.operation_id != identity.operation_id
        || model::digest(row.spec_json.as_bytes()) != identity.check_spec_sha256
    {
        return Err(Error::new(
            "CHECK_PENDING_LINEAGE_CHANGED",
            "CheckRun identity changed during one pending transaction",
        ));
    }
    decode_subject_fields(
        &identity.check_id,
        row.operation_id,
        row.spec_json,
        row.process_identity_json,
        root,
    )
}

fn rollback_pending_subject(tx: &Transaction<'_>) -> Result<()> {
    if let Err(rollback_error) = tx.execute_batch(&format!(
        "ROLLBACK TO SAVEPOINT {PENDING_SUBJECT_SAVEPOINT}; RELEASE SAVEPOINT {PENDING_SUBJECT_SAVEPOINT}"
    )) {
        let error = Error::new(
            "CHECK_PENDING_SUBJECT_SAVEPOINT_UNCERTAIN",
            "pending subject savepoint could not be rolled back and released",
        )
        .with_secondary_error(rollback_error.into());
        return Err(error);
    }
    Ok(())
}

fn materialize_pending_subject(
    tx: &Transaction<'_>,
    check_id: &str,
    root: PathBuf,
) -> Result<PendingSubjectOutcome> {
    tx.execute_batch(&format!("SAVEPOINT {PENDING_SUBJECT_SAVEPOINT}"))?;
    let outcome = (|| -> Result<PendingSubjectOutcome> {
        let identity = read_subject_identity(tx, check_id)?;
        let row = read_work_row(tx, check_id)?.ok_or_else(|| {
            Error::new(
                "CHECK_PENDING_SUBJECT_MISSING",
                "selected CheckRun disappeared before its typed read",
            )
        })?;
        let materialized = decode_pending_row(&identity, row, root)?;
        if let Some(record) = read_pending_quarantine(tx, check_id, &identity)? {
            match materialized {
                SubjectMaterialization::Damaged { field, reason }
                    if record.field == field && record.reason == reason.name() =>
                {
                    // Unchanged deterministic damage stays stable across polls and
                    // restarts: no occurrence/last_seen rewrite and no writer churn.
                    // A changed digest or classification remains a hard conflict.
                    return Ok(PendingSubjectOutcome::AlreadyQuarantined(Box::new(record)));
                }
                SubjectMaterialization::Decoded(_) | SubjectMaterialization::Damaged { .. } => {
                    return Err(Error::new(
                        "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT",
                        "retained quarantine classification no longer matches the CheckRun spec",
                    ));
                }
            }
        }
        match materialized {
            SubjectMaterialization::Decoded(work) => Ok(PendingSubjectOutcome::Applied(work)),
            SubjectMaterialization::Damaged { field, reason } => {
                Ok(PendingSubjectOutcome::Quarantined {
                    identity: Box::new(identity),
                    field,
                    reason,
                })
            }
        }
    })();

    match outcome {
        Ok(PendingSubjectOutcome::Applied(work)) => {
            tx.execute_batch(&format!("RELEASE SAVEPOINT {PENDING_SUBJECT_SAVEPOINT}"))?;
            Ok(PendingSubjectOutcome::Applied(work))
        }
        Ok(already @ PendingSubjectOutcome::AlreadyQuarantined(_)) => {
            tx.execute_batch(&format!("RELEASE SAVEPOINT {PENDING_SUBJECT_SAVEPOINT}"))?;
            Ok(already)
        }
        Ok(quarantined @ PendingSubjectOutcome::Quarantined { .. }) => {
            rollback_pending_subject(tx)?;
            Ok(quarantined)
        }
        Err(primary) => match rollback_pending_subject(tx) {
            Ok(()) => Err(primary),
            Err(rollback) => Err(rollback.with_secondary_error(primary)),
        },
    }
}

fn retain_pending_quarantine(
    tx: &Transaction<'_>,
    identity: PendingSubjectIdentity,
    field: &'static str,
    reason: SubjectDamageReason,
    now_ms: i64,
) -> Result<()> {
    let record = PendingQuarantineEvidence {
        schema_version: PENDING_QUARANTINE_SCHEMA,
        identity,
        code: "CHECK_SPEC_SUBJECT_CORRUPT".to_owned(),
        field: field.to_owned(),
        reason: reason.name().to_owned(),
        first_seen_at_ms: now_ms,
        last_seen_at_ms: now_ms,
        occurrences: 1,
    };
    validate_pending_quarantine(&record, &record.identity.check_id)?;
    let value = json!(&record);
    if model::canonical(&value)?.len() > MAX_PENDING_QUARANTINE_BYTES {
        return Err(Error::new(
            "CHECK_PENDING_QUARANTINE_INVALID",
            "pending quarantine evidence exceeds its bounded size",
        ));
    }
    set_meta(tx, &quarantine_key(&record.identity.check_id), &value)?;
    // The held inventory index is derived from the same immutable evidence
    // so the unchanged poison leaves the generic active page without a
    // second scheduler or queue table. It starts unacknowledged; only the
    // explicit acknowledgement flips it, never the quarantine itself.
    let held = held_record_for_quarantine(&record);
    validate_held_record(&held, &record.identity.check_id)?;
    set_meta(tx, &held_key(&record.identity.check_id), &json!(&held))?;
    Ok(())
}

/// Durable pending CheckRun selection with completed-subject cursor motion.
///
/// Selection itself never advances the subject cursor past healthy Work:
/// healthy subjects advance only after their stable disposition is durable,
/// through retain_pending_progress in the startup reconciliation and
/// supervisor seams. Deterministic damage retains its exact quarantine
/// evidence; the cursor advances through a page only when the page carries no
/// healthy Work, so unexamined Work is never marked visited.
struct PendingSelection {
    work: Vec<Work>,
    cycle: Option<PendingCycle>,
}

fn finish_pending_selection(
    tx: Transaction<'_>,
    selection: PendingSelection,
    held_audit_written: bool,
) -> Result<PendingSelection> {
    // A successful bounded held audit is independent of active Work progress.
    // Preserve its cursor even when the active selector otherwise stays read-only.
    if held_audit_written {
        tx.commit()?;
    }
    Ok(selection)
}

fn pending_selection(db: &mut Connection, root: PathBuf) -> Result<PendingSelection> {
    // Deferred snapshot: empty scans and unchanged stable pages stay read-only
    // and never reserve the SQLite writer. Stale-cursor cleanup and new
    // quarantine dispositions upgrade the same transaction atomically.
    let tx = db.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let cursor = read_pending_cursor(&tx)?;
    let held_audit_written = validate_bounded_held_quarantines(&tx)?;
    let (ids, cycle) = select_pending_page(&tx, cursor.as_deref())?;
    let mut work = Vec::with_capacity(ids.len());
    let mut last_id = None;
    let mut pending_metadata_written = false;
    for check_id in ids {
        last_id = Some(check_id.clone());
        match materialize_pending_subject(&tx, &check_id, root.clone())? {
            PendingSubjectOutcome::Applied(item) => work.push(*item),
            PendingSubjectOutcome::AlreadyQuarantined(quarantine) => {
                match load_held_record(&tx, &check_id)? {
                    Some(held) => validate_held_matches_quarantine(&held, &quarantine)?,
                    None => {
                        let held = held_record_for_quarantine(&quarantine);
                        validate_held_record(&held, &check_id)?;
                        set_meta(&tx, &held_key(&check_id), &json!(&held))?;
                        pending_metadata_written = true;
                    }
                }
            }
            PendingSubjectOutcome::Quarantined {
                identity,
                field,
                reason,
            } => {
                retain_pending_quarantine(&tx, *identity, field, reason, model::now_ms()?)?;
                pending_metadata_written = true;
            }
        }
    }
    let selection = PendingSelection { work, cycle };
    if selection.work.is_empty() {
        // Pages without healthy Work advance the cursor through their stable
        // subjects (new or already-retained quarantine) atomically with the
        // evidence, or stay read-only when nothing changed.
        if let Some(check_id) = last_id {
            let cursor_unchanged = cursor.as_deref() == Some(check_id.as_str());
            if pending_metadata_written || !cursor_unchanged {
                set_meta(&tx, PENDING_CURSOR_KEY, &json!(check_id))?;
                match &selection.cycle {
                    Some(snapshot) if snapshot.cut.as_str() == check_id.as_str() => {
                        // Reaching the cut closes the cycle exactly at the
                        // completed subject: the next selection captures a
                        // fresh cut.
                        tx.execute("DELETE FROM meta WHERE key=?1", [PENDING_CYCLE_KEY])?;
                    }
                    Some(snapshot) => {
                        set_meta(&tx, PENDING_CYCLE_KEY, &json!(snapshot))?;
                    }
                    None => {}
                }
            } else {
                return finish_pending_selection(tx, selection, held_audit_written);
            }
        } else if pending_subject_count(&tx)? == 0 {
            if cursor.is_none() && read_pending_cycle(&tx)?.is_none() {
                // Empty scan with no retained cursor or cycle: read-only, no
                // meta mutation.
                return finish_pending_selection(tx, selection, held_audit_written);
            }
            // Stale cursor or cycle with no subjects: delete once; the next
            // empty scan is read-only.
            tx.execute("DELETE FROM meta WHERE key=?1", [PENDING_CURSOR_KEY])?;
            tx.execute("DELETE FROM meta WHERE key=?1", [PENDING_CYCLE_KEY])?;
        } else {
            // Subjects remain but none are selected (only post-cut admissions
            // remain): keep the cursor and cycle untouched and read-only.
            return finish_pending_selection(tx, selection, held_audit_written);
        }
    } else if pending_metadata_written {
        // New quarantine evidence commits even though healthy Work is also
        // returned. The subject cursor and cycle cut stay untouched here:
        // unexamined healthy Work is never marked visited.
    } else {
        // Healthy page with no new disposition: read-only.
        return finish_pending_selection(tx, selection, held_audit_written);
    }
    tx.commit()?;
    Ok(selection)
}

#[cfg(test)]
fn pending(db: &mut Connection, root: PathBuf) -> Result<Vec<Work>> {
    pending_selection(db, root).map(|selection| selection.work)
}

/// Persist the subject cursor (and its cycle snapshot) after one subject
/// reached a stable, truthfully retained disposition: terminal finish,
/// accepted ready, examined no-op, retained incident, or exact quarantine.
/// The disposition itself is already durable in its own transaction; this
/// write only moves the ordering coordinate, so a cursor-write failure never
/// erases CheckRun truth and retry converges by exact readback of the same
/// subject. Reaching the cut closes the cycle exactly at the completed
/// subject.
fn retain_pending_progress(
    db: &mut Connection,
    check_id: &str,
    cycle: &Option<PendingCycle>,
) -> Result<()> {
    if !valid_pending_cursor_id(check_id) {
        return Err(Error::new(
            "CHECK_PENDING_CURSOR_CORRUPT",
            "completed CheckRun ordering identity is not canonical",
        ));
    }
    if let Some(snapshot) = cycle
        && (snapshot.schema_version != PENDING_CYCLE_SCHEMA
            || !valid_pending_cursor_id(&snapshot.cut))
    {
        return Err(Error::new(
            "CHECK_PENDING_CYCLE_CORRUPT",
            "pending cycle snapshot is not a canonical CheckRun ordering identity",
        ));
    }
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    set_meta(&tx, PENDING_CURSOR_KEY, &json!(check_id))?;
    match cycle {
        Some(snapshot) if snapshot.cut.as_str() == check_id => {
            tx.execute("DELETE FROM meta WHERE key=?1", [PENDING_CYCLE_KEY])?;
        }
        Some(snapshot) => {
            set_meta(&tx, PENDING_CYCLE_KEY, &json!(snapshot))?;
        }
        None => {}
    }
    tx.commit()?;
    Ok(())
}

fn ready(db: &mut Connection, w: &Work, identity: Value) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let status = describe(&tx, &json!({"check_id":w.check_id}))?;
    if !matches!(status["state"].as_str(), Some("running" | "reconciling")) {
        return Ok(false);
    }
    let cleanup_retained: bool = tx.query_row(
        "SELECT json_type(spec_json,'$.cleanup_pending') IS NOT NULL FROM check_runs WHERE check_id=?1",
        [&w.check_id],
        |row| row.get(0),
    )?;
    if cleanup_retained {
        // Execution already produced a retained completion. A ready receipt
        // cannot authorize a second go or regress it back to running.
        return Ok(false);
    }
    if !status["process"].is_null() && status["process"] != identity {
        return Err(Error::new(
            "CHECK_OWNER_CHANGED",
            "worker identity cannot be replaced",
        ));
    }
    if status["state"] == "running" && status["process"] == identity {
        let op = operations::get_operation(&tx, &w.operation_id)?;
        if op["state"] == "native_accepted" {
            return Ok(true);
        }
    }
    tx.execute(
        "UPDATE check_runs SET state='running',process_identity_json=?2 WHERE check_id=?1",
        params![w.check_id, model::canonical(&identity)?],
    )?;
    tx.execute("UPDATE operations SET state='native_accepted',updated_at_ms=?2 WHERE operation_id=?1 AND state IN ('sending','outcome_unknown')",params![w.operation_id,model::now_ms()?])?;
    tx.commit()?;
    Ok(true)
}
fn resolve_process_diagnostic(tx: &Transaction<'_>, work: &Work, now: i64) -> Result<()> {
    let row: Option<(String, String, Option<String>)> = tx
        .query_row(
            "SELECT operation_id,spec_json,process_identity_json FROM check_runs WHERE check_id=?1",
            [&work.check_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((operation_id, spec_json, identity_json)) = row else {
        return Err(Error::conflict("CheckRun disappeared before finish"));
    };
    if operation_id != work.operation_id {
        return Err(Error::conflict("CheckRun operation changed before finish"));
    }
    let spec: Value = serde_json::from_str(&spec_json)?;
    let Some(diagnostic) = spec.get("process_diagnostic") else {
        return Ok(());
    };
    let Some(expected_worker) = work.expected_worker.as_ref() else {
        return Err(Error::conflict(
            "process diagnostic cannot resolve without an accepted process owner",
        ));
    };
    let Some(identity_json) = identity_json else {
        return Err(Error::conflict(
            "process diagnostic cannot resolve without current process identity",
        ));
    };
    let identity: Value = serde_json::from_str(&identity_json)?;
    if &identity != expected_worker
        || identity["token"] != work.token
        || spec.get("token").and_then(Value::as_str) != Some(work.token.as_str())
        || diagnostic["version"] != 1
        || diagnostic["check_id"] != work.check_id
        || diagnostic["operation_id"] != work.operation_id
        || diagnostic["token"] != work.token
        || diagnostic["process"] != identity["process"]
    {
        return Err(Error::conflict(
            "process diagnostic does not match the current exact process proof",
        ));
    }
    match diagnostic["public"]["status"].as_str() {
        Some("unresolved") => {
            tx.execute(
                "UPDATE check_runs SET spec_json=json_set(spec_json,'$.process_diagnostic.public.status','resolved','$.process_diagnostic.public.resolved_at_ms',?2) WHERE check_id=?1 AND process_identity_json=?3",
                params![work.check_id, now, identity_json],
            )?;
        }
        Some("resolved") => {}
        _ => return Err(Error::conflict("process diagnostic status is invalid")),
    }
    Ok(())
}

fn finish(db: &mut Connection, w: &Work, c: Completion) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let status = describe(&tx, &json!({"check_id":w.check_id}))?;
    if !matches!(status["state"].as_str(), Some("running" | "reconciling"))
        && !(status["state"] == "queued" && w.preflight_error.is_some())
    {
        return Ok(());
    }
    if c.check_id != w.check_id || c.operation_id != w.operation_id || c.token != w.token {
        return Err(Error::conflict("check completion mismatched"));
    }
    artifact(&tx, &c.result)?;
    for out in &c.outputs {
        artifact(&tx, out)?;
    }
    let now = model::now_ms()?;
    if !c.resource_released {
        // Capture is useful evidence even when process-family cleanup is still
        // unknown. Retain it without publishing a completed check or releasing
        // the exact resource claim. Reconciliation reads this same execution;
        // it must never relaunch the command.
        let retained = model::canonical(&json!(c))?;
        let previous: Option<String> = tx.query_row(
            "SELECT json_extract(spec_json,'$.cleanup_pending') FROM check_runs WHERE check_id=?1",
            [&w.check_id],
            |row| row.get(0),
        )?;
        if previous.as_deref() == Some(retained.as_str()) {
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "UPDATE check_runs SET state='reconciling',exit_code=?2,result_ref=?3,coverage_json=?4,spec_json=json_set(spec_json,'$.cleanup_pending',json(?5),'$.process_facts',json(?6),'$.completion_resource_released',json('false')) WHERE check_id=?1 AND resource_released_at_ms IS NULL",
            params![w.check_id,c.exit_code,c.result.artifact_id,model::canonical(&c.coverage)?,retained,model::canonical(&json!(c.process_facts))?],
        )?;
        let report = json!({"operation_id":w.operation_id,"check_id":w.check_id,
            "state":"reconciling","outcome":"unknown","resource_released":false,
            "cleanup_pending":true,"result_ref":c.result.artifact_id,
            "process_facts":c.process_facts,
            "output_refs":c.outputs.iter().map(|output| &output.artifact_id).collect::<Vec<_>>(),
            "task_accepted":false});
        tx.execute(
            "UPDATE operations SET state='outcome_unknown',result_json=?2,settled_at_ms=NULL,updated_at_ms=?3 WHERE operation_id=?1",
            params![w.operation_id,model::canonical(&report)?,now],
        )?;
        incident(
            &tx,
            &format!("check:{}:CHECK_CLEANUP_PENDING", w.check_id),
            Error::new(
                "CHECK_CLEANUP_PENDING",
                "exact process-family departure is not confirmed; captured output is retained and the resource remains held",
            ),
        )?;
        tx.commit()?;
        return Ok(());
    }
    resolve_process_diagnostic(&tx, w, now)?;
    tx.execute("UPDATE check_runs SET spec_json=json_remove(json_set(spec_json,'$.cancellation',json(?2),'$.cleanup_history',json_extract(spec_json,'$.cleanup_pending'),'$.process_facts',json(?3),'$.release_evidence',json(?4),'$.completion_resource_released',json('true')),'$.cleanup_pending') WHERE check_id=?1",params![w.check_id,model::canonical(&json!(c.cancellation))?,model::canonical(&json!(c.process_facts))?,model::canonical(&json!(c.release_evidence))?])?;
    tx.execute("UPDATE check_runs SET state=?2,resource_released_at_ms=CASE WHEN resource_claimed_at_ms IS NOT NULL THEN ?3 ELSE NULL END,finished_at_ms=?3,exit_code=?4,result_ref=?5,coverage_json=?6 WHERE check_id=?1",params![w.check_id,c.state,now,c.exit_code,c.result.artifact_id,model::canonical(&c.coverage)?])?;
    let owner:String=tx.query_row("SELECT a.owner_id FROM attempts a JOIN check_runs c ON c.attempt_id=a.attempt_id WHERE c.check_id=?1",[&w.check_id],|r|r.get(0))?;
    let report = json!({"operation_id":w.operation_id,"outcome":"applied","check_id":w.check_id,"state":c.state,"exit_code":c.exit_code,"result_ref":c.result.artifact_id,"recipient":owner,"source_checkout_verified":c.state=="passed","output_refs":c.outputs.iter().map(|o|&o.artifact_id).collect::<Vec<_>>(),"resource_released":true,"cleanup_pending":false,"process_facts":c.process_facts,"release_evidence":c.release_evidence,"task_accepted":false});
    settle(&tx, &w.operation_id, &report)?;
    tx.execute("UPDATE incidents SET state='resolved',last_seen_at_ms=?2 WHERE state='open' AND dedup_key LIKE ?1", params![format!("check:{}:%", w.check_id), now])?;
    tx.commit()?;
    Ok(())
}
fn incident(db: &Connection, key: &str, error: Error) -> Result<()> {
    // One durable incident, not a log line or model nudge on every scheduler tick.
    let now = model::now_ms()?;
    db.execute("INSERT INTO incidents(incident_id,dedup_key,state,occurrences,details_json,opened_at_ms,last_seen_at_ms) VALUES(?1,?2,'open',1,?3,?4,?4) ON CONFLICT(dedup_key) WHERE state='open' DO NOTHING",params![model::new_id(),key,model::canonical(&json!({"error":error}))?,now])?;
    Ok(())
}

#[derive(Clone, Copy)]
struct CheckChildExit {
    pid: u32,
    success: bool,
    code: Option<i32>,
}

struct CheckChildOwner {
    work: Work,
    child: Child,
    launch: Option<Value>,
    launch_error: Option<Error>,
    launch_persistence_pending: bool,
    exit: Option<CheckChildExit>,
    terminal_retained: bool,
}

#[derive(Clone, Default)]
pub(super) struct CheckChildCustody {
    owners: std::sync::Arc<tokio::sync::Mutex<BTreeMap<String, CheckChildOwner>>>,
    git_cleanup_owners: std::sync::Arc<tokio::sync::Mutex<Vec<GitCleanupOwner>>>,
    git_cleanup_operation: std::sync::Arc<tokio::sync::Mutex<()>>,
    probe_cleanup_owners: std::sync::Arc<tokio::sync::Mutex<Vec<ProbeCleanupOwner>>>,
    probe_cleanup_operation:
        std::sync::Arc<tokio::sync::Mutex<Vec<std::sync::Weak<ProbeResolutionReservation>>>>,
}

/// A live preflight uses the same short custody critical section as cleanup
/// installation. Weak entries disappear when both the async caller and its
/// blocking probe release the reservation; no asynchronous Drop is needed.
struct ProbeResolutionReservation {
    resource: String,
    scope: std::sync::OnceLock<inputs::ProbeCleanupScope>,
}

impl CheckChildCustody {
    async fn lock(&self) -> tokio::sync::MutexGuard<'_, BTreeMap<String, CheckChildOwner>> {
        self.owners.lock().await
    }

    pub(super) async fn retain_git_cleanup(&self, owner: GitCleanupOwner) {
        if owner.is_pending() {
            self.git_cleanup_owners.lock().await.push(owner);
        }
    }

    async fn reap_git_cleanup_owners(&self) {
        let mut owners = self.git_cleanup_owners.lock().await;
        owners.retain_mut(|owner| {
            owner.poll();
            owner.is_pending()
        });
    }

    fn retain_probe_cleanup(&self, mut owner: ProbeCleanupOwner) -> bool {
        let _operation = self.probe_cleanup_operation.blocking_lock();
        owner.poll();
        if !owner.is_pending() {
            return false;
        }
        let scope = owner.scope().cloned();
        let mut owners = self.probe_cleanup_owners.blocking_lock();
        owners.retain_mut(|retained| {
            retained.poll();
            retained.is_pending()
        });
        let conflict = owners
            .iter()
            .any(|retained| retained.conflicts_with(scope.as_ref()));
        owners.push(owner);
        conflict
    }

    async fn probe_cleanup_conflicts(&self, incoming: Option<&inputs::ProbeCleanupScope>) -> bool {
        let _operation = self.probe_cleanup_operation.lock().await;
        let mut owners = self.probe_cleanup_owners.lock().await;
        // This guard covers only custody polling/classification; preflight work
        // runs after it drops.
        owners.retain_mut(|owner| {
            owner.poll();
            owner.is_pending()
        });
        owners.iter().any(|owner| owner.conflicts_with(incoming))
    }

    async fn reserve_probe_resolution(
        &self,
        incoming_resource: Option<&str>,
    ) -> Option<std::sync::Arc<ProbeResolutionReservation>> {
        let incoming_resource = incoming_resource?;
        let mut resolutions = self.probe_cleanup_operation.lock().await;
        resolutions.retain(|reservation| reservation.strong_count() > 0);
        let mut owners = self.probe_cleanup_owners.lock().await;
        owners.retain_mut(|owner| {
            owner.poll();
            owner.is_pending()
        });
        if owners
            .iter()
            .any(|owner| owner.conflicts_with_resource(Some(incoming_resource)))
            || resolutions
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .any(|reservation| reservation.resource == incoming_resource)
        {
            return None;
        }
        let reservation = std::sync::Arc::new(ProbeResolutionReservation {
            resource: incoming_resource.to_owned(),
            scope: std::sync::OnceLock::new(),
        });
        resolutions.push(std::sync::Arc::downgrade(&reservation));
        Some(reservation)
    }

    async fn pin_probe_resolution_scope(
        &self,
        reservation: &std::sync::Arc<ProbeResolutionReservation>,
        incoming: Option<&inputs::ProbeCleanupScope>,
    ) -> bool {
        let Some(incoming) = incoming else {
            return false;
        };
        let mut resolutions = self.probe_cleanup_operation.lock().await;
        resolutions.retain(|reservation| reservation.strong_count() > 0);
        if reservation.resource != incoming.resource
            || resolutions
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .any(|other| {
                    !std::sync::Arc::ptr_eq(&other, reservation)
                        && (other.resource == incoming.resource
                            || other
                                .scope
                                .get()
                                .is_some_and(|scope| scope.overlaps(incoming)))
                })
        {
            return false;
        }
        let mut owners = self.probe_cleanup_owners.lock().await;
        owners.retain_mut(|owner| {
            owner.poll();
            owner.is_pending()
        });
        if owners
            .iter()
            .any(|owner| owner.conflicts_with(Some(incoming)))
        {
            return false;
        }
        reservation.scope.set(incoming.clone()).is_ok()
    }

    async fn reap_probe_cleanup_owners(&self) {
        let mut owners = self.probe_cleanup_owners.lock().await;
        owners.retain_mut(|owner| {
            owner.poll();
            owner.is_pending()
        });
    }
    #[cfg(test)]
    pub(super) async fn retain_test_live_child(&self, work: Work, child: Child) {
        let replaced = self.lock().await.insert(
            work.check_id.clone(),
            CheckChildOwner {
                work,
                child,
                launch: None,
                launch_error: None,
                launch_persistence_pending: false,
                exit: None,
                terminal_retained: false,
            },
        );
        assert!(
            replaced.is_none(),
            "test custody already owns this CheckRun"
        );
    }
}

impl Store {
    /// Run one closed configured-project Git lifecycle operation in Store
    /// custody. The owner mutex also prevents a retry from launching another
    /// helper while an earlier process family is still pending departure.
    pub(super) async fn execute_hook_local_operation(
        &self,
        principal: Principal,
        request: crate::hooks::git::HookLocalRequest,
    ) -> Result<Value> {
        let auth_principal = principal.clone();
        self.run(move |db| {
            let current = current_principal(db, auth_principal)?;
            super::gm::require_authority(db, &current)?;
            Ok(())
        })
        .await?;

        let config = self.config.clone();
        let project = config
            .forge
            .projects
            .get(&request.project_id)
            .ok_or_else(|| {
                Error::new(
                    "HOOK_PROJECT_UNAVAILABLE",
                    "project has no configured trusted repository",
                )
            })?;
        let repository = project.repository_path.clone();
        let git_executable = config.forge.git_executable.clone();
        if !repository.is_absolute()
            || !repository.is_dir()
            || !git_executable.is_absolute()
            || !git_executable.is_file()
        {
            return Err(Error::new(
                "HOOK_REPOSITORY_UNAVAILABLE",
                "configured repository and Git executable must be absolute existing paths",
            ));
        }

        let operation_name = request.operation.name();
        let _operation_guard = self.check_child_custody.git_cleanup_operation.lock().await;
        self.check_child_custody.reap_git_cleanup_owners().await;
        if !self
            .check_child_custody
            .git_cleanup_owners
            .lock()
            .await
            .is_empty()
        {
            return serde_json::to_value(crate::hooks::git::HookLocalResult::CleanupPending {
                operation: operation_name.to_owned(),
                cause_code: Some("HOOK_GIT_CLEANUP_PENDING".to_owned()),
            })
            .map_err(Into::into);
        }

        let (operation_result, cleanup_owner) = tokio::task::spawn_blocking(move || {
            let swarm_executable = std::env::current_exe().map_err(|_| {
                Error::new(
                    "HOOK_EXECUTABLE_UNAVAILABLE",
                    "Store executable path is unavailable",
                )
            })?;
            Ok::<_, Error>(crate::hooks::git::run_local_operation_with_cleanup(
                request,
                &repository,
                &git_executable,
                &swarm_executable,
            ))
        })
        .await
        .map_err(|_| Error::new("HOOK_GIT_FAILED", "hook Git verification worker ended"))??;

        if let Some(owner) = cleanup_owner {
            let pending = owner.is_pending();
            self.check_child_custody.retain_git_cleanup(owner).await;
            if pending {
                let cause_code = operation_result.err().map(|error| error.code);
                return serde_json::to_value(crate::hooks::git::HookLocalResult::CleanupPending {
                    operation: operation_name.to_owned(),
                    cause_code,
                })
                .map_err(Into::into);
            }
        }
        let result = operation_result?;
        serde_json::to_value(result).map_err(Into::into)
    }
}

fn check_launch_receipt(work: &Work, launch: &Value) -> Value {
    json!({
        "version": 1,
        "check_id": work.check_id,
        "operation_id": work.operation_id,
        "token": work.token,
        "launch": launch
    })
}

fn check_launch_unknown_receipt(work: &Work, child_pid: u32, error: &Error) -> Value {
    json!({
        "version": 1,
        "check_id": work.check_id,
        "operation_id": work.operation_id,
        "token": work.token,
        "child_pid": child_pid,
        "error_code": error.code
    })
}

fn check_child_departure_receipt(
    work: &Work,
    pid: u32,
    exit_success: bool,
    exit_code: Option<i32>,
    launch_error: &Error,
) -> Value {
    json!({
        "version": 1,
        "check_id": work.check_id,
        "operation_id": work.operation_id,
        "token": work.token,
        "child_pid": pid,
        "exit_observed": true,
        "exit_success": exit_success,
        "exit_code": exit_code,
        "go_published": false,
        "error_code": launch_error.code
    })
}

const MAX_CHECK_LAUNCH_RECEIPT_BYTES: u64 = 1024 * 1024;

fn write_check_receipt(path: &Path, value: &Value) -> Result<()> {
    let bytes = model::canonical(value)?.into_bytes();
    swarm_process::write_private_exact(path, &bytes, MAX_CHECK_LAUNCH_RECEIPT_BYTES)
        .map_err(Into::into)
}

fn read_check_receipt(path: &Path) -> Result<Option<Value>> {
    if !path.try_exists()? {
        return Ok(None);
    }
    let metadata = fs::symlink_metadata(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Err(Error::invalid("CheckRun launch receipt is a reparse point"));
        }
    }
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::invalid(
            "CheckRun launch receipt is not a regular file",
        ));
    }
    if metadata.len() > MAX_CHECK_LAUNCH_RECEIPT_BYTES {
        return Err(Error::invalid(
            "CheckRun launch receipt exceeds its size limit",
        ));
    }
    let bytes = swarm_process::read_private_durable(path, MAX_CHECK_LAUNCH_RECEIPT_BYTES)?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn validate_check_receipt_owner(work: &Work, receipt: &Value) -> Result<()> {
    if receipt["version"] != 1
        || receipt["check_id"] != work.check_id
        || receipt["operation_id"] != work.operation_id
        || receipt["token"] != work.token
    {
        return Err(Error::conflict(
            "CheckRun launch receipt belongs to a different check, operation, or token",
        ));
    }
    Ok(())
}

fn retain_check_launch_fact(work: &Work, launch: &Value) -> Result<()> {
    let directory = worker::directory(&work.data_dir, &work.check_id)?;
    let receipt = check_launch_receipt(work, launch);
    write_check_receipt(&directory.join("launch.json"), &receipt)
}

fn read_check_launch_fact(work: &Work) -> Result<Option<Value>> {
    let directory = worker::directory(&work.data_dir, &work.check_id)?;
    if let Some(receipt) = read_check_receipt(&directory.join("launch.json"))? {
        validate_check_receipt_owner(work, &receipt)?;
        let launch = receipt
            .get("launch")
            .filter(|launch| launch.is_object())
            .cloned()
            .ok_or_else(|| Error::new("CHECK_LAUNCH_RECEIPT_INVALID", "launch is missing"))?;
        return Ok(Some(launch));
    }
    if let Some(departure) = read_check_receipt(&directory.join("launch-departed.json"))? {
        validate_check_receipt_owner(work, &departure)?;
        return Ok(None);
    }
    if let Some(unknown) = read_check_receipt(&directory.join("launch-unknown.json"))? {
        validate_check_receipt_owner(work, &unknown)?;
        return Err(Error::new(
            "CHECK_LAUNCH_IDENTITY_UNKNOWN",
            "spawned CheckRun child has no exact launch identity; its job remains held",
        ));
    }
    Ok(None)
}

fn retain_check_launch_unknown(work: &Work, child_pid: u32, error: &Error) -> Result<()> {
    let directory = worker::directory(&work.data_dir, &work.check_id)?;
    write_check_receipt(
        &directory.join("launch-unknown.json"),
        &check_launch_unknown_receipt(work, child_pid, error),
    )
}

fn retain_check_child_departure(
    work: &Work,
    pid: u32,
    exit_success: bool,
    exit_code: Option<i32>,
    launch_error: &Error,
) -> Result<()> {
    let directory = worker::directory(&work.data_dir, &work.check_id)?;
    write_check_receipt(
        &directory.join("launch-departed.json"),
        &check_child_departure_receipt(work, pid, exit_success, exit_code, launch_error),
    )
}

fn retain_launch(db: &Connection, work: &Work, launch: Value) -> Result<()> {
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT operation_id,spec_json FROM check_runs WHERE check_id=?1 AND state IN ('running','reconciling')",
            [&work.check_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((operation_id, raw_spec)) = row else {
        return Err(Error::conflict(
            "CheckRun launch receipt has no active owner",
        ));
    };
    if operation_id != work.operation_id {
        return Err(Error::conflict("CheckRun launch operation changed"));
    }
    let mut spec: Value = serde_json::from_str(&raw_spec)?;
    if spec.get("token").and_then(Value::as_str) != Some(work.token.as_str()) {
        return Err(Error::conflict("CheckRun launch token changed"));
    }
    if let Some(existing) = spec.get("launch").filter(|value| !value.is_null()) {
        return if existing == &launch {
            Ok(())
        } else {
            Err(Error::conflict("CheckRun launch receipt changed"))
        };
    }
    spec["launch"] = launch.clone();
    let changed = db.execute(
        "UPDATE check_runs SET spec_json=?4 WHERE check_id=?1 AND operation_id=?2 AND state IN ('running','reconciling') AND json_extract(spec_json,'$.token')=?3",
        params![work.check_id, work.operation_id, work.token, model::canonical(&spec)?],
    )?;
    if changed != 1 {
        return Err(Error::conflict("CheckRun launch receipt was not retained"));
    }
    Ok(())
}

fn check_terminal_evidence_retained(db: &Connection, work: &Work) -> Result<bool> {
    let row: Option<(String, String, Option<i64>, String)> = db
        .query_row(
            "SELECT operation_id,state,finished_at_ms,spec_json FROM check_runs WHERE check_id=?1",
            [&work.check_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((operation_id, state, finished_at_ms, raw_spec)) = row else {
        return Ok(false);
    };
    let spec: Value = serde_json::from_str(&raw_spec)?;
    Ok(operation_id == work.operation_id
        && spec.get("token").and_then(Value::as_str) == Some(work.token.as_str())
        && finished_at_ms.is_some()
        && !matches!(state.as_str(), "queued" | "running" | "reconciling")
        && spec.get("cleanup_pending").is_none_or(Value::is_null))
}

fn check_supervisor_infrastructure(error: &Error) -> bool {
    error.code.starts_with("STORE_") || error.code.starts_with("KERNEL_")
}

fn check_incident_persistence_error(primary: Error, persistence: Error) -> Error {
    persistence.with_secondary_error(primary)
}

#[cfg(test)]
async fn stop_unstarted_child_bounded(child: &mut Child, grace: Duration) -> Result<bool> {
    match child.try_wait() {
        Ok(Some(_)) => return Ok(true),
        Ok(None) => {}
        Err(error) => return Err(error.into()),
    }
    let _ = child.kill();
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(true),
            Ok(None) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(None) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
    }
}

fn launch_persistence_error(error: Error, file_error: Option<Error>) -> Error {
    let mut combined = error.with_secondary_code("CHECK_LAUNCH_RECEIPT_NOT_RETAINED");
    if let Some(file_error) = file_error {
        combined = combined.with_secondary_error(file_error);
    }
    combined
}
impl Store {
    async fn record_check_process_diagnostic(&self, work: &Work) -> Result<()> {
        let scan = work.clone();
        let diagnostic = self
            .file_io(move |_| worker::process_diagnostic(&scan))
            .await?;
        let Some(diagnostic) = diagnostic else {
            return Ok(());
        };
        let update = work.clone();
        if self
            .run(move |db| record_process_diagnostic(db, &update, diagnostic))
            .await?
        {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }

    /// Advance the pending subject cursor after one subject reached a stable,
    /// truthfully retained disposition. The disposition is already durable;
    /// a cursor-write failure propagates as a hard error without erasing
    /// CheckRun truth, and retry converges by exact readback because the
    /// cursor still names the last completed subject.
    async fn advance_pending_cursor(
        &self,
        check_id: &str,
        cycle: &Option<PendingCycle>,
    ) -> Result<()> {
        let id = check_id.to_owned();
        let snapshot = cycle.clone();
        self.run(move |db| retain_pending_progress(db, &id, &snapshot))
            .await
    }

    /// Startup readback of retained CheckRun evidence before schedule catch-up.
    /// It never launches a worker or repeats a command. Live workers remain
    /// under the normal supervisor, which owns their acknowledged go-ahead.
    pub(crate) async fn reconcile_checks_once(&self) -> Result<()> {
        let root = self.data_dir.clone();
        let selection = self.run(move |db| pending_selection(db, root)).await?;
        let PendingSelection { work: items, cycle } = selection;
        // The cursor advances only through subjects that reached a stable,
        // truthfully retained disposition. A hard failure returns with the
        // cursor at the last completed subject; the failed subject stays
        // eligible and converges on retry by exact readback.
        for work in items {
            let check_id = work.check_id.clone();
            if self.terminalize_unstarted_check(&work).await? {
                self.advance_pending_cursor(&check_id, &cycle).await?;
                continue;
            }
            self.record_check_process_diagnostic(&work).await?;
            let scan = work.clone();
            if let Some(completion) = self
                .file_io(move |files| {
                    if standalone_host::owns_work(&scan)? {
                        standalone_host::finalize_execution(&scan, &files)
                    } else {
                        worker::completion(&scan, &files)
                    }
                })
                .await?
            {
                self.record_check_process_diagnostic(&work).await?;
                self.run(move |db| finish(db, &work, completion)).await?;
                self.changed
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
                self.advance_pending_cursor(&check_id, &cycle).await?;
                continue;
            }
            let scan = work.clone();
            if let Some(completion) = self
                .file_io(move |files| worker::recover_pre_identity(&scan, &files))
                .await?
            {
                self.run(move |db| finish(db, &work, completion)).await?;
                self.changed
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
                self.advance_pending_cursor(&check_id, &cycle).await?;
                continue;
            }
            let scan = work.clone();
            match self.file_io(move |_| worker::ready(&scan)).await {
                Ok(Some(identity)) => {
                    self.run(move |db| ready(db, &work, identity)).await?;
                    self.advance_pending_cursor(&check_id, &cycle).await?;
                }
                Ok(None) => {
                    self.advance_pending_cursor(&check_id, &cycle).await?;
                }
                Err(error) if error.code == "CHECK_WORKER_LOST" => {
                    let id = work.check_id.clone();
                    self.run(move |db| {
                        db.execute("UPDATE check_runs SET state='reconciling' WHERE check_id=?1 AND state='running'", [id])?;
                        Ok(())
                    }).await?;
                    let scan = work.clone();
                    if let Some(completion) = self
                        .file_io(move |files| worker::recover(&scan, &files))
                        .await?
                    {
                        self.record_check_process_diagnostic(&work).await?;
                        self.run(move |db| finish(db, &work, completion)).await?;
                        self.changed
                            .send_modify(|revision| *revision = revision.wrapping_add(1));
                    }
                    self.advance_pending_cursor(&check_id, &cycle).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(super) async fn capture_source(
        &self,
        principal: Principal,
        params: Value,
    ) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                if p.role != Role::Participant {
                    p.require_writer()?;
                }
                super::mutate(db, &p, "source.capture", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_owned();
        let start = id.clone();
        let p = principal.clone();
        if let Some((input, git, identity, authorized_repository)) =
            self.run(move |db| begin_source(db, p, &start)).await?
        {
            let root = self.data_dir.clone();
            let op = id.clone();
            let outcome = self
                .file_io(move |files| {
                    source::capture(
                        &root,
                        &files,
                        &input,
                        &op,
                        &git,
                        identity,
                        authorized_repository.as_deref(),
                    )
                })
                .await;
            self.run(move |db| finish_source(db, principal, &id, outcome))
                .await?;
        }
        Ok(receipt)
    }
    async fn terminalize_unstarted_check(&self, work: &Work) -> Result<bool> {
        let Some(error) = work.preflight_error.clone() else {
            return Ok(false);
        };
        if work.launch.is_some()
            || work.expected_worker.is_some()
            || self.check_child_is_owned(&work.check_id).await
        {
            return Err(Error::conflict("unstarted CheckRun has a process owner"));
        }
        let failed = work.clone();
        let completion = self
            .file_io(move |files| match worker::completion(&failed, &files)? {
                Some(completion) => Ok(completion),
                None => worker::failure(&failed, &files, error),
            })
            .await?;
        self.finish_supervised_check(work, completion).await?;
        Ok(true)
    }

    async fn record_check_incident(&self, key: String, error: Error) -> Result<()> {
        let primary = error.clone();
        self.run(move |db| incident(db, &key, error))
            .await
            .map_err(|persistence| check_incident_persistence_error(primary, persistence))
    }

    async fn retain_check_child_owner(&self, owner: CheckChildOwner) -> Result<()> {
        let check_id = owner.work.check_id.clone();
        let mut owners = self.check_child_custody.lock().await;
        if owners.contains_key(&check_id) {
            return Err(Error::new(
                "CHECK_CHILD_CUSTODY_DUPLICATE",
                "a CheckRun already has a retained child owner",
            ));
        }
        owners.insert(check_id, owner);
        Ok(())
    }

    async fn check_child_count(&self) -> usize {
        let check_children = self.check_child_custody.lock().await.len();
        let git_helpers = self
            .check_child_custody
            .git_cleanup_owners
            .lock()
            .await
            .len();
        let input_probes = self
            .check_child_custody
            .probe_cleanup_owners
            .lock()
            .await
            .len();
        let resolutions = self
            .check_child_custody
            .probe_cleanup_operation
            .lock()
            .await
            .iter()
            .filter(|reservation| reservation.strong_count() > 0)
            .count();
        check_children + git_helpers + input_probes + resolutions
    }

    async fn check_child_is_owned(&self, check_id: &str) -> bool {
        self.check_child_custody.lock().await.contains_key(check_id)
    }

    pub(crate) async fn check_child_custody_count(&self) -> usize {
        self.check_child_count().await
    }

    async fn check_child_has_launch_error(&self, check_id: &str) -> bool {
        self.check_child_custody
            .lock()
            .await
            .get(check_id)
            .is_some_and(|owner| owner.launch_error.is_some())
    }

    async fn check_child_exit_confirmed(&self, check_id: &str) -> bool {
        self.check_child_custody
            .lock()
            .await
            .get(check_id)
            .is_none_or(|owner| owner.exit.is_some())
    }

    async fn mark_check_launch_persistence_pending(
        &self,
        check_id: &str,
        launch: &Value,
        error: Error,
    ) -> Result<()> {
        let mut owners = self.check_child_custody.lock().await;
        let owner = owners.get_mut(check_id).ok_or_else(|| {
            Error::new(
                "KERNEL_CHECK_CUSTODY_UNAVAILABLE",
                "spawned CheckRun child lost its custody owner",
            )
        })?;
        if owner.launch.as_ref() != Some(launch) {
            return Err(Error::new(
                "CHECK_LAUNCH_RECEIPT_CONFLICT",
                "spawned CheckRun launch identity differs from its custody owner",
            ));
        }
        owner.launch_persistence_pending = true;
        owner.launch_error = Some(error);
        Ok(())
    }

    async fn stop_check_child_before_go(&self, check_id: &str) -> Result<()> {
        let mut owners = self.check_child_custody.lock().await;
        let Some(owner) = owners.get_mut(check_id) else {
            return Err(Error::new(
                "KERNEL_CHECK_CUSTODY_UNAVAILABLE",
                "spawned CheckRun child lost its custody owner",
            ));
        };
        if owner.exit.is_some() {
            return Ok(());
        }
        match owner.child.try_wait() {
            Ok(Some(status)) => {
                owner.exit = Some(CheckChildExit {
                    pid: owner.child.id(),
                    success: status.success(),
                    code: status.code(),
                });
                Ok(())
            }
            Ok(None) => owner.child.kill().map_err(|error| {
                Error::new(
                    "CHECK_CHILD_STOP_FAILED",
                    "the unacknowledged CheckRun child could not be stopped before Store Go",
                )
                .with_secondary_error(error.into())
            }),
            Err(error) => Err(Error::new(
                "CHECK_CHILD_STATUS_UNKNOWN",
                "the unacknowledged CheckRun child could not be observed before Store Go",
            )
            .with_secondary_error(error.into())),
        }
    }

    async fn finish_supervised_check(&self, work: &Work, completion: Completion) -> Result<()> {
        if !self.check_child_exit_confirmed(&work.check_id).await {
            return Err(Error::new(
                "CHECK_CHILD_EXIT_UNCONFIRMED",
                "CheckRun completion is retained until its owned child exit is confirmed",
            ));
        }
        let done = work.clone();
        let evidence_work = work.clone();
        let retained = self
            .run(move |db| {
                finish(db, &done, completion)?;
                check_terminal_evidence_retained(db, &evidence_work)
            })
            .await?;
        if !retained {
            return Err(Error::new(
                "CHECK_TERMINAL_EVIDENCE_UNRETAINED",
                "CheckRun terminal evidence was not retained by the Store",
            ));
        }
        if let Some(owner) = self
            .check_child_custody
            .lock()
            .await
            .get_mut(&work.check_id)
        {
            owner.terminal_retained = true;
        }
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn reap_check_children(&self) -> Result<()> {
        self.check_child_custody.reap_git_cleanup_owners().await;
        self.check_child_custody.reap_probe_cleanup_owners().await;
        let check_ids = self
            .check_child_custody
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for check_id in &check_ids {
            let observation = {
                let mut owners = self.check_child_custody.lock().await;
                let Some(owner) = owners.get_mut(check_id) else {
                    continue;
                };
                if owner.exit.is_some() {
                    continue;
                }
                match owner.child.try_wait() {
                    Ok(Some(status)) => {
                        owner.exit = Some(CheckChildExit {
                            pid: owner.child.id(),
                            success: status.success(),
                            code: status.code(),
                        });
                        None
                    }
                    Ok(None) => None,
                    Err(error) => Some(error),
                }
            };
            if let Some(error) = observation {
                let primary = Error::new(
                    "CHECK_CHILD_STATUS_UNKNOWN",
                    "the supervisor could not observe its owned CheckRun child",
                )
                .with_secondary_error(error.into());
                self.record_check_incident(
                    format!("check:{check_id}:{}", primary.code),
                    primary.clone(),
                )
                .await?;
                return Err(primary);
            }
        }

        for check_id in check_ids {
            let (work, launch, launch_error, launch_persistence_pending) = {
                let owners = self.check_child_custody.lock().await;
                let Some(owner) = owners.get(&check_id) else {
                    continue;
                };
                (
                    owner.work.clone(),
                    owner.launch.clone(),
                    owner.launch_error.clone(),
                    owner.launch_persistence_pending,
                )
            };

            if launch_persistence_pending {
                let launch = launch.ok_or_else(|| {
                    Error::new(
                        "KERNEL_CHECK_CUSTODY_UNAVAILABLE",
                        "pending CheckRun launch persistence has no exact launch identity",
                    )
                })?;
                let launch_file_work = work.clone();
                let launch_file = launch.clone();
                if let Err(error) = self
                    .file_io(move |_| retain_check_launch_fact(&launch_file_work, &launch_file))
                    .await
                {
                    return Err(launch_persistence_error(error, None));
                }
                let update = work.clone();
                let retained = launch.clone();
                if let Err(error) = self
                    .run(move |db| retain_launch(db, &update, retained))
                    .await
                {
                    return Err(launch_persistence_error(error, None));
                }
                let uncertainty = Error::new(
                    "CHECK_LAUNCH_PERSISTENCE_UNKNOWN",
                    "the spawned CheckRun launch identity was reconciled from retained child custody",
                )
                .with_secondary_error(launch_error.unwrap_or_else(|| {
                    Error::new(
                        "CHECK_LAUNCH_RECEIPT_NOT_RETAINED",
                        "the initial Store launch receipt write did not complete",
                    )
                }));
                self.record_check_incident(
                    format!("check-launch-persistence:{}", work.check_id),
                    uncertainty,
                )
                .await?;
                let mut owners = self.check_child_custody.lock().await;
                if let Some(owner) = owners.get_mut(&check_id) {
                    if owner.launch.as_ref() != Some(&launch) {
                        return Err(Error::new(
                            "CHECK_LAUNCH_RECEIPT_CONFLICT",
                            "retained child launch identity changed during persistence recovery",
                        ));
                    }
                    owner.launch_persistence_pending = false;
                }
            }

            let (launch_error, exit, terminal_retained) = {
                let owners = self.check_child_custody.lock().await;
                let Some(owner) = owners.get(&check_id) else {
                    continue;
                };
                (
                    owner.launch_error.clone(),
                    owner.exit,
                    owner.terminal_retained,
                )
            };

            if let (Some(launch_error), Some(exit)) = (launch_error, exit) {
                let departure_work = work.clone();
                let failure = json!(launch_error.clone());
                let completion = self
                    .file_io(move |files| {
                        retain_check_child_departure(
                            &departure_work,
                            exit.pid,
                            exit.success,
                            exit.code,
                            &launch_error,
                        )?;
                        worker::failure(&departure_work, &files, failure)
                    })
                    .await?;
                self.finish_supervised_check(&work, completion).await?;
            }

            let terminal_retained = if exit.is_some() && !terminal_retained {
                let evidence_work = work.clone();
                let retained = self
                    .run(move |db| check_terminal_evidence_retained(db, &evidence_work))
                    .await?;
                if retained
                    && let Some(owner) = self.check_child_custody.lock().await.get_mut(&check_id)
                {
                    owner.terminal_retained = true;
                }
                retained
            } else {
                terminal_retained
            };

            if exit.is_some() && terminal_retained {
                let mut owners = self.check_child_custody.lock().await;
                if owners
                    .get(&check_id)
                    .is_some_and(|owner| owner.exit.is_some() && owner.terminal_retained)
                {
                    owners.remove(&check_id);
                }
            }
        }
        Ok(())
    }

    pub async fn supervise_checks(self, mut stopping: watch::Receiver<bool>) -> Result<()> {
        let mut changed = self.changed.subscribe();
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut shutdown_requested = *stopping.borrow() || stopping.has_changed().is_err();
        loop {
            self.reap_check_children().await?;
            if shutdown_requested && self.check_child_count().await == 0 {
                return Ok(());
            }

            let root = self.data_dir.clone();
            let selection = self.run(move |db| pending_selection(db, root)).await?;
            let PendingSelection { work: items, cycle } = selection;
            // Same corrected seam as startup reconciliation: the cursor
            // advances only through subjects with a durable disposition.
            // Infrastructure failures return with the cursor at the last
            // completed subject; handled subject-local errors advance only
            // after their incident is retained.
            for mut w in items {
                if self.check_child_has_launch_error(&w.check_id).await {
                    continue;
                }
                if let Some(owner_launch) = self
                    .check_child_custody
                    .lock()
                    .await
                    .get(&w.check_id)
                    .and_then(|owner| owner.launch.clone())
                {
                    if w.launch
                        .as_ref()
                        .is_some_and(|launch| launch != &owner_launch)
                    {
                        return Err(Error::new(
                            "CHECK_LAUNCH_RECEIPT_CONFLICT",
                            "Store launch identity differs from the retained child owner",
                        ));
                    }
                    if w.launch.is_none() {
                        return Err(Error::new(
                            "KERNEL_CHECK_CUSTODY_UNAVAILABLE",
                            "retained child launch identity is not reflected in Store state",
                        ));
                    }
                }
                if w.launch.is_none() {
                    let scan = w.clone();
                    match self.file_io(move |_| read_check_launch_fact(&scan)).await {
                        Ok(Some(launch)) => {
                            let uncertainty = Error::new(
                                "CHECK_LAUNCH_PERSISTENCE_UNKNOWN",
                                "the exact private launch receipt exists but Store had not retained it",
                            );
                            self.record_check_incident(
                                format!("check-launch-persistence:{}", w.check_id),
                                uncertainty.clone(),
                            )
                            .await?;
                            let update = w.clone();
                            let retained = launch.clone();
                            if let Err(error) = self
                                .run(move |db| retain_launch(db, &update, retained))
                                .await
                            {
                                return Err(error.with_secondary_error(uncertainty));
                            }
                            w.launch = Some(launch);
                            self.changed
                                .send_modify(|revision| *revision = revision.wrapping_add(1));
                        }
                        Ok(None) => {}
                        Err(error) => {
                            if check_supervisor_infrastructure(&error) {
                                return Err(error);
                            }
                            self.record_check_incident(
                                format!("check:{}:{}", w.check_id, error.code),
                                error,
                            )
                            .await?;
                            self.advance_pending_cursor(&w.check_id, &cycle).await?;
                            continue;
                        }
                    }
                }

                let result = async {
                    self.record_check_process_diagnostic(&w).await?;
                    if self.terminalize_unstarted_check(&w).await? {
                        return Ok(());
                    }
                    let scan = w.clone();
                    let complete = self
                        .file_io(move |files| {
                            if standalone_host::owns_work(&scan)? {
                                standalone_host::finalize_execution(&scan, &files)
                            } else {
                                worker::completion(&scan, &files)
                            }
                        })
                        .await?;
                    if let Some(completion) = complete {
                        self.record_check_process_diagnostic(&w).await?;
                        self.finish_supervised_check(&w, completion).await?;
                        return Ok(());
                    }
                    let scan = w.clone();
                    if let Some(completion) = self
                        .file_io(move |files| worker::recover_pre_identity(&scan, &files))
                        .await?
                    {
                        self.finish_supervised_check(&w, completion).await?;
                        return Ok(());
                    }
                    let scan = w.clone();
                    match self.file_io(move |_| worker::ready(&scan)).await {
                        Ok(Some(identity)) => {
                            let active = w.clone();
                            let accepted_worker = identity.clone();
                            if self.run(move |db| ready(db, &active, identity)).await? {
                                let allow = w.clone();
                                self.file_io(move |files| {
                                    worker::deliver_cancel(&allow)?;
                                    worker::allow(&allow)?;
                                    if standalone_host::owns_work(&allow)?
                                        && let Err(error) = standalone_host::materialize_plan(
                                            &allow,
                                            &files,
                                            &accepted_worker,
                                        )
                                    {
                                        standalone_host::publish_plan_failure(
                                            &allow,
                                            &accepted_worker,
                                        )?;
                                        return Err(error);
                                    }
                                    Ok(())
                                })
                                .await?;
                            }
                        }
                        Ok(None) => {}
                        Err(error) if error.code == "CHECK_WORKER_LOST" => {
                            let id = w.check_id.clone();
                            let transition = self
                                .run(move |db| {
                                    let state: Option<String> = db
                                        .query_row(
                                            "SELECT state FROM check_runs WHERE check_id=?1",
                                            [&id],
                                            |row| row.get(0),
                                        )
                                        .optional()?;
                                    match state.as_deref() {
                                        Some("reconciling") => Ok(()),
                                        Some("running") => {
                                            let changed = db.execute(
                                                "UPDATE check_runs SET state='reconciling' WHERE check_id=?1 AND state='running'",
                                                [&id],
                                            )?;
                                            if changed == 1 {
                                                Ok(())
                                            } else {
                                                Err(Error::conflict(
                                                    "lost CheckRun transition was not retained",
                                                ))
                                            }
                                        }
                                        _ => Err(Error::conflict(
                                            "lost CheckRun has no active durable state",
                                        )),
                                    }
                                })
                                .await;
                            if let Err(storage_error) = transition {
                                return Err(storage_error.with_secondary_error(error));
                            }
                            let scan = w.clone();
                            if let Some(completion) = self
                                .file_io(move |files| worker::recover(&scan, &files))
                                .await?
                            {
                                self.record_check_process_diagnostic(&w).await?;
                                self.finish_supervised_check(&w, completion).await?;
                                return Ok(());
                            }
                            return Err(error);
                        }
                        Err(error) => return Err(error),
                    }
                    Ok(())
                }
                .await;
                if let Err(error) = result {
                    if check_supervisor_infrastructure(&error) {
                        return Err(error);
                    }
                    self.record_check_incident(
                        format!("check:{}:{}", w.check_id, error.code),
                        error,
                    )
                    .await?;
                    self.advance_pending_cursor(&w.check_id, &cycle).await?;
                } else {
                    self.advance_pending_cursor(&w.check_id, &cycle).await?;
                }
            }

            if !shutdown_requested
                && self.check_child_count().await < self.config.checks.max_running
            {
                let root = self.data_dir.clone();
                let config = self.config.clone();
                match self.run(move |db| next(db, &config, root)).await {
                    Ok(Some(w)) => {
                        if self.check_child_is_owned(&w.check_id).await {
                            return Err(Error::new(
                                "CHECK_CHILD_CUSTODY_DUPLICATE",
                                "CheckRun admission selected work that already has a retained child owner",
                            ));
                        }
                        if let Some(error) = w.preflight_error.clone() {
                            let failed = w.clone();
                            let completion = self
                                .file_io(move |files| worker::failure(&failed, &files, error))
                                .await?;
                            self.finish_supervised_check(&w, completion).await?;
                        } else {
                            let launch = w.clone();
                            let executor = launch.executor.clone();
                            let prepared = self
                                .file_io(move |_| {
                                    Ok(match executor {
                                        Some(pin) => {
                                            standalone_host::prepare_and_spawn(&launch, &pin)
                                        }
                                        None => worker::prepare_and_spawn(&launch),
                                    })
                                })
                                .await?;
                            match prepared {
                                Err(error) if error.code == "CHECK_LAUNCH_UNKNOWN" => {
                                    self.record_check_incident(
                                        format!("check-launch:{}", w.check_id),
                                        error.clone(),
                                    )
                                    .await?;
                                    return Err(error);
                                }
                                Err(error) => {
                                    let retain = w.clone();
                                    let failure = json!(error.clone());
                                    let retained_error = failure.clone();
                                    let terminalizing = self
                                        .run(move |db| {
                                            retain_pre_spawn_failure(db, &retain, retained_error)
                                        })
                                        .await
                                        .map_err(|storage| {
                                            storage.with_secondary_error(error.clone())
                                        })?;
                                    let failed = terminalizing.clone();
                                    match self
                                        .file_io(move |files| {
                                            worker::failure(&failed, &files, failure)
                                        })
                                        .await
                                    {
                                        Ok(completion) => {
                                            self.finish_supervised_check(
                                                &terminalizing,
                                                completion,
                                            )
                                            .await?;
                                        }
                                        Err(file_error) => {
                                            let combined = error.with_secondary_error(file_error);
                                            self.record_check_incident(
                                                format!("check:{}:{}", w.check_id, combined.code),
                                                combined.clone(),
                                            )
                                            .await?;
                                            return Err(combined);
                                        }
                                    }
                                }
                                Ok((Err(error), child)) => {
                                    let check_id = w.check_id.clone();
                                    let child_pid = child.id();
                                    self.retain_check_child_owner(CheckChildOwner {
                                        work: w.clone(),
                                        child,
                                        launch: None,
                                        launch_error: Some(error.clone()),
                                        launch_persistence_pending: false,
                                        exit: None,
                                        terminal_retained: false,
                                    })
                                    .await?;
                                    let marker_work = w.clone();
                                    let marker_error = error.clone();
                                    let marker_result = self
                                        .file_io(move |_| {
                                            retain_check_launch_unknown(
                                                &marker_work,
                                                child_pid,
                                                &marker_error,
                                            )
                                        })
                                        .await;
                                    let mut diagnostic = match marker_result {
                                        Ok(()) => error,
                                        Err(marker_error) => {
                                            error.with_secondary_error(marker_error)
                                        }
                                    };
                                    let stop_error =
                                        self.stop_check_child_before_go(&check_id).await.err();
                                    if let Some(stop_error) = stop_error.clone() {
                                        diagnostic = diagnostic.with_secondary_error(stop_error);
                                    }
                                    self.record_check_incident(
                                        format!("check:{}:{}", check_id, diagnostic.code),
                                        diagnostic,
                                    )
                                    .await?;
                                    if let Some(stop_error) = stop_error {
                                        return Err(stop_error);
                                    }
                                }
                                Ok((Ok(launch), child)) => {
                                    let check_id = w.check_id.clone();
                                    self.retain_check_child_owner(CheckChildOwner {
                                        work: w.clone(),
                                        child,
                                        launch: Some(launch.clone()),
                                        launch_error: None,
                                        launch_persistence_pending: true,
                                        exit: None,
                                        terminal_retained: false,
                                    })
                                    .await?;
                                    let launch_work = w.clone();
                                    let launch_fact = launch.clone();
                                    let file_result = self
                                        .file_io(move |_| {
                                            retain_check_launch_fact(&launch_work, &launch_fact)
                                        })
                                        .await;
                                    let file_error = file_result.err();
                                    let update = w.clone();
                                    let retained = launch.clone();
                                    if let Err(error) = self
                                        .run(move |db| retain_launch(db, &update, retained))
                                        .await
                                    {
                                        let mut persistence_error =
                                            launch_persistence_error(error, file_error.clone());
                                        if let Err(owner_error) = self
                                            .mark_check_launch_persistence_pending(
                                                &check_id,
                                                &launch,
                                                persistence_error.clone(),
                                            )
                                            .await
                                        {
                                            persistence_error =
                                                persistence_error.with_secondary_error(owner_error);
                                        }
                                        if let Err(stop_error) =
                                            self.stop_check_child_before_go(&check_id).await
                                        {
                                            persistence_error =
                                                persistence_error.with_secondary_error(stop_error);
                                        }
                                        return Err(persistence_error);
                                    }
                                    if let Some(file_error) = file_error {
                                        let retry_work = w.clone();
                                        let retry_launch = launch.clone();
                                        if let Err(retry_error) = self
                                            .file_io(move |_| {
                                                retain_check_launch_fact(&retry_work, &retry_launch)
                                            })
                                            .await
                                        {
                                            let mut persistence_error = launch_persistence_error(
                                                file_error,
                                                Some(retry_error),
                                            );
                                            if let Err(owner_error) = self
                                                .mark_check_launch_persistence_pending(
                                                    &check_id,
                                                    &launch,
                                                    persistence_error.clone(),
                                                )
                                                .await
                                            {
                                                persistence_error = persistence_error
                                                    .with_secondary_error(owner_error);
                                            }
                                            if let Err(stop_error) =
                                                self.stop_check_child_before_go(&check_id).await
                                            {
                                                persistence_error = persistence_error
                                                    .with_secondary_error(stop_error);
                                            }
                                            return Err(persistence_error);
                                        }
                                    }
                                    let mut owners = self.check_child_custody.lock().await;
                                    let owner = owners.get_mut(&check_id).ok_or_else(|| {
                                        Error::new(
                                            "KERNEL_CHECK_CUSTODY_UNAVAILABLE",
                                            "spawned CheckRun child lost its custody owner",
                                        )
                                    })?;
                                    if owner.launch.as_ref() != Some(&launch) {
                                        return Err(Error::new(
                                            "CHECK_LAUNCH_RECEIPT_CONFLICT",
                                            "spawned CheckRun launch identity differs from its custody owner",
                                        ));
                                    }
                                    owner.launch_persistence_pending = false;
                                    drop(owners);
                                    self.changed.send_modify(|revision| {
                                        *revision = revision.wrapping_add(1)
                                    });
                                }
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if check_supervisor_infrastructure(&error) {
                            return Err(error);
                        }
                        self.record_check_incident(
                            format!("check-admission:{}", error.code),
                            error.clone(),
                        )
                        .await?;
                        return Err(error);
                    }
                }
            }

            tokio::select! {
                result = stopping.changed(), if !shutdown_requested => {
                    if result.is_err() || *stopping.borrow() {
                        shutdown_requested = true;
                    }
                },
                result = changed.changed() => {
                    if result.is_err() {
                        return Err(Error::new(
                            "STORE_CLOSED",
                            "CheckRun supervisor change stream ended",
                        ));
                    }
                },
                _ = tick.tick() => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[path = "check_child_departure_durable_tests.rs"]
    mod check_child_departure_durable_tests;
    #[path = "check_held_tests.rs"]
    mod check_held_tests;
    #[path = "check_launch_fact_durable_tests.rs"]
    mod check_launch_fact_durable_tests;
    #[path = "check_launch_unknown_durable_tests.rs"]
    mod check_launch_unknown_durable_tests;
    #[path = "reg23_pending_isolation_tests.rs"]
    mod reg23_pending_isolation_tests;
    use crate::{
        artifacts::ArtifactFiles,
        checks::{
            inputs,
            model::{CheckProfile, Parser},
            source::{self, SourceFile, SourceManifest},
            worker,
        },
        config::Config,
        model,
        platform::{DataRoot, bootstrap_credential},
        store::{StoreOwner, current_principal, mutate_in_transaction_with_check_plan},
    };
    use rusqlite::{TransactionBehavior, params};
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
        process::{Command, Stdio},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };
    use tokio::time::timeout;

    static PR61_PROBE_FAILURE_CALLS: AtomicUsize = AtomicUsize::new(0);
    static PR61_PROBE_SUCCESS_CALLS: AtomicUsize = AtomicUsize::new(0);

    const PROBE_OWNER_FIXTURE_MODE: &str = "ELIOT_PROBE_OWNER_FIXTURE_MODE";
    const PROBE_OWNER_FIXTURE_READY: &str = "ELIOT_PROBE_OWNER_FIXTURE_READY";
    const PROBE_OWNER_FIXTURE_RELEASE: &str = "ELIOT_PROBE_OWNER_FIXTURE_RELEASE";

    fn fail_pr61_input_probe(
        _program: &Path,
        _args: &[String],
        _env: &BTreeMap<String, String>,
        _cwd: &Path,
    ) -> Result<Vec<u8>> {
        PR61_PROBE_FAILURE_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(Error::new(
            "TEST_PROBE_FAILURE",
            "injected bounded CheckRunner version-probe failure",
        ))
    }

    fn count_pr61_successful_input_probe(
        _program: &Path,
        _args: &[String],
        _env: &BTreeMap<String, String>,
        _cwd: &Path,
    ) -> Result<Vec<u8>> {
        PR61_PROBE_SUCCESS_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(b"fixture resolver version 1\n".to_vec())
    }

    #[test]
    fn probe_owner_fixture_child_entry() {
        if std::env::var(PROBE_OWNER_FIXTURE_MODE).as_deref() != Ok("held") {
            return;
        }
        let ready = PathBuf::from(
            std::env::var_os(PROBE_OWNER_FIXTURE_READY).expect("probe owner ready path"),
        );
        let release = PathBuf::from(
            std::env::var_os(PROBE_OWNER_FIXTURE_RELEASE).expect("probe owner release path"),
        );
        fs::write(ready, b"ready").expect("publish live child readiness");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !release.exists() && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
    }

    struct ProbeOwnerProcessFixture {
        directory: PathBuf,
        release: PathBuf,
    }

    impl ProbeOwnerProcessFixture {
        fn allow_departure(&self) {
            fs::write(&self.release, b"depart").expect("release the exact probe child");
        }

        fn cleanup(&self) {
            fs::remove_dir_all(&self.directory).expect("remove the departed probe fixture");
        }
    }

    impl Drop for ProbeOwnerProcessFixture {
        fn drop(&mut self) {
            let _ = fs::write(&self.release, b"depart");
        }
    }

    fn pending_probe_owner(
        scope: inputs::ProbeCleanupScope,
    ) -> (ProbeCleanupOwner, ProbeOwnerProcessFixture) {
        pending_probe_owner_with_scope(Some(scope))
    }

    fn pending_unscoped_probe_owner() -> (ProbeCleanupOwner, ProbeOwnerProcessFixture) {
        pending_probe_owner_with_scope(None)
    }

    fn pending_probe_owner_with_scope(
        scope: Option<inputs::ProbeCleanupScope>,
    ) -> (ProbeCleanupOwner, ProbeOwnerProcessFixture) {
        let directory = std::env::temp_dir().join(format!("swarm-probe-owner-{}", model::new_id()));
        fs::create_dir(&directory).expect("create private probe owner fixture directory");
        let ready = directory.join("ready");
        let release = directory.join("release");
        let mut child = Command::new(std::env::current_exe().expect("test executable path"))
            .arg("probe_owner_fixture_child_entry")
            .env(PROBE_OWNER_FIXTURE_MODE, "held")
            .env(PROBE_OWNER_FIXTURE_READY, &ready)
            .env(PROBE_OWNER_FIXTURE_RELEASE, &release)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn controlled probe owner child");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !ready.exists() && std::time::Instant::now() < deadline {
            if let Ok(Some(status)) = child.try_wait() {
                panic!("probe owner child exited before becoming live: {status}");
            }
            thread::sleep(Duration::from_millis(10));
        }
        if !ready.exists() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("probe owner child did not publish readiness");
        }
        let fixture = ProbeOwnerProcessFixture { directory, release };
        let owner =
            inputs::pending_probe_cleanup_owner_for_test(child, fixture.directory.clone(), scope);
        (owner, fixture)
    }

    async fn install_probe_owner(store: &Store, owner: ProbeCleanupOwner) -> bool {
        let custody = store.check_child_custody.clone();
        store
            .file_io(move |_files| Ok(custody.retain_probe_cleanup(owner)))
            .await
            .expect("install retained probe owner")
    }

    async fn reap_probe_owners_to(custody: &CheckChildCustody, expected: usize) {
        timeout(Duration::from_secs(5), async {
            loop {
                custody.reap_probe_cleanup_owners().await;
                if custody.probe_cleanup_owners.lock().await.len() == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("poll exact probe child departure");
    }

    fn probe_scope(resource: &str, workspace: &str) -> inputs::ProbeCleanupScope {
        inputs::ProbeCleanupScope {
            resource: resource.to_owned(),
            workspace_identity: json!({"workspace":workspace}),
        }
    }

    #[tokio::test]
    async fn retained_probe_owner_install_conflict_keeps_custody_and_allows_independent_resource() {
        let fixture = fixture().await;
        let store = &fixture.owner.store;
        let custody = &store.check_child_custody;
        let held_scope = probe_scope("checks-a", "workspace-a");
        let (first_owner, first_child) = pending_probe_owner(held_scope.clone());
        assert!(!install_probe_owner(store, first_owner).await);

        let (conflicting_owner, conflicting_child) = pending_probe_owner(held_scope.clone());
        assert!(install_probe_owner(store, conflicting_owner).await);
        assert_eq!(custody.probe_cleanup_owners.lock().await.len(), 2);
        assert!(custody.probe_cleanup_conflicts(Some(&held_scope)).await);

        let independent_scope = probe_scope("checks-b", "workspace-b");
        assert!(
            !custody
                .probe_cleanup_conflicts(Some(&independent_scope))
                .await
        );
        let independent = custody
            .reserve_probe_resolution(Some("checks-b"))
            .await
            .expect("independent CheckRunner resource remains admissible");
        drop(independent);
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-a"))
                .await
                .is_none()
        );

        first_child.allow_departure();
        reap_probe_owners_to(custody, 1).await;
        assert!(custody.probe_cleanup_conflicts(Some(&held_scope)).await);
        conflicting_child.allow_departure();
        reap_probe_owners_to(custody, 0).await;
        assert!(!custody.probe_cleanup_conflicts(Some(&held_scope)).await);

        first_child.cleanup();
        conflicting_child.cleanup();
        fixture.owner.close().await.unwrap();
    }

    #[tokio::test]
    async fn late_probe_owner_departure_releases_only_its_checkrunner_resource() {
        let fixture = fixture().await;
        let store = &fixture.owner.store;
        let custody = &store.check_child_custody;
        let scope_a = probe_scope("checks-a", "workspace-a");
        let scope_b = probe_scope("checks-b", "workspace-b");
        let (owner_a, child_a) = pending_probe_owner(scope_a.clone());
        let (owner_b, child_b) = pending_probe_owner(scope_b.clone());
        assert!(!install_probe_owner(store, owner_a).await);
        assert!(!install_probe_owner(store, owner_b).await);

        assert!(custody.probe_cleanup_conflicts(Some(&scope_a)).await);
        assert!(custody.probe_cleanup_conflicts(Some(&scope_b)).await);
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-a"))
                .await
                .is_none()
        );
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-b"))
                .await
                .is_none()
        );

        child_a.allow_departure();
        reap_probe_owners_to(custody, 1).await;
        assert!(!custody.probe_cleanup_conflicts(Some(&scope_a)).await);
        assert!(custody.probe_cleanup_conflicts(Some(&scope_b)).await);
        let released_a = custody
            .reserve_probe_resolution(Some("checks-a"))
            .await
            .expect("late exact departure releases resource A");
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-b"))
                .await
                .is_none()
        );
        drop(released_a);

        child_b.allow_departure();
        reap_probe_owners_to(custody, 0).await;
        child_a.cleanup();
        child_b.cleanup();
        fixture.owner.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_probe_retains_owner_drops_resolution_and_holds_only_its_resource() {
        PR61_PROBE_FAILURE_CALLS.store(0, Ordering::SeqCst);
        let fixture = probe_error_fixture().await;
        let store = &fixture.owner.store;
        let (task_id, attempt_id) = create_attempt(&fixture, "cache-project", None).await;
        let candidate =
            source_snapshot(&fixture, &task_id, &attempt_id, b"probe error source").await;
        let params = json!({
            "client_request_id":model::new_id(),
            "attempt_id":attempt_id,
            "candidate_ref":candidate.artifact_id,
            "profile_id":"strict",
            "profile_revision":"pr61-probe-error",
        });
        let principal = fixture.principal.clone();
        let config = fixture.config.clone();
        let snapshot = store
            .run(move |db| {
                let principal = current_principal(db, principal)?;
                plan_inputs(db, &principal, &params, &config)
            })
            .await
            .unwrap();

        let files = ArtifactFiles::new(&fixture.directory).unwrap();
        let verified = source::verified_content(&files, &fixture.directory, &candidate).unwrap();
        let owner_profile = fixture
            .config
            .checks
            .profile("strict", "pr61-owner")
            .unwrap();
        let probe_profile = fixture
            .config
            .checks
            .profile("strict", "pr61-probe-error")
            .unwrap();
        let owner_scope = inputs::ProbeCleanupScope::for_candidate(&owner_profile, &verified)
            .expect("derive owner scope from configured profile and verified source");
        let probe_scope = inputs::ProbeCleanupScope::for_candidate(&probe_profile, &verified)
            .expect("derive preflight scope from configured profile and verified source");
        assert_ne!(owner_scope.resource, probe_scope.resource);

        let (owner, child) = pending_unscoped_probe_owner();
        let _probe_override =
            inputs::override_probe_cleanup_for_test(&probe_profile, fail_pr61_input_probe, owner);
        let resolution = store
            .resolve_check_plan_snapshot(Ok(snapshot))
            .await
            .unwrap();
        let error = match resolution.result {
            Err(error) => error,
            Ok(_) => panic!("pending external probe custody must fail preflight"),
        };
        assert_eq!(error.code, "CHECK_PROBE_CLEANUP_PENDING");
        assert_eq!(PR61_PROBE_FAILURE_CALLS.load(Ordering::SeqCst), 1);

        let custody = &store.check_child_custody;
        assert_eq!(
            store.check_child_custody_count().await,
            1,
            "the retained owner remains; the failed preflight reservation is gone"
        );
        {
            let owners = custody.probe_cleanup_owners.lock().await;
            assert_eq!(owners.len(), 1);
            assert!(owners[0].is_pending());
            assert_eq!(owners[0].scope(), Some(&probe_scope));
        }
        assert!(custody.probe_cleanup_conflicts(Some(&probe_scope)).await);
        assert!(!custody.probe_cleanup_conflicts(Some(&owner_scope)).await);
        assert!(
            custody
                .reserve_probe_resolution(Some(&probe_scope.resource))
                .await
                .is_none()
        );
        let independent = custody
            .reserve_probe_resolution(Some(&owner_scope.resource))
            .await
            .expect("the unrelated CheckRunner resource remains admissible");
        drop(independent);

        child.allow_departure();
        reap_probe_owners_to(custody, 0).await;
        child.cleanup();
        fixture.owner.close().await.unwrap();
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn real_probe_owner_blocks_only_conflicting_preflight_until_exact_family_departure() {
        PR61_PROBE_SUCCESS_CALLS.store(0, Ordering::SeqCst);
        let fixture = probe_error_fixture().await;
        let store = &fixture.owner.store;
        let (task_id, attempt_id) = create_attempt(&fixture, "cache-project", None).await;
        let candidate =
            source_snapshot(&fixture, &task_id, &attempt_id, b"real probe custody A/B").await;
        let snapshot_a = probe_plan_snapshot(
            &fixture,
            &attempt_id,
            &candidate.artifact_id,
            "pr61-probe-error",
        )
        .await;

        let files = ArtifactFiles::new(&fixture.directory).unwrap();
        let verified = source::verified_content(&files, &fixture.directory, &candidate).unwrap();
        let profile_a = fixture
            .config
            .checks
            .profile("strict", "pr61-probe-error")
            .unwrap();
        let profile_b = fixture
            .config
            .checks
            .profile("strict", "pr61-owner")
            .unwrap();
        let scope_a = inputs::ProbeCleanupScope::for_candidate(&profile_a, &verified)
            .expect("derive actual A resource and workspace identity");
        let scope_b = inputs::ProbeCleanupScope::for_candidate(&profile_b, &verified)
            .expect("derive actual independent B resource and workspace identity");
        assert!(!scope_a.overlaps(&scope_b));

        let (owner, process_fixture) =
            inputs::tests::pending_probe_custody_fixture(scope_a.clone());
        let target_identity = process_fixture.process_identity("target.identity.json");
        let descendant_identity = process_fixture.process_identity("descendant.identity.json");
        let helper_identity = process_fixture.helper_launch_identity().clone();
        let _probe_a = inputs::override_probe_cleanup_for_test(
            &profile_a,
            count_pr61_successful_input_probe,
            owner,
        );
        let first_a = store
            .resolve_check_plan_snapshot(Ok(snapshot_a))
            .await
            .unwrap();
        match first_a.result {
            Err(error) => assert_eq!(error.code, "CHECK_PROBE_CLEANUP_PENDING"),
            Ok(_) => panic!("pending real probe-family custody must hold A preflight"),
        }
        assert_eq!(PR61_PROBE_SUCCESS_CALLS.load(Ordering::SeqCst), 1);

        let custody = &store.check_child_custody;
        assert_eq!(store.check_child_custody_count().await, 1);
        {
            let owners = custody.probe_cleanup_owners.lock().await;
            assert_eq!(owners.len(), 1);
            assert!(owners[0].is_pending());
            assert_eq!(owners[0].scope(), Some(&scope_a));
        }
        assert!(custody.probe_cleanup_conflicts(None).await);
        assert!(custody.probe_cleanup_conflicts(Some(&scope_a)).await);
        assert!(!custody.probe_cleanup_conflicts(Some(&scope_b)).await);
        assert!(
            custody
                .reserve_probe_resolution(Some(&scope_a.resource))
                .await
                .is_none(),
            "conflicting A reservation remains held"
        );

        let snapshot_a2 = probe_plan_snapshot(
            &fixture,
            &attempt_id,
            &candidate.artifact_id,
            "pr61-probe-error",
        )
        .await;
        let _probe_a2 =
            inputs::override_probe_for_test(&profile_a, count_pr61_successful_input_probe);
        let second_a = store
            .resolve_check_plan_snapshot(Ok(snapshot_a2))
            .await
            .unwrap();
        match second_a.result {
            Err(error) => assert_eq!(error.code, "CHECK_PROBE_CLEANUP_PENDING"),
            Ok(_) => panic!("second conflicting A preflight must stay held"),
        }
        assert_eq!(
            PR61_PROBE_SUCCESS_CALLS.load(Ordering::SeqCst),
            1,
            "blocked A2 never starts a second resolver probe"
        );
        drop(_probe_a2);

        let snapshot_b =
            probe_plan_snapshot(&fixture, &attempt_id, &candidate.artifact_id, "pr61-owner").await;
        let _probe_b =
            inputs::override_probe_for_test(&profile_b, count_pr61_successful_input_probe);
        let independent_b = store
            .resolve_check_plan_snapshot(Ok(snapshot_b))
            .await
            .unwrap();
        assert!(
            independent_b.result.is_ok(),
            "independent B completes normal CheckRunner preflight while A remains held"
        );
        assert_eq!(PR61_PROBE_SUCCESS_CALLS.load(Ordering::SeqCst), 2);
        assert_eq!(store.check_child_custody_count().await, 1);
        assert!(custody.probe_cleanup_conflicts(Some(&scope_a)).await);
        assert!(!custody.probe_cleanup_conflicts(Some(&scope_b)).await);
        let b_reservation = custody
            .reserve_probe_resolution(Some(&scope_b.resource))
            .await
            .expect("independent B remains admissible while A is pending");
        drop(b_reservation);

        process_fixture.release_all();
        reap_probe_owners_to(custody, 0).await;
        assert!(process_fixture.process_departed(&helper_identity));
        assert!(process_fixture.process_departed(&target_identity));
        assert!(process_fixture.process_departed(&descendant_identity));
        assert!(!custody.probe_cleanup_conflicts(Some(&scope_a)).await);
        let a_reservation = custody
            .reserve_probe_resolution(Some(&scope_a.resource))
            .await
            .expect("exact helper and nested-family departure releases A");
        drop(a_reservation);
        let b_reservation = custody
            .reserve_probe_resolution(Some(&scope_b.resource))
            .await
            .expect("A departure leaves independent B available");
        drop(b_reservation);

        let evidence = json!({
            "helper_pid":process_fixture.helper_pid(),
            "helper_launch_identity":helper_identity,
            "target_identity":target_identity,
            "descendant_identity":descendant_identity,
            "owner_resource":scope_a.resource,
            "owner_workspace_identity":scope_a.workspace_identity,
            "independent_resource":scope_b.resource,
            "independent_workspace_identity":scope_b.workspace_identity,
            "first_a_probe_calls":1,
            "blocked_a2_probe_calls":1,
            "independent_b_probe_calls":2,
            "b_completed_while_a_pending":true,
            "owner_count_after_exact_departure":0,
            "helper_departed":true,
            "target_departed":true,
            "descendant_departed":true,
        });
        eprintln!("probe_store_custody_ab_evidence={evidence}");
        drop(process_fixture);
        fixture.owner.close().await.unwrap();
    }

    #[tokio::test]
    async fn probe_resolution_reservations_hold_conflicts_until_both_owners_depart() {
        let custody = CheckChildCustody::default();
        assert!(custody.reserve_probe_resolution(None).await.is_none());
        let first = custody
            .reserve_probe_resolution(Some("checks-a"))
            .await
            .unwrap();
        let first_scope = inputs::ProbeCleanupScope {
            resource: "checks-a".into(),
            workspace_identity: json!({"workspace":"a"}),
        };
        assert!(
            custody
                .pin_probe_resolution_scope(&first, Some(&first_scope))
                .await
        );
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-a"))
                .await
                .is_none()
        );

        let independent = custody
            .reserve_probe_resolution(Some("checks-b"))
            .await
            .unwrap();
        let conflicting_workspace = inputs::ProbeCleanupScope {
            resource: "checks-b".into(),
            workspace_identity: first_scope.workspace_identity.clone(),
        };
        assert!(
            !custody
                .pin_probe_resolution_scope(&independent, Some(&conflicting_workspace))
                .await
        );
        let independent_scope = inputs::ProbeCleanupScope {
            resource: "checks-b".into(),
            workspace_identity: json!({"workspace":"b"}),
        };
        assert!(
            custody
                .pin_probe_resolution_scope(&independent, Some(&independent_scope))
                .await
        );

        let blocking_owner = first.clone();
        drop(first);
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-a"))
                .await
                .is_none()
        );
        drop(blocking_owner);
        let replacement = custody
            .reserve_probe_resolution(Some("checks-a"))
            .await
            .unwrap();
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-b"))
                .await
                .is_none()
        );
        drop(replacement);
        drop(independent);
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-b"))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn independent_probe_resolution_io_overlaps_and_remains_in_store_custody() {
        let fixture = fixture().await;
        let custody = &fixture.owner.store.check_child_custody;
        let first = custody
            .reserve_probe_resolution(Some("checks-a"))
            .await
            .unwrap();
        let second = custody
            .reserve_probe_resolution(Some("checks-b"))
            .await
            .unwrap();
        assert_eq!(fixture.owner.store.check_child_custody_count().await, 2);
        let started = Arc::new((std::sync::Mutex::new(0usize), std::sync::Condvar::new()));
        let overlap =
            |reservation: Arc<ProbeResolutionReservation>,
             started: Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>| {
                move |_files: ArtifactFiles| {
                    let _reservation = reservation;
                    let (count, changed) = &*started;
                    let mut count = count.lock().unwrap();
                    *count += 1;
                    changed.notify_all();
                    let (count, _) = changed
                        .wait_timeout_while(count, Duration::from_secs(2), |count| *count < 2)
                        .unwrap();
                    if *count != 2 {
                        return Err(Error::new(
                            "TEST_PROBE_SERIALIZED",
                            "independent I/O did not overlap",
                        ));
                    }
                    Ok(())
                }
            };
        let (a, b) = tokio::join!(
            fixture
                .owner
                .store
                .file_io(overlap(first.clone(), started.clone())),
            fixture
                .owner
                .store
                .file_io(overlap(second.clone(), started)),
        );
        a.unwrap();
        b.unwrap();
        assert!(
            custody
                .reserve_probe_resolution(Some("checks-a"))
                .await
                .is_none()
        );
        drop(first);
        assert_eq!(fixture.owner.store.check_child_custody_count().await, 1);
        drop(second);
        assert_eq!(fixture.owner.store.check_child_custody_count().await, 0);
        fixture.owner.close().await.unwrap();
    }

    struct Fixture {
        directory: PathBuf,
        owner: StoreOwner,
        principal: Principal,
        writer: Principal,
        config: Arc<Config>,
    }

    async fn fixture() -> Fixture {
        let directory = std::env::temp_dir().join(format!("swarm-check-cache-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let directory = root.path.clone();
        let credential = bootstrap_credential(&root.path).unwrap();
        let executable = std::env::current_exe().unwrap();
        let profile = |revision: &str| CheckProfile {
            profile_id: "strict".into(),
            profile_revision: revision.into(),
            executable: executable.clone(),
            // libtest --list is a finite, successful command with observable
            // stdout, so the end-to-end test records real process/output proof.
            args: vec!["--list".into()],
            parser: Parser::ExitCode,
            resource: "cache-tests".into(),
            environment: BTreeMap::new(),
            inherit_env: Vec::new(),
            expected_targets: Vec::new(),
            reproducible: true,
            fingerprint_env: Vec::new(),
            versioned_inputs: BTreeMap::new(),
        };
        let mut config = Config::default();
        config.storage.data_dir = directory.clone();
        config.checks.enabled = true;
        config.checks.profiles = vec![profile("v1"), profile("v2"), profile("v3")];
        let config = Arc::new(config);
        let owner = StoreOwner::start(root, config.clone(), credential.clone())
            .await
            .unwrap();
        let principal = owner.store.authenticate(credential).await.unwrap();
        let writer = Principal {
            link_id: model::new_id(),
            client_id: "cache-writer".into(),
            role: Role::Manager,
        };
        let writer_id = writer.client_id.clone();
        owner
            .store
            .run(move |db| {
                super::set_meta(
                    db,
                    &format!("client:{writer_id}"),
                    &json!({"role":Role::Manager,"disabled":false}),
                )
            })
            .await
            .unwrap();
        Fixture {
            directory,
            owner,
            principal,
            writer,
            config,
        }
    }

    async fn probe_error_fixture() -> Fixture {
        let directory =
            std::env::temp_dir().join(format!("swarm-check-probe-error-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let directory = root.path.clone();
        let credential = bootstrap_credential(&root.path).unwrap();
        let executable = std::env::current_exe().unwrap();
        let profile = |profile_revision: &str, resource: &str| CheckProfile {
            profile_id: "strict".into(),
            profile_revision: profile_revision.into(),
            executable: executable.clone(),
            args: vec!["--list".into()],
            parser: Parser::ExitCode,
            resource: resource.into(),
            environment: BTreeMap::new(),
            inherit_env: Vec::new(),
            expected_targets: Vec::new(),
            reproducible: true,
            fingerprint_env: Vec::new(),
            versioned_inputs: BTreeMap::new(),
        };
        let mut config = Config::default();
        config.storage.data_dir = directory.clone();
        config.checks.enabled = true;
        config.checks.profiles = vec![
            profile("pr61-owner", "checks-a"),
            profile("pr61-probe-error", "checks-b"),
        ];
        let config = Arc::new(config);
        let owner = StoreOwner::start(root, config.clone(), credential.clone())
            .await
            .unwrap();
        let principal = owner.store.authenticate(credential).await.unwrap();
        let writer = Principal {
            link_id: model::new_id(),
            client_id: "pr61-probe-error-writer".into(),
            role: Role::Manager,
        };
        let writer_id = writer.client_id.clone();
        owner
            .store
            .run(move |db| {
                super::set_meta(
                    db,
                    &format!("client:{writer_id}"),
                    &json!({"role":Role::Manager,"disabled":false}),
                )
            })
            .await
            .unwrap();
        Fixture {
            directory,
            owner,
            principal,
            writer,
            config,
        }
    }

    async fn probe_plan_snapshot(
        fixture: &Fixture,
        attempt_id: &str,
        candidate_ref: &str,
        profile_revision: &str,
    ) -> CheckPlanInputs {
        let params = json!({
            "client_request_id":model::new_id(),
            "attempt_id":attempt_id,
            "candidate_ref":candidate_ref,
            "profile_id":"strict",
            "profile_revision":profile_revision,
        });
        let principal = fixture.principal.clone();
        let config = fixture.config.clone();
        fixture
            .owner
            .store
            .run(move |db| {
                let principal = current_principal(db, principal)?;
                plan_inputs(db, &principal, &params, &config)
            })
            .await
            .expect("capture preflight snapshot for the selected profile")
    }

    async fn create_attempt(
        fixture: &Fixture,
        project_id: &str,
        baseline_candidate_ref: Option<&str>,
    ) -> (String, String) {
        let mut spec = json!({
            "objective":"Exercise verified CheckRunner admission",
            "phase":"verification",
            "owner_policy_id":"owner-policy-v1",
            "requirements":[{"id":"R1","statement":"Preserve source identity and process evidence"}],
        });
        if let Some(reference) = baseline_candidate_ref {
            spec["baseline_candidate_ref"] = json!(reference);
        }
        let task = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.create".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "project_id":project_id,
                    "spec":spec,
                }),
            )
            .await
            .unwrap();
        let task_id = task["task_id"].as_str().unwrap().to_owned();
        let claim = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
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
        (task_id, attempt_id)
    }

    async fn create_acceptance_attempt(
        fixture: &Fixture,
        project_id: &str,
        profile_revision: Option<&str>,
        baseline_candidate_ref: Option<&str>,
    ) -> (String, String) {
        let check_profiles = profile_revision
            .map(|revision| json!([{"profile_id":"strict","profile_revision":revision}]))
            .unwrap_or_else(|| json!([]));
        let mut spec = json!({
            "acceptance":{"schema_id":"swarm.acceptance_policy","schema_version":2},
            "objective":"Exercise verified CheckRunner admission",
            "phase":"verification",
            "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
            "requirements":[{
                "id":"R1",
                "statement":"Preserve source identity and process evidence",
                "verification":{"review_required":true,"check_profiles":check_profiles},
            }],
        });
        if let Some(reference) = baseline_candidate_ref {
            spec["baseline_candidate_ref"] = json!(reference);
        }
        let task = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.create".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "project_id":project_id,
                    "spec":spec,
                }),
            )
            .await
            .unwrap();
        let task_id = task["task_id"].as_str().unwrap().to_owned();
        let claim = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.claim".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "task_id":task_id,
                    "expected_revision":1,
                    "owner_id":fixture.writer.client_id.clone(),
                }),
            )
            .await
            .unwrap();
        let attempt_id = claim["attempt_id"].as_str().unwrap().to_owned();
        (task_id, attempt_id)
    }

    async fn source_snapshot(
        fixture: &Fixture,
        task_id: &str,
        attempt_id: &str,
        content: &[u8],
    ) -> ArtifactRecord {
        let artifact_id = format!("source-{}", model::digest(model::new_id().as_bytes()));
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
        let metadata = json!({
            "task_id":task_id,
            "attempt_id":attempt_id,
            "task_revision":1,
            "commit":manifest.commit,
            "tree":manifest.tree,
            "file_count":1,
            "coverage":"complete",
        });
        let (record, bytes) =
            ArtifactFiles::document("source_snapshot", &artifact_id, &json!(manifest), metadata)
                .unwrap();
        let source_dir = fixture.directory.join("sources").join(&record.artifact_id);
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(source_dir.join("fixture.txt"), content).unwrap();
        ArtifactFiles::new(&fixture.directory)
            .unwrap()
            .publish(&record, &bytes)
            .unwrap();
        fixture
            .owner
            .store
            .run({
                let record = record.clone();
                move |db| artifact(db, &record)
            })
            .await
            .unwrap();
        record
    }

    async fn submit_candidate(
        fixture: &Fixture,
        attempt_id: &str,
        candidate: &ArtifactRecord,
    ) -> (String, i64, String) {
        let submitted = fixture
            .owner
            .store
            .call(
                fixture.writer.clone(),
                "task.submit".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "attempt_id":attempt_id,
                    "expected_revision":1,
                    "expected_submission_ref":null,
                    "candidate_ref":candidate.artifact_id,
                    "summary":"Submit the exact retained source candidate.",
                    "claims":[],
                }),
            )
            .await
            .unwrap();
        let submission_operation_id = submitted["operation_id"].as_str().unwrap().to_owned();
        let submit_operation = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "operation.get".into(),
                json!({"operation_id":submission_operation_id}),
            )
            .await
            .unwrap();
        assert_eq!(submit_operation["result"]["outcome"], "applied");
        let submission_ref = submit_operation["result"]["submission_ref"]
            .as_str()
            .unwrap()
            .to_owned();
        let submission = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.submission".into(),
                json!({"submission_ref":submission_ref}),
            )
            .await
            .unwrap();
        let feedback_cursor = submission["latest_feedback_observation_id"]
            .as_i64()
            .unwrap();
        (submission_ref, feedback_cursor, submission_operation_id)
    }

    fn acceptance_request(
        attempt_id: &str,
        submission_ref: &str,
        candidate_ref: &str,
        feedback_cursor: i64,
        check_ids: &[String],
    ) -> Value {
        json!({
            "client_request_id":model::new_id(),
            "attempt_id":attempt_id,
            "expected_revision":1,
            "submission_ref":submission_ref,
            "candidate_ref":candidate_ref,
            "expected_feedback_observation_id":feedback_cursor,
            "reason":"The frozen requirement evidence supports this exact candidate.",
            "reviews":[{
                "requirement_id":"R1",
                "rationale":"The retained candidate addresses R1.",
                "evidence":[candidate_ref],
            }],
            "check_ids":check_ids,
        })
    }

    async fn accept_source(
        fixture: &Fixture,
        attempt_id: &str,
        candidate: &ArtifactRecord,
        check_ids: &[String],
    ) -> String {
        let (submission_ref, feedback_cursor, _) =
            submit_candidate(fixture, attempt_id, candidate).await;
        let receipt = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.accept".into(),
                acceptance_request(
                    attempt_id,
                    &submission_ref,
                    &candidate.artifact_id,
                    feedback_cursor,
                    check_ids,
                ),
            )
            .await
            .unwrap();
        let acceptance_operation_id = receipt["operation_id"].as_str().unwrap().to_owned();
        let acceptance = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "operation.get".into(),
                json!({"operation_id":acceptance_operation_id}),
            )
            .await
            .unwrap();
        assert_eq!(
            acceptance["result"]["outcome"], "applied",
            "public task.accept result: {acceptance}"
        );
        acceptance_operation_id
    }

    async fn set_check_operation_id(fixture: &Fixture, check_id: &str, operation_id: &str) {
        let check_id = check_id.to_owned();
        let operation_id = operation_id.to_owned();
        fixture
            .owner
            .store
            .run(move |db| {
                db.execute(
                    "UPDATE check_runs SET operation_id=?2 WHERE check_id=?1",
                    params![check_id, operation_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn set_cached_from(fixture: &Fixture, check_id: &str, source_check_id: &str) {
        let check_id = check_id.to_owned();
        let source_check_id = source_check_id.to_owned();
        fixture
            .owner
            .store
            .run(move |db| {
                db.execute(
                    "UPDATE check_runs SET cached_from_check_id=?2 WHERE check_id=?1",
                    params![check_id, source_check_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    fn test_version_probe(
        program: &Path,
        _args: &[String],
        env: &BTreeMap<String, String>,
        cwd: &Path,
    ) -> Result<Vec<u8>> {
        // The libtest executable cannot route `check-worker` like the normal
        // host binary. Its bounded `--list` output is a test-only synthetic
        // executable fingerprint (pinned with the executable hash), not a
        // native tool-version claim. Check execution below still goes through
        // the real worker process-control and artifact pipeline.
        let version_args = ["--list"];
        let output = Command::new(program)
            .args(version_args)
            .current_dir(cwd)
            .env_clear()
            .envs(env)
            .stdin(Stdio::null())
            .output()?;
        if !output.status.success() || output.stdout.len() > 1024 * 1024 {
            return Err(Error::new(
                "CHECK_INPUT_RESOLUTION",
                "test version probe failed or exceeded its output bound",
            ));
        }
        Ok(output.stdout)
    }

    async fn resolve_for_test(
        store: &Store,
        principal: Principal,
        params: Value,
        config: Arc<Config>,
    ) -> CheckPlanResolution {
        let snapshot = store
            .run({
                let params = params.clone();
                let config = config.clone();
                move |db| {
                    let principal = current_principal(db, principal)?;
                    plan_inputs(db, &principal, &params, &config)
                }
            })
            .await
            .unwrap();
        let context_fingerprint = snapshot.context_fingerprint.clone();
        let resolved_context = context_fingerprint.clone();
        let data_dir = store.data_dir.clone();
        let prepared = store
            .file_io(move |files| {
                let candidate = source::verified_content(&files, &data_dir, &snapshot.candidate)?;
                let baseline = match snapshot.baseline.as_ref() {
                    Some(record) => source::verified_content(&files, &data_dir, record).ok(),
                    None => None,
                };
                let baseline_reason = snapshot.baseline_reason.or_else(|| {
                    (snapshot.baseline.is_some() && baseline.is_none())
                        .then(|| "baseline_source_unverified".to_owned())
                });
                let mut resolved = inputs::resolve_with_probe_for_test(
                    &snapshot.profile,
                    &candidate,
                    baseline.as_ref(),
                    test_version_probe,
                )?;
                if let Some(reason) = baseline_reason {
                    resolved.add_widening_reason(&reason)?;
                }
                Ok(PreparedCheckPlan {
                    context_fingerprint: resolved_context,
                    project_id: snapshot.project_id,
                    resolved_inputs: resolved.resolved_inputs,
                    scope_plan: serde_json::to_value(&resolved.scope_plan)?,
                    input_fingerprint: resolved.input_fingerprint,
                    reproducible: snapshot.profile.reproducible,
                })
            })
            .await;
        let (result, cache_hit) = match prepared {
            Ok(prepared) if prepared.resolved_inputs["cache_reusable"] == true => {
                let lookup = prepared.clone();
                let project_id = prepared.project_id.clone();
                let cache_candidate = store
                    .run(move |db| completed_cache_candidate(db, &project_id, &lookup))
                    .await
                    .unwrap();
                let hit = if let Some(candidate) = cache_candidate {
                    let expected = prepared.clone();
                    let data_dir = store.data_dir.clone();
                    store
                        .file_io(move |files| {
                            verify_cache_candidate(&files, &data_dir, &expected, candidate)
                        })
                        .await
                        .unwrap_or(None)
                } else {
                    None
                };
                (Ok(prepared), hit)
            }
            Ok(prepared) => (Ok(prepared), None),
            Err(error) => (Err(error), None),
        };
        CheckPlanResolution {
            context_fingerprint,
            result,
            cache_hit,
        }
    }

    async fn admit_check(
        fixture: &Fixture,
        attempt_id: &str,
        candidate: &ArtifactRecord,
        revision: &str,
    ) -> Value {
        let params = json!({
            "client_request_id":model::new_id(),
            "attempt_id":attempt_id,
            "candidate_ref":candidate.artifact_id,
            "profile_id":"strict",
            "profile_revision":revision,
        });
        let resolution = resolve_for_test(
            &fixture.owner.store,
            fixture.principal.clone(),
            params.clone(),
            fixture.config.clone(),
        )
        .await;
        assert!(
            resolution.result.is_ok(),
            "plan resolution failed: {:?}",
            resolution.result.as_ref().err()
        );
        fixture
            .owner
            .store
            .run({
                let principal = fixture.principal.clone();
                let config = fixture.config.clone();
                move |db| {
                    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let principal = current_principal(&tx, principal)?;
                    let value = mutate_in_transaction_with_check_plan(
                        &tx,
                        &principal,
                        "check.run",
                        &params,
                        &config,
                        model::now_ms()?,
                        Some(&resolution),
                    )?;
                    tx.commit()?;
                    value
                }
            })
            .await
            .unwrap()
    }

    async fn run_check_process(fixture: &Fixture, check_id: &str) {
        let config = fixture.config.clone();
        let root = fixture.directory.clone();
        let id = check_id.to_owned();
        let work = fixture
            .owner
            .store
            .run(move |db| {
                next(db, &config, root)?
                    .filter(|work| work.check_id == id)
                    .ok_or_else(|| Error::new("TEST_STATE", "queued CheckRun was not next"))
            })
            .await
            .unwrap();
        let directory = worker::directory(&work.data_dir, &work.check_id).unwrap();
        std::fs::create_dir_all(&directory).unwrap();
        let work_path = directory.join("work.json");
        std::fs::write(
            &work_path,
            model::canonical(&json!(work)).unwrap().as_bytes(),
        )
        .unwrap();
        let worker_path = work_path.clone();
        let worker_task = tokio::task::spawn_blocking(move || worker::run(&worker_path));
        let identity_path = directory.join("worker.json");
        let identity = timeout(Duration::from_secs(20), async {
            loop {
                if let Ok(bytes) = std::fs::read(&identity_path) {
                    break serde_json::from_slice::<Value>(&bytes).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker did not publish its process identity");
        let expected_identity = identity.clone();
        let expected_check = work.check_id.clone();
        let ready_work = work.clone();
        let persisted = fixture
            .owner
            .store
            .run(move |db| ready(db, &ready_work, identity))
            .await
            .unwrap();
        assert!(
            persisted,
            "CheckRun {expected_check} did not accept its worker identity"
        );
        worker::allow(&work).unwrap();
        worker_task.await.unwrap().unwrap();
        let completion =
            worker::completion(&work, &ArtifactFiles::new(&fixture.directory).unwrap())
                .unwrap()
                .expect("actual worker did not publish completion evidence");
        fixture
            .owner
            .store
            .run(move |db| finish(db, &work, completion))
            .await
            .unwrap();
        let result = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "check.get".into(),
                json!({"check_id":check_id}),
            )
            .await
            .unwrap();
        assert_eq!(result["state"], "passed");
        assert_eq!(result["exit_code"], 0);
        assert!(!result["resource_released_at_ms"].is_null());
        assert!(
            result.get("process").is_none(),
            "public check.get must not expose the raw process identity"
        );
        let retained_identity = fixture
            .owner
            .store
            .run({
                let root = fixture.directory.clone();
                let id = check_id.to_owned();
                move |db| Ok(super::work(db, &id, root)?.expected_worker)
            })
            .await
            .unwrap();
        assert_eq!(
            retained_identity,
            Some(expected_identity),
            "the internal Store read retains the exact accepted worker identity"
        );
    }

    async fn output_paths(fixture: &Fixture, operation_id: &str) -> Vec<PathBuf> {
        let operation_id = operation_id.to_owned();
        let refs = fixture
            .owner
            .store
            .run(move |db| {
                Ok(operations::get_operation(db, &operation_id)?["result"]["output_refs"].clone())
            })
            .await
            .unwrap();
        refs.as_array()
            .unwrap()
            .iter()
            .map(|reference| {
                fixture
                    .directory
                    .join("artifacts")
                    .join(format!("{}.bin", reference.as_str().unwrap()))
            })
            .collect()
    }

    async fn corrupt_retained_cargo_coverage(fixture: &Fixture, check_id: &str) {
        let check_id = check_id.to_owned();
        let query_check_id = check_id.clone();
        let (mut spec, scope_plan) = fixture
            .owner
            .store
            .run(move |db| {
                let raw: String = db.query_row(
                    "SELECT spec_json FROM check_runs WHERE check_id=?1",
                    [&query_check_id],
                    |row| row.get(0),
                )?;
                let spec: Value = serde_json::from_str(&raw)?;
                Ok((spec.clone(), spec["scope_plan"].clone()))
            })
            .await
            .unwrap();
        let mut profile: CheckProfile = serde_json::from_value(spec["profile"].clone()).unwrap();
        profile.parser = Parser::CargoJson;
        profile.expected_targets = vec!["fixture-workspace".into()];
        let identity = inputs::profile_identity(&profile).unwrap();
        let fingerprint = "fixture-invalid-cargo-coverage".to_owned();
        let mut resolved = spec["resolved_inputs"].clone();
        resolved["profile_identity"] = identity.clone();
        resolved["profile_sha256"] = json!(model::digest(
            model::canonical(&identity).unwrap().as_bytes()
        ));
        resolved["expected_targets"] = json!(profile.expected_targets);
        resolved["input_fingerprint"] = json!(fingerprint);
        resolved["cache_reusable"] = json!(true);
        spec["profile"] = json!(profile);
        spec["resolved_inputs"] = resolved.clone();
        spec["input_fingerprint"] = json!(fingerprint);
        spec["cache_key"] = json!(fingerprint);
        let incomplete_cargo_coverage = json!({
            "requested":["fixture-workspace"],
            "checked":["fixture-workspace"],
            "gaps":[],
            // Deliberately tampered: CargoJson requires build_finished=true
            // and errors=0 even when the generic requested/checked test passes.
        });
        assert!(
            incomplete_cargo_coverage["gaps"]
                .as_array()
                .is_some_and(Vec::is_empty)
        );
        let update_id = check_id.clone();
        let stored_fingerprint = fingerprint.clone();
        fixture
            .owner
            .store
            .run(move |db| {
                db.execute(
                    "UPDATE check_runs SET cache_key=?2,spec_json=?3,coverage_json=?4 WHERE check_id=?1",
                    params![
                        update_id,
                        stored_fingerprint,
                        model::canonical(&spec)?,
                        model::canonical(&incomplete_cargo_coverage)?
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let plan = PreparedCheckPlan {
            context_fingerprint: "fixture-context".into(),
            project_id: "cache-project".into(),
            resolved_inputs: resolved,
            scope_plan,
            input_fingerprint: fingerprint,
            reproducible: true,
        };
        let project_id = plan.project_id.clone();
        let retained = fixture
            .owner
            .store
            .run(move |db| completed_cache_candidate(db, &project_id, &plan))
            .await
            .unwrap();
        assert!(
            retained.is_none(),
            "tampered CargoJson coverage must not be retained as a cache origin"
        );
    }

    #[tokio::test]
    async fn reusable_admission_requires_current_accepted_original_process_and_intact_outputs() {
        let fixture = fixture().await;
        let content = b"same captured source for every cache admission";

        let (origin_task, origin_attempt) =
            create_acceptance_attempt(&fixture, "cache-project", Some("v1"), None).await;
        let origin_source = source_snapshot(&fixture, &origin_task, &origin_attempt, content).await;
        let original = admit_check(&fixture, &origin_attempt, &origin_source, "v1").await;
        assert_eq!(original["state"], "queued");
        let original_check = original["check_id"].as_str().unwrap().to_owned();
        let original_operation = original["operation_id"].as_str().unwrap().to_owned();
        assert_ne!(
            original_check, original_operation,
            "CheckRunID and OperationID are separate identities"
        );
        run_check_process(&fixture, &original_check).await;
        let original_status = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "check.get".into(),
                json!({"check_id":original_check}),
            )
            .await
            .unwrap();
        assert_eq!(original_status["operation_id"], original_operation);
        assert_eq!(original_status["state"], "passed");
        let origin_acceptance = accept_source(
            &fixture,
            &origin_attempt,
            &origin_source,
            std::slice::from_ref(&original_check),
        )
        .await;

        let (cached_task, cached_attempt) =
            create_acceptance_attempt(&fixture, "cache-project", Some("v1"), None).await;
        let cached_source = source_snapshot(&fixture, &cached_task, &cached_attempt, content).await;
        let cached = admit_check(&fixture, &cached_attempt, &cached_source, "v1").await;
        assert_eq!(cached["cached"], true);
        assert_eq!(cached["cached_from_check_id"], original_check);
        let cached_check = cached["check_id"].as_str().unwrap().to_owned();
        let cached_operation = cached["operation_id"].as_str().unwrap().to_owned();
        assert_ne!(
            cached_check, cached_operation,
            "cached CheckRunID and OperationID are separate identities"
        );
        let cached_status = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "check.get".into(),
                json!({"check_id":cached_check}),
            )
            .await
            .unwrap();
        assert_eq!(cached_status["cached_from"], original_check);
        assert!(cached_status["process"].is_null());
        assert!(cached_status["exit_code"].is_null());
        assert!(cached_status["resource_claimed_at_ms"].is_null());
        assert!(cached_status["resource_released_at_ms"].is_null());

        let (cached_submission, cached_feedback_cursor, _) =
            submit_candidate(&fixture, &cached_attempt, &cached_source).await;
        let cached_acceptance_receipt = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.accept".into(),
                acceptance_request(
                    &cached_attempt,
                    &cached_submission,
                    &cached_source.artifact_id,
                    cached_feedback_cursor,
                    std::slice::from_ref(&cached_check),
                ),
            )
            .await
            .unwrap();
        let cached_acceptance_id = cached_acceptance_receipt["operation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let cached_acceptance = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "operation.get".into(),
                json!({"operation_id":cached_acceptance_id}),
            )
            .await
            .unwrap();
        assert_eq!(
            cached_acceptance["result"]["outcome"], "applied",
            "real task.accept must consume the legitimate cache hit: {cached_acceptance}"
        );
        let cached_decision = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.acceptance".into(),
                json!({"acceptance_operation_id":cached_acceptance_id}),
            )
            .await
            .unwrap();
        assert_eq!(cached_decision["check_ids"], json!([cached_check]));

        let (refusal_task, refusal_attempt) =
            create_acceptance_attempt(&fixture, "cache-project", Some("v1"), None).await;
        let refusal_source =
            source_snapshot(&fixture, &refusal_task, &refusal_attempt, content).await;
        let refusal_admission =
            admit_check(&fixture, &refusal_attempt, &refusal_source, "v1").await;
        assert_eq!(refusal_admission["cached"], true);
        let refusal_check = refusal_admission["check_id"].as_str().unwrap().to_owned();
        let (refusal_submission, refusal_feedback_cursor, refusal_submit_operation) =
            submit_candidate(&fixture, &refusal_attempt, &refusal_source).await;

        // The schema requires a retained Operation reference. Pointing the
        // source CheckRun at a different, existing task.submit Operation is a
        // mismatched source Operation and must not consume the cached run.
        set_check_operation_id(&fixture, &original_check, &refusal_submit_operation).await;
        let mismatched_source_operation = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.accept".into(),
                acceptance_request(
                    &refusal_attempt,
                    &refusal_submission,
                    &refusal_source.artifact_id,
                    refusal_feedback_cursor,
                    std::slice::from_ref(&refusal_check),
                ),
            )
            .await
            .expect_err("cached acceptance rejects a source CheckRun/Operation mismatch");
        assert_eq!(mismatched_source_operation.code, "CHECK_NOT_READY");
        set_check_operation_id(&fixture, &original_check, &original_operation).await;

        // A cache hit whose source is another cache row would form a chain;
        // acceptance must continue to require the direct original process.
        set_cached_from(&fixture, &refusal_check, &cached_check).await;
        let chained_cache = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.accept".into(),
                acceptance_request(
                    &refusal_attempt,
                    &refusal_submission,
                    &refusal_source.artifact_id,
                    refusal_feedback_cursor,
                    std::slice::from_ref(&refusal_check),
                ),
            )
            .await
            .expect_err("cached acceptance rejects a chained cache source");
        assert_eq!(chained_cache.code, "CHECK_NOT_READY");
        set_cached_from(&fixture, &refusal_check, &original_check).await;

        let invalidated = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.invalidate_acceptance".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "acceptance_operation_id":origin_acceptance,
                    "reason":"Revoke the source acceptance used to authorize cache reuse.",
                    "evidence":[original_check],
                }),
            )
            .await
            .unwrap();
        assert_eq!(invalidated["invalidated"], true);
        let revoked_source = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.accept".into(),
                acceptance_request(
                    &refusal_attempt,
                    &refusal_submission,
                    &refusal_source.artifact_id,
                    refusal_feedback_cursor,
                    std::slice::from_ref(&refusal_check),
                ),
            )
            .await
            .expect_err("cached acceptance rejects a revoked source acceptance");
        assert_eq!(revoked_source.code, "CHECK_NOT_READY");

        let (chain_task, chain_attempt) = create_attempt(&fixture, "cache-project", None).await;
        let chain_source = source_snapshot(&fixture, &chain_task, &chain_attempt, content).await;
        let after_invalidation = admit_check(&fixture, &chain_attempt, &chain_source, "v1").await;
        assert_eq!(after_invalidation["state"], "queued");
        assert_ne!(after_invalidation["cached"], true);
        // This cache miss is now the oldest queued CheckRun. Run it before
        // admitting/running the v2 origin below, preserving the host's FIFO
        // scheduler behavior. Its source is intentionally not accepted, so
        // this process result cannot become a reusable origin.
        let after_invalidation_check = after_invalidation["check_id"].as_str().unwrap().to_owned();
        run_check_process(&fixture, &after_invalidation_check).await;

        // New profile revisions produce distinct fingerprints. Their only
        // process-passed origin is verified separately below; deleting one
        // output and tampering with another must turn that candidate into a miss.
        let (second_origin_task, second_origin_attempt) =
            create_acceptance_attempt(&fixture, "cache-project", Some("v2"), None).await;
        let second_origin_source = source_snapshot(
            &fixture,
            &second_origin_task,
            &second_origin_attempt,
            content,
        )
        .await;
        let second_original = admit_check(
            &fixture,
            &second_origin_attempt,
            &second_origin_source,
            "v2",
        )
        .await;
        assert_eq!(second_original["state"], "queued");
        let second_check = second_original["check_id"].as_str().unwrap().to_owned();
        let second_operation = second_original["operation_id"].as_str().unwrap().to_owned();
        assert_ne!(second_check, second_operation);
        run_check_process(&fixture, &second_check).await;
        let _second_acceptance = accept_source(
            &fixture,
            &second_origin_attempt,
            &second_origin_source,
            std::slice::from_ref(&second_check),
        )
        .await;
        let outputs = output_paths(&fixture, &second_operation).await;
        assert_eq!(
            outputs.len(),
            2,
            "the real process should seal stdout and stderr"
        );
        let damaged = std::fs::read(&outputs[0]).unwrap();
        let removed = std::fs::read(&outputs[1]).unwrap();
        std::fs::write(&outputs[0], b"tampered immutable check output").unwrap();
        std::fs::remove_file(&outputs[1]).unwrap();

        let (tampered_task, tampered_attempt) =
            create_attempt(&fixture, "cache-project", None).await;
        let tampered_source =
            source_snapshot(&fixture, &tampered_task, &tampered_attempt, content).await;
        let tampered_miss = admit_check(&fixture, &tampered_attempt, &tampered_source, "v2").await;
        assert_eq!(tampered_miss["state"], "queued");
        assert_ne!(tampered_miss["cached"], true);
        std::fs::write(&outputs[0], damaged).unwrap();
        std::fs::write(&outputs[1], removed).unwrap();

        // A retained CargoJson row cannot reuse generic target coverage after
        // its parser-specific terminal fields have been removed.
        corrupt_retained_cargo_coverage(&fixture, &second_check).await;

        let (changed_task, changed_attempt) = create_attempt(&fixture, "cache-project", None).await;
        let changed_source =
            source_snapshot(&fixture, &changed_task, &changed_attempt, content).await;
        let changed_profile = admit_check(&fixture, &changed_attempt, &changed_source, "v3").await;
        assert_eq!(changed_profile["state"], "queued");
        assert_ne!(changed_profile["cached"], true);

        fixture.owner.close().await.unwrap();
        std::fs::remove_dir_all(fixture.directory).unwrap();
    }

    #[tokio::test]
    async fn unproven_claim_baseline_is_preserved_as_a_wide_scope_reason() {
        let fixture = fixture().await;
        let missing_baseline = format!("source-{}", model::digest(b"not-registered"));
        let (task_id, attempt_id) =
            create_attempt(&fixture, "baseline-project", Some(&missing_baseline)).await;
        let attempt = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "attempt.get".into(),
                json!({"attempt_id":attempt_id}),
            )
            .await
            .unwrap();
        assert_eq!(
            attempt["task_snapshot"]["baseline_candidate"]["status"],
            "wide"
        );
        assert_eq!(
            attempt["task_snapshot"]["baseline_candidate"]["reason"],
            "baseline_artifact_unregistered"
        );

        let candidate = source_snapshot(
            &fixture,
            &task_id,
            &attempt_id,
            b"candidate without a verified baseline",
        )
        .await;
        let admitted = admit_check(&fixture, &attempt_id, &candidate, "v1").await;
        assert_eq!(admitted["state"], "queued");
        let check_id = admitted["check_id"].as_str().unwrap().to_owned();
        let (scope, resolved) = fixture
            .owner
            .store
            .run(move |db| {
                let raw: String = db.query_row(
                    "SELECT spec_json FROM check_runs WHERE check_id=?1",
                    [&check_id],
                    |row| row.get(0),
                )?;
                let spec: Value = serde_json::from_str(&raw)?;
                Ok((spec["scope_plan"].clone(), spec["resolved_inputs"].clone()))
            })
            .await
            .unwrap();
        assert_eq!(scope["mode"], "wide");
        assert_eq!(resolved["baseline_content_sha256"], Value::Null);
        assert!(
            scope["widening_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "baseline_artifact_unregistered")
        );

        fixture.owner.close().await.unwrap();
        std::fs::remove_dir_all(fixture.directory).unwrap();
    }

    #[tokio::test]
    async fn accepted_baseline_is_frozen_consumed_and_revocation_stales_admission() {
        let fixture = fixture().await;
        let (baseline_task, baseline_attempt) =
            create_acceptance_attempt(&fixture, "baseline-project", None, None).await;
        let baseline_source = source_snapshot(
            &fixture,
            &baseline_task,
            &baseline_attempt,
            b"accepted baseline bytes",
        )
        .await;
        let baseline_content_sha256 = source::verified_content(
            &ArtifactFiles::new(&fixture.directory).unwrap(),
            &fixture.directory,
            &baseline_source,
        )
        .unwrap()
        .content_sha256;
        assert_ne!(baseline_content_sha256, baseline_source.content_digest);
        let baseline_acceptance =
            accept_source(&fixture, &baseline_attempt, &baseline_source, &[]).await;

        let (target_task, target_attempt) = create_attempt(
            &fixture,
            "baseline-project",
            Some(&baseline_source.artifact_id),
        )
        .await;
        let attempt = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "attempt.get".into(),
                json!({"attempt_id":target_attempt}),
            )
            .await
            .unwrap();
        assert_eq!(
            attempt["task_snapshot"]["baseline_candidate"]["status"],
            "verified"
        );
        assert_eq!(
            attempt["task_snapshot"]["baseline_candidate"]["candidate_ref"],
            baseline_source.artifact_id
        );

        let target_source = source_snapshot(
            &fixture,
            &target_task,
            &target_attempt,
            b"candidate bytes after the accepted baseline",
        )
        .await;
        let params = json!({
            "client_request_id":model::new_id(),
            "attempt_id":target_attempt,
            "candidate_ref":target_source.artifact_id,
            "profile_id":"strict",
            "profile_revision":"v1",
        });
        let resolution = resolve_for_test(
            &fixture.owner.store,
            fixture.principal.clone(),
            params.clone(),
            fixture.config.clone(),
        )
        .await;
        let resolved = resolution.result.as_ref().unwrap();
        assert_eq!(
            resolved.resolved_inputs["baseline_content_sha256"],
            baseline_content_sha256
        );
        let first_resolution = resolution.clone();
        let first_params = params.clone();
        let first_config = fixture.config.clone();
        let first_principal = fixture.principal.clone();
        let first_admission = fixture
            .owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let principal = current_principal(&tx, first_principal)?;
                let value = mutate_in_transaction_with_check_plan(
                    &tx,
                    &principal,
                    "check.run",
                    &first_params,
                    &first_config,
                    model::now_ms()?,
                    Some(&first_resolution),
                )?;
                tx.commit()?;
                value
            })
            .await
            .unwrap();
        assert_eq!(first_admission["state"], "queued");
        let admitted_id = first_admission["check_id"].as_str().unwrap().to_owned();
        let retained_baseline = fixture
            .owner
            .store
            .run(move |db| {
                let raw: String = db.query_row(
                    "SELECT spec_json FROM check_runs WHERE check_id=?1",
                    [&admitted_id],
                    |row| row.get(0),
                )?;
                let spec: Value = serde_json::from_str(&raw)?;
                Ok(spec["resolved_inputs"]["baseline_content_sha256"].clone())
            })
            .await
            .unwrap();
        assert_eq!(retained_baseline, baseline_content_sha256);

        // Resolve another valid plan from the same accepted source, revoke its
        // acceptance before final admission, then ensure the stale plan creates
        // no CheckRun and cannot consume the old baseline reference.
        let (stale_task, stale_attempt) = create_attempt(
            &fixture,
            "baseline-project",
            Some(&baseline_source.artifact_id),
        )
        .await;
        let stale_source = source_snapshot(
            &fixture,
            &stale_task,
            &stale_attempt,
            b"candidate after baseline revocation",
        )
        .await;
        let stale_params = json!({
            "client_request_id":model::new_id(),
            "attempt_id":stale_attempt,
            "candidate_ref":stale_source.artifact_id,
            "profile_id":"strict",
            "profile_revision":"v1",
        });
        let stale_resolution = resolve_for_test(
            &fixture.owner.store,
            fixture.principal.clone(),
            stale_params.clone(),
            fixture.config.clone(),
        )
        .await;
        assert_eq!(
            stale_resolution.result.as_ref().unwrap().resolved_inputs["baseline_content_sha256"],
            baseline_content_sha256
        );
        let invalidated = fixture
            .owner
            .store
            .call(
                fixture.principal.clone(),
                "task.invalidate_acceptance".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "acceptance_operation_id":baseline_acceptance,
                    "reason":"Revoke the accepted baseline before stale admission.",
                    "evidence":[baseline_source.artifact_id],
                }),
            )
            .await
            .unwrap();
        assert_eq!(invalidated["invalidated"], true);
        let config = fixture.config.clone();
        let principal = fixture.principal.clone();
        let error = fixture
            .owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let principal = current_principal(&tx, principal)?;
                let value = mutate_in_transaction_with_check_plan(
                    &tx,
                    &principal,
                    "check.run",
                    &stale_params,
                    &config,
                    model::now_ms()?,
                    Some(&stale_resolution),
                )?;
                tx.commit()?;
                value
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, "CHECK_PLAN_STALE");
        let stale_attempt_id = stale_attempt.clone();
        let check_count = fixture
            .owner
            .store
            .run(move |db| {
                Ok(db.query_row(
                    "SELECT count(*) FROM check_runs WHERE attempt_id=?1",
                    [&stale_attempt_id],
                    |row| row.get::<_, i64>(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(check_count, 0);

        fixture.owner.close().await.unwrap();
        std::fs::remove_dir_all(fixture.directory).unwrap();
    }

    async fn queued_check(fixture: &Fixture, project_id: &str) -> (Value, Work) {
        let (task_id, attempt_id) = create_attempt(fixture, project_id, None).await;
        let candidate = source_snapshot(
            fixture,
            &task_id,
            &attempt_id,
            b"check child custody fault fixture",
        )
        .await;
        let admitted = admit_check(fixture, &attempt_id, &candidate, "v1").await;
        let check_id = admitted["check_id"].as_str().unwrap().to_owned();
        let root = fixture.directory.clone();
        let work = fixture
            .owner
            .store
            .run(move |db| work(db, &check_id, root))
            .await
            .unwrap();
        (admitted, work)
    }

    async fn child_custody_snapshot(
        store: &Store,
        check_id: &str,
    ) -> (
        u32,
        usize,
        Option<Value>,
        bool,
        Option<String>,
        Option<(u32, bool, Option<i32>)>,
    ) {
        let owners = store.check_child_custody.lock().await;
        let owner = owners
            .get(check_id)
            .expect("CheckRun child custody is retained");
        (
            owner.child.id(),
            std::ptr::from_ref(&owner.child) as usize,
            owner.launch.clone(),
            owner.launch_persistence_pending,
            owner.launch_error.as_ref().map(|error| error.code.clone()),
            owner.exit.map(|exit| (exit.pid, exit.success, exit.code)),
        )
    }

    #[tokio::test]
    async fn pre_spawn_failure_publication_retry_terminalizes_without_launching() {
        let fixture = fixture().await;
        let (admitted, work) = queued_check(&fixture, "pre-spawn-publication-retry").await;
        let store = fixture.owner.store.clone();
        let pin = crate::checks::model::ExecutorPin {
            executable: fixture.directory.join("mismatched-check-executor"),
            sha256: "a".repeat(64),
            artifact_id: "checks-fixture".into(),
            version: "1".into(),
        };
        std::fs::write(&pin.executable, b"untrusted check executor fixture").unwrap();
        let expected_error = standalone_host::prepare_and_spawn(&work, &pin).unwrap_err();
        assert_eq!(expected_error.code, "CONFLICT");
        let check_id = work.check_id.clone();
        store.run(move |db| {
            db.execute(
                "UPDATE check_runs SET spec_json=json_set(spec_json,'$.executor',json(?2)) WHERE check_id=?1",
                params![check_id, model::canonical(&json!(pin))?],
            )?;
            Ok(())
        }).await.unwrap();
        let directory = worker::directory(&fixture.directory, &work.check_id).unwrap();
        // The mismatched pinned image fails before spawn. Failure report publication
        // succeeds, but a directory at the completion path rejects its receipt.
        std::fs::create_dir_all(directory.join("completion.json")).unwrap();
        let (stop, stopping) = watch::channel(false);
        let first_error = timeout(
            Duration::from_secs(15),
            store.clone().supervise_checks(stopping),
        )
        .await
        .expect("failed publication must return from the supervisor")
        .unwrap_err();
        assert_eq!(first_error.code, expected_error.code);
        assert!(!first_error.secondary_codes.is_empty());
        assert_eq!(store.check_child_custody_count().await, 0);
        for receipt in [
            "launch.json",
            "launch-unknown.json",
            "launch-departed.json",
            "worker.json",
            "go.json",
            "worker-bootstrap.json",
        ] {
            assert!(
                !directory.join(receipt).exists(),
                "pre-spawn failure must not create {receipt}"
            );
        }
        assert!(!directory.join("completion.json").is_file());
        let check_id = work.check_id.clone();
        let root = fixture.directory.clone();
        let retry = store
            .run(move |db| {
                let pending = pending(db, root)?;
                let retained = pending
                    .into_iter()
                    .find(|work| work.check_id == check_id)
                    .expect("failed publication remains pending for terminalization");
                assert_eq!(
                    describe(db, &json!({"check_id":check_id}))?["state"],
                    "reconciling"
                );
                Ok(retained)
            })
            .await
            .unwrap();
        assert_eq!(retry.preflight_error, Some(json!(expected_error)));
        assert_eq!(retry.operation_id, work.operation_id);
        let files = ArtifactFiles::new(&fixture.directory).unwrap();
        assert!(
            worker::recover_pre_identity(&retry, &files)
                .unwrap()
                .is_none()
        );
        let result_id = format!("check-{}", model::digest(work.operation_id.as_bytes()));
        let result_path = fixture
            .directory
            .join("artifacts")
            .join(format!("{result_id}.bin"));
        let published_report = std::fs::read(&result_path).unwrap();

        std::fs::remove_dir(directory.join("completion.json")).unwrap();
        store.reconcile_checks_once().await.unwrap();
        // The pinned image remains invalid: reconciliation must consume
        // the retained failure, not re-prepare work or wait for a worker receipt.
        let completion = worker::completion(&retry, &files).unwrap().unwrap();
        assert_eq!(completion.state, "error");
        assert_eq!(completion.operation_id, work.operation_id);
        assert_eq!(std::fs::read(&result_path).unwrap(), published_report);
        let check_id = work.check_id.clone();
        let description = store
            .run(move |db| describe(db, &json!({"check_id":check_id})))
            .await
            .unwrap();
        assert_eq!(description["state"], "error");
        assert_eq!(description["resource_released"], true);
        assert!(!description["resource_released_at_ms"].is_null());
        assert_eq!(description["process"], Value::Null);
        let completion_bytes = std::fs::read(directory.join("completion.json")).unwrap();
        store.reconcile_checks_once().await.unwrap();
        stop.send_replace(true);
        store
            .clone()
            .supervise_checks(stop.subscribe())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(directory.join("completion.json")).unwrap(),
            completion_bytes
        );
        assert_eq!(store.check_child_custody_count().await, 0);
        for receipt in [
            "launch.json",
            "launch-unknown.json",
            "launch-departed.json",
            "worker.json",
            "go.json",
            "worker-bootstrap.json",
        ] {
            assert!(!directory.join(receipt).exists());
        }
        assert_eq!(description["operation_id"], admitted["operation_id"]);
        fixture.owner.close().await.unwrap();
        std::fs::remove_dir_all(fixture.directory).unwrap();
    }

    fn spawn_blocking_custody_child() -> Child {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::checks::launch_custody_fixtures::blocking_child_fixture",
                "--nocapture",
            ])
            .env("SWARM_CHECK_CUSTODY_BLOCKING_CHILD", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }

    #[tokio::test]
    async fn store_launch_failure_and_supervisor_retry_keep_the_same_child_until_departure() {
        let fixture = fixture().await;
        let (admitted, work) = queued_check(&fixture, "custody-store-retry").await;
        let check_id = admitted["check_id"].as_str().unwrap().to_owned();
        let store = fixture.owner.store.clone();
        store
            .run(|db| {
                db.execute_batch(
                    "CREATE TRIGGER fixture_fail_check_launch BEFORE UPDATE OF spec_json ON check_runs WHEN json_type(NEW.spec_json, '$.launch') IS NOT NULL BEGIN SELECT RAISE(ABORT, 'injected CheckRun launch persistence failure'); END;",
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let (stop, stopping) = watch::channel(false);
        let first_error = timeout(
            Duration::from_secs(15),
            store.clone().supervise_checks(stopping),
        )
        .await
        .expect("the injected Store failure must return from the supervisor")
        .unwrap_err();
        assert_eq!(first_error.code, "STORE_ERROR");
        let first = child_custody_snapshot(&store, &check_id).await;
        assert!(first.2.is_some());
        assert!(first.3);
        assert_eq!(first.4.as_deref(), Some("STORE_ERROR"));

        let retry_error = timeout(
            Duration::from_secs(15),
            store.clone().supervise_checks(stop.subscribe()),
        )
        .await
        .expect("the retry must report the same Store failure")
        .unwrap_err();
        assert_eq!(retry_error.code, "STORE_ERROR");
        let retry = child_custody_snapshot(&store, &check_id).await;
        assert_eq!(retry.0, first.0);
        assert_eq!(retry.1, first.1, "retry must retain the same Child object");
        assert_eq!(
            retry.2, first.2,
            "retry must retain the exact launch identity"
        );
        assert!(retry.3);

        store
            .run(|db| {
                db.execute_batch("DROP TRIGGER fixture_fail_check_launch")?;
                Ok(())
            })
            .await
            .unwrap();
        stop.send_replace(true);
        timeout(
            Duration::from_secs(15),
            store.clone().supervise_checks(stop.subscribe()),
        )
        .await
        .expect("shutdown must finish after the exact child departure is retained")
        .unwrap();

        assert_eq!(store.check_child_custody_count().await, 0);
        let directory = worker::directory(&work.data_dir, &work.check_id).unwrap();
        let launch = read_check_receipt(&directory.join("launch.json"))
            .unwrap()
            .unwrap();
        let departure = read_check_receipt(&directory.join("launch-departed.json"))
            .unwrap()
            .unwrap();
        assert_eq!(launch["check_id"], work.check_id);
        assert_eq!(launch["operation_id"], work.operation_id);
        assert_eq!(launch["token"], work.token);
        assert_eq!(departure["child_pid"], first.0);
        assert!(departure["exit_observed"].as_bool().unwrap());
        assert!(!departure["go_published"].as_bool().unwrap());
        assert!(
            store
                .run(move |db| check_terminal_evidence_retained(db, &work))
                .await
                .unwrap()
        );

        fixture.owner.close().await.unwrap();
        std::fs::remove_dir_all(fixture.directory).unwrap();
    }

    #[tokio::test]
    async fn departure_receipt_write_failure_keeps_the_same_child_and_record_for_retry() {
        let fixture = fixture().await;
        let (admitted, _) = queued_check(&fixture, "custody-departure-retry").await;
        let check_id = admitted["check_id"].as_str().unwrap().to_owned();
        let store = fixture.owner.store.clone();
        let config = fixture.config.clone();
        let root = fixture.directory.clone();
        let work = store
            .run(move |db| {
                next(db, &config, root)?
                    .filter(|work| work.check_id == check_id)
                    .ok_or_else(|| Error::new("TEST_STATE", "queued CheckRun was not next"))
            })
            .await
            .unwrap();
        let directory = worker::directory(&work.data_dir, &work.check_id).unwrap();
        std::fs::create_dir_all(&directory).unwrap();
        let launch_error = Error::new("CHECK_LAUNCH_UNKNOWN", "launch identity was not recorded");
        let child = spawn_blocking_custody_child();
        let child_pid = child.id();
        retain_check_launch_unknown(&work, child_pid, &launch_error).unwrap();
        store
            .retain_check_child_owner(CheckChildOwner {
                work: work.clone(),
                child,
                launch: None,
                launch_error: Some(launch_error.clone()),
                launch_persistence_pending: false,
                exit: None,
                terminal_retained: false,
            })
            .await
            .unwrap();

        let exit = {
            let mut owners = store.check_child_custody.lock().await;
            let owner = owners.get_mut(&work.check_id).unwrap();
            owner.child.kill().unwrap();
            let status = owner.child.wait().unwrap();
            let exit = CheckChildExit {
                pid: child_pid,
                success: status.success(),
                code: status.code(),
            };
            owner.exit = Some(exit);
            exit
        };
        let mut conflicting_work = work.clone();
        conflicting_work.token = model::new_id();
        retain_check_child_departure(
            &conflicting_work,
            child_pid,
            exit.success,
            exit.code,
            &launch_error,
        )
        .unwrap();
        let departure_path = directory.join("launch-departed.json");
        let conflicting_record = std::fs::read(&departure_path).unwrap();

        let (_stop, stopping) = watch::channel(true);
        let first_error = store.clone().supervise_checks(stopping).await.unwrap_err();
        assert_eq!(first_error.code, "CONFLICT");
        let first = child_custody_snapshot(&store, &work.check_id).await;
        assert_eq!(first.0, child_pid);
        assert_eq!(first.4.as_deref(), Some("CHECK_LAUNCH_UNKNOWN"));
        assert_eq!(first.5, Some((exit.pid, exit.success, exit.code)));
        assert_eq!(std::fs::read(&departure_path).unwrap(), conflicting_record);

        let retry_error = store
            .clone()
            .supervise_checks(watch::channel(true).1)
            .await
            .unwrap_err();
        assert_eq!(retry_error.code, "CONFLICT");
        let retry = child_custody_snapshot(&store, &work.check_id).await;
        assert_eq!(retry.0, first.0);
        assert_eq!(retry.1, first.1, "retry must retain the same Child object");
        assert_eq!(retry.5, first.5);
        assert_eq!(std::fs::read(&departure_path).unwrap(), conflicting_record);

        std::fs::remove_file(&departure_path).unwrap();
        let (_retry_stop, retry_stopping) = watch::channel(true);
        store
            .clone()
            .supervise_checks(retry_stopping)
            .await
            .unwrap();
        assert_eq!(store.check_child_custody_count().await, 0);
        let retained_departure = read_check_receipt(&departure_path).unwrap().unwrap();
        assert_eq!(
            retained_departure,
            check_child_departure_receipt(&work, child_pid, exit.success, exit.code, &launch_error,)
        );
        assert!(
            store
                .run(move |db| check_terminal_evidence_retained(db, &work))
                .await
                .unwrap()
        );

        fixture.owner.close().await.unwrap();
        std::fs::remove_dir_all(fixture.directory).unwrap();
    }
}

#[cfg(test)]
mod launch_custody_fixtures {
    use super::*;
    use crate::checks::model::{CheckProfile, Parser};
    use std::{
        collections::BTreeMap,
        process::{Command, Stdio},
        thread,
    };

    fn fixture() -> (PathBuf, Work) {
        let root = std::env::temp_dir().join(format!("swarm-check-custody-{}", model::new_id()));
        fs::create_dir_all(&root).unwrap();
        let root = fs::canonicalize(root).unwrap();
        let work = Work {
            check_id: model::new_id(),
            operation_id: model::new_id(),
            token: model::new_id(),
            data_dir: root.clone(),
            candidate: ArtifactRecord {
                kind: "source_snapshot".into(),
                artifact_id: "custody-candidate".into(),
                relative_path: "artifacts/custody-candidate".into(),
                byte_length: 0,
                content_digest: "0".repeat(64),
                metadata: json!({}),
            },
            profile: CheckProfile {
                profile_id: "custody-fixture".into(),
                profile_revision: "1".into(),
                executable: "fixture-executor".into(),
                args: Vec::new(),
                parser: Parser::ExitCode,
                resource: "custody-fixture".into(),
                environment: BTreeMap::new(),
                inherit_env: Vec::new(),
                expected_targets: Vec::new(),
                reproducible: false,
                fingerprint_env: Vec::new(),
                versioned_inputs: BTreeMap::new(),
            },
            executor: None,
            resolved_inputs: None,
            scope_plan: None,
            input_fingerprint: None,
            preflight_error: None,
            cancel_request: None,
            expected_worker: None,
            launch: None,
        };
        fs::create_dir_all(worker::directory(&root, &work.check_id).unwrap()).unwrap();
        (root, work)
    }

    fn retain_unknown_and_departure(work: &Work, child_pid: u32) {
        let error = Error::new("CHECK_LAUNCH_UNKNOWN", "launch identity was not recorded");
        retain_check_launch_unknown(work, child_pid, &error).unwrap();
        retain_check_child_departure(work, child_pid, true, Some(0), &error).unwrap();
    }

    fn assert_control_file_holds_recovery(control_file: &str) {
        let (root, work) = fixture();
        retain_unknown_and_departure(&work, 42_011);
        let dir = worker::directory(&root, &work.check_id).unwrap();
        fs::write(dir.join(control_file), b"{}").unwrap();
        let files = ArtifactFiles::new(&root).unwrap();
        assert!(
            worker::recover_pre_identity(&work, &files)
                .unwrap()
                .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn incident_write_failure_keeps_store_error_authoritative() {
        let semantic = Error::new("CHECK_RECONCILIATION_FAILED", "check recovery failed")
            .with_secondary_code("CHECK_RECEIPT_INVALID")
            .with_secondary_code("CHECK_OUTPUT_INVALID");
        let db = Connection::open_in_memory().unwrap();
        let persistence = incident(&db, "check-fixture", semantic.clone()).unwrap_err();
        let combined = check_incident_persistence_error(semantic, persistence);
        assert_eq!(combined.code, "STORE_ERROR");
        assert_eq!(combined.secondary_codes.len(), 2);
        assert_eq!(combined.secondary_codes[0], "CHECK_RECONCILIATION_FAILED");
        assert_eq!(combined.secondary_codes[1], "CHECK_RECEIPT_INVALID");
    }

    #[test]
    fn private_launch_fact_survives_store_retention_failure() {
        let (root, work) = fixture();
        let launch = json!({
            "spawned_at_ms": 1,
            "process": {"pid": 123, "scope": "launcher_spawned_process", "purpose": "check"}
        });
        retain_check_launch_fact(&work, &launch).unwrap();

        let db = Connection::open_in_memory().unwrap();
        let error = retain_launch(&db, &work, launch.clone()).unwrap_err();
        assert_eq!(error.code, "STORE_ERROR");
        assert_eq!(read_check_launch_fact(&work).unwrap(), Some(launch));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn private_launch_fact_is_idempotent_and_bound_to_the_check() {
        let (root, work) = fixture();
        let launch = json!({
            "spawned_at_ms": 1,
            "process": {"pid": 123, "scope": "launcher_spawned_process", "purpose": "check"}
        });
        retain_check_launch_fact(&work, &launch).unwrap();
        retain_check_launch_fact(&work, &launch).unwrap();
        assert_eq!(read_check_launch_fact(&work).unwrap(), Some(launch.clone()));

        let changed = json!({"spawned_at_ms": 2, "process": {"pid": 123}});
        assert!(retain_check_launch_fact(&work, &changed).is_err());

        let mut wrong_owner = work.clone();
        wrong_owner.token = model::new_id();
        assert_eq!(
            read_check_launch_fact(&wrong_owner).unwrap_err().code,
            "CONFLICT"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn uncertain_launch_stays_held_until_its_exact_child_departure_is_retained() {
        let (root, work) = fixture();
        let error = Error::new("CHECK_LAUNCH_UNKNOWN", "launch identity was not recorded");
        retain_check_launch_unknown(&work, 42_001, &error).unwrap();
        assert_eq!(
            read_check_launch_fact(&work).unwrap_err().code,
            "CHECK_LAUNCH_IDENTITY_UNKNOWN"
        );
        let files = ArtifactFiles::new(&root).unwrap();
        assert_eq!(
            worker::recover_pre_identity(&work, &files)
                .unwrap_err()
                .code,
            "CHECK_LAUNCH_IDENTITY_UNKNOWN"
        );

        retain_check_child_departure(&work, 42_002, true, Some(0), &error).unwrap();
        assert_eq!(
            worker::recover_pre_identity(&work, &files)
                .unwrap_err()
                .code,
            "CHECK_LAUNCH_DEPARTURE_INVALID"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_pre_go_departure_recovers_as_command_not_started_error_and_never_after_go_or_plan() {
        let (root, work) = fixture();
        retain_unknown_and_departure(&work, 42_010);
        let files = ArtifactFiles::new(&root).unwrap();
        let completion = worker::recover_pre_identity(&work, &files)
            .unwrap()
            .expect("confirmed pre-Go child departure should resolve as incomplete");
        assert_eq!(completion.state, "error");
        assert_eq!(completion.exit_code, None);
        assert!(completion.resource_released);
        let _ = fs::remove_dir_all(root);

        assert_control_file_holds_recovery("go.json");
        assert_control_file_holds_recovery("execution-plan.json");
    }

    #[tokio::test]
    async fn bounded_stop_observes_exit_through_the_owned_child_handle() {
        let executable = std::env::current_exe().unwrap();
        let mut child = Command::new(executable)
            .args([
                "--exact",
                "store::checks::launch_custody_fixtures::blocking_child_fixture",
                "--nocapture",
            ])
            .env("SWARM_CHECK_CUSTODY_BLOCKING_CHILD", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stopped = tokio::time::timeout(
            Duration::from_secs(4),
            stop_unstarted_child_bounded(&mut child, Duration::from_secs(2)),
        )
        .await
        .expect("owned child cleanup must stay bounded")
        .unwrap();
        assert!(stopped);
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn blocking_child_fixture() {
        if std::env::var_os("SWARM_CHECK_CUSTODY_BLOCKING_CHILD").is_some() {
            thread::park();
        }
    }
}
