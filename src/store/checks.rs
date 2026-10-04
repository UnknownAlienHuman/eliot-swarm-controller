//! CheckRun transactions and one host scheduler, using the existing nine tables.
use super::{Store, acceptance, current_principal, meta, operations, results, tasks};
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    checks::{
        inputs,
        model::{CaptureRequest, CheckRequest},
        source,
        worker::{self, CancelRequest, Completion, Work},
    },
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
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
    let task = tasks::get_task(db, model::text(&a, "task_id")?)?;
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
        let context_fingerprint = snapshot.context_fingerprint.clone();
        let resolved_context = context_fingerprint.clone();
        let data_dir = self.data_dir.clone();
        let result = self
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
                let mut resolved =
                    inputs::resolve(&snapshot.profile, &candidate, baseline.as_ref())?;
                if let Some(reason) = baseline_reason {
                    resolved.add_widening_reason(&reason)?;
                }
                let scope_plan = serde_json::to_value(&resolved.scope_plan)?;
                Ok(PreparedCheckPlan {
                    context_fingerprint: resolved_context,
                    project_id: snapshot.project_id,
                    resolved_inputs: resolved.resolved_inputs,
                    scope_plan,
                    input_fingerprint: resolved.input_fingerprint,
                    reproducible: snapshot.profile.reproducible,
                })
            })
            .await;
        let (result, cache_hit) = match result {
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
    p.require_writer()?;
    let a = tasks::get_attempt(db, id)?;
    super::gm::require_attempt_control(db, p, &a)?;
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
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",params![id,a["task_id"].as_str(),input.attempt_id,model::canonical(&json!({"capture":input,"git_executable":config.checks.git_executable,"identity":{"task_id":a["task_id"],"attempt_id":a["attempt_id"],"task_revision":a["task_revision"]}}))?])?;
    Ok(json!({"operation_id":id,"state":"queued","admission":"durable_local"}))
}
fn begin_source(
    db: &mut Connection,
    p: Principal,
    id: &str,
) -> Result<Option<(CaptureRequest, PathBuf, Value)>> {
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
    attempt(&tx, &p, &input.attempt_id)?;
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1",params![id,model::now_ms()?])?;
    tx.commit()?;
    Ok(Some((
        input,
        serde_json::from_value(v["git_executable"].clone())?,
        v["identity"].clone(),
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
    let result=outcome.and_then(|r|{
        let p=current_principal(&tx,p)?;let a=attempt(&tx,&p,model::text(&op,"attempt_id")?)?;
        if r.metadata["attempt_id"]!=a["attempt_id"]||r.metadata["task_revision"]!=a["task_revision"]{return Err(Error::new("STALE_REVISION","capture revision changed before publication"));}
        artifact(&tx,&r)?;Ok(json!({"operation_id":id,"outcome":"applied","candidate_ref":r.artifact_id,"commit":r.metadata["commit"],"tree":r.metadata["tree"],"file_count":r.metadata["file_count"],"task_accepted":false}))
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
    let resolution = resolution.ok_or_else(|| {
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
pub(super) fn describe(db: &Connection, v: &Value) -> Result<Value> {
    model::fields(v, &["check_id"])?;
    let id = model::text(v, "check_id")?;
    let raw:Option<String>=db.query_row("SELECT json_object('check_id',check_id,'operation_id',operation_id,'attempt_id',attempt_id,'candidate_ref',candidate_ref,'cached_from',cached_from_check_id,'state',state,'resource_key',resource_key,'resource_claimed_at_ms',resource_claimed_at_ms,'resource_released_at_ms',resource_released_at_ms,'process',json(process_identity_json),'coverage',json(coverage_json),'result_ref',result_ref,'exit_code',exit_code,'profile_id',json_extract(spec_json,'$.profile_id'),'profile_revision',json_extract(spec_json,'$.profile_revision'),'cancel_request',json_extract(spec_json,'$.cancel_request'),'cancellation',json_extract(spec_json,'$.cancellation')) FROM check_runs WHERE check_id=?1",[id],|r|r.get(0)).optional()?;
    Ok(serde_json::from_str(&raw.ok_or_else(|| {
        Error::new("NOT_FOUND", "unknown CheckRun")
    })?)?)
}
fn work(db: &Connection, id: &str, root: PathBuf) -> Result<Work> {
    let (op, raw, identity): (String, String, Option<String>) = db.query_row(
        "SELECT operation_id,spec_json,process_identity_json FROM check_runs WHERE check_id=?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let spec: Value = serde_json::from_str(&raw)?;
    Ok(Work {
        check_id: id.into(),
        operation_id: op,
        token: model::text(&spec, "token")?.into(),
        preflight_error: spec
            .get("preflight_error")
            .filter(|v| !v.is_null())
            .cloned(),
        data_dir: root,
        candidate: serde_json::from_value(spec["candidate"].clone())?,
        profile: serde_json::from_value(spec["profile"].clone())?,
        resolved_inputs: spec
            .get("resolved_inputs")
            .filter(|value| !value.is_null())
            .cloned(),
        scope_plan: spec
            .get("scope_plan")
            .filter(|value| !value.is_null())
            .cloned(),
        input_fingerprint: spec
            .get("input_fingerprint")
            .and_then(Value::as_str)
            .map(str::to_owned),
        cancel_request: spec
            .get("cancel_request")
            .filter(|v| !v.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose()?,
        expected_worker: identity.map(|raw| serde_json::from_str(&raw)).transpose()?,
        launch: spec.get("launch").filter(|v| !v.is_null()).cloned(),
    })
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
    let row:Option<(String,String,String)>=tx.query_row("SELECT c.check_id,o.caller_id,c.attempt_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE c.state='queued' AND o.state='queued' AND (json_extract(c.spec_json,'$.cancel_requested') IS NOT NULL OR (?1 AND ?2 AND NOT EXISTS(SELECT 1 FROM check_runs active WHERE active.resource_key=c.resource_key AND active.resource_claimed_at_ms IS NOT NULL AND active.resource_released_at_ms IS NULL))) ORDER BY c.created_at_ms,c.check_id LIMIT 1",params![enabled,running < config.checks.max_running as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
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
        let p = current_principal(
            &tx,
            Principal {
                client_id: caller,
                link_id: String::new(),
                role: Role::Manager,
            },
        )?;
        attempt(&tx, &p, &attempt_id)?;
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
fn pending(db: &Connection, root: PathBuf) -> Result<Vec<Work>> {
    let mut s =
        db.prepare("SELECT c.check_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE c.state IN ('running','reconciling') OR (c.state='queued' AND o.state IN ('sending','outcome_unknown'))")?;
    let ids = s
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ids.iter().map(|id| work(db, id, root.clone())).collect()
}
fn ready(db: &mut Connection, w: &Work, identity: Value) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let status = describe(&tx, &json!({"check_id":w.check_id}))?;
    if !matches!(status["state"].as_str(), Some("running" | "reconciling")) {
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
fn finish(db: &mut Connection, w: &Work, c: Completion) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let status = describe(&tx, &json!({"check_id":w.check_id}))?;
    if !matches!(status["state"].as_str(), Some("running" | "reconciling"))
        && !(status["state"] == "queued" && w.preflight_error.is_some())
    {
        return Ok(());
    }
    if c.check_id != w.check_id
        || c.operation_id != w.operation_id
        || c.token != w.token
        || !c.resource_released
    {
        return Err(Error::conflict("check completion mismatched"));
    }
    artifact(&tx, &c.result)?;
    for out in &c.outputs {
        artifact(&tx, out)?;
    }
    let now = model::now_ms()?;
    tx.execute("UPDATE check_runs SET spec_json=json_set(spec_json,'$.cancellation',json(?2)) WHERE check_id=?1",params![w.check_id,model::canonical(&json!(c.cancellation))?])?;
    tx.execute("UPDATE check_runs SET state=?2,resource_released_at_ms=CASE WHEN resource_claimed_at_ms IS NOT NULL THEN ?3 ELSE NULL END,finished_at_ms=?3,exit_code=?4,result_ref=?5,coverage_json=?6 WHERE check_id=?1",params![w.check_id,c.state,now,c.exit_code,c.result.artifact_id,model::canonical(&c.coverage)?])?;
    let owner:String=tx.query_row("SELECT a.owner_id FROM attempts a JOIN check_runs c ON c.attempt_id=a.attempt_id WHERE c.check_id=?1",[&w.check_id],|r|r.get(0))?;
    let report = json!({"operation_id":w.operation_id,"outcome":"applied","check_id":w.check_id,"state":c.state,"exit_code":c.exit_code,"result_ref":c.result.artifact_id,"recipient":owner,"source_checkout_verified":c.state=="passed","output_refs":c.outputs.iter().map(|o|&o.artifact_id).collect::<Vec<_>>(),"task_accepted":false});
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
impl Store {
    /// Startup readback of retained CheckRun evidence before schedule catch-up.
    /// It never launches a worker or repeats a command. Live workers remain
    /// under the normal supervisor, which owns their acknowledged go-ahead.
    pub(crate) async fn reconcile_checks_once(&self) -> Result<()> {
        let root = self.data_dir.clone();
        let items = self.run(move |db| pending(db, root)).await?;
        for work in items {
            let scan = work.clone();
            if let Some(completion) = self
                .file_io(move |files| worker::completion(&scan, &files))
                .await?
            {
                self.run(move |db| finish(db, &work, completion)).await?;
                continue;
            }
            let scan = work.clone();
            if let Some(completion) = self
                .file_io(move |files| worker::recover_pre_identity(&scan, &files))
                .await?
            {
                self.run(move |db| finish(db, &work, completion)).await?;
                continue;
            }
            let scan = work.clone();
            match self.file_io(move |_| worker::ready(&scan)).await {
                Ok(Some(identity)) => {
                    self.run(move |db| ready(db, &work, identity)).await?;
                }
                Ok(None) => {}
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
                        self.run(move |db| finish(db, &work, completion)).await?;
                    }
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
                p.require_writer()?;
                super::mutate(db, &p, "source.capture", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_owned();
        let start = id.clone();
        let p = principal.clone();
        if let Some((input, git, identity)) =
            self.run(move |db| begin_source(db, p, &start)).await?
        {
            let root = self.data_dir.clone();
            let op = id.clone();
            let outcome = self
                .file_io(move |files| source::capture(&root, &files, &input, &op, &git, identity))
                .await;
            self.run(move |db| finish_source(db, principal, &id, outcome))
                .await?;
        }
        Ok(receipt)
    }
    pub async fn supervise_checks(self, mut stopping: watch::Receiver<bool>) {
        let mut changed = self.changed.subscribe();
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *stopping.borrow() {
                break;
            }
            let root = self.data_dir.clone();
            if let Ok(items) = self.run(move |db| pending(db, root)).await {
                for w in items {
                    let result = async {
                        if let Some(e) = w.preflight_error.clone() {
                            let failed = w.clone();
                            let c = self
                                .file_io(move |files| worker::failure(&failed, &files, e))
                                .await?;
                            let done = w.clone();
                            return self.run(move |db| finish(db, &done, c)).await;
                        }
                        let scan = w.clone();
                        let complete = self
                            .file_io(move |files| worker::completion(&scan, &files))
                            .await?;
                        if let Some(c) = complete {
                            let done = w.clone();
                            return self.run(move |db| finish(db, &done, c)).await;
                        }
                        // A launch whose worker died before publishing an
                        // identity has no admitted worker to recover; only the
                        // host's launch receipt can prove it departed.
                        let scan = w.clone();
                        if let Some(c) = self
                            .file_io(move |files| worker::recover_pre_identity(&scan, &files))
                            .await?
                        {
                            let done = w.clone();
                            return self.run(move |db| finish(db, &done, c)).await;
                        }
                        let scan = w.clone();
                        match self.file_io(move |_| worker::ready(&scan)).await {
                            Ok(Some(identity)) => {
                                let active = w.clone();
                                if self.run(move |db| ready(db, &active, identity)).await? {
                                    let allow = w.clone();
                                    // Persisted cancellation is delivered before go-ahead when both are pending.
                                    self.file_io(move |_| { worker::deliver_cancel(&allow)?; worker::allow(&allow) }).await?;
                                }
                            }
                            Ok(None) => {},
                            Err(e) if e.code == "CHECK_WORKER_LOST" => {
                                let id = w.check_id.clone();
                                self.run(move |db| { db.execute("UPDATE check_runs SET state='reconciling' WHERE check_id=?1 AND state='running'",[id])?; Ok(()) }).await?;
                                let scan = w.clone();
                                if let Some(c) = self.file_io(move |files| worker::recover(&scan, &files)).await? {
                                    let done = w.clone();
                                    return self.run(move |db| finish(db, &done, c)).await;
                                }
                                return Err(e);
                            }
                            Err(e) => return Err(e),
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(e) = result {
                        if e.code == "CHECK_WORKER_LOST" {
                            let id = w.check_id.clone();
                            let _=self.run(move|db|{db.execute("UPDATE check_runs SET state='reconciling' WHERE check_id=?1 AND state='running'",[id])?;Ok(())}).await;
                        }
                        let key = format!("check:{}:{}", w.check_id, e.code);
                        let _ = self.run(move |db| incident(db, &key, e)).await;
                    }
                }
            }
            let root = self.data_dir.clone();
            let config = self.config.clone();
            match self.run(move |db| next(db, &config, root)).await {
                Ok(Some(w)) => {
                    let launch = w.clone();
                    let error = if let Some(e) = w.preflight_error.clone() {
                        Some(e)
                    } else {
                        match self
                            .file_io(move |_| worker::prepare_and_spawn(&launch))
                            .await
                        {
                            Ok(receipt) => {
                                // Persist the launch receipt before relying on
                                // it: a host restart must not erase the only
                                // pre-identity evidence this launch will get.
                                let id = w.check_id.clone();
                                let _ = self
                                    .run(move |db| {
                                        db.execute("UPDATE check_runs SET spec_json=json_set(spec_json,'$.launch',json(?2)) WHERE check_id=?1",params![id,model::canonical(&receipt)?])?;
                                        Ok(())
                                    })
                                    .await;
                                None
                            }
                            Err(e) if e.code == "CHECK_LAUNCH_UNKNOWN" => {
                                let key = format!("check-launch:{}", w.check_id);
                                let _ = self.run(move |db| incident(db, &key, e)).await;
                                None
                            }
                            Err(e) => Some(json!(e)),
                        }
                    };
                    if let Some(error) = error {
                        let failed = w.clone();
                        if let Ok(c) = self
                            .file_io(move |files| worker::failure(&failed, &files, error))
                            .await
                        {
                            let _ = self.run(move |db| finish(db, &w, c)).await;
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    let key = format!("check-admission:{}", e.code);
                    let _ = self.run(move |db| incident(db, &key, e)).await;
                }
            }
            tokio::select! {_=stopping.changed()=>{},_=changed.changed()=>{},_=tick.tick()=>{}}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        sync::Arc,
        time::Duration,
    };
    use tokio::time::timeout;

    struct Fixture {
        directory: PathBuf,
        owner: StoreOwner,
        principal: Principal,
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
        Fixture {
            directory,
            owner,
            principal,
            config,
        }
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

    async fn accept_source(
        fixture: &Fixture,
        task_id: &str,
        attempt_id: &str,
        candidate: &ArtifactRecord,
    ) -> String {
        let acceptance_id = model::new_id();
        let submission_doc = json!({
            "version":1,
            "task_id":task_id,
            "attempt_id":attempt_id,
            "task_revision":1,
            "candidate_ref":candidate.artifact_id,
            "candidate_sha256":candidate.content_digest,
            "candidate_byte_length":candidate.byte_length,
        });
        let (submission, bytes) =
            ArtifactFiles::submission(&acceptance_id, &submission_doc).unwrap();
        ArtifactFiles::new(&fixture.directory)
            .unwrap()
            .publish(&submission, &bytes)
            .unwrap();
        let acceptance_operation = acceptance_id.clone();
        let task = task_id.to_owned();
        let attempt = attempt_id.to_owned();
        let candidate_ref = candidate.artifact_id.clone();
        let candidate_digest = candidate.content_digest.clone();
        let submission_ref = submission.artifact_id.clone();
        let reviewer = fixture.principal.client_id.clone();
        fixture
            .owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                artifact(&tx, &submission)?;
                let now = model::now_ms()?;
                let result = json!({
                    "operation_id":acceptance_operation,
                    "acceptance_operation_id":acceptance_operation,
                    "outcome":"applied",
                    "task_id":task,
                    "attempt_id":attempt,
                    "task_revision":1,
                    "phase":"verification",
                    "submission_ref":submission_ref,
                    "candidate_ref":candidate_ref,
                    "reviewer_id":reviewer,
                    "source_checkout_verified":true,
                    "task_accepted":true,
                });
                tx.execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,'task.accept','{}','{}',?4,?5,'settled',?6,?7,?7,?7,?7)",
                    params![acceptance_operation, reviewer, format!("accept-{acceptance_operation}"), task, attempt, model::canonical(&result)?, now],
                )?;
                tx.execute(
                    "UPDATE attempts SET state='accepted',submission_ref=?2,candidate_ref=?3,updated_at_ms=?4 WHERE attempt_id=?1",
                    params![attempt, submission_ref, candidate_ref, now],
                )?;
                tx.execute(
                    "UPDATE tasks SET state='accepted',accepted_attempt_id=?2,accepted_operation_id=?3,accepted_revision=1,accepted_phase='verification',accepted_candidate_ref=?4,updated_at_ms=?5 WHERE task_id=?1",
                    params![task, attempt, acceptance_operation, candidate_ref, now],
                )?;
                let stored: String = tx.query_row(
                    "SELECT content_digest FROM artifacts WHERE artifact_id=?1",
                    [&candidate_ref],
                    |row| row.get(0),
                )?;
                if stored != candidate_digest {
                    return Err(Error::conflict("accepted candidate digest changed in fixture"));
                }
                tx.commit()?;
                Ok(acceptance_operation)
            })
            .await
            .unwrap()
    }

    async fn invalidate_acceptance(fixture: &Fixture, operation_id: &str) {
        let operation_id = operation_id.to_owned();
        fixture
            .owner
            .store
            .run(move |db| {
                db.execute(
                    "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:acceptance',?1,?2,'task.acceptance_invalidated','{}',?3)",
                    params![format!("invalidate:{operation_id}"), operation_id, model::now_ms()?],
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
        assert!(result["process"].is_object());
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

        let (origin_task, origin_attempt) = create_attempt(&fixture, "cache-project", None).await;
        let origin_source = source_snapshot(&fixture, &origin_task, &origin_attempt, content).await;
        let original = admit_check(&fixture, &origin_attempt, &origin_source, "v1").await;
        assert_eq!(original["state"], "queued");
        let original_check = original["check_id"].as_str().unwrap().to_owned();
        let original_operation = original["operation_id"].as_str().unwrap().to_owned();
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
        let origin_acceptance =
            accept_source(&fixture, &origin_task, &origin_attempt, &origin_source).await;

        let (cached_task, cached_attempt) = create_attempt(&fixture, "cache-project", None).await;
        let cached_source = source_snapshot(&fixture, &cached_task, &cached_attempt, content).await;
        let cached = admit_check(&fixture, &cached_attempt, &cached_source, "v1").await;
        assert_eq!(cached["cached"], true);
        assert_eq!(cached["cached_from_check_id"], original_check);
        let cached_check = cached["check_id"].as_str().unwrap().to_owned();
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

        // A passing cached row cannot itself become a cache origin, even when
        // its candidate is later accepted and the first origin is invalidated.
        let _cached_acceptance =
            accept_source(&fixture, &cached_task, &cached_attempt, &cached_source).await;
        invalidate_acceptance(&fixture, &origin_acceptance).await;
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
            create_attempt(&fixture, "cache-project", None).await;
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
        run_check_process(&fixture, &second_check).await;
        let _second_acceptance = accept_source(
            &fixture,
            &second_origin_task,
            &second_origin_attempt,
            &second_origin_source,
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
            create_attempt(&fixture, "baseline-project", None).await;
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
        let baseline_acceptance = accept_source(
            &fixture,
            &baseline_task,
            &baseline_attempt,
            &baseline_source,
        )
        .await;

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
        invalidate_acceptance(&fixture, &baseline_acceptance).await;
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
}
