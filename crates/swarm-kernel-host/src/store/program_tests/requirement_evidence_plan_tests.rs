use super::{
    ReviewSubject, assign_sponsored_reviewer, principal, seed_clients, start_store_with_config,
};
use crate::{
    checks::model::{CheckProfile, Parser},
    config::Config,
    error::Result,
    model::{self, Principal, Role},
    store::Store,
};
use rusqlite::params;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, process::Command, time::Duration};

fn git(git_executable: &Path, repository: &Path, args: &[&str]) -> String {
    let output = Command::new(git_executable)
        .args(args)
        .current_dir(repository)
        .output()
        .expect("run fixture Git command");
    assert!(
        output.status.success(),
        "Git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Git output is UTF-8")
        .trim()
        .to_owned()
}

async fn settled_operation(store: &Store, caller: &Principal, operation_id: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let operation = store
                .call(
                    caller.clone(),
                    "operation.get".into(),
                    json!({"operation_id":operation_id}),
                )
                .await
                .expect("read retained Operation");
            if operation["state"] == "settled" {
                return operation;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("Operation settles within the fixture bound")
}

async fn run_check(
    store: &Store,
    caller: &Principal,
    attempt_id: &str,
    candidate_ref: &str,
    profile_revision: &str,
) -> String {
    let receipt = store
        .call(
            caller.clone(),
            "check.run".into(),
            json!({
                "client_request_id":model::new_id(),
                "attempt_id":attempt_id,
                "candidate_ref":candidate_ref,
                "profile_id":"requirement-check",
                "profile_revision":profile_revision,
            }),
        )
        .await
        .expect("admit configured CheckRun through Store");
    let check_id = receipt["check_id"]
        .as_str()
        .expect("check admission returns its exact CheckRun ID")
        .to_owned();
    let operation_id = receipt["operation_id"]
        .as_str()
        .expect("check admission returns its Operation ID");
    let operation = settled_operation(store, caller, operation_id).await;
    assert_eq!(operation["result"]["outcome"], "applied");
    let check = store
        .call(
            caller.clone(),
            "check.get".into(),
            json!({"check_id":check_id}),
        )
        .await
        .expect("read retained CheckRun");
    assert_eq!(check["state"], "passed", "retained CheckRun: {check}");
    check_id
}

async fn rejected_acceptance(store: &Store, gm: &Principal, request: Value) -> crate::error::Error {
    store
        .call(gm.clone(), "task.accept".into(), request)
        .await
        .expect_err("invalid acceptance evidence is rejected before Operation admission")
}

fn acceptance_request(
    attempt_id: &str,
    submission_ref: &str,
    candidate_ref: &str,
    feedback_cursor: i64,
    reviews: &[Value],
    check_ids: &[String],
) -> Value {
    json!({
        "client_request_id":model::new_id(),
        "attempt_id":attempt_id,
        "expected_revision":1,
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref,
        "expected_feedback_observation_id":feedback_cursor,
        "reason":"The frozen requirement evidence satisfies the explicit acceptance policy.",
        "reviews":reviews,
        "check_ids":check_ids,
    })
}

fn expected_review(requirement_id: &str, candidate_ref: &str) -> Value {
    json!({
        "requirement_id":requirement_id,
        "rationale":format!("Retained candidate evidence satisfies {requirement_id}."),
        "evidence":[candidate_ref],
    })
}

async fn configure_automatic_acceptance(store: &Store, project_id: &str) {
    let project_id = project_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut entry = crate::automation::config::AutomationEntry::new(
                "review-gm",
                &project_id,
                "requirement-evidence-plan-acceptance",
                model::now_ms()?,
            );
            entry.enabled = true;
            entry.steps = vec![crate::automation::actions::AutomationStep::Acceptance];
            crate::automation::config::validate_entry(&entry)?;
            let entry_key = crate::automation::config::entry_key(
                "review-gm",
                &project_id,
                &entry.automation_id,
            )?;
            crate::automation::config::write_record(&tx, &entry_key, &entry.value()?)?;
            let cut: i64 = tx.query_row(
                "SELECT COALESCE(MAX(observation_id),0) FROM observations",
                [],
                |row| row.get(0),
            )?;
            crate::store::review_disposition::configure_activation(
                &tx, None, &entry, false, cut, 10,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn consume_automatic_acceptance(
    store: &Store,
    project_id: &str,
    assignment_id: &str,
    result_operation_id: &str,
) -> Result<Value> {
    let project_id = project_id.to_owned();
    let assignment_id = assignment_id.to_owned();
    let result_operation_id = result_operation_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let entry = crate::automation::config::load_entry(
                &tx,
                "review-gm",
                &project_id,
                "requirement-evidence-plan-acceptance",
            )?
            .ok_or_else(|| {
                crate::error::Error::new("TEST_FIXTURE", "acceptance entry is not retained")
            })?;
            let result = crate::store::automation_acceptance::consume_review_result_for_entry(
                &tx,
                &entry,
                &assignment_id,
                &result_operation_id,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
}

#[tokio::test]
async fn mixed_requirement_evidence_plan_flows_through_review_and_acceptance() {
    let config = requirement_check_config();
    let git_executable = config.checks.git_executable.clone();
    let (owner, directory, _bootstrap) =
        start_store_with_config("requirement-evidence-plan", config).await;
    let store = owner.store.clone();
    seed_clients(&store).await;
    let task_owner = principal("review-owner-v2", Role::Manager);
    let gm = principal("review-gm", Role::Manager);

    let created = store
        .call(
            task_owner.clone(),
            "task.create".into(),
            json!({
                "client_request_id":model::new_id(),
                "project_id":"requirement-evidence-plan-project",
                "spec":{
                    "objective":"Demonstrate requirement-specific evidence through real Store callers.",
                    "phase":"verification",
                    "requirements":[
                        {
                            "id":"review-only",
                            "statement":"Independent review confirms the behavior.",
                            "verification":{"review_required":true,"check_profiles":[]},
                        },
                        {
                            "id":"check-only",
                            "statement":"The configured CheckRun proves the criterion.",
                            "verification":{"review_required":false,"check_profiles":[
                                {"profile_id":"requirement-check","profile_revision":"1"}
                            ]},
                        },
                        {
                            "id":"review-and-check",
                            "statement":"Both independent review and the configured CheckRun are required.",
                            "verification":{"review_required":true,"check_profiles":[
                                {"profile_id":"requirement-check","profile_revision":"1"}
                            ]},
                        },
                    ],
                    "acceptance":{"schema_id":"swarm.acceptance_policy","schema_version":2},
                    "dependencies":[],
                    "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
                    "source_refs":[],
                },
            }),
        )
        .await
        .unwrap();
    let task_id = created["task_id"].as_str().unwrap().to_owned();
    configure_automatic_acceptance(&store, "requirement-evidence-plan-project").await;
    let claimed = store
        .call(
            task_owner.clone(),
            "task.claim".into(),
            json!({
                "client_request_id":model::new_id(),
                "task_id":task_id,
                "expected_revision":1,
            }),
        )
        .await
        .unwrap();
    let attempt_id = claimed["attempt_id"].as_str().unwrap().to_owned();
    let attempt = store
        .call(
            task_owner.clone(),
            "attempt.get".into(),
            json!({"attempt_id":attempt_id}),
        )
        .await
        .unwrap();
    let frozen_snapshot = attempt["task_snapshot"].clone();
    assert_eq!(
        frozen_snapshot["spec"]["acceptance"],
        json!({"schema_id":"swarm.acceptance_policy","schema_version":2})
    );
    assert!(
        frozen_snapshot["spec"]["acceptance"]
            .get("required_check_profiles")
            .is_none()
    );
    assert_eq!(
        frozen_snapshot["requirement_evidence_plan"],
        json!({
            "review_requirement_ids":["review-only","review-and-check"],
            "required_profiles":[{"profile_id":"requirement-check","profile_revision":"1"}],
            "profile_to_requirements":[{
                "profile_id":"requirement-check",
                "profile_revision":"1",
                "requirement_ids":["check-only","review-and-check"],
            }],
        })
    );
    assert_eq!(
        attempt["task_brief"]["requirement_evidence_plan"],
        frozen_snapshot["requirement_evidence_plan"]
    );

    let repository = directory.join("requirement-evidence-repository");
    std::fs::create_dir_all(&repository).unwrap();
    git(&git_executable, &repository, &["init", "--quiet"]);
    git(
        &git_executable,
        &repository,
        &["config", "user.name", "Fixture Author"],
    );
    git(
        &git_executable,
        &repository,
        &["config", "user.email", "fixture@example.invalid"],
    );
    std::fs::write(
        repository.join("evidence.txt"),
        b"frozen candidate evidence\n",
    )
    .unwrap();
    git(&git_executable, &repository, &["add", "evidence.txt"]);
    git(
        &git_executable,
        &repository,
        &["commit", "--quiet", "-m", "requirement evidence fixture"],
    );
    let commit = git(&git_executable, &repository, &["rev-parse", "HEAD"]);
    let captured = store
        .call(
            task_owner.clone(),
            "source.capture".into(),
            json!({
                "client_request_id":model::new_id(),
                "attempt_id":attempt_id,
                "expected_revision":1,
                "repository":repository,
                "commit":commit,
            }),
        )
        .await
        .unwrap();
    let capture_operation = settled_operation(
        &store,
        &task_owner,
        captured["operation_id"]
            .as_str()
            .expect("source.capture returns its exact Operation receipt"),
    )
    .await;
    assert_eq!(capture_operation["result"]["outcome"], "applied");
    let candidate_ref = capture_operation["result"]["candidate_ref"]
        .as_str()
        .expect("settled source.capture retains the exact candidate artifact")
        .to_owned();
    let submitted = store
        .call(
            task_owner.clone(),
            "task.submit".into(),
            json!({
                "client_request_id":model::new_id(),
                "attempt_id":attempt_id,
                "expected_revision":1,
                "expected_submission_ref":null,
                "candidate_ref":candidate_ref,
                "summary":"Submit the exact source.capture candidate.",
                "claims":[],
            }),
        )
        .await
        .unwrap();
    let submit_operation = settled_operation(
        &store,
        &task_owner,
        submitted["operation_id"].as_str().unwrap(),
    )
    .await;
    assert_eq!(submit_operation["result"]["outcome"], "applied");
    let submitted_attempt = store
        .call(
            task_owner.clone(),
            "attempt.get".into(),
            json!({"attempt_id":attempt_id}),
        )
        .await
        .unwrap();
    let submission_ref = submitted_attempt["submission_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let submission = store
        .call(
            task_owner.clone(),
            "task.submission".into(),
            json!({"submission_ref":submission_ref}),
        )
        .await
        .unwrap();
    let feedback_cursor = submission["latest_feedback_observation_id"]
        .as_i64()
        .unwrap();

    let subject = ReviewSubject {
        task_id,
        attempt_id: attempt_id.clone(),
        submission_ref: submission_ref.clone(),
        candidate_ref: candidate_ref.clone(),
    };
    let (reviewer, assignment_id) = assign_sponsored_reviewer(
        &store,
        task_owner.clone(),
        &subject,
        "requirement-evidence-reviewer",
        "requirement-evidence-reviewer-token",
    )
    .await;
    let review_only = expected_review("review-only", &candidate_ref);
    let review_and_check = expected_review("review-and-check", &candidate_ref);
    let submitted_reviews = vec![review_only.clone(), review_and_check.clone()];
    let review_request = |reviews: Vec<Value>, request_id: String| {
        json!({
            "client_request_id":request_id,
            "review_assignment_id":assignment_id,
            "submission_ref":submission_ref,
            "candidate_ref":candidate_ref,
            "verdict":"pass",
            "coverage":"complete",
            "findings":[],
            "evidence_refs":[candidate_ref],
            "requirement_reviews":reviews,
        })
    };
    let missing_subset_error = store
        .call(
            reviewer.clone(),
            "review.submit".into(),
            review_request(vec![review_only.clone()], model::new_id()),
        )
        .await
        .unwrap_err();
    assert_eq!(missing_subset_error.code, "REVIEW_INCOMPLETE");
    let extra_check_only_error = store
        .call(
            reviewer.clone(),
            "review.submit".into(),
            review_request(
                vec![
                    review_only.clone(),
                    review_and_check.clone(),
                    expected_review("check-only", &candidate_ref),
                ],
                model::new_id(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(extra_check_only_error.code, "REVIEW_INCOMPLETE");
    let review_result = store
        .call(
            reviewer,
            "review.submit".into(),
            review_request(submitted_reviews.clone(), model::new_id()),
        )
        .await
        .unwrap();
    let review_result_operation_id = review_result["operation_id"]
        .as_str()
        .expect("review.submit retains its producer Operation ID")
        .to_owned();
    let required_reviews = review_result["requirement_reviews"]
        .as_array()
        .expect("retained review result returns its structured evidence")
        .clone();
    assert_eq!(required_reviews, submitted_reviews);
    assert_eq!(required_reviews.len(), 2);
    assert!(
        !review_result["requirement_reviews"]
            .as_array()
            .unwrap()
            .iter()
            .any(|review| review["requirement_id"] == "check-only")
    );

    let (stop_checks, stopping_checks) = tokio::sync::watch::channel(false);
    let check_supervisor = tokio::spawn(store.clone().supervise_checks(stopping_checks));
    let missing_automatic = consume_automatic_acceptance(
        &store,
        "requirement-evidence-plan-project",
        &assignment_id,
        &review_result_operation_id,
    )
    .await
    .unwrap();
    assert_eq!(missing_automatic["status"], "capability_gap");
    assert_eq!(missing_automatic["code"], "required_checks_incomplete");
    assert_eq!(
        missing_automatic["missing_profiles"],
        json!([{"profile_id":"requirement-check","profile_revision":"1"}])
    );

    let wrong_revision_check =
        run_check(&store, &task_owner, &attempt_id, &candidate_ref, "2").await;
    let wrong_revision_automatic = consume_automatic_acceptance(
        &store,
        "requirement-evidence-plan-project",
        &assignment_id,
        &review_result_operation_id,
    )
    .await
    .unwrap();
    assert_eq!(wrong_revision_automatic["status"], "capability_gap");
    assert_eq!(
        wrong_revision_automatic["missing_profiles"],
        json!([{"profile_id":"requirement-check","profile_revision":"1"}])
    );
    let missing_review = vec![review_only.clone()];
    let missing_check_error = rejected_acceptance(
        &store,
        &gm,
        acceptance_request(
            &attempt_id,
            &submission_ref,
            &candidate_ref,
            feedback_cursor,
            &required_reviews,
            &[],
        ),
    )
    .await;
    assert_eq!(missing_check_error.code, "CHECKS_REQUIRED");
    let missing_review_error = rejected_acceptance(
        &store,
        &gm,
        acceptance_request(
            &attempt_id,
            &submission_ref,
            &candidate_ref,
            feedback_cursor,
            &missing_review,
            &[wrong_revision_check.clone()],
        ),
    )
    .await;
    assert_eq!(missing_review_error.code, "REVIEW_INCOMPLETE");
    let wrong_revision_error = rejected_acceptance(
        &store,
        &gm,
        acceptance_request(
            &attempt_id,
            &submission_ref,
            &candidate_ref,
            feedback_cursor,
            &required_reviews,
            &[wrong_revision_check],
        ),
    )
    .await;
    assert_eq!(wrong_revision_error.code, "CHECK_PROFILE_MISMATCH");

    let mut stale_snapshot = frozen_snapshot.clone();
    stale_snapshot["requirement_evidence_plan"]["review_requirement_ids"] = json!(["check-only"]);
    let stale_json = model::canonical(&stale_snapshot).unwrap();
    let stale_attempt_id = attempt_id.clone();
    store
        .run(move |db| {
            db.execute(
                "UPDATE attempts SET task_snapshot_json=?2 WHERE attempt_id=?1",
                params![stale_attempt_id, stale_json],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let stale_error = rejected_acceptance(
        &store,
        &gm,
        acceptance_request(
            &attempt_id,
            &submission_ref,
            &candidate_ref,
            feedback_cursor,
            &required_reviews,
            &[],
        ),
    )
    .await;
    assert_eq!(stale_error.code, "REQUIREMENT_EVIDENCE_PLAN_STALE");
    let stale_automatic = consume_automatic_acceptance(
        &store,
        "requirement-evidence-plan-project",
        &assignment_id,
        &review_result_operation_id,
    )
    .await
    .unwrap_err();
    assert_eq!(stale_automatic.code, "REQUIREMENT_EVIDENCE_PLAN_STALE");
    let restored_snapshot = model::canonical(&frozen_snapshot).unwrap();
    let restore_attempt_id = attempt_id.clone();
    store
        .run(move |db| {
            db.execute(
                "UPDATE attempts SET task_snapshot_json=?2 WHERE attempt_id=?1",
                params![restore_attempt_id, restored_snapshot],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let check_id = run_check(&store, &task_owner, &attempt_id, &candidate_ref, "1").await;
    let reconciled = store
        .reconcile_review_dispositions_once()
        .await
        .expect("consume the mixed-plan review through the real result reconciler");
    assert_eq!(reconciled["review_results"]["quarantined"], 0);
    let expected_result_id = review_result_operation_id.clone();
    let automatic_projection = store
        .run(move |db| {
            let entry = crate::automation::config::load_entry(
                db,
                "review-gm",
                "requirement-evidence-plan-project",
                "requirement-evidence-plan-acceptance",
            )?
            .ok_or_else(|| {
                crate::error::Error::new("TEST_FIXTURE", "acceptance entry is missing")
            })?;
            let state = crate::store::review_disposition::disposition_state(db, &entry)?;
            state["recent"]
                .as_array()
                .and_then(|recent| {
                    recent
                        .iter()
                        .rev()
                        .find(|item| item["review_result_operation_id"] == expected_result_id)
                })
                .cloned()
                .ok_or_else(|| {
                    crate::error::Error::new(
                        "TEST_FIXTURE",
                        "reconciler did not retain this review result",
                    )
                })
        })
        .await
        .unwrap();
    assert_eq!(automatic_projection["status"], "acceptance_reserved");
    let acceptance_operation_id = automatic_projection["operation_id"]
        .as_str()
        .expect("automatic consumer reserves its exact task.accept Operation")
        .to_owned();
    let accepted_operation = settled_operation(&store, &gm, &acceptance_operation_id).await;
    assert_eq!(accepted_operation["result"]["outcome"], "applied");
    assert_eq!(accepted_operation["result"]["task_accepted"], true);
    let retained_effective = store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [acceptance_operation_id],
                |row| row.get(0),
            )?;
            Ok(serde_json::from_str::<Value>(&raw)?)
        })
        .await
        .unwrap();
    assert_eq!(
        retained_effective["request"]["reviews"],
        json!(required_reviews)
    );
    assert_eq!(
        retained_effective["request"]["check_ids"],
        json!([check_id])
    );
    assert_eq!(
        retained_effective["evidence"]["requirement_evidence"],
        json!([
            {
                "requirement_id":"review-only",
                "review":{"rationale":review_only["rationale"],"evidence":[candidate_ref]},
                "checks":[],
            },
            {
                "requirement_id":"check-only",
                "review":null,
                "checks":[{"profile_id":"requirement-check","profile_revision":"1","check_id":check_id}],
            },
            {
                "requirement_id":"review-and-check",
                "review":{"rationale":review_and_check["rationale"],"evidence":[candidate_ref]},
                "checks":[{"profile_id":"requirement-check","profile_revision":"1","check_id":check_id}],
            },
        ])
    );

    assert_original_acceptance_mapping(
        &store,
        &gm,
        accepted_operation["operation_id"].as_str().unwrap(),
        Some(&retained_effective["evidence"]["requirement_evidence"]),
    )
    .await;

    stop_checks.send_replace(true);
    check_supervisor.await.unwrap().unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

fn review_only_v2_spec() -> Value {
    json!({
        "objective":"Retain truthful requirement evidence through the actual Store boundary.",
        "phase":"verification",
        "requirements":[{
            "id":"R1",
            "statement":"Retain the exact independent review result.",
            "verification":{"review_required":true,"check_profiles":[]},
        }],
        "acceptance":{"schema_id":"swarm.acceptance_policy","schema_version":2},
        "dependencies":[],
        "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
        "source_refs":[],
    })
}

async fn create_v2_attempt(
    store: &Store,
    project_id: &str,
    spec: Value,
    start_owner: &str,
    binding_id: Option<&str>,
) -> (String, String) {
    let task_owner = principal("review-owner-v2", Role::Manager);
    let created = store
        .call(
            task_owner.clone(),
            "task.create".into(),
            json!({"client_request_id":model::new_id(),"project_id":project_id,"spec":spec}),
        )
        .await
        .unwrap();
    let task_id = created["task_id"].as_str().unwrap().to_owned();
    let mut request = json!({
        "client_request_id":model::new_id(),
        "task_id":task_id,
        "expected_revision":1,
        "start_owner":start_owner,
    });
    if let Some(binding_id) = binding_id {
        request["binding_id"] = json!(binding_id);
        request["binding_generation"] = json!(1);
    }
    let claimed = store
        .call(task_owner, "task.claim".into(), request)
        .await
        .unwrap();
    (task_id, claimed["attempt_id"].as_str().unwrap().to_owned())
}

async fn capture_and_submit_attempt(
    store: &Store,
    directory: &Path,
    git_executable: &Path,
    task_id: String,
    attempt_id: String,
) -> (ReviewSubject, i64) {
    let task_owner = principal("review-owner-v2", Role::Manager);
    let repository = directory.join(format!("repository-{attempt_id}"));
    std::fs::create_dir_all(&repository).unwrap();
    git(git_executable, &repository, &["init", "--quiet"]);
    git(
        git_executable,
        &repository,
        &["config", "user.name", "Fixture Author"],
    );
    git(
        git_executable,
        &repository,
        &["config", "user.email", "fixture@example.invalid"],
    );
    std::fs::write(
        repository.join("evidence.txt"),
        b"frozen candidate evidence\n",
    )
    .unwrap();
    git(git_executable, &repository, &["add", "evidence.txt"]);
    git(
        git_executable,
        &repository,
        &["commit", "--quiet", "-m", "requirement evidence fixture"],
    );
    let commit = git(git_executable, &repository, &["rev-parse", "HEAD"]);
    let captured = store
        .call(
            task_owner.clone(),
            "source.capture".into(),
            json!({
                "client_request_id":model::new_id(),
                "attempt_id":attempt_id,
                "expected_revision":1,
                "repository":repository,
                "commit":commit,
            }),
        )
        .await
        .unwrap();
    let capture = settled_operation(
        store,
        &task_owner,
        captured["operation_id"].as_str().unwrap(),
    )
    .await;
    assert_eq!(capture["result"]["outcome"], "applied");
    let candidate_ref = capture["result"]["candidate_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let submitted = store
        .call(
            task_owner.clone(),
            "task.submit".into(),
            json!({
                "client_request_id":model::new_id(),
                "attempt_id":attempt_id,
                "expected_revision":1,
                "expected_submission_ref":null,
                "candidate_ref":candidate_ref,
                "summary":"Submit the exact source.capture candidate.",
                "claims":[],
            }),
        )
        .await
        .unwrap();
    let submission = settled_operation(
        store,
        &task_owner,
        submitted["operation_id"].as_str().unwrap(),
    )
    .await;
    assert_eq!(submission["result"]["outcome"], "applied");
    let attempt = store
        .call(
            task_owner.clone(),
            "attempt.get".into(),
            json!({"attempt_id":attempt_id}),
        )
        .await
        .unwrap();
    let submission_ref = attempt["submission_ref"].as_str().unwrap().to_owned();
    let document = store
        .call(
            task_owner,
            "task.submission".into(),
            json!({"submission_ref":submission_ref}),
        )
        .await
        .unwrap();
    (
        ReviewSubject {
            task_id,
            attempt_id,
            submission_ref,
            candidate_ref,
        },
        document["latest_feedback_observation_id"].as_i64().unwrap(),
    )
}

async fn assert_original_acceptance_mapping(
    store: &Store,
    gm: &Principal,
    operation_id: &str,
    expected: Option<&Value>,
) {
    let original_id = operation_id.to_owned();
    let retained = store
        .run(move |db| {
            let (effective, result): (String, String) = db.query_row(
                "SELECT effective_request_json,result_json FROM operations \
                 WHERE operation_id=?1 AND method='task.accept' AND state='settled'",
                [&original_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let observation: String = db.query_row(
                "SELECT payload_json FROM observations WHERE operation_id=?1 \
                 AND source_stream_id='controller:acceptance' AND source_event_key=?2 \
                 AND kind='task.acceptance'",
                params![original_id, format!("accept:{original_id}")],
                |row| row.get(0),
            )?;
            Ok(json!({
                "effective":serde_json::from_str::<Value>(&effective)?,
                "result":serde_json::from_str::<Value>(&result)?,
                "observation":serde_json::from_str::<Value>(&observation)?,
            }))
        })
        .await
        .unwrap();
    assert_eq!(retained["result"]["outcome"], "applied");
    assert!(retained["effective"]["evidence"].is_object());
    assert_eq!(retained["result"]["acceptance_operation_id"], operation_id);
    assert_eq!(retained["observation"], retained["result"]);
    let readback = store
        .call(
            gm.clone(),
            "task.acceptance".into(),
            json!({"acceptance_operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(readback["acceptance_operation_id"], operation_id);
    assert_eq!(
        readback["candidate_ref"],
        retained["result"]["candidate_ref"]
    );
    assert_eq!(readback["task_accepted"], true);
    assert_eq!(readback["current"], true);
    assert_eq!(readback["invalidated"], false);
    for value in [
        &retained["effective"]["evidence"],
        &retained["result"],
        &retained["observation"],
        &readback,
    ] {
        match expected {
            Some(mapping) => assert_eq!(&value["requirement_evidence"], mapping),
            None => assert!(
                value.get("requirement_evidence").is_none(),
                "historical V1 must not fabricate requirement associations: {value}"
            ),
        }
    }
}

async fn assert_v2_nonpass_retained_without_autoaccept(verdict: &str) {
    let config = Config::default();
    let git_executable = config.checks.git_executable.clone();
    let (owner, directory, _) = start_store_with_config(verdict, config).await;
    let store = &owner.store;
    seed_clients(store).await;
    let project_id = "r22-nonpass-project";
    let (task_id, attempt_id) = create_v2_attempt(
        store,
        project_id,
        review_only_v2_spec(),
        "native_manager",
        None,
    )
    .await;
    let (subject, _) =
        capture_and_submit_attempt(store, &directory, &git_executable, task_id, attempt_id).await;
    configure_automatic_acceptance(store, project_id).await;
    let (reviewer, assignment_id) = assign_sponsored_reviewer(
        store,
        principal("review-owner-v2", Role::Manager),
        &subject,
        "r22-nonpass-reviewer",
        "r22-nonpass-reviewer-token",
    )
    .await;
    let findings = if verdict == "changes_requested" {
        json!([{
            "finding_id":"R1-defect",
            "requirement_ids":["R1"],
            "reason":"The captured candidate does not meet the frozen requirement.",
            "evidence_refs":[subject.candidate_ref],
            "requested_change":"Repair the demonstrated requirement violation.",
        }])
    } else {
        assert_eq!(verdict, "inconclusive");
        json!([])
    };
    let mut request = json!({
        "client_request_id":model::new_id(),
        "review_assignment_id":assignment_id,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
        "verdict":verdict,
        "coverage":if verdict == "inconclusive" {"partial"} else {"complete"},
        "findings":findings,
        "evidence_refs":[subject.candidate_ref],
        "requirement_reviews":[expected_review("R1", &subject.candidate_ref)],
    });
    let invalid = store
        .call(reviewer.clone(), "review.submit".into(), request.clone())
        .await
        .expect_err("structured acceptance evidence cannot accompany a non-pass");
    assert_eq!(invalid.code, "INVALID_PARAMS");
    let rejected_assignment = assignment_id.clone();
    let result_count = store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT COUNT(*) FROM observations WHERE kind='review.result' \
                 AND source_event_key=?1",
                [format!("result:{rejected_assignment}")],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(result_count, 0);

    request["client_request_id"] = json!(model::new_id());
    request
        .as_object_mut()
        .unwrap()
        .remove("requirement_reviews");
    let original_snapshot = snapshot_json(store, &subject.attempt_id).await;
    let mut stale: Value = serde_json::from_str(&original_snapshot).unwrap();
    stale["requirement_evidence_plan"]["review_requirement_ids"] = json!(["check-only"]);
    replace_snapshot_json(
        store,
        &subject.attempt_id,
        model::canonical(&stale).unwrap(),
    )
    .await;
    let stale_error = store
        .call(reviewer.clone(), "review.submit".into(), request.clone())
        .await
        .expect_err("non-pass results still require the exact frozen V2 plan");
    assert_eq!(stale_error.code, "REQUIREMENT_EVIDENCE_PLAN_STALE");
    replace_snapshot_json(store, &subject.attempt_id, original_snapshot.clone()).await;
    assert_eq!(
        snapshot_json(store, &subject.attempt_id).await,
        original_snapshot
    );
    request["client_request_id"] = json!(model::new_id());
    let result = store
        .call(reviewer.clone(), "review.submit".into(), request)
        .await
        .expect("legitimate negative/uncertain evidence must be retained");
    assert_eq!(result["verdict"], verdict);
    assert_eq!(result["findings"], findings);
    assert_eq!(result["applicability"], "current_candidate");
    assert_eq!(result["task_feedback_applied"], false);
    assert_eq!(result["acceptance_changed"], false);
    assert_eq!(result["publication_started"], false);
    assert!(result.get("requirement_reviews").is_none());
    let readback = store
        .call(
            reviewer,
            "review.get".into(),
            json!({"review_assignment_id":assignment_id}),
        )
        .await
        .unwrap();
    assert_eq!(readback["result"], result);
    let automatic = consume_automatic_acceptance(
        store,
        project_id,
        &assignment_id,
        result["operation_id"].as_str().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(automatic["status"], "skipped");
    assert_eq!(automatic["code"], "assigned_review_pass_required");
    assert_eq!(automatic["acceptance_started"], false);
    assert_eq!(automatic["task_accepted"], false);
    let reconciled = store.reconcile_review_dispositions_once().await.unwrap();
    assert_eq!(reconciled["review_results"]["quarantined"], 0);
    let retained_attempt = subject.attempt_id.clone();
    let counts = store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT (SELECT COUNT(*) FROM operations WHERE method='task.accept' AND attempt_id=?1), \
                        (SELECT COUNT(*) FROM observations WHERE kind='review.result' AND operation_id=?2)",
                params![retained_attempt, result["operation_id"].as_str().unwrap()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(counts, (0, 1));
    let task = store
        .call(
            principal("review-owner-v2", Role::Manager),
            "task.get".into(),
            json!({"task_id":subject.task_id}),
        )
        .await
        .unwrap();
    assert_eq!(task["state"], "open");
    assert!(task["accepted_operation_id"].is_null());
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn v2_changes_requested_is_retained_without_structured_pass_or_autoaccept() {
    assert_v2_nonpass_retained_without_autoaccept("changes_requested").await;
}

#[tokio::test]
async fn v2_inconclusive_is_retained_without_structured_pass_or_autoaccept() {
    assert_v2_nonpass_retained_without_autoaccept("inconclusive").await;
}

fn requirement_check_config() -> Config {
    let mut config = Config::default();
    config.checks.enabled = true;
    let test_executable = std::env::current_exe().expect("resolve test executable");
    let executor = test_executable
        .parent()
        .and_then(Path::parent)
        .expect("test executable is in the Cargo deps directory")
        .join(format!("swarm-checks{}", std::env::consts::EXE_SUFFIX))
        .canonicalize()
        .expect("build the same-source swarm-checks binary before this real Store fixture");
    config.checks.executor = Some(crate::checks::model::ExecutorPin {
        sha256: model::digest(&std::fs::read(&executor).expect("read real check executor")),
        executable: executor,
        artifact_id: "swarm-checks".to_owned(),
        version: "0.1.0".to_owned(),
    });
    config.checks.profiles = ["1", "2"]
        .into_iter()
        .map(|revision| CheckProfile {
            profile_id: "requirement-check".to_owned(),
            profile_revision: revision.to_owned(),
            executable: config.checks.git_executable.clone(),
            args: vec!["--version".to_owned()],
            parser: Parser::ExitCode,
            resource: "requirement-evidence-plan-checks".to_owned(),
            environment: BTreeMap::new(),
            inherit_env: Vec::new(),
            expected_targets: Vec::new(),
            reproducible: false,
            fingerprint_env: Vec::new(),
            versioned_inputs: BTreeMap::new(),
        })
        .collect();
    config
}

fn mixed_v2_spec() -> Value {
    let mut spec = review_only_v2_spec();
    spec["requirements"] = json!([
        {
            "id":"review-only",
            "statement":"Independent review confirms the behavior.",
            "verification":{"review_required":true,"check_profiles":[]},
        },
        {
            "id":"check-only",
            "statement":"The configured CheckRun proves the criterion.",
            "verification":{"review_required":false,"check_profiles":[
                {"profile_id":"requirement-check","profile_revision":"1"}
            ]},
        },
        {
            "id":"review-and-check",
            "statement":"Both independent review and the configured CheckRun are required.",
            "verification":{"review_required":true,"check_profiles":[
                {"profile_id":"requirement-check","profile_revision":"1"}
            ]},
        },
    ]);
    spec
}

#[tokio::test]
async fn direct_mixed_v2_acceptance_readback_keeps_original_ordered_mapping() {
    let config = requirement_check_config();
    let git_executable = config.checks.git_executable.clone();
    let (owner, directory, _) = start_store_with_config("r22-direct-mapping", config).await;
    let store = &owner.store;
    seed_clients(store).await;
    let (task_id, attempt_id) = create_v2_attempt(
        store,
        "r22-direct-mapping",
        mixed_v2_spec(),
        "native_manager",
        None,
    )
    .await;
    let (subject, feedback_cursor) =
        capture_and_submit_attempt(store, &directory, &git_executable, task_id, attempt_id).await;
    let task_owner = principal("review-owner-v2", Role::Manager);
    let gm = principal("review-gm", Role::Manager);
    let (stop_checks, stopping_checks) = tokio::sync::watch::channel(false);
    let supervisor = tokio::spawn(store.clone().supervise_checks(stopping_checks));
    let check_id = run_check(
        store,
        &task_owner,
        &subject.attempt_id,
        &subject.candidate_ref,
        "1",
    )
    .await;
    let review_only = expected_review("review-only", &subject.candidate_ref);
    let review_and_check = expected_review("review-and-check", &subject.candidate_ref);
    let accepted = store
        .call(
            gm.clone(),
            "task.accept".into(),
            acceptance_request(
                &subject.attempt_id,
                &subject.submission_ref,
                &subject.candidate_ref,
                feedback_cursor,
                &[review_only.clone(), review_and_check.clone()],
                std::slice::from_ref(&check_id),
            ),
        )
        .await
        .unwrap();
    assert_eq!(accepted["coalesced"], Value::Null);
    let operation_id = accepted["operation_id"].as_str().unwrap();
    let settled = settled_operation(store, &gm, operation_id).await;
    assert_eq!(settled["result"]["outcome"], "applied");
    assert_eq!(settled["result"]["task_accepted"], true);
    let mapping = json!([
        {
            "requirement_id":"review-only",
            "review":{"rationale":review_only["rationale"],"evidence":[subject.candidate_ref]},
            "checks":[],
        },
        {
            "requirement_id":"check-only",
            "review":null,
            "checks":[{"profile_id":"requirement-check","profile_revision":"1","check_id":check_id}],
        },
        {
            "requirement_id":"review-and-check",
            "review":{"rationale":review_and_check["rationale"],"evidence":[subject.candidate_ref]},
            "checks":[{"profile_id":"requirement-check","profile_revision":"1","check_id":check_id}],
        },
    ]);
    assert_original_acceptance_mapping(store, &gm, operation_id, Some(&mapping)).await;
    stop_checks.send_replace(true);
    supervisor.await.unwrap().unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

async fn seed_retained_v1_attempt(
    store: &Store,
    project_id: &str,
    binding_id: Option<&str>,
) -> (String, String) {
    // Load an already-frozen historical record. Never admit V1 through current
    // task.create/task.claim or rewrite a live V2 Attempt to label it historical.
    let task_id = model::new_id();
    let attempt_id = model::new_id();
    let project_id = project_id.to_owned();
    let binding_id = binding_id.map(str::to_owned);
    store
        .run(move |db| {
            let mut spec = review_only_v2_spec();
            spec["acceptance"] = json!({"required_check_profiles":[]});
            spec["requirements"][0].as_object_mut().unwrap().remove("verification");
            let typed: crate::model::TaskSpec = serde_json::from_value(spec.clone())?;
            typed.validate_retained()?;
            let mut brief = serde_json::to_value(typed.try_brief()?)?;
            brief.as_object_mut().unwrap().remove("requirement_evidence_plan");
            let snapshot = json!({
                "spec":spec,
                "revision":1,
                "dependency_acceptances":[],
                "baseline_candidate":{"status":"wide","reason":"baseline_not_configured"},
                "owner_policy":crate::policy::accepted_edition(Some(crate::policy::OWNER_POLICY_V2_ID))?,
                "brief":brief,
            });
            let start_owner = if binding_id.is_some() {"controller"} else {"native_manager"};
            let generation = binding_id.as_ref().map(|_| 1_i64);
            let tx = db.transaction()?;
            tx.execute(
                "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,1,'open',?3,1,1)",
                params![task_id, project_id, model::canonical(&spec)?],
            )?;
            tx.execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner, \
                 binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,1,?3,'review-owner-v2',?4,?5,?6,'reserved','[]',1,1)",
                params![
                    attempt_id, task_id, model::canonical(&snapshot)?, start_owner, binding_id, generation,
                ],
            )?;
            tx.commit()?;
            Ok((task_id, attempt_id))
        })
        .await
        .unwrap()
}

async fn snapshot_json(store: &Store, attempt_id: &str) -> String {
    let attempt_id = attempt_id.to_owned();
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT task_snapshot_json FROM attempts WHERE attempt_id=?1",
                [attempt_id],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap()
}

async fn replace_snapshot_json(store: &Store, attempt_id: &str, snapshot: String) {
    let attempt_id = attempt_id.to_owned();
    store
        .run(move |db| {
            let changed = db.execute(
                "UPDATE attempts SET task_snapshot_json=?2 WHERE attempt_id=?1",
                params![attempt_id, snapshot],
            )?;
            assert_eq!(changed, 1);
            Ok(())
        })
        .await
        .unwrap();
}

async fn assert_retained_v1_acceptance_has_no_mapping(automatic: bool) {
    let config = Config::default();
    let git_executable = config.checks.git_executable.clone();
    let (owner, directory, _) = start_store_with_config("r22-v1-readback", config).await;
    let store = &owner.store;
    seed_clients(store).await;
    let project_id = "r22-v1-readback";
    let (task_id, attempt_id) = seed_retained_v1_attempt(store, project_id, None).await;
    let original_snapshot = snapshot_json(store, &attempt_id).await;
    let (subject, cursor) =
        capture_and_submit_attempt(store, &directory, &git_executable, task_id, attempt_id).await;
    let reviews = vec![expected_review("R1", &subject.candidate_ref)];
    let gm = principal("review-gm", Role::Manager);
    let operation_id = if automatic {
        configure_automatic_acceptance(store, project_id).await;
        let (reviewer, assignment_id) = assign_sponsored_reviewer(
            store,
            principal("review-owner-v2", Role::Manager),
            &subject,
            "r22-v1-reviewer",
            "r22-v1-reviewer-token",
        )
        .await;
        let review = store
            .call(
                reviewer,
                "review.submit".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "review_assignment_id":assignment_id,
                    "submission_ref":subject.submission_ref,
                    "candidate_ref":subject.candidate_ref,
                    "verdict":"pass","coverage":"complete","findings":[],
                    "evidence_refs":[subject.candidate_ref],
                    "requirement_reviews":reviews,
                }),
            )
            .await
            .unwrap();
        assert_eq!(review["verdict"], "pass");
        let reconciled = store.reconcile_review_dispositions_once().await.unwrap();
        assert_eq!(reconciled["review_results"]["quarantined"], 0);
        let accepted_attempt = subject.attempt_id.clone();
        store
            .run(move |db| {
                Ok(db.query_row(
                    "SELECT operation_id FROM operations WHERE method='task.accept' AND attempt_id=?1",
                    [accepted_attempt],
                    |row| row.get::<_, String>(0),
                )?)
            })
            .await
            .unwrap()
    } else {
        let receipt = store
            .call(
                gm.clone(),
                "task.accept".into(),
                acceptance_request(
                    &subject.attempt_id,
                    &subject.submission_ref,
                    &subject.candidate_ref,
                    cursor,
                    &reviews,
                    &[],
                ),
            )
            .await
            .unwrap();
        receipt["operation_id"].as_str().unwrap().to_owned()
    };
    let settled = settled_operation(store, &gm, &operation_id).await;
    assert_eq!(settled["result"]["outcome"], "applied");
    assert_eq!(settled["result"]["task_accepted"], true);
    assert_original_acceptance_mapping(store, &gm, &operation_id, None).await;
    assert_eq!(
        snapshot_json(store, &subject.attempt_id).await,
        original_snapshot
    );
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn retained_v1_direct_acceptance_readback_does_not_fabricate_requirement_mapping() {
    assert_retained_v1_acceptance_has_no_mapping(false).await;
}

#[tokio::test]
async fn retained_v1_automatic_acceptance_readback_does_not_fabricate_requirement_mapping() {
    assert_retained_v1_acceptance_has_no_mapping(true).await;
}

fn dispatch_fixture_config() -> Config {
    Config {
        routes: vec![
            serde_json::from_value(json!({
                "alias":"r22-dispatch-fixture",
                "runtime":"muse",
                "module_artifact_id":"muse-sdk-1.3.0-bridge.5",
                "enabled":true,
                "native_options":{
                    "workspaceRoot":if cfg!(windows) {r"C:\fixture"} else {"/fixture"},
                    "modelId":"fixture-model",
                },
            }))
            .unwrap(),
        ],
        ..Config::default()
    }
}

async fn retained_dispatch_binding(store: &Store, uses_task_prompt: bool) -> (String, Principal) {
    // Seed a ready retained module, including its exact trusted selector, link
    // and boot. The non-TaskPrompt contract is retained-only: no new binding
    // admission or native executable is simulated by this metadata fixture.
    let binding_id = model::new_id();
    let module = principal("r22-dispatch-module", Role::Module);
    let stored_module = module.clone();
    store
        .run(move |db| {
            let tx = db.transaction()?;
            let mut command_schemas = json!([
                {"schema_id":"swarm.runtime_command","version":"1"},
                {"schema_id":"swarm.task_dispatch_context","version":"1"},
            ]);
            if uses_task_prompt {
                command_schemas.as_array_mut().unwrap().push(json!({
                    "schema_id":"swarm.task_prompt","version":"1",
                }));
            }
            let descriptor = json!({
                "schema_version":1,
                "module_id":"r22-dispatch-module",
                "artifact":{"artifact_id":"muse-sdk-1.3.0-bridge.5","version":"5"},
                "launch":{
                    "executable":if cfg!(windows) {r"C:\modules\muse-bridge"} else {"/modules/muse-bridge"},
                    "argv":[],"environment":[],"inherited_environment_allowlist":[],
                },
                "command_schemas":command_schemas,
                "event_schemas":[
                    {"schema_id":"swarm.runtime_outcome","version":"1"},
                    {"schema_id":"swarm.task_dispatch_admission","version":"1"},
                ],
                "protocol":{"minimum":{"major":1,"minor":0},"maximum":{"major":1,"minor":0}},
                "capabilities":["task.dispatch"],
                "lifecycle":"owned_service","activation":"on_demand","enabled":true,
            });
            let registered = crate::store::module_handshake::register_trusted_descriptor(
                &tx,
                serde_json::from_value(descriptor)?,
            )?;
            let selector = json!({
                "schema_version":1,
                "module_id":registered["module_id"],
                "artifact":registered["artifact"],
                "registered_revision":registered["registered_revision"],
                "selected_revision":registered["registered_revision"],
            });
            crate::store::set_meta(
                &tx,
                &format!("client:{}", stored_module.client_id),
                &json!({
                    "role":"module","disabled":false,
                    "binding_id":binding_id,"binding_generation":1,
                }),
            )?;
            let config = dispatch_fixture_config();
            let route = serde_json::to_value(&config.routes[0])?;
            let state = json!({
                "connection":"connected",
                "module_client_id":stored_module.client_id,
                "module_link_id":stored_module.link_id,
                "bridge_boot_id":"r22-dispatch-boot",
                "module_contract_selector":selector,
            });
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id, \
                 state,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,'r22-dispatch-lane','r22-dispatch-module','muse-sdk-1.3.0-bridge.5','ready',?2,?3,1)",
                params![binding_id, model::canonical(&route)?, model::canonical(&state)?],
            )?;
            let binding = crate::store::operations::get_binding(&tx, &binding_id, 1)?;
            assert_eq!(
                crate::store::task_prompt::selected(&tx, &binding)?,
                uses_task_prompt,
            );
            tx.commit()?;
            Ok(binding_id)
        })
        .await
        .map(|binding_id| (binding_id, module))
        .unwrap()
}

async fn dispatch_request(store: &Store, attempt_id: &str) -> Result<Value> {
    store
        .call(
            principal("review-owner-v2", Role::Manager),
            "task.dispatch".into(),
            json!({
                "client_request_id":model::new_id(),
                "attempt_id":attempt_id,
                "text":"Implement the exact frozen requirement evidence contract.",
            }),
        )
        .await
}

async fn assert_valid_dispatch_keeps_original_snapshot(
    uses_task_prompt: bool,
    historical_v1: bool,
) {
    let (owner, directory, _) =
        start_store_with_config("r22-valid-dispatch", dispatch_fixture_config()).await;
    let store = &owner.store;
    seed_clients(store).await;
    let (binding_id, module) = retained_dispatch_binding(store, uses_task_prompt).await;
    let (task_id, attempt_id) = if historical_v1 {
        seed_retained_v1_attempt(store, "r22-valid-dispatch", Some(&binding_id)).await
    } else {
        create_v2_attempt(
            store,
            "r22-valid-dispatch",
            review_only_v2_spec(),
            "controller",
            Some(&binding_id),
        )
        .await
    };
    let original_json = snapshot_json(store, &attempt_id).await;
    let original: Value = serde_json::from_str(&original_json).unwrap();
    if historical_v1 {
        assert_eq!(
            original["spec"]["acceptance"],
            json!({"required_check_profiles":[]})
        );
        assert!(original.get("requirement_evidence_plan").is_none());
        assert!(original["brief"].get("requirement_evidence_plan").is_none());
    } else {
        assert_eq!(
            original["requirement_evidence_plan"]["review_requirement_ids"],
            json!(["R1"])
        );
        assert_eq!(
            original["brief"]["requirement_evidence_plan"],
            original["requirement_evidence_plan"]
        );
    }
    let admitted = dispatch_request(store, &attempt_id).await.unwrap();
    assert_eq!(admitted["state"], "queued");
    let operation_id = admitted["operation_id"].as_str().unwrap();
    let delivered = store
        .call(module, "module.next".into(), json!({}))
        .await
        .unwrap();
    let command = &delivered["command"];
    assert_eq!(command["method"], "task.dispatch");
    assert_eq!(command["operation_id"], operation_id);
    assert_eq!(command["binding_id"], binding_id);
    assert_eq!(command["generation"], 1);
    assert_eq!(command["input"]["attempt_id"], attempt_id);
    let context = &command["input"]["task_dispatch_context"];
    assert_eq!(context["task_id"], task_id);
    assert_eq!(context["attempt_id"], attempt_id);
    assert_eq!(context["binding_generation"], 1);
    assert_eq!(context["worker_boot_id"], "r22-dispatch-boot");
    assert_eq!(
        context["task_snapshot_sha256"],
        model::digest(model::canonical(&original).unwrap().as_bytes()),
    );
    if uses_task_prompt {
        let envelope = &command["input"]["task_prompt"];
        assert_eq!(envelope["task_id"], task_id);
        assert_eq!(envelope["attempt_id"], attempt_id);
        assert_eq!(envelope["task_revision"], 1);
        assert_eq!(
            envelope["task_snapshot_sha256"],
            context["task_snapshot_sha256"]
        );
        assert!(
            envelope["prompt"]
                .as_str()
                .unwrap()
                .contains(&model::canonical(&original["brief"]).unwrap())
        );
        assert!(command["input"].get("task_snapshot").is_none());
    } else {
        assert!(command["input"].get("task_prompt").is_none());
        assert_eq!(command["input"]["task_snapshot"], original);
    }
    assert_eq!(snapshot_json(store, &attempt_id).await, original_json);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

async fn assert_v2_dispatch_rejects_stale_plan(
    corrupt_brief: bool,
    uses_task_prompt: bool,
    after_reservation: bool,
) {
    let (owner, directory, _) =
        start_store_with_config("r22-stale-dispatch", dispatch_fixture_config()).await;
    let store = &owner.store;
    seed_clients(store).await;
    let (binding_id, module) = retained_dispatch_binding(store, uses_task_prompt).await;
    let (task_id, attempt_id) = create_v2_attempt(
        store,
        "r22-stale-dispatch",
        review_only_v2_spec(),
        "controller",
        Some(&binding_id),
    )
    .await;
    let original: Value = serde_json::from_str(&snapshot_json(store, &attempt_id).await).unwrap();
    let mut stale = original.clone();
    if corrupt_brief {
        stale["brief"]["requirement_evidence_plan"]["review_requirement_ids"] =
            json!(["check-only"]);
        assert_eq!(
            stale["requirement_evidence_plan"],
            original["requirement_evidence_plan"]
        );
    } else {
        // The same retained-state fault used by the original acceptance negative.
        stale["requirement_evidence_plan"]["review_requirement_ids"] = json!(["check-only"]);
        assert_eq!(stale["brief"], original["brief"]);
    }
    assert_eq!(stale["spec"], original["spec"]);
    assert_eq!(stale["revision"], original["revision"]);
    let queued_operation = if after_reservation {
        let admitted = dispatch_request(store, &attempt_id).await.unwrap();
        assert_eq!(admitted["state"], "queued");
        Some(admitted["operation_id"].as_str().unwrap().to_owned())
    } else {
        None
    };
    let stale_json = model::canonical(&stale).unwrap();
    replace_snapshot_json(store, &attempt_id, stale_json.clone()).await;
    if let Some(operation_id) = queued_operation.as_deref() {
        let rejected = store
            .call(module, "module.next".into(), json!({}))
            .await
            .unwrap();
        assert!(rejected["command"].is_null());
        assert_eq!(rejected["rejected_operation_id"], operation_id);
        assert_eq!(rejected["error"]["code"], "REQUIREMENT_EVIDENCE_PLAN_STALE");
    } else {
        let rejected = dispatch_request(store, &attempt_id)
            .await
            .expect_err("inconsistent V2 plan must reject before reserving a start");
        assert_eq!(rejected.code, "REQUIREMENT_EVIDENCE_PLAN_STALE");
    }
    let inspect_attempt = attempt_id.clone();
    let inspect_binding = binding_id.clone();
    let inspect_operation = queued_operation.clone();
    store
        .run(move |db| {
            let (owner_id, start_owner, revision, binding, generation, state, start, released): (
                String,
                String,
                i64,
                String,
                i64,
                String,
                Option<String>,
                Option<i64>,
            ) = db.query_row(
                "SELECT owner_id,start_owner,task_revision,binding_id,binding_generation,state, \
                        start_operation_id,released_at_ms FROM attempts WHERE attempt_id=?1",
                [&inspect_attempt],
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
            )?;
            assert_eq!(owner_id, "review-owner-v2");
            assert_eq!(start_owner, "controller");
            assert_eq!(revision, 1);
            assert_eq!(binding, inspect_binding);
            assert_eq!(generation, 1);
            assert_eq!(state, "reserved");
            assert_eq!(start, inspect_operation);
            assert_eq!(released, None);
            let undenied: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method='task.dispatch' \
                 AND attempt_id=?1 AND state!='rejected'",
                [&inspect_attempt],
                |row| row.get(0),
            )?;
            assert_eq!(
                undenied, 0,
                "no start command may escape the prerequisite guard"
            );
            Ok(())
        })
        .await
        .unwrap();
    let task = store
        .call(
            principal("review-owner-v2", Role::Manager),
            "task.get".into(),
            json!({"task_id":task_id}),
        )
        .await
        .unwrap();
    assert_eq!(task["revision"], 1);
    assert_eq!(task["state"], "open");
    assert_eq!(task["current_attempt_id"], attempt_id);
    assert_eq!(task["spec"], original["spec"]);
    assert_eq!(snapshot_json(store, &attempt_id).await, stale_json);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn v2_task_prompt_dispatch_preserves_valid_frozen_plan() {
    assert_valid_dispatch_keeps_original_snapshot(true, false).await;
}

#[tokio::test]
async fn retained_v1_task_prompt_dispatch_preserves_original_snapshot() {
    assert_valid_dispatch_keeps_original_snapshot(true, true).await;
}

#[tokio::test]
async fn v2_non_task_prompt_dispatch_preserves_valid_frozen_plan() {
    assert_valid_dispatch_keeps_original_snapshot(false, false).await;
}

#[tokio::test]
async fn retained_v1_non_task_prompt_dispatch_preserves_original_snapshot() {
    assert_valid_dispatch_keeps_original_snapshot(false, true).await;
}

#[tokio::test]
async fn v2_task_dispatch_rejects_stale_top_level_plan_before_reservation() {
    for uses_task_prompt in [true, false] {
        assert_v2_dispatch_rejects_stale_plan(false, uses_task_prompt, false).await;
    }
}

#[tokio::test]
async fn v2_task_dispatch_rejects_stale_brief_plan_before_reservation() {
    for uses_task_prompt in [true, false] {
        assert_v2_dispatch_rejects_stale_plan(true, uses_task_prompt, false).await;
    }
}

#[tokio::test]
async fn queued_v2_task_dispatch_rejects_stale_top_level_plan_before_delivery() {
    for uses_task_prompt in [true, false] {
        assert_v2_dispatch_rejects_stale_plan(false, uses_task_prompt, true).await;
    }
}

#[tokio::test]
async fn queued_v2_task_dispatch_rejects_stale_brief_plan_before_delivery() {
    for uses_task_prompt in [true, false] {
        assert_v2_dispatch_rejects_stale_plan(true, uses_task_prompt, true).await;
    }
}
