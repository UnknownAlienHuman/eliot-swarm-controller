//! Focused Store regressions for the sponsored review disposition lifecycle.
use super::*;
use rusqlite::params;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone)]
struct ReviewSubject {
    task_id: String,
    attempt_id: String,
    submission_ref: String,
    candidate_ref: String,
}

async fn start_store(label: &str) -> (StoreOwner, PathBuf, Credential) {
    start_store_with_config(label, Config::default()).await
}

async fn start_store_with_config(
    label: &str,
    mut config: Config,
) -> (StoreOwner, PathBuf, Credential) {
    let directory = std::env::temp_dir().join(format!("swarm-program-{label}-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    (owner, directory, credential)
}

fn principal(client_id: &str, role: Role) -> Principal {
    Principal {
        link_id: model::new_id(),
        client_id: client_id.to_owned(),
        role,
    }
}

async fn seed_clients(store: &Store) {
    store
        .run(|db| {
            for (client_id, role) in [
                ("review-owner-v2", Role::Manager),
                ("review-owner-v1", Role::Manager),
                ("review-gm", Role::Manager),
                ("unrelated-manager", Role::Manager),
                ("unrelated-observer", Role::Observer),
            ] {
                set_meta(
                    db,
                    &format!("client:{client_id}"),
                    &json!({"role":role,"disabled":false}),
                )?;
            }
            set_meta(db, "gm", &json!({"client_id":"review-gm","epoch":1}))?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn seed_subject(
    store: &Store,
    suffix: &str,
    owner_id: &str,
    owner_policy_id: &str,
    add_private_checks: bool,
) -> ReviewSubject {
    let suffix = suffix.to_owned();
    let owner_id = owner_id.to_owned();
    let owner_policy_id = owner_policy_id.to_owned();
    store
        .run(move |db| {
            let task_id = format!("task-{suffix}");
            let attempt_id = format!("attempt-{suffix}");
            let candidate_ref = format!(
                "source-{}",
                model::digest(format!("candidate-{suffix}").as_bytes())
            );
            let submission_ref = format!(
                "submission-{}",
                model::digest(format!("submission-{suffix}").as_bytes())
            );
            let submission_operation_id = format!("submit-op-{suffix}");
            let candidate_bytes = b"explicit source snapshot fixture";
            let candidate_digest = model::digest(candidate_bytes);
            let candidate_metadata = json!({
                "attempt_id":attempt_id,
                "task_revision":1,
                "fixture":"program review lifecycle",
            });
            let spec_json = json!({
                "objective":format!("Review fixture {suffix}"),
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Preserve the exact review finding scope"}],
                "owner_policy_id":owner_policy_id,
            });
            let spec: crate::model::TaskSpec = serde_json::from_value(spec_json.clone())?;
            let edition = crate::policy::accepted_edition(Some(&owner_policy_id))?;
            let owner_policy = serde_json::to_value(edition)?;
            let snapshot = json!({
                "spec":spec_json,
                "revision":1,
                "dependency_acceptances":[],
                "baseline_candidate":null,
                "owner_policy":owner_policy,
                "brief":spec.brief(),
            });
            db.execute(
                "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,'fixture',1,'open',?2,1,1)",
                params![task_id, model::canonical(&spec_json)?],
            )?;
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'source_snapshot',?3,?4,1,?5)",
                params![
                    candidate_ref,
                    format!("artifacts/{candidate_ref}.bin"),
                    i64::try_from(candidate_bytes.len()).unwrap(),
                    candidate_digest,
                    model::canonical(&candidate_metadata)?,
                ],
            )?;
            let submission_document = json!({
                "schema_version":1,
                "operation_id":submission_operation_id,
                "task_id":task_id,
                "attempt_id":attempt_id,
                "task_revision":1,
                "phase":"implementation",
                "owner_id":owner_id,
                "submitted_by":owner_id,
                "previous_submission_ref":null,
                "candidate_ref":candidate_ref,
                "candidate_sha256":candidate_digest,
                "candidate_kind":"source_snapshot",
                "candidate_byte_length":candidate_bytes.len(),
                "summary":"fixture submission",
                "claims":[],
                "claim_counts":{"total":0,"met":0,"not_met":0,"deferred":0,"unreported":0},
                "evidence_level":"submitter_report",
                "source_checkout_verified":false,
            });
            let submission_digest = model::digest(model::canonical(&submission_document)?.as_bytes());
            let submission_metadata = json!({"operation_id":submission_operation_id});
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'task_submission',?3,?4,1,?5)",
                params![
                    submission_ref,
                    format!("artifacts/{submission_ref}.bin"),
                    i64::try_from(model::canonical(&submission_document)?.len()).unwrap(),
                    submission_digest,
                    model::canonical(&submission_metadata)?,
                ],
            )?;
            db.execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,submission_ref,candidate_ref,created_at_ms,updated_at_ms) VALUES(?1,?2,1,?3,?4,'controller','submitted','[]',?5,?6,1,1)",
                params![
                    attempt_id,
                    task_id,
                    model::canonical(&snapshot)?,
                    owner_id,
                    submission_ref,
                    candidate_ref,
                ],
            )?;
            let submit_request = json!({
                "client_request_id":format!("submit-request-{suffix}"),
                "attempt_id":attempt_id,
                "expected_revision":1,
                "expected_submission_ref":null,
                "candidate_ref":candidate_ref,
            });
            db.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,'task.submit',?4,?5,?6,?7,'settled',?8,1,1,1,1)",
                params![
                    submission_operation_id,
                    owner_id,
                    format!("submit-request-{suffix}"),
                    model::canonical(&submit_request)?,
                    model::canonical(&json!({"submission_document":submission_document}))?,
                    task_id,
                    attempt_id,
                    model::canonical(&json!({
                        "operation_id":submission_operation_id,
                        "outcome":"applied",
                        "attempt_id":attempt_id,
                        "submission_ref":submission_ref,
                        "candidate_ref":candidate_ref,
                        "task_accepted":false,
                    }))?,
                ],
            )?;
            if add_private_checks {
                for (operation_id, method, observation_kind) in [
                    (format!("check-run-{suffix}"), "check.run", "check.completed"),
                    (format!("check-cancel-{suffix}"), "check.cancel", "check.cancel"),
                ] {
                    db.execute(
                        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,'{}','{}',?5,?6,'settled','{}',1,1,2,2)",
                        params![
                            operation_id,
                            owner_id,
                            format!("{operation_id}-request"),
                            method,
                            task_id,
                            attempt_id,
                        ],
                    )?;
                    db.execute(
                        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller',?1,?2,?3,?4,2)",
                        params![
                            format!("fixture:{operation_id}"),
                            operation_id,
                            observation_kind,
                            model::canonical(&json!({"fixture":true,"attempt_id":attempt_id}))?,
                        ],
                    )?;
                }
            }

            Ok(ReviewSubject {
                task_id,
                attempt_id,
                submission_ref,
                candidate_ref,
            })
        })
        .await
        .unwrap()
}

async fn configure_acceptance_fixture(store: &Store, manager_id: &str, subjects: &[ReviewSubject]) {
    let manager_id = manager_id.to_owned();
    let subjects = subjects
        .iter()
        .map(|subject| (subject.task_id.clone(), subject.attempt_id.clone()))
        .collect::<Vec<_>>();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            for (task_id, attempt_id) in &subjects {
                let task_spec_raw: String = tx.query_row(
                    "SELECT spec_json FROM tasks WHERE task_id=?1",
                    [task_id],
                    |row| row.get(0),
                )?;
                let mut spec: Value = serde_json::from_str(&task_spec_raw)?;
                spec["acceptance"] = json!({"required_check_profiles":[]});
                let validated: crate::model::TaskSpec = serde_json::from_value(spec.clone())?;
                validated.validate()?;
                tx.execute(
                    "UPDATE tasks SET spec_json=?2 WHERE task_id=?1",
                    params![task_id, model::canonical(&spec)?],
                )?;

                let snapshot_raw: String = tx.query_row(
                    "SELECT task_snapshot_json FROM attempts WHERE attempt_id=?1",
                    [attempt_id],
                    |row| row.get(0),
                )?;
                let mut snapshot: Value = serde_json::from_str(&snapshot_raw)?;
                snapshot["spec"] = spec;
                tx.execute(
                    "UPDATE attempts SET task_snapshot_json=?2 WHERE attempt_id=?1",
                    params![attempt_id, model::canonical(&snapshot)?],
                )?;
            }

            let mut entry = crate::automation::config::AutomationEntry::new(
                &manager_id,
                "fixture",
                "cross-owner-acceptance",
                10,
            );
            entry.enabled = true;
            entry.steps = vec![crate::automation::actions::AutomationStep::Acceptance];
            crate::automation::config::validate_entry(&entry)?;
            let entry_key =
                crate::automation::config::entry_key(&manager_id, "fixture", &entry.automation_id)?;
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

async fn assign_sponsored_reviewer(
    store: &Store,
    owner: Principal,
    subject: &ReviewSubject,
    reviewer_id: &str,
    token: &str,
) -> (Principal, String) {
    let pending_scope = json!({
        "review_assignment_id":null,
        "task_id":subject.task_id,
        "attempt_id":subject.attempt_id,
        "task_revision":1,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
    });
    store
        .call(
            owner.clone(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":format!("register-{reviewer_id}"),
                "client_id":reviewer_id,
                "token_hash":model::digest(token.as_bytes()),
                "task_id":subject.task_id,
                "task_revision":1,
                "attempt_id":subject.attempt_id,
                "participation_basis":{"kind":"sponsored_reviewer","review_scope":pending_scope},
            }),
        )
        .await
        .unwrap();
    let assignment = store
        .call(
            owner,
            "review.assign".into(),
            json!({
                "client_request_id":format!("assign-{reviewer_id}"),
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "reviewer_client_id":reviewer_id,
            }),
        )
        .await
        .unwrap();
    let assignment_id = assignment["review_assignment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let reviewer = store
        .authenticate(Credential {
            client_id: reviewer_id.to_owned(),
            token: token.to_owned(),
        })
        .await
        .unwrap();
    (reviewer, assignment_id)
}

fn review_result_request(subject: &ReviewSubject, assignment_id: &str, request_id: &str) -> Value {
    json!({
        "client_request_id":request_id,
        "review_assignment_id":assignment_id,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
        "verdict":"changes_requested",
        "coverage":"complete",
        "findings":[{
            "finding_id":"missing-r1-evidence",
            "requirement_ids":["R1"],
            "reason":"The candidate omits the requested evidence.",
            "evidence_refs":["evidence://review/r1"],
            "requested_change":"Add the retained evidence for R1.",
        }],
        "evidence_refs":["evidence://review/r1"],
    })
}

fn passing_review_result_request(
    subject: &ReviewSubject,
    assignment_id: &str,
    request_id: &str,
) -> Value {
    json!({
        "client_request_id":request_id,
        "review_assignment_id":assignment_id,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
        "verdict":"pass",
        "coverage":"complete",
        "findings":[],
        "evidence_refs":[subject.candidate_ref],
        "requirement_reviews":[{
            "requirement_id":"R1",
            "rationale":"The retained candidate evidence satisfies the frozen requirement.",
            "evidence":[subject.candidate_ref]
        }]
    })
}

#[tokio::test]
async fn current_gm_acceptance_consumes_owner_sponsored_review_without_owner_self_acceptance() {
    let (owner, directory, bootstrap_credential) = start_store("cross-owner-acceptance").await;
    let local_operator = owner
        .store
        .authenticate(bootstrap_credential)
        .await
        .unwrap();
    assert_eq!(local_operator.role, Role::Operator);
    seed_clients(&owner.store).await;
    let owner_subject = seed_subject(
        &owner.store,
        "cross-owner",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let gm_owned_subject = seed_subject(
        &owner.store,
        "gm-owned",
        "review-gm",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let operator_sponsored_subject = seed_subject(
        &owner.store,
        "operator-sponsored",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    configure_acceptance_fixture(
        &owner.store,
        "review-gm",
        &[
            owner_subject.clone(),
            gm_owned_subject.clone(),
            operator_sponsored_subject.clone(),
        ],
    )
    .await;

    let owner_manager = principal("review-owner-v2", Role::Manager);
    let (owner_reviewer, owner_assignment_id) = assign_sponsored_reviewer(
        &owner.store,
        owner_manager.clone(),
        &owner_subject,
        "cross-owner-reviewer",
        "cross-owner-reviewer-token",
    )
    .await;
    let gm = principal("review-gm", Role::Manager);
    let (gm_subject_reviewer, gm_assignment_id) = assign_sponsored_reviewer(
        &owner.store,
        gm.clone(),
        &gm_owned_subject,
        "gm-owned-reviewer",
        "gm-owned-reviewer-token",
    )
    .await;

    let operator_pending_scope = json!({
        "review_assignment_id":null,
        "task_id":operator_sponsored_subject.task_id.clone(),
        "attempt_id":operator_sponsored_subject.attempt_id.clone(),
        "task_revision":1,
        "submission_ref":operator_sponsored_subject.submission_ref.clone(),
        "candidate_ref":operator_sponsored_subject.candidate_ref.clone(),
    });
    owner
        .store
        .call(
            local_operator.clone(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":"register-operator-sponsored-reviewer",
                "client_id":"operator-sponsored-reviewer",
                "token_hash":model::digest(b"operator-sponsored-reviewer-token"),
                "task_id":operator_sponsored_subject.task_id.clone(),
                "task_revision":1,
                "attempt_id":operator_sponsored_subject.attempt_id.clone(),
                "participation_basis":{"kind":"sponsored_reviewer","review_scope":operator_pending_scope},
            }),
        )
        .await
        .unwrap();
    let operator_assignment = owner
        .store
        .call(
            local_operator.clone(),
            "review.assign".into(),
            json!({
                "client_request_id":"assign-operator-sponsored-reviewer",
                "attempt_id":operator_sponsored_subject.attempt_id.clone(),
                "expected_revision":1,
                "submission_ref":operator_sponsored_subject.submission_ref.clone(),
                "candidate_ref":operator_sponsored_subject.candidate_ref.clone(),
                "reviewer_client_id":"operator-sponsored-reviewer",
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        operator_assignment["sponsor_client_id"].as_str(),
        Some(local_operator.client_id.as_str())
    );
    let operator_assignment_id = operator_assignment["review_assignment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let operator_reviewer = owner
        .store
        .authenticate(Credential {
            client_id: "operator-sponsored-reviewer".into(),
            token: "operator-sponsored-reviewer-token".into(),
        })
        .await
        .unwrap();

    owner
        .store
        .call(
            owner_reviewer,
            "review.submit".into(),
            passing_review_result_request(&owner_subject, &owner_assignment_id, "cross-owner-pass"),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            gm_subject_reviewer,
            "review.submit".into(),
            passing_review_result_request(&gm_owned_subject, &gm_assignment_id, "gm-owned-pass"),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            operator_reviewer,
            "review.submit".into(),
            passing_review_result_request(
                &operator_sponsored_subject,
                &operator_assignment_id,
                "operator-sponsored-pass",
            ),
        )
        .await
        .unwrap();

    let config = Config::default();
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let projection = review_disposition::reconcile(&tx, &config, 16, 64, model::now_ms()?)?;
            tx.commit()?;
            Ok(projection)
        })
        .await
        .unwrap();

    let owner_task_id = owner_subject.task_id.clone();
    let gm_task_id = gm_owned_subject.task_id.clone();
    let operator_task_id = operator_sponsored_subject.task_id.clone();
    let (owner_acceptance, gm_acceptance_count, operator_acceptance_count): (
        Vec<(String, String)>,
        i64,
        i64,
    ) = owner
        .store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT operation_id,effective_request_json FROM operations \
                 WHERE method='task.accept' AND task_id=?1 ORDER BY operation_id",
            )?;
            let rows = statement
                .query_map([owner_task_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let rejected_count: i64 = db.query_row(
                "SELECT count(*) FROM operations WHERE method='task.accept' AND task_id=?1",
                [gm_task_id],
                |row| row.get(0),
            )?;
            let operator_rejected_count: i64 = db.query_row(
                "SELECT count(*) FROM operations WHERE method='task.accept' AND task_id=?1",
                [operator_task_id],
                |row| row.get(0),
            )?;
            Ok((rows, rejected_count, operator_rejected_count))
        })
        .await
        .unwrap();
    assert_eq!(owner_acceptance.len(), 1);
    assert_eq!(gm_acceptance_count, 0);
    assert_eq!(operator_acceptance_count, 0);
    let operation_id = owner_acceptance[0].0.clone();
    let effective: Value = serde_json::from_str(&owner_acceptance[0].1).unwrap();
    assert_eq!(
        effective["automation_on_behalf"]["effective_manager_id"],
        "review-gm"
    );
    assert_eq!(
        effective["automation_on_behalf"]["cause"]["review_assignment_sponsor_id"],
        "review-owner-v2"
    );
    let visible_acceptance = owner
        .store
        .call(
            gm.clone(),
            "operation.get".into(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(visible_acceptance["method"], "task.accept");
    let history_operation_id = operation_id.clone();
    let history_link = owner
        .store
        .run(move |db| {
            crate::automation::authorization::operation_link(db, &history_operation_id)?
                .ok_or_else(|| Error::new("NOT_FOUND", "retained acceptance history link"))
        })
        .await
        .unwrap();
    assert_eq!(history_link.action, "task.accept");
    assert_eq!(history_link.effective_manager_id, "review-gm");
    assert_eq!(
        history_link.cause["review_assignment_sponsor_id"],
        "review-owner-v2"
    );
    let context = owner
        .store
        .run(move |db| {
            crate::automation::acceptance::AcceptanceContext::from_committed_operation(
                db,
                &operation_id,
            )
        })
        .await
        .unwrap();
    assert_eq!(context.effective_manager_id(), "review-gm");
    assert_eq!(context.review_assignment_sponsor_id(), "review-owner-v2");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

fn fixture_git_executable() -> PathBuf {
    let name = if cfg!(windows) { "git.exe" } else { "git" };
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH is set"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| candidate.canonicalize().ok())
        .expect("Git executable is available for the configured Forge mapping")
}

#[tokio::test]
async fn automatic_publication_retains_exact_cause_and_reuses_slot_after_gm_handover() {
    let repository_path =
        std::env::temp_dir().join(format!("swarm-publication-repository-{}", model::new_id()));
    std::fs::create_dir_all(&repository_path).unwrap();
    let mut config = Config::default();
    config.forge.enabled = true;
    config.forge.git_executable = fixture_git_executable();
    config.forge.projects.insert(
        "fixture".into(),
        crate::forge::ForgeProject {
            canonical_repository: "github.com/owner/publication-fixture".into(),
            repository_path: repository_path.clone(),
            remote_name: "origin".into(),
            policy_revision: crate::policy::OWNER_POLICY_V2_ID.into(),
            target_refs: vec!["refs/heads/main".into()],
        },
    );
    let (owner, directory, bootstrap_credential) =
        start_store_with_config("automatic-publication", config).await;
    let local_operator = owner
        .store
        .authenticate(bootstrap_credential)
        .await
        .unwrap();
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        "automatic-publication",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    configure_acceptance_fixture(&owner.store, "review-gm", std::slice::from_ref(&subject)).await;

    let candidate_ref = subject.candidate_ref.clone();
    let submission_ref = subject.submission_ref.clone();
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    let (candidate, submission, submission_bytes) = owner
        .store
        .run(move |db| {
            let mut metadata = results::get(db, &candidate_ref)?.metadata;
            metadata["task_id"] = json!(task_id);
            metadata["attempt_id"] = json!(attempt_id);
            metadata["task_revision"] = json!(1);
            metadata["coverage"] = json!("complete");
            metadata["commit"] = json!("1".repeat(40));
            metadata["tree"] = json!("2".repeat(40));
            db.execute(
                "UPDATE artifacts SET metadata_json=?2 WHERE artifact_id=?1 AND kind='source_snapshot'",
                params![candidate_ref, model::canonical(&metadata)?],
            )?;
            let candidate = results::get(db, &candidate_ref)?;
            let submission = results::get(db, &submission_ref)?;
            let document = submissions::document(db, &submission_ref)?;
            Ok((
                candidate,
                submission,
                model::canonical(&document)?.into_bytes(),
            ))
        })
        .await
        .unwrap();
    let candidate_bytes = b"explicit source snapshot fixture".to_vec();
    owner
        .store
        .file_io(move |files| {
            files.publish(&candidate, &candidate_bytes)?;
            files.publish(&submission, &submission_bytes)?;
            Ok(())
        })
        .await
        .unwrap();

    let gm = principal("review-gm", Role::Manager);
    let acceptance_receipt = owner
        .store
        .call(
            gm.clone(),
            "task.accept".into(),
            json!({
                "client_request_id":"accept-automatic-publication-fixture",
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "expected_feedback_observation_id":0,
                "reason":"Independent review accepted this exact retained candidate.",
                "reviews":[{
                    "requirement_id":"R1",
                    "rationale":"The candidate satisfies the frozen fixture requirement.",
                    "evidence":[subject.candidate_ref]
                }],
                "check_ids":[]
            }),
        )
        .await
        .unwrap();
    let acceptance_operation_id = acceptance_receipt["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let acceptance = owner
        .store
        .call(
            gm.clone(),
            "operation.get".into(),
            json!({"operation_id":acceptance_operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(acceptance["method"], "task.accept");
    assert_eq!(acceptance["state"], "settled");
    assert_eq!(acceptance["result"]["outcome"], "applied");

    let configured = owner
        .store
        .call(
            gm.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"enable-automatic-publication-fixture",
                "project_id":"fixture",
                "changes":[{
                    "automation_id":"accepted-candidate-publication",
                    "expected_revision":0,
                    "include_existing":true,
                    "patch":{
                        "enabled":true,
                        "scope":{"work_pool_id":null},
                        "steps":["publication"],
                        "publication":{
                            "target_ref":"refs/heads/main",
                            "expected_old_ref":null,
                            "expected_create":true
                        }
                    }
                }]
            }),
        )
        .await
        .unwrap();
    assert_eq!(configured["applied"], true);

    let first_pass = owner.store.reconcile_automations_once().await.unwrap();
    assert_eq!(first_pass["publication"]["processed"], 1);
    let (automatic_operation_id, automatic_count): (Option<String>, i64) = owner
        .store
        .run(|db| {
            let count = db.query_row(
                "SELECT count(*) FROM operations WHERE method='forge.publish_ref' AND caller_id=?1",
                [crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
                |row| row.get(0),
            )?;
            let id = if count == 0 {
                None
            } else {
                Some(db.query_row(
                    "SELECT operation_id FROM operations WHERE method='forge.publish_ref' AND caller_id=?1 ORDER BY created_at_ms,operation_id LIMIT 1",
                    [crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
                    |row| row.get(0),
                )?)
            };
            Ok((id, count))
        })
        .await
        .unwrap();
    assert_eq!(
        automatic_count, 1,
        "expected exactly one automatic Forge operation; publication consumer projection: {}",
        first_pass["publication"]
    );
    let automatic_operation_id = automatic_operation_id
        .expect("the counted automatic Forge operation has a retained operation ID");

    let automatic = owner
        .store
        .call(
            gm.clone(),
            "operation.get".into(),
            json!({"operation_id":automatic_operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(automatic["method"], "forge.publish_ref");
    assert_eq!(automatic["state"], "queued");
    assert_eq!(
        automatic["caller_id"],
        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    );

    let explanation = owner
        .store
        .call(
            gm.clone(),
            "automation.config.explain".into(),
            json!({
                "project_id":"fixture",
                "automation_id":"accepted-candidate-publication"
            }),
        )
        .await
        .unwrap();
    assert_eq!(explanation["publication"]["status"], "ready");
    let publication_history = explanation["linked_operation_history"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["operation_id"] == automatic_operation_id)
        .expect("automatic Forge Operation is in its owner's scoped history");
    assert_eq!(publication_history["action"], "forge.publish_ref");
    assert_eq!(explanation["entry"]["owner_manager_id"], "review-gm");
    assert_eq!(publication_history["cause"]["kind"], "task.acceptance");
    assert_eq!(
        publication_history["cause"]["operation_id"],
        acceptance_operation_id
    );
    assert_eq!(publication_history["cause"]["gm_epoch"], 1);
    assert_eq!(
        publication_history["cause"]["candidate_ref"],
        subject.candidate_ref
    );

    let repeated_pass = owner.store.reconcile_automations_once().await.unwrap();
    assert_eq!(repeated_pass["publication"]["processed"], 0);
    let manual_receipt = owner
        .store
        .call(
            gm.clone(),
            "forge.publish_ref".into(),
            json!({
                "client_request_id":"manual-same-publication-fixture",
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "accepted_operation_id":acceptance_operation_id,
                "candidate_ref":subject.candidate_ref,
                "expected_policy_revision":crate::policy::OWNER_POLICY_V2_ID,
                "target_ref":"refs/heads/main",
                "expected_old_ref":null,
                "expected_create":true
            }),
        )
        .await
        .unwrap();
    assert_eq!(manual_receipt["coalesced"], true);
    assert_eq!(manual_receipt["coalesced_to"], automatic_operation_id);
    assert_eq!(manual_receipt["publication_may_have_started"], false);
    let (forge_operation_count, effect_bearing_count): (i64, i64) = owner
        .store
        .run(|db| {
            let total = db.query_row(
                "SELECT count(*) FROM operations WHERE method='forge.publish_ref'",
                [],
                |row| row.get(0),
            )?;
            let active = db.query_row(
                "SELECT count(*) FROM operations WHERE method='forge.publish_ref' AND state IN ('queued','sending','outcome_unknown')",
                [],
                |row| row.get(0),
            )?;
            Ok((total, active))
        })
        .await
        .unwrap();
    assert_eq!(forge_operation_count, 2);
    assert_eq!(effect_bearing_count, 1);

    owner
        .store
        .call(
            gm,
            "gm.handover".into(),
            json!({
                "client_request_id":"handover-before-publication-effect",
                "client_id":"review-owner-v2"
            }),
        )
        .await
        .unwrap();
    owner.store.supervise_forge_once().await.unwrap();
    let new_manager_error = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "operation.get".into(),
            json!({"operation_id":automatic_operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(new_manager_error.code, "NOT_FOUND");
    let retained_operation = owner
        .store
        .call(
            local_operator.clone(),
            "operation.get".into(),
            json!({"operation_id":automatic_operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(retained_operation["state"], "settled");
    assert_eq!(retained_operation["result"]["outcome"], "stale_gm_epoch");
    assert_eq!(retained_operation["result"]["publication"], "not_started");
    let active_effects: i64 = owner
        .store
        .run(|db| {
            Ok(db.query_row(
                "SELECT count(*) FROM operations WHERE method='forge.publish_ref' AND state IN ('queued','sending','outcome_unknown')",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(active_effects, 0);

    let fresh_publication = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "forge.publish_ref".into(),
            json!({
                "client_request_id":"fresh-publication-after-gm-handover",
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "accepted_operation_id":acceptance_operation_id,
                "candidate_ref":subject.candidate_ref,
                "expected_policy_revision":crate::policy::OWNER_POLICY_V2_ID,
                "target_ref":"refs/heads/main",
                "expected_old_ref":null,
                "expected_create":true
            }),
        )
        .await
        .unwrap();
    assert_ne!(fresh_publication["coalesced"], true);
    assert_eq!(fresh_publication["state"], "queued");
    assert_ne!(fresh_publication["operation_id"], automatic_operation_id);
    let automatic_count: i64 = owner
        .store
        .run(|db| {
            Ok(db.query_row(
                "SELECT count(*) FROM operations WHERE method='forge.publish_ref' AND caller_id=?1",
                [crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(automatic_count, 1);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    std::fs::remove_dir_all(repository_path).unwrap();
}

#[tokio::test]
async fn sponsored_review_result_drives_only_exact_v2_owner_disposition_and_private_reads() {
    let (owner, directory, _) = start_store("disposition").await;
    seed_clients(&owner.store).await;
    let v2 = seed_subject(
        &owner.store,
        "v2",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        true,
    )
    .await;
    let v1 = seed_subject(
        &owner.store,
        "v1",
        "review-owner-v1",
        crate::policy::OWNER_POLICY_V1_ID,
        false,
    )
    .await;
    let manager_v2 = principal("review-owner-v2", Role::Manager);
    let manager_v1 = principal("review-owner-v1", Role::Manager);
    let (reviewer, assignment_id) = assign_sponsored_reviewer(
        &owner.store,
        manager_v2.clone(),
        &v2,
        "assigned-reviewer",
        "reviewer-test-token",
    )
    .await;

    let bound_scope = owner
        .store
        .run(|db| {
            let registration = meta(db, "client:assigned-reviewer")?
                .ok_or_else(|| Error::new("NOT_FOUND", "reviewer registration"))?;
            Ok(registration["participation_basis"]["review_scope"].clone())
        })
        .await
        .unwrap();
    assert_eq!(bound_scope["review_assignment_id"], assignment_id);
    assert_eq!(bound_scope["task_id"], v2.task_id);
    assert_eq!(bound_scope["submission_ref"], v2.submission_ref);
    assert_eq!(bound_scope["candidate_ref"], v2.candidate_ref);

    let result = owner
        .store
        .call(
            reviewer,
            "review.submit".into(),
            review_result_request(&v2, &assignment_id, "review-result-v2"),
        )
        .await
        .unwrap();
    assert_eq!(result["verdict"], "changes_requested");
    assert_eq!(result["task_transition"], "none");
    assert_eq!(result["acceptance_changed"], false);
    assert_eq!(result["publication_started"], false);

    let feedback = owner
        .store
        .call(
            manager_v2.clone(),
            "task.request_changes".into(),
            json!({
                "client_request_id":"owner-v2-disposition",
                "attempt_id":v2.attempt_id,
                "expected_revision":1,
                "submission_ref":v2.submission_ref,
                "candidate_ref":v2.candidate_ref,
                "finding_id":"missing-r1-evidence",
                "reason":"Add the retained evidence for R1.",
                "requirement_ids":["R1"],
                "evidence":["evidence://review/r1"],
            }),
        )
        .await
        .unwrap();
    assert_eq!(feedback["applied"], true);
    assert_eq!(feedback["status"], "needs_correction");
    assert_eq!(
        feedback["review_provenance"]["review_assignment_id"],
        assignment_id
    );
    let review = owner
        .store
        .call(
            manager_v2.clone(),
            "review.get".into(),
            json!({"review_assignment_id":assignment_id}),
        )
        .await
        .unwrap();
    assert_eq!(review["assignment"]["identity"]["task_id"], v2.task_id);
    assert_eq!(
        review["assignment"]["identity"]["attempt_id"],
        v2.attempt_id
    );
    assert_eq!(review["result"]["review_assignment_id"], assignment_id);
    assert_eq!(
        review["latest_disposition"]["disposition"],
        "return_for_correction"
    );
    let after_feedback = owner
        .store
        .run(move |db| {
            Ok((
                tasks::get_task(db, &v2.task_id)?,
                tasks::get_attempt(db, &v2.attempt_id)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(after_feedback.0["state"], "open");
    assert!(after_feedback.0["accepted_attempt_id"].is_null());
    assert_eq!(after_feedback.1["state"], "needs_correction");

    let rejected_v1 = owner
        .store
        .call(
            manager_v1,
            "task.request_changes".into(),
            json!({
                "client_request_id":"non-gm-v1-disposition",
                "attempt_id":v1.attempt_id,
                "expected_revision":1,
                "submission_ref":v1.submission_ref,
                "candidate_ref":v1.candidate_ref,
                "finding_id":"missing-r1-evidence",
                "reason":"Add the retained evidence for R1.",
                "requirement_ids":["R1"],
                "evidence":["evidence://review/r1"],
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(rejected_v1.code, "FORBIDDEN");

    let feedback_id = feedback["operation_id"].as_str().unwrap().to_owned();
    let private_operation_ids = [
        feedback_id.clone(),
        "check-run-v2".to_owned(),
        "check-cancel-v2".to_owned(),
    ];
    let outsiders = [
        principal("unrelated-manager", Role::Manager),
        principal("unrelated-observer", Role::Observer),
    ];
    for outsider in outsiders {
        for operation_id in &private_operation_ids {
            let error = owner
                .store
                .call(
                    outsider.clone(),
                    "operation.get".into(),
                    json!({"operation_id":operation_id}),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "NOT_FOUND");
        }
        let listed = owner
            .store
            .call(
                outsider.clone(),
                "operation.list".into(),
                json!({"state":"settled","limit":200,"after":0}),
            )
            .await
            .unwrap();
        assert_eq!(
            listed["next_after"].as_i64(),
            Some(listed["items"].as_array().unwrap().len() as i64)
        );
        assert!(listed["items"].as_array().unwrap().iter().all(|item| {
            !private_operation_ids
                .iter()
                .any(|operation_id| item["operation_id"] == operation_id.as_str())
        }));
        let delta = owner
            .store
            .call(
                outsider,
                "report.delta".into(),
                json!({"after":0,"limit":1}),
            )
            .await
            .unwrap();
        assert!(delta["items"].as_array().unwrap().is_empty());
        assert_eq!(delta["next_cursor"], 0);
        assert_eq!(delta["projection"]["has_newer"], false);
    }
    let visible_private_operations = owner
        .store
        .call(
            manager_v2.clone(),
            "operation.list".into(),
            json!({"state":"settled","limit":200,"after":0}),
        )
        .await
        .unwrap();
    for operation_id in &private_operation_ids {
        assert!(
            visible_private_operations["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["operation_id"] == operation_id.as_str())
        );
    }
    let visible_delta = owner
        .store
        .call(
            manager_v2,
            "report.delta".into(),
            json!({"after":0,"limit":100}),
        )
        .await
        .unwrap();
    let visible_kinds = visible_delta["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item["kind"].as_str())
        .collect::<Vec<_>>();
    assert!(visible_kinds.contains(&"task.feedback"));
    assert!(visible_kinds.contains(&"check.completed"));
    assert!(visible_kinds.contains(&"check.cancel"));

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn assigned_reviewer_can_record_and_read_only_the_historical_result_after_release() {
    let (owner, directory, _) = start_store("historical").await;
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        "late",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let manager = principal("review-owner-v2", Role::Manager);
    let (reviewer, assignment_id) = assign_sponsored_reviewer(
        &owner.store,
        manager.clone(),
        &subject,
        "late-reviewer",
        "late-reviewer-token",
    )
    .await;

    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            let revision_two = json!({
                "objective":"Revision two supersedes the reviewed candidate",
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"A later revision has a new candidate"}],
                "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
            });
            db.execute(
                "UPDATE tasks SET revision=2,spec_json=?2,updated_at_ms=2 WHERE task_id=?1",
                params![task_id, model::canonical(&revision_two)?],
            )?;
            db.execute(
                "UPDATE attempts SET state='superseded',released_at_ms=2,updated_at_ms=2 WHERE attempt_id=?1",
                [&attempt_id],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let result = owner
        .store
        .call(
            reviewer.clone(),
            "review.submit".into(),
            review_result_request(&subject, &assignment_id, "late-review-result"),
        )
        .await
        .unwrap();
    assert_eq!(result["applicability"], "historical_candidate");
    assert_eq!(result["task_transition"], "none");
    assert_eq!(result["acceptance_changed"], false);
    assert_eq!(result["publication_started"], false);
    let review = owner
        .store
        .call(
            reviewer.clone(),
            "review.get".into(),
            json!({"review_assignment_id":assignment_id}),
        )
        .await
        .unwrap();
    assert_eq!(review["result"]["operation_id"], result["operation_id"]);
    assert_eq!(review["result"]["applicability"], "historical_candidate");
    assert_eq!(review["current_candidate"], false);
    let operation = owner
        .store
        .call(
            reviewer.clone(),
            "operation.get".into(),
            json!({"operation_id":result["operation_id"]}),
        )
        .await
        .unwrap();
    assert_eq!(operation["method"], "review.submit");
    assert_eq!(operation["result"]["review_assignment_id"], assignment_id);

    let context_error = owner
        .store
        .call(
            reviewer.clone(),
            "swarm.review.context".into(),
            json!({"review_assignment_id":assignment_id}),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        context_error.code.as_str(),
        "STALE_REVISION" | "STALE_PARTICIPANT"
    ));
    assert!(
        owner
            .store
            .call(
                reviewer.clone(),
                "artifact.read".into(),
                json!({"artifact_id":subject.candidate_ref}),
            )
            .await
            .is_err()
    );
    assert!(
        owner
            .store
            .call(
                reviewer.clone(),
                "task.submission".into(),
                json!({"submission_ref":subject.submission_ref,"after":0,"limit":50}),
            )
            .await
            .is_err()
    );
    let stale_write = owner
        .store
        .call(
            reviewer.clone(),
            "coordination.send".into(),
            json!({"client_request_id":"late-stale-send","recipient":"unregistered-peer","body":{"attempt":"write after release"}}),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        stale_write.code.as_str(),
        "STALE_REVISION" | "STALE_PARTICIPANT"
    ));
    assert_eq!(
        owner
            .store
            .call(
                reviewer.clone(),
                "task.get".into(),
                json!({"task_id":subject.task_id})
            )
            .await
            .unwrap_err()
            .code,
        "FORBIDDEN"
    );
    assert_eq!(
        owner
            .store
            .call(reviewer.clone(), "forge.publish_ref".into(), json!({}))
            .await
            .unwrap_err()
            .code,
        "FORBIDDEN"
    );

    let persisted = owner
        .store
        .run(move |db| {
            Ok((
                tasks::get_task(db, &subject.task_id)?,
                tasks::get_attempt(db, &subject.attempt_id)?,
                db.query_row(
                    "SELECT count(*) FROM operations WHERE method='forge.publish_ref'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(persisted.0["revision"], 2);
    assert_eq!(persisted.0["state"], "open");
    assert!(persisted.0["accepted_attempt_id"].is_null());
    assert_eq!(persisted.1["state"], "superseded");
    assert!(!persisted.1["released_at_ms"].is_null());
    assert_eq!(persisted.2, 0);

    owner
        .store
        .call(
            manager,
            "coordination.participant.disable".into(),
            json!({
                "client_request_id":"revoke-late-reviewer",
                "client_id":"late-reviewer",
                "expected_grant_revision":1,
            }),
        )
        .await
        .unwrap();
    let revoked = owner
        .store
        .authenticate(Credential {
            client_id: "late-reviewer".into(),
            token: "late-reviewer-token".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(revoked.code, "UNAUTHORIZED");
    let after_revoke = owner
        .store
        .call(
            reviewer,
            "review.get".into(),
            json!({"review_assignment_id":assignment_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(after_revoke.code, "UNAUTHORIZED");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
