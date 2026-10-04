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
    let successor_operation = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "operation.get".into(),
            json!({"operation_id":automatic_operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(successor_operation["state"], "settled");
    assert_eq!(successor_operation["result"]["outcome"], "stale_gm_epoch");
    assert_eq!(successor_operation["result"]["publication"], "not_started");
    assert_eq!(
        successor_operation["caller_id"],
        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    let retained_link_id = automatic_operation_id.clone();
    let retained_link = owner
        .store
        .run(move |db| {
            crate::automation::authorization::operation_link(db, &retained_link_id)?
                .ok_or_else(|| Error::new("NOT_FOUND", "retained publication attribution"))
        })
        .await
        .unwrap();
    assert_eq!(retained_link.effective_manager_id, "review-gm");
    let successor_history = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "operation.list".into(),
            json!({"state":"settled","limit":200,"after":0}),
        )
        .await
        .unwrap();
    assert!(
        successor_history["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["operation_id"] == automatic_operation_id)
    );
    let unrelated = principal("unrelated-manager", Role::Manager);
    let unrelated_error = owner
        .store
        .call(
            unrelated.clone(),
            "operation.get".into(),
            json!({"operation_id":automatic_operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(unrelated_error.code, "NOT_FOUND");
    let unrelated_history = owner
        .store
        .call(
            unrelated,
            "operation.list".into(),
            json!({"state":"settled","limit":200,"after":0}),
        )
        .await
        .unwrap();
    assert!(
        unrelated_history["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["operation_id"] != automatic_operation_id)
    );
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

async fn register_profile_reviewer(
    store: &Store,
    sponsor: &Principal,
    subject: &ReviewSubject,
    reviewer_id: &str,
    token: &str,
    profile: &str,
) -> Principal {
    let review_scope = json!({
        "review_assignment_id":null,
        "task_id":subject.task_id,
        "attempt_id":subject.attempt_id,
        "task_revision":1,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
    });
    store
        .call(
            sponsor.clone(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":format!("register-{reviewer_id}"),
                "client_id":reviewer_id,
                "token_hash":model::digest(token.as_bytes()),
                "task_id":subject.task_id,
                "task_revision":1,
                "attempt_id":subject.attempt_id,
                "review_profile":profile,
                "participation_basis":{"kind":"sponsored_reviewer","review_scope":review_scope},
            }),
        )
        .await
        .unwrap();
    store
        .authenticate(Credential {
            client_id: reviewer_id.to_owned(),
            token: token.to_owned(),
        })
        .await
        .unwrap()
}

async fn add_submission_source_fact(store: &Store, suffix: &str, subject: &ReviewSubject) {
    let operation_id = format!("submit-op-{suffix}");
    let event_key = format!("submission:{operation_id}");
    let payload = json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "attempt_id":subject.attempt_id,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
        "task_accepted":false,
    });
    store
        .run(move |db| {
            db.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
                 VALUES('controller',?1,?2,'task.submission',?3,3)",
                params![event_key, operation_id, model::canonical(&payload)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn add_submission_source_fact_with_key(
    store: &Store,
    suffix: &str,
    event_key: &str,
    subject: &ReviewSubject,
) {
    let operation_id = format!("submit-op-{suffix}");
    let event_key = event_key.to_owned();
    let attempt_id = subject.attempt_id.clone();
    let submission_ref = subject.submission_ref.clone();
    let candidate_ref = subject.candidate_ref.clone();
    let payload = json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "attempt_id":attempt_id,
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref,
        "task_accepted":false,
    });
    store
        .run(move |db| {
            db.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
                 VALUES('controller',?1,?2,'task.submission',?3,3)",
                params![event_key, operation_id, model::canonical(&payload)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn attach_ready_repair_binding(store: &Store, subject: &ReviewSubject) -> String {
    let binding_id = format!("repair-binding-{}", subject.attempt_id);
    let module_client_id = format!("repair-module-{}", subject.attempt_id);
    let module_link_id = format!("repair-module-link-{}", subject.attempt_id);
    let attempt_id = subject.attempt_id.clone();
    let returned_binding_id = binding_id.clone();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            set_meta(
                &tx,
                &format!("client:{module_client_id}"),
                &json!({
                    "role":"module",
                    "disabled":false,
                    "binding_id":binding_id,
                    "binding_generation":1
                }),
            )?;
            let route = json!({
                "alias":"repair-continuation-fixture",
                "runtime":"muse",
                "module_artifact_id":"muse-sdk-1.3.0-bridge.5",
                "enabled":true,
                "native_options":{"workspaceRoot":"C:\\fixture","modelId":"fixture-model"}
            });
            let state = json!({
                "connection":"connected",
                "module_client_id":module_client_id,
                "module_link_id":module_link_id,
                "bridge_boot_id":"repair-fixture-boot"
            });
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,'repair-fixture-lane',?2,'muse-sdk-1.3.0-bridge.5','ready',?3,?4,1)",
                params![
                    binding_id,
                    module_client_id,
                    model::canonical(&route)?,
                    model::canonical(&state)?,
                ],
            )?;
            tx.execute(
                "UPDATE attempts SET binding_id=?2,binding_generation=1 WHERE attempt_id=?1",
                params![attempt_id, binding_id],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    returned_binding_id
}

async fn consume_repair_result_for_entry(
    store: &Store,
    manager_id: &str,
    project_id: &str,
    automation_id: &str,
    assignment_id: &str,
    result_operation_id: &str,
) -> Result<Value> {
    let manager_id = manager_id.to_owned();
    let project_id = project_id.to_owned();
    let automation_id = automation_id.to_owned();
    let assignment_id = assignment_id.to_owned();
    let result_operation_id = result_operation_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let entry = crate::automation::config::load_entry(
                &tx,
                &manager_id,
                &project_id,
                &automation_id,
            )?
            .ok_or_else(|| Error::new("NOT_FOUND", "transferred RepairDispatch entry"))?;
            let value = super::automation_repair::consume_review_result_for_entry(
                &tx,
                &Config::default(),
                &entry,
                &assignment_id,
                &result_operation_id,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(value)
        })
        .await
}

async fn configure_subject_acceptance_policy(store: &Store, subject: &ReviewSubject) {
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let task_spec_raw: String = tx.query_row(
                "SELECT spec_json FROM tasks WHERE task_id=?1",
                [&task_id],
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
                [&attempt_id],
                |row| row.get(0),
            )?;
            let mut snapshot: Value = serde_json::from_str(&snapshot_raw)?;
            snapshot["spec"] = spec;
            tx.execute(
                "UPDATE attempts SET task_snapshot_json=?2 WHERE attempt_id=?1",
                params![attempt_id, model::canonical(&snapshot)?],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn transferred_gm_dispatches_exact_reviews_and_keeps_attempt_owner_provenance() {
    let (owner, directory, bootstrap) = start_store("gm-successor-review").await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    seed_clients(&owner.store).await;
    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":"designate-former-review-gm","client_id":"review-owner-v2"}),
        )
        .await
        .unwrap();
    let former = principal("review-owner-v2", Role::Manager);
    let successor = principal("review-gm", Role::Manager);
    let final_manager = principal("review-owner-v1", Role::Manager);
    let unrelated = principal("unrelated-manager", Role::Manager);
    let project_id = "fixture";
    let automation_id = "successor-review-continuation";
    let changes = json!([{
        "automation_id":automation_id,
        "expected_revision":0,
        "include_existing":false,
        "patch":{
            "enabled":true,
            "scope":{"work_pool_id":null},
            "steps":["review_dispatch","review_disposition","acceptance"],
            "review":{"profile":"successor-auditor","required_reviewers":1},
        }
    }]);
    let preview = owner
        .store
        .call(
            former.clone(),
            "automation.config.preview".into(),
            json!({"project_id":project_id,"changes":changes.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true);
    owner
        .store
        .call(
            former,
            "automation.config.apply".into(),
            json!({
                "client_request_id":"enable-successor-review-continuation",
                "project_id":project_id,
                "changes":changes,
                "preview_digest":preview["plan_sha256"]
            }),
        )
        .await
        .unwrap();

    let correction = seed_subject(
        &owner.store,
        "successor-correction",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let accepted = seed_subject(
        &owner.store,
        "successor-acceptance",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    configure_subject_acceptance_policy(&owner.store, &accepted).await;
    add_submission_source_fact(&owner.store, "successor-correction", &correction).await;
    add_submission_source_fact(&owner.store, "successor-acceptance", &accepted).await;

    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":"designate-successor-review-gm","client_id":"review-gm"}),
        )
        .await
        .unwrap();
    let transfer = owner
        .store
        .call(
            successor.clone(),
            "automation.config.transfer".into(),
            json!({
                "client_request_id":"transfer-successor-review-continuation",
                "project_id":project_id,
                "former_owner_manager_id":"review-owner-v2",
                "automation_id":automation_id,
                "expected_revision":1
            }),
        )
        .await
        .unwrap();
    assert_eq!(transfer["status"], "transferred");
    assert_eq!(transfer["new_owner_manager_id"], successor.client_id);

    let correction_reviewer = register_profile_reviewer(
        &owner.store,
        &successor,
        &correction,
        "successor-correction-reviewer",
        "successor-correction-reviewer-token",
        "successor-auditor",
    )
    .await;
    let acceptance_reviewer = register_profile_reviewer(
        &owner.store,
        &successor,
        &accepted,
        "successor-acceptance-reviewer",
        "successor-acceptance-reviewer-token",
        "successor-auditor",
    )
    .await;

    let dispatch = owner.store.reconcile_automations_once().await.unwrap();
    assert!(
        dispatch["review_dispatch"]["entries"]
            .as_array()
            .is_some_and(|entries| !entries.is_empty())
    );

    let assignments = owner
        .store
        .run({
            let correction_task = correction.task_id.clone();
            let accepted_task = accepted.task_id.clone();
            move |db| {
                let read_assignment = |task_id: &str| -> Result<(String, Value)> {
                    let (operation_id, result_raw): (String, String) = db.query_row(
                        "SELECT operation_id,result_json FROM operations \
                         WHERE method='review.assign' AND task_id=?1 AND state='settled'",
                        [task_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )?;
                    Ok((operation_id, serde_json::from_str(&result_raw)?))
                };
                Ok((
                    read_assignment(&correction_task)?,
                    read_assignment(&accepted_task)?,
                ))
            }
        })
        .await
        .unwrap();
    assert_eq!(assignments.0.1["sponsor_client_id"], successor.client_id);
    assert_eq!(assignments.1.1["sponsor_client_id"], successor.client_id);
    assert_eq!(
        assignments.0.1["reviewer_client_id"],
        correction_reviewer.client_id
    );
    assert_eq!(
        assignments.1.1["reviewer_client_id"],
        acceptance_reviewer.client_id
    );
    for assignment in [&assignments.0.1, &assignments.1.1] {
        assert_ne!(assignment["reviewer_client_id"], "review-owner-v2");
        assert_ne!(assignment["reviewer_client_id"], successor.client_id);
        assert_ne!(assignment["reviewer_client_id"], "review-owner-v1");
    }
    let correction_assignment_id = assignments.0.1["review_assignment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let acceptance_assignment_id = assignments.1.1["review_assignment_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let denied = owner
        .store
        .call(
            unrelated,
            "task.request_changes".into(),
            json!({
                "client_request_id":"unrelated-manager-successor-feedback",
                "attempt_id":correction.attempt_id,
                "expected_revision":1,
                "submission_ref":correction.submission_ref,
                "candidate_ref":correction.candidate_ref,
                "finding_id":"missing-r1-evidence",
                "reason":"Add the retained evidence for R1.",
                "requirement_ids":["R1"],
                "evidence":["evidence://review/r1"],
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(denied.code, "FORBIDDEN");

    owner
        .store
        .call(
            correction_reviewer,
            "review.submit".into(),
            review_result_request(
                &correction,
                &correction_assignment_id,
                "successor-correction-result",
            ),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            acceptance_reviewer,
            "review.submit".into(),
            passing_review_result_request(
                &accepted,
                &acceptance_assignment_id,
                "successor-acceptance-result",
            ),
        )
        .await
        .unwrap();

    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":"designate-final-review-gm","client_id":final_manager.client_id}),
        )
        .await
        .unwrap();
    let continued_transfer = owner
        .store
        .call(
            final_manager.clone(),
            "automation.config.transfer".into(),
            json!({
                "client_request_id":"transfer-successor-review-continuation-again",
                "project_id":project_id,
                "former_owner_manager_id":successor.client_id,
                "automation_id":automation_id,
                "expected_revision":transfer["new_owner_revision"]
            }),
        )
        .await
        .unwrap();
    assert_eq!(continued_transfer["status"], "transferred");
    assert_eq!(
        continued_transfer["new_owner_manager_id"],
        final_manager.client_id
    );
    let retired_manager_denied = owner
        .store
        .call(
            successor.clone(),
            "task.request_changes".into(),
            json!({
                "client_request_id":"retired-review-gm-successor-feedback",
                "attempt_id":correction.attempt_id,
                "expected_revision":1,
                "submission_ref":correction.submission_ref,
                "candidate_ref":correction.candidate_ref,
                "finding_id":"missing-r1-evidence",
                "reason":"Add the retained evidence for R1.",
                "requirement_ids":["R1"],
                "evidence":["evidence://review/r1"]
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(retired_manager_denied.code, "FORBIDDEN");

    let config = Config::default();
    let dispositions = owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let result = review_disposition::reconcile(&tx, &config, 16, 64, model::now_ms()?)?;
            tx.commit()?;
            Ok(result)
        })
        .await
        .unwrap();
    let operations = owner
        .store
        .run({
            let correction_task = correction.task_id.clone();
            let accepted_task = accepted.task_id.clone();
            move |db| {
                let read_action = |task_id: &str, method: &str| -> Result<(String, String, String, String)> {
                    db.query_row(
                        "SELECT caller_id,effective_request_json,result_json,state FROM operations \
                         WHERE method=?1 AND task_id=?2 ORDER BY created_at_ms,operation_id LIMIT 1",
                        params![method, task_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .map_err(Into::into)
                };
                Ok((
                    read_action(&correction_task, "task.request_changes")?,
                    read_action(&accepted_task, "task.accept")?,
                    tasks::get_attempt(db, &correction.attempt_id)?,
                    submissions::document(db, &correction.submission_ref)?,
                    tasks::get_attempt(db, &accepted.attempt_id)?,
                    submissions::document(db, &accepted.submission_ref)?,
                    db.query_row(
                        "SELECT COUNT(*) FROM operations WHERE method='review.assign'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?,
                ))
            }
        })
        .await
        .unwrap();

    let correction_effective: Value = serde_json::from_str(&operations.0.1).unwrap();
    let correction_result: Value = serde_json::from_str(&operations.0.2).unwrap();
    assert_eq!(
        operations.0.0,
        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(
        correction_effective["automation_on_behalf"]["effective_manager_id"],
        final_manager.client_id
    );
    assert_eq!(
        correction_effective["automation_on_behalf"]["cause"]["review_assignment_sponsor_id"],
        successor.client_id
    );
    assert_eq!(correction_result["applied"], true);
    assert_eq!(correction_result["sender"], final_manager.client_id);
    assert_eq!(correction_result["recipient"], "review-owner-v2");
    assert_eq!(operations.2["owner_id"], "review-owner-v2");
    assert_eq!(operations.3["submitted_by"], "review-owner-v2");
    assert_eq!(operations.2["state"], "needs_correction");

    let acceptance_effective: Value = serde_json::from_str(&operations.1.1).unwrap();
    let acceptance_result: Value = serde_json::from_str(&operations.1.2).unwrap();
    assert_eq!(
        operations.1.0,
        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(
        acceptance_effective["automation_on_behalf"]["effective_manager_id"],
        final_manager.client_id
    );
    assert_eq!(
        acceptance_effective["automation_on_behalf"]["cause"]["review_assignment_sponsor_id"],
        successor.client_id
    );
    assert_eq!(
        acceptance_effective["automation_on_behalf"]["action"],
        "task.accept"
    );
    assert_eq!(operations.1.3, "queued");
    assert_eq!(acceptance_result["state"], "queued");
    assert_eq!(acceptance_result["attempt_id"], operations.4["attempt_id"]);
    assert_eq!(acceptance_result["task_accepted"], false);
    assert_eq!(operations.4["owner_id"], "review-owner-v2");
    assert_eq!(operations.5["submitted_by"], "review-owner-v2");
    assert_eq!(operations.6, 2);
    assert!(dispositions["entries"].is_array());

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn transferred_repair_dispatch_preserves_sponsor_decider_owner_and_reuses_current_slot() {
    let (owner, directory, bootstrap) = start_store("gm-successor-repair").await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    seed_clients(&owner.store).await;

    let source_owner = principal("review-owner-v2", Role::Manager);
    let decision_manager = principal("review-gm", Role::Manager);
    let current_manager = principal("review-owner-v1", Role::Manager);
    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":"designate-repair-source-owner","client_id":source_owner.client_id}),
        )
        .await
        .unwrap();

    let project_id = "fixture";
    let automation_id = "successor-repair-continuation";
    let changes = json!([{
        "automation_id":automation_id,
        "expected_revision":0,
        "include_existing":false,
        "patch":{
            "enabled":true,
            "scope":{"work_pool_id":null},
            "steps":["repair_dispatch"]
        }
    }]);
    let preview = owner
        .store
        .call(
            source_owner.clone(),
            "automation.config.preview".into(),
            json!({"project_id":project_id,"changes":changes.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true);
    owner
        .store
        .call(
            source_owner.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"enable-successor-repair-continuation",
                "project_id":project_id,
                "changes":changes,
                "preview_digest":preview["plan_sha256"]
            }),
        )
        .await
        .unwrap();

    let subject = seed_subject(
        &owner.store,
        "successor-repair",
        &source_owner.client_id,
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    add_submission_source_fact(&owner.store, "successor-repair", &subject).await;
    let binding_id = attach_ready_repair_binding(&owner.store, &subject).await;
    let (reviewer, assignment_id) = assign_sponsored_reviewer(
        &owner.store,
        source_owner.clone(),
        &subject,
        "successor-repair-reviewer",
        "successor-repair-reviewer-token",
    )
    .await;
    let review_result = owner
        .store
        .call(
            reviewer,
            "review.submit".into(),
            review_result_request(&subject, &assignment_id, "successor-repair-result"),
        )
        .await
        .unwrap();
    assert_eq!(review_result["verdict"], "changes_requested");
    let result_operation_id = review_result["operation_id"].as_str().unwrap().to_owned();

    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":"designate-repair-decision-manager","client_id":decision_manager.client_id}),
        )
        .await
        .unwrap();
    let feedback = owner
        .store
        .call(
            decision_manager.clone(),
            "task.request_changes".into(),
            json!({
                "client_request_id":"successor-repair-return-for-correction",
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "finding_id":"missing-r1-evidence",
                "reason":"Add the retained evidence for R1.",
                "requirement_ids":["R1"],
                "evidence":["evidence://review/r1"]
            }),
        )
        .await
        .unwrap();
    assert_eq!(feedback["applied"], true);
    assert_eq!(feedback["sender"], decision_manager.client_id);
    assert_eq!(feedback["recipient"], source_owner.client_id);
    assert_eq!(
        feedback["finding"]["reason"],
        "Add the retained evidence for R1."
    );
    assert_ne!(
        feedback["finding"]["reason"],
        "The candidate omits the requested evidence."
    );
    assert_eq!(
        feedback["review_provenance"]["review_assignment_id"],
        assignment_id
    );

    let first_transfer = owner
        .store
        .call(
            decision_manager.clone(),
            "automation.config.transfer".into(),
            json!({
                "client_request_id":"transfer-repair-to-decision-manager",
                "project_id":project_id,
                "former_owner_manager_id":source_owner.client_id,
                "automation_id":automation_id,
                "expected_revision":1
            }),
        )
        .await
        .unwrap();
    let first_transfer_id = first_transfer["transfer_operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":"designate-current-repair-manager","client_id":current_manager.client_id}),
        )
        .await
        .unwrap();
    let second_transfer = owner
        .store
        .call(
            current_manager.clone(),
            "automation.config.transfer".into(),
            json!({
                "client_request_id":"transfer-repair-to-current-manager",
                "project_id":project_id,
                "former_owner_manager_id":decision_manager.client_id,
                "automation_id":automation_id,
                "expected_revision":first_transfer["new_owner_revision"]
            }),
        )
        .await
        .unwrap();
    let second_transfer_id = second_transfer["transfer_operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        second_transfer["new_owner_manager_id"],
        current_manager.client_id
    );

    let first = consume_repair_result_for_entry(
        &owner.store,
        &current_manager.client_id,
        project_id,
        automation_id,
        &assignment_id,
        &result_operation_id,
    )
    .await
    .unwrap();
    assert_eq!(first["status"], "pending");
    let second = consume_repair_result_for_entry(
        &owner.store,
        &current_manager.client_id,
        project_id,
        automation_id,
        &assignment_id,
        &result_operation_id,
    )
    .await
    .unwrap();
    assert_eq!(second["status"], "pending");
    assert_eq!(second["delivery_verified"], false);

    let operation_id = owner
        .store
        .run({
            let assignment_id = assignment_id.clone();
            let result_operation_id = result_operation_id.clone();
            move |db| {
                Ok(db.query_row(
                    "SELECT operation_id FROM operations WHERE method='agent.send' \
                     AND json_extract(effective_request_json,'$.automation_on_behalf.cause.review_assignment_id')=?1 \
                     AND json_extract(effective_request_json,'$.automation_on_behalf.cause.review_result_operation_id')=?2 \
                     ORDER BY created_at_ms,operation_id LIMIT 1",
                    params![assignment_id, result_operation_id],
                    |row| row.get::<_, String>(0),
                )?)
            }
        })
        .await
        .unwrap();

    let observed = owner
        .store
        .run({
            let operation_id = operation_id.clone();
            let attempt_id = subject.attempt_id.clone();
            let assignment_id = assignment_id.clone();
            let result_operation_id = result_operation_id.clone();
            move |db| {
                let row: (String, String, String, String, String) = db.query_row(
                    "SELECT caller_id,client_request_id,state,effective_request_json,original_request_json FROM operations WHERE operation_id=?1 AND method='agent.send'",
                    [&operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )?;
                let effective: Value = serde_json::from_str(&row.3)?;
                let original: Value = serde_json::from_str(&row.4)?;
                let duplicate_count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE method='agent.send' \
                     AND json_extract(effective_request_json,'$.automation_on_behalf.cause.review_assignment_id')=?1 \
                     AND json_extract(effective_request_json,'$.automation_on_behalf.cause.review_result_operation_id')=?2",
                    params![assignment_id, result_operation_id],
                    |row| row.get(0),
                )?;
                let attempt = tasks::get_attempt(db, &attempt_id)?;
                let link = super::automation_repair::operation_link(db, &operation_id)?
                    .ok_or_else(|| Error::new("NOT_FOUND", "retained RepairDispatch link"))?;
                let current_gm_epoch: i64 = db.query_row(
                    "SELECT json_extract(value_json,'$.epoch') FROM meta WHERE key='gm'",
                    [],
                    |row| row.get(0),
                )?;
                Ok((
                    row,
                    effective,
                    original,
                    duplicate_count,
                    attempt,
                    serde_json::to_value(link)?,
                    current_gm_epoch,
                ))
            }
        })
        .await
        .unwrap();
    let (operation_row, effective, original, duplicate_count, attempt, link, captured_gm_epoch) =
        observed;
    assert_eq!(
        operation_row.0,
        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(operation_row.2, "queued");
    assert_eq!(duplicate_count, 1);
    assert_eq!(original["binding_id"], binding_id);
    assert_eq!(original["generation"], 1);
    assert_eq!(original["delivery"], "next_turn");
    assert_eq!(
        effective["automation_on_behalf"]["effective_manager_id"],
        current_manager.client_id
    );
    let cause = &effective["automation_on_behalf"]["cause"];
    assert_eq!(operation_row.1, cause["semantic_slot_id"]);
    assert_eq!(cause["source_attempt_owner_id"], source_owner.client_id);
    assert_eq!(
        cause["review_assignment_sponsor_id"],
        source_owner.client_id
    );
    assert_eq!(cause["decision_manager_id"], decision_manager.client_id);
    assert_eq!(
        cause["transfer_operation_ids"],
        json!([first_transfer_id, second_transfer_id])
    );
    assert_eq!(cause["transferred_gm_epoch"], captured_gm_epoch);
    assert_eq!(link["captured_transfer_gm_epoch"], captured_gm_epoch);
    assert_eq!(link["effective_manager_id"], current_manager.client_id);
    assert_eq!(link["binding_id"], binding_id);
    assert_eq!(link["binding_generation"], 1);
    assert_eq!(link["cause"], cause.clone());
    assert_eq!(attempt["owner_id"], source_owner.client_id);
    assert_eq!(attempt["binding_id"], binding_id);
    assert_eq!(attempt["binding_generation"], 1);
    assert_eq!(attempt["state"], "needs_correction");

    let admitted_effect = owner
        .store
        .run({
            let operation_id = operation_id.clone();
            move |db| {
                let context =
                    super::automation_repair::context_for_delivery_operation(db, &operation_id)?;
                context.require_current_for_effect(db, &operation_id)
            }
        })
        .await;
    assert!(admitted_effect.is_ok());

    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":"temporarily-remove-repair-manager","client_id":decision_manager.client_id}),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            operator,
            "gm.handover".into(),
            json!({"client_request_id":"restore-repair-manager","client_id":current_manager.client_id}),
        )
        .await
        .unwrap();

    let renewed_gm_epoch = owner
        .store
        .run(|db| {
            Ok(db.query_row(
                "SELECT json_extract(value_json,'$.epoch') FROM meta WHERE key='gm'",
                [],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert!(renewed_gm_epoch > captured_gm_epoch);
    let stale_effect = owner
        .store
        .run({
            let operation_id = operation_id.clone();
            move |db| {
                let context =
                    super::automation_repair::context_for_delivery_operation(db, &operation_id)?;
                context.require_current_for_effect(db, &operation_id)
            }
        })
        .await
        .unwrap_err();
    assert_eq!(stale_effect.code, "AUTOMATION_TRANSFER_SCOPE");

    let retained_operation = owner
        .store
        .run({
            let operation_id = operation_id.clone();
            let slot_id = cause["semantic_slot_id"].as_str().unwrap().to_owned();
            move |db| {
                let state: String = db.query_row(
                    "SELECT state FROM operations WHERE operation_id=?1 AND method='agent.send'",
                    [&operation_id],
                    |row| row.get(0),
                )?;
                let count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE method='agent.send' AND client_request_id=?1",
                    [&slot_id],
                    |row| row.get(0),
                )?;
                Ok((state, count))
            }
        })
        .await
        .unwrap();
    assert_eq!(retained_operation, ("queued".to_owned(), 1));

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

#[tokio::test]
async fn typed_applied_submission_rule_uses_review_operation_and_live_entry_authority() {
    let (owner, directory, bootstrap) = start_store("typed-event-review-rule").await;
    let _operator = owner.store.authenticate(bootstrap).await.unwrap();
    seed_clients(&owner.store).await;
    let manager = principal("review-gm", Role::Manager);
    let admitted = seed_subject(
        &owner.store,
        "typed-event-admitted",
        "review-gm",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    register_profile_reviewer(
        &owner.store,
        &manager,
        &admitted,
        "typed-event-reviewer",
        "typed-event-reviewer-token",
        "typed-event-reviewers",
    )
    .await;

    let configured = owner
        .store
        .call(
            manager.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"configure-typed-event-review-rule",
                "project_id":"fixture",
                "changes":[{
                    "automation_id":"typed-event-review-rule",
                    "expected_revision":0,
                    "include_existing":false,
                    "patch":{
                        "enabled":true,
                        "steps":["review_dispatch"],
                        "review":{"profile":"typed-event-reviewers","required_reviewers":1},
                        "event_rules":[{
                            "source":"task.submission",
                            "predicate":"applied",
                            "action":"review_dispatch"
                        }]
                    }
                }]
            }),
        )
        .await
        .unwrap();
    assert_eq!(configured["applied"], true);
    assert_eq!(
        configured["entries"][0]["event_rules"][0]["source"],
        "task.submission"
    );

    let primary_event_key = format!("submission:typed-rule:{}", admitted.submission_ref);
    let duplicate_event_key = format!(
        "submission:typed-rule-duplicate:{}",
        admitted.submission_ref
    );
    add_submission_source_fact_with_key(
        &owner.store,
        "typed-event-admitted",
        &primary_event_key,
        &admitted,
    )
    .await;
    add_submission_source_fact_with_key(
        &owner.store,
        "typed-event-admitted",
        &duplicate_event_key,
        &admitted,
    )
    .await;

    let pass = owner.store.reconcile_automations_once().await.unwrap();
    assert!(pass["review_dispatch"]["entries"].is_array());
    let (_operation_id, operation_caller, operation_state, effective, linked_operation, receipts) = owner
        .store
        .run({
            let task_id = admitted.task_id.clone();
            let submission_ref = admitted.submission_ref.clone();
            move |db| {
                let count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE method='review.assign' AND caller_id=?1 AND task_id=?2",
                    params![
                        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                        task_id
                    ],
                    |row| row.get(0),
                )?;
                let (operation_id, caller, state, effective_raw): (String, String, String, String) = db.query_row(
                    "SELECT operation_id,caller_id,state,effective_request_json FROM operations \
                     WHERE method='review.assign' AND caller_id=?1 AND task_id=?2 \
                     ORDER BY created_at_ms,operation_id LIMIT 1",
                    params![
                        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                        task_id
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
                let link = crate::automation::authorization::operation_link(db, &operation_id)?
                    .ok_or_else(|| Error::new("TEST_LINK_MISSING", "typed rule Operation lacks its manager link"))?;
                let intake = crate::store::automation_intake::pending_page(
                    db,
                    crate::automation::intake::LocalProducer::TaskSubmission.source_id(),
                    0,
                    64,
                )?;
                let receipts = intake
                    .items
                    .into_iter()
                    .filter_map(|item| match item {
                        crate::automation::intake::IntakeItem::Receipt(receipt)
                            if receipt.payload["submission_ref"] == submission_ref =>
                        {
                            Some((receipt.source_id, receipt.event_kind, receipt.source_event_key))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if count != 1 {
                    return Err(Error::new(
                        "TEST_AUTOMATION_OPERATION_COUNT",
                        format!("expected one typed review Operation, found {count}"),
                    ));
                }
                Ok((
                    operation_id,
                    caller,
                    state,
                    serde_json::from_str::<Value>(&effective_raw)?,
                    serde_json::to_value(link)?,
                    receipts,
                ))
            }
        })
        .await
        .unwrap();
    assert_eq!(
        operation_caller,
        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(operation_state, "settled");
    assert_eq!(effective["automation_on_behalf"]["action"], "review.assign");
    assert_eq!(
        effective["automation_on_behalf"]["automation_id"],
        "typed-event-review-rule"
    );
    assert_eq!(linked_operation["effective_manager_id"], manager.client_id);
    assert_eq!(linked_operation["automation_id"], "typed-event-review-rule");
    assert_eq!(linked_operation["automation_revision"], 1);
    assert_eq!(linked_operation["action"], "review.assign");
    let linked_cause = &linked_operation["cause"];
    assert_eq!(linked_cause["kind"], "applied_submission");
    assert_eq!(
        linked_cause["operation_id"],
        "submit-op-typed-event-admitted"
    );
    assert_eq!(linked_cause["id"], admitted.submission_ref);
    assert_eq!(receipts.len(), 2);
    assert!(receipts.iter().all(|(source, kind, _)| {
        source == crate::automation::intake::LocalProducer::TaskSubmission.source_id()
            && kind == crate::automation::intake::LocalProducer::TaskSubmission.event_kind()
    }));
    let mut observed_event_keys = receipts
        .iter()
        .map(|(_, _, event_key)| event_key.clone())
        .collect::<Vec<_>>();
    observed_event_keys.sort();
    let mut expected_event_keys = vec![primary_event_key, duplicate_event_key];
    expected_event_keys.sort();
    assert_eq!(observed_event_keys, expected_event_keys);

    let stale_context = owner
        .store
        .run({
            let manager_id = manager.client_id.clone();
            let submission_ref = admitted.submission_ref.clone();
            let cause_operation_id = linked_cause["operation_id"]
                .as_str()
                .expect("retained applied submission cause has its Operation ID")
                .to_owned();
            let cause_observation_id = linked_cause["observation_id"]
                .as_i64()
                .expect("retained applied submission cause has its observation ID");
            move |db| {
                let entry = crate::automation::config::load_entry(
                    db,
                    &manager_id,
                    "fixture",
                    "typed-event-review-rule",
                )?
                .ok_or_else(|| Error::new("TEST_ENTRY_MISSING", "typed event entry disappeared"))?;
                let cause = crate::automation::actions::AutomationCause::AppliedSubmission {
                    observation_id: cause_observation_id,
                    operation_id: cause_operation_id,
                    submission_ref,
                };
                crate::automation::authorization::ManagerExecutionContext::from_committed_entry(
                    db, &entry, cause,
                )
            }
        })
        .await
        .unwrap();

    let narrowed = owner
        .store
        .call(
            manager.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"narrow-typed-event-review-target",
                "project_id":"fixture",
                "changes":[{
                    "automation_id":"typed-event-review-rule",
                    "expected_revision":1,
                    "include_existing":false,
                    "patch":{"review":{"profile":"typed-event-reviewers-v2"}}
                }]
            }),
        )
        .await
        .unwrap();
    assert_eq!(narrowed["applied"], true);
    let changed_target = owner
        .store
        .run({
            let stale_context = stale_context.clone();
            let attempt_id = admitted.attempt_id.clone();
            let submission_ref = admitted.submission_ref.clone();
            let candidate_ref = admitted.candidate_ref.clone();
            move |db| {
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let request = crate::review::ReviewAssignRequest::for_automation(
                    "stale-event-rule-target".to_owned(),
                    attempt_id,
                    1,
                    submission_ref,
                    candidate_ref,
                    "typed-event-reviewers".to_owned(),
                );
                let result = super::reviews::reserve_assign(
                    &tx,
                    super::reviews::ReviewActor::OnBehalf(&stale_context),
                    &request,
                    "stale-event-rule-target-operation",
                    model::now_ms()?,
                );
                let code = result.err().map(|error| error.code);
                drop(tx);
                Ok(code)
            }
        })
        .await
        .unwrap();
    assert_eq!(changed_target.as_deref(), Some("FORBIDDEN"));

    let disabled = owner
        .store
        .call(
            manager.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"disable-typed-event-review-rule",
                "project_id":"fixture",
                "changes":[{
                    "automation_id":"typed-event-review-rule",
                    "expected_revision":2,
                    "include_existing":false,
                    "patch":{"enabled":false}
                }]
            }),
        )
        .await
        .unwrap();
    assert_eq!(disabled["applied"], true);

    let manual_subject = seed_subject(
        &owner.store,
        "typed-event-disabled",
        "review-gm",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    add_submission_source_fact_with_key(
        &owner.store,
        "typed-event-disabled",
        "submission:typed-rule-disabled-entry",
        &manual_subject,
    )
    .await;
    owner.store.reconcile_automations_once().await.unwrap();
    let automatic_count = owner
        .store
        .run({
            let task_id = manual_subject.task_id.clone();
            move |db| {
                db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE method='review.assign' AND caller_id=?1 AND task_id=?2",
                    params![
                        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                        task_id
                    ],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();
    assert_eq!(automatic_count, 0);

    let (_reviewer, _assignment_id) = assign_sponsored_reviewer(
        &owner.store,
        manager.clone(),
        &manual_subject,
        "typed-event-manual-reviewer",
        "typed-event-manual-reviewer-token",
    )
    .await;
    let manual_count = owner
        .store
        .run({
            let task_id = manual_subject.task_id.clone();
            move |db| {
                db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE method='review.assign' AND caller_id=?1 AND task_id=?2",
                    params![manager.client_id, task_id],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();
    assert_eq!(manual_count, 1);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(test)]
#[path = "o7_repair_cycle_tests.rs"]
mod o7_repair_cycle_tests;

#[tokio::test]
async fn hook_emit_replay_is_one_durable_source_observation_with_exact_registration_readback() {
    use std::{fs, process::Command};

    fn run_git(
        git: &std::path::Path,
        repository: &std::path::Path,
        hooks: &std::path::Path,
        args: &[&str],
    ) -> String {
        let output = Command::new(git)
            .arg("-c")
            .arg(format!("core.hooksPath={}", hooks.display()))
            .arg("-C")
            .arg(repository)
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "fixture Git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    let fixture_root = std::env::temp_dir().join(format!("swarm-hook-runtime-{}", model::new_id()));
    let repository = fixture_root.join("repository");
    let workspace_root = fixture_root.join("workspaces");
    let empty_hooks = fixture_root.join("empty-hooks");
    std::fs::create_dir_all(&repository).unwrap();
    std::fs::create_dir_all(&workspace_root).unwrap();
    std::fs::create_dir_all(&empty_hooks).unwrap();
    let configured_repository_path = fs::canonicalize(&repository).unwrap();

    let git = fixture_git_executable();
    run_git(&git, &repository, &empty_hooks, &["init", "--quiet"]);
    run_git(
        &git,
        &repository,
        &empty_hooks,
        &["config", "user.name", "Hook Fixture"],
    );
    run_git(
        &git,
        &repository,
        &empty_hooks,
        &["config", "user.email", "hook-fixture@example.invalid"],
    );
    fs::write(repository.join("candidate.txt"), "committed hook fixture\n").unwrap();
    run_git(&git, &repository, &empty_hooks, &["add", "candidate.txt"]);
    run_git(
        &git,
        &repository,
        &empty_hooks,
        &["commit", "--quiet", "-m", "hook fixture"],
    );
    let commit_oid = run_git(
        &git,
        &repository,
        &empty_hooks,
        &["rev-parse", "--verify", "HEAD^{commit}"],
    );

    let mut config = Config::default();
    config.forge.enabled = true;
    config.forge.git_executable = git;
    config.forge.projects.insert(
        "fixture".into(),
        crate::forge::ForgeProject {
            canonical_repository: "github.com/owner/hook-fixture".into(),
            repository_path: configured_repository_path,
            remote_name: "origin".into(),
            policy_revision: crate::policy::OWNER_POLICY_V2_ID.into(),
            target_refs: vec!["refs/heads/main".into()],
        },
    );
    config.workspace.projects.insert(
        "fixture".into(),
        crate::workspace::WorkspaceProjectConfig {
            allowed_roots: vec![workspace_root],
        },
    );
    let (owner, data_directory, bootstrap) =
        start_store_with_config("hook-runtime-replay", config).await;
    owner.store.initialize_workspace_authority().await.unwrap();
    let operator = owner.store.authenticate(bootstrap).await.unwrap();

    let source_id = model::new_id();
    let credential = Credential {
        client_id: format!("hook-source:{source_id}"),
        token: format!("{}{}", model::new_id(), model::new_id()),
    };
    owner
        .store
        .call(
            operator,
            "hook.source.setup".into(),
            json!({
                "client_request_id":model::new_id(),
                "project_id":"fixture",
                "source_id":source_id,
                "credential":credential,
            }),
        )
        .await
        .unwrap();
    let hook_source = owner.store.authenticate(credential).await.unwrap();

    let emit = json!({"source_id":source_id,"commit_oid":commit_oid});
    let first = owner
        .store
        .call(hook_source.clone(), "hook.emit".into(), emit.clone())
        .await
        .unwrap();
    assert_eq!(first["recorded"], true);
    assert_eq!(first["duplicate"], false);
    assert_eq!(first["readback_verified"], true);
    let observation_id = first["observation_id"].as_i64().unwrap();

    // This is the lost-ack retry path: resend the immutable native envelope.
    // It must resolve to the first observation, never append another fact.
    let replay = owner
        .store
        .call(hook_source.clone(), "hook.emit".into(), emit)
        .await
        .unwrap();
    assert_eq!(replay["recorded"], false);
    assert_eq!(replay["duplicate"], true);
    assert_eq!(replay["observation_id"], observation_id);

    let readback = owner
        .store
        .call(
            hook_source.clone(),
            "hook.source.get".into(),
            json!({"source_id":source_id,"after":0,"limit":64}),
        )
        .await
        .unwrap();
    assert_eq!(readback["events"].as_array().unwrap().len(), 1);
    assert_eq!(readback["events"][0]["observation_id"], observation_id);
    assert_eq!(readback["events"][0]["fact"]["source_id"], source_id);
    assert_eq!(readback["events"][0]["fact"]["commit_oid"], commit_oid);
    assert_eq!(
        readback["events"][0]["fact"]["registration_id"],
        readback["source"]["registration_id"]
    );
    assert_eq!(
        readback["events"][0]["fact"]["registration_generation"],
        readback["source"]["registration_generation"]
    );

    let exact_source = source_id.clone();
    let exact_commit = commit_oid.clone();
    let (hook_intake, indexed) = owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let intake =
                automation_dispatch::reconcile_source_intake(&tx, 64, true, model::now_ms()?)?;
            let hook_page = intake.hook_commit.ok_or_else(|| {
                Error::new("TEST_INTAKE_MISSING", "HookCommit was not reconciled")
            })?;
            let indexed = automation_intake::hook_commit_by_identity(
                &tx,
                &exact_source,
                "fixture",
                &exact_commit,
            )?;
            tx.commit()?;
            Ok((hook_page, indexed))
        })
        .await
        .unwrap();
    assert_eq!(hook_intake.processed, 1);
    let (receipt, fact) = indexed.expect("exact source and commit have one retained receipt");
    assert_eq!(receipt.observation_id, observation_id);
    assert_eq!(fact.source_id, source_id);
    assert_eq!(fact.commit_oid, commit_oid);
    assert_eq!(
        fact.registration_id,
        readback["source"]["registration_id"].as_str().unwrap()
    );
    assert_eq!(
        fact.registration_generation,
        readback["source"]["registration_generation"]
            .as_i64()
            .unwrap()
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(data_directory).unwrap();
    std::fs::remove_dir_all(fixture_root).unwrap();
}

#[cfg(test)]
mod o9_goal_progression_transfer_regression;
