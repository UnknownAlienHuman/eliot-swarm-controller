//! Focused Store regressions for the sponsored review disposition lifecycle.
use super::*;
use rusqlite::params;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

struct ReviewSubject {
    task_id: String,
    attempt_id: String,
    submission_ref: String,
    candidate_ref: String,
}

async fn start_store(label: &str) -> (StoreOwner, PathBuf, Credential) {
    let directory = std::env::temp_dir().join(format!("swarm-program-{label}-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
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
            let candidate_ref = format!("candidate-{suffix}");
            let submission_ref = format!("submission-{suffix}");
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
                    format!("fixtures/{candidate_ref}"),
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
                    format!("fixtures/{submission_ref}"),
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
                        "outcome":"applied",
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
