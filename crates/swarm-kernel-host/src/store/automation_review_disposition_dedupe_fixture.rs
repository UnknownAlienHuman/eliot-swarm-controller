//! Cross-review same-content correction dedupe.
//!
//! The ordered finding content is the correction identity. Two distinct
//! assigned review results carrying identical content must resolve to one
//! content identity, so the retained delivery is recorded as the disposition of
//! the current assignment instead of emitting a second send. A materially
//! changed finding remains a new correction.
use super::*;
use crate::model::{Credential, Principal, Role};
use serde_json::json;
use std::sync::Arc;

const MANAGER_ID: &str = "review-owner-cross-dedupe";
const FIRST_REVIEWER_ID: &str = "assigned-reviewer-cross-dedupe-one";
const FIRST_REVIEWER_TOKEN: &str = "reviewer-cross-dedupe-token-one";
const SECOND_REVIEWER_ID: &str = "assigned-reviewer-cross-dedupe-two";
const SECOND_REVIEWER_TOKEN: &str = "reviewer-cross-dedupe-token-two";
const PROJECT_ID: &str = "fixture";
const AUTOMATION_ID: &str = "cross-review-dedupe";

fn retained_finding() -> serde_json::Value {
    json!({
        "finding_id":"missing-r1-evidence",
        "requirement_ids":["R1"],
        "reason":"The candidate omits the requested evidence.",
        "evidence_refs":["evidence://review/r1"],
        "requested_change":"Add the retained evidence for R1."
    })
}

/// Identical ordered finding content for both reviews.
fn same_content_findings() -> Vec<serde_json::Value> {
    vec![retained_finding()]
}

/// A materially changed finding must remain a new correction.
fn changed_content_findings() -> Vec<serde_json::Value> {
    let mut changed = retained_finding();
    changed["reason"] = json!("The candidate also omits the verification record.");
    vec![changed]
}

fn manager() -> Principal {
    Principal {
        link_id: model::new_id(),
        client_id: MANAGER_ID.to_owned(),
        role: Role::Manager,
    }
}

async fn start_store(label: &str) -> (StoreOwner, std::path::PathBuf, Credential) {
    let directory = std::env::temp_dir().join(format!(
        "swarm-cross-review-dedupe-{label}-{}",
        model::new_id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let root = crate::platform::DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    (owner, directory, credential)
}

async fn seed_manager(store: &Store) {
    store
        .run(|db| {
            set_meta(
                db,
                &format!("client:{MANAGER_ID}"),
                &json!({"role":Role::Manager,"disabled":false}),
            )?;
            set_meta(db, "gm", &json!({"client_id":MANAGER_ID,"epoch":1}))?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn seed_subject(store: &Store, suffix: &str) -> (String, String, String, String) {
    let suffix = suffix.to_owned();
    store
        .run(move |db| {
            let task_id = format!("task-{suffix}");
            let attempt_id = format!("attempt-{suffix}");
            let candidate_ref =
                format!("source-{}", model::digest(format!("candidate-{suffix}").as_bytes()));
            let submission_ref =
                format!("submission-{}", model::digest(format!("submission-{suffix}").as_bytes()));
            let submission_operation_id = format!("submit-op-{suffix}");
            let candidate_bytes = b"explicit source snapshot fixture";
            let candidate_digest = model::digest(candidate_bytes);
            let candidate_metadata = json!({
                "attempt_id":attempt_id,
                "task_revision":1,
                "fixture":"cross review dedupe",
            });
            let spec_json = json!({
                "objective":format!("Cross review dedupe fixture {suffix}"),
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Preserve the exact review finding scope"}],
                "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
            });
            let spec: crate::model::TaskSpec = serde_json::from_value(spec_json.clone())?;
            let edition =
                crate::policy::accepted_edition(Some(crate::policy::OWNER_POLICY_V2_ID))?;
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
                "owner_id":MANAGER_ID,
                "submitted_by":MANAGER_ID,
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
            let submission_digest =
                model::digest(model::canonical(&submission_document)?.as_bytes());
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
                    MANAGER_ID,
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
                    MANAGER_ID,
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
            Ok((task_id, attempt_id, submission_ref, candidate_ref))
        })
        .await
        .unwrap()
}

async fn register_reviewer(
    store: &Store,
    subject: &(String, String, String, String),
    reviewer_id: &str,
    token: &str,
) {
    let pending_scope = json!({
        "review_assignment_id":null,
        "task_id":subject.0,
        "attempt_id":subject.1,
        "task_revision":1,
        "submission_ref":subject.2,
        "candidate_ref":subject.3,
    });
    store
        .call(
            manager(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":format!("register-{reviewer_id}"),
                "client_id":reviewer_id,
                "token_hash":model::digest(token.as_bytes()),
                "task_id":subject.0,
                "task_revision":1,
                "attempt_id":subject.1,
                "participation_basis":{"kind":"sponsored_reviewer","review_scope":pending_scope},
            }),
        )
        .await
        .unwrap();
}

async fn assign_reviewer(
    store: &Store,
    subject: &(String, String, String, String),
    reviewer_id: &str,
    token: &str,
    replaces: Option<&str>,
) -> (Principal, String) {
    let mut assign = json!({
        "client_request_id":format!("assign-{reviewer_id}"),
        "attempt_id":subject.1,
        "expected_revision":1,
        "submission_ref":subject.2,
        "candidate_ref":subject.3,
        "reviewer_client_id":reviewer_id,
    });
    if let Some(prior) = replaces {
        assign["replaces_review_assignment_id"] = json!(prior);
        assign["replacement_reason"] =
            json!("an independent second review of the same candidate is retained");
        assign["replacement_evidence_refs"] = json!([subject.3]);
    }
    let assignment = store
        .call(manager(), "review.assign".into(), assign)
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

async fn submit_changes_requested(
    store: &Store,
    reviewer: &Principal,
    subject: &(String, String, String, String),
    assignment_id: &str,
    request_id: &str,
    findings: &[serde_json::Value],
) -> String {
    let result = store
        .call(
            reviewer.clone(),
            "review.submit".into(),
            json!({
                "client_request_id":request_id,
                "review_assignment_id":assignment_id,
                "submission_ref":subject.2,
                "candidate_ref":subject.3,
                "verdict":"changes_requested",
                "coverage":"complete",
                "findings":findings,
                "evidence_refs":["evidence://review/r1"],
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["verdict"], "changes_requested");
    result["operation_id"].as_str().unwrap().to_owned()
}

async fn configure_disposition_entry(store: &Store) {
    store
        .run(|db| {
            let tx = db.transaction()?;
            let mut entry = crate::automation::config::AutomationEntry::new(
                MANAGER_ID,
                PROJECT_ID,
                AUTOMATION_ID,
                10,
            );
            entry.enabled = true;
            entry.steps = vec![crate::automation::actions::AutomationStep::ReviewDisposition];
            crate::automation::config::validate_entry(&entry)?;
            let entry_key =
                crate::automation::config::entry_key(MANAGER_ID, PROJECT_ID, AUTOMATION_ID)?;
            crate::automation::config::write_record(&tx, &entry_key, &entry.value()?)?;
            let cut: i64 = tx.query_row(
                "SELECT COALESCE(MAX(observation_id),0) FROM observations",
                [],
                |row| row.get(0),
            )?;
            crate::store::review_disposition::configure_activation(
                &tx, None, &entry, true, cut, 10,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

fn request_changes_v2(
    subject: &(String, String, String, String),
    assignment_id: &str,
    result_operation_id: &str,
    request_id: &str,
) -> serde_json::Value {
    json!({
        "schema_version":2,
        "client_request_id":request_id,
        "attempt_id":subject.1,
        "expected_revision":1,
        "submission_ref":subject.2,
        "candidate_ref":subject.3,
        "review_assignment_id":assignment_id,
        "review_result_operation_id":result_operation_id,
        "finding_ids":["missing-r1-evidence"],
    })
}

async fn consume_feedback(store: &Store, assignment_id: &str, result_id: &str) -> Value {
    let reconciled = store.reconcile_review_dispositions_once().await.unwrap();
    assert_eq!(
        reconciled["review_results"]["quarantined"], 0,
        "{reconciled}"
    );
    let assignment_id = assignment_id.to_owned();
    let result_id = result_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction()?;
            let entry =
                crate::automation::config::load_entry(&tx, MANAGER_ID, PROJECT_ID, AUTOMATION_ID)?
                    .unwrap();
            let result = super::automation_disposition::consume_review_result_for_entry(
                &tx,
                &Config::default(),
                &entry,
                &assignment_id,
                &result_id,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
        .unwrap()
}

async fn retained_delivery_count(store: &Store) -> i64 {
    store.run(|db| Ok(db.query_row(
        "SELECT COUNT(*) FROM observations WHERE source_stream_id='controller:review' AND kind='task.feedback' AND json_extract(payload_json,'$.message_id') IS NOT NULL",
        [], |row| row.get(0),
    )?)).await.unwrap()
}

async fn retained_disposition(store: &Store, assignment_id: &str) -> Value {
    let key = format!("disposition:{assignment_id}");
    store.run(move |db| {
        let raw: String = db.query_row(
            "SELECT payload_json FROM observations WHERE source_stream_id='controller:review' AND source_event_key=?1 AND kind='review.disposition'",
            [&key], |row| row.get(0),
        )?;
        Ok(serde_json::from_str(&raw)?)
    }).await.unwrap()
}

#[tokio::test]
async fn identical_correction_content_under_another_review_records_one_send_and_changed_content_is_new()
 {
    let (owner, directory, _credential) = start_store("dedupe").await;
    let store = &owner.store;
    seed_manager(store).await;
    let subject = seed_subject(store, "cross-dedupe").await;
    // Sponsorship is admitted only while submitted. Replacement later uses
    // these exact credentials; no SQL state reset or authorization relaxation.
    register_reviewer(store, &subject, FIRST_REVIEWER_ID, FIRST_REVIEWER_TOKEN).await;
    register_reviewer(store, &subject, SECOND_REVIEWER_ID, SECOND_REVIEWER_TOKEN).await;
    register_reviewer(
        store,
        &subject,
        "assigned-reviewer-cross-dedupe-three",
        "reviewer-cross-dedupe-token-three",
    )
    .await;
    let (first_reviewer, first_assignment) = assign_reviewer(
        store,
        &subject,
        FIRST_REVIEWER_ID,
        FIRST_REVIEWER_TOKEN,
        None,
    )
    .await;
    let first_result = submit_changes_requested(
        store,
        &first_reviewer,
        &subject,
        &first_assignment,
        "review-result-cross-dedupe-one",
        &same_content_findings(),
    )
    .await;
    configure_disposition_entry(store).await;
    let first = consume_feedback(store, &first_assignment, &first_result).await;
    assert_eq!(first["disposition_applied"], true, "{first}");
    let original_feedback = first["feedback"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let original_digest = first["feedback"]["findings_digest"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(retained_delivery_count(store).await, 1);
    let saved_id = original_feedback.clone();
    let saved_operation = store.run(move |db| {
        let row: (String,String,String) = db.query_row(
            "SELECT original_request_json,effective_request_json,result_json FROM operations WHERE operation_id=?1",
            [&saved_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        )?;
        Ok(row)
    }).await.unwrap();

    // Exercise the immutable legacy-key lookup as well as current content keys.
    // Only the fixture index coordinate changes; historical payload bytes stay.
    let saved_id = original_feedback.clone();
    store.run(move |db| {
        let raw: String = db.query_row(
            "SELECT payload_json FROM observations WHERE source_stream_id='controller:review' AND kind='task.feedback' AND operation_id=?1",
            [&saved_id], |row| row.get(0),
        )?;
        let feedback: Value = serde_json::from_str(&raw)?;
        let legacy_key = format!("findings-package:{}", model::digest(model::canonical(&json!([
            MANAGER_ID,feedback["findings_package"]["identity"]["submission_ref"],
            feedback["findings_package"]["identity"]["candidate_ref"],feedback["findings_digest"]
        ]))?.as_bytes()));
        db.execute("UPDATE observations SET source_event_key=?2 WHERE source_stream_id='controller:review' AND kind='task.feedback' AND operation_id=?1", params![saved_id,legacy_key])?;
        Ok(())
    }).await.unwrap();

    let (second_reviewer, second_assignment) = assign_reviewer(
        store,
        &subject,
        SECOND_REVIEWER_ID,
        SECOND_REVIEWER_TOKEN,
        Some(&first_assignment),
    )
    .await;
    let second_result = submit_changes_requested(
        store,
        &second_reviewer,
        &subject,
        &second_assignment,
        "review-result-cross-dedupe-two",
        &same_content_findings(),
    )
    .await;
    let second = consume_feedback(store, &second_assignment, &second_result).await;
    assert_eq!(second["disposition_applied"], true, "{second}");
    assert_eq!(second["feedback"]["operation_id"], original_feedback);
    assert_eq!(second["feedback"]["resend_suppressed"], true);
    assert_ne!(
        second["feedback"]["findings_digest"], original_digest,
        "provenance digests remain review-specific"
    );
    assert_eq!(retained_delivery_count(store).await, 1);
    let disposition = retained_disposition(store, &second_assignment).await;
    assert_eq!(disposition["review_result_operation_id"], second_result);
    assert_eq!(disposition["task_feedback_operation_id"], original_feedback);
    assert_eq!(disposition["finding_ids"], json!(["missing-r1-evidence"]));
    let manual = store
        .call(
            manager(),
            "task.request_changes".into(),
            request_changes_v2(
                &subject,
                &second_assignment,
                &second_result,
                "owner-cross-dedupe-second",
            ),
        )
        .await
        .unwrap();
    assert_eq!(manual["message_id"], original_feedback);
    assert_eq!(manual["coalesced"], true);
    assert_eq!(retained_delivery_count(store).await, 1);

    let (third_reviewer, third_assignment) = assign_reviewer(
        store,
        &subject,
        "assigned-reviewer-cross-dedupe-three",
        "reviewer-cross-dedupe-token-three",
        Some(&second_assignment),
    )
    .await;
    let third_result = submit_changes_requested(
        store,
        &third_reviewer,
        &subject,
        &third_assignment,
        "review-result-cross-dedupe-changed",
        &changed_content_findings(),
    )
    .await;
    let changed = consume_feedback(store, &third_assignment, &third_result).await;
    assert_eq!(changed["disposition_applied"], true, "{changed}");
    assert_ne!(changed["feedback"]["operation_id"], original_feedback);
    assert_eq!(retained_delivery_count(store).await, 2);
    let repeat = consume_feedback(store, &third_assignment, &third_result).await;
    assert_eq!(
        repeat["feedback"]["operation_id"],
        changed["feedback"]["operation_id"]
    );
    assert_eq!(retained_delivery_count(store).await, 2);
    let saved_id = original_feedback.clone();
    let after = store.run(move |db| {
        let row: (String,String,String) = db.query_row(
            "SELECT original_request_json,effective_request_json,result_json FROM operations WHERE operation_id=?1",
            [&saved_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        )?;
        Ok(row)
    }).await.unwrap();
    assert_eq!(
        after, saved_operation,
        "coalescing must preserve immutable historical Operation payloads"
    );
    drop(owner);
    let _ = std::fs::remove_dir_all(&directory);
}
