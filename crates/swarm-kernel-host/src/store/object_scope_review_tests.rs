use super::super::{SCHEMA, set_meta};
use super::{OperationReadBasis, OperationReadLevel, resolve_operation_read};
use crate::{
    model::{self, Principal, Role},
    review::ReviewSlotIdentity,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

const TASK_ID: &str = "review-task";
const ATTEMPT_ID: &str = "review-attempt";
const SUBMISSION_REF: &str = "review-submission";
const CANDIDATE_REF: &str = "review-candidate";
const ASSIGNMENT_ID: &str = "review-assignment";
const ASSIGNMENT_OPERATION_ID: &str = "review-assign-operation";
const SUBMISSION_OPERATION_ID: &str = "task-submit-operation";

fn principal(client_id: &str) -> Principal {
    Principal {
        link_id: format!("link-{client_id}"),
        client_id: client_id.to_owned(),
        role: Role::Participant,
    }
}

fn insert_operation(
    db: &Connection,
    operation_id: &str,
    caller_id: &str,
    method: &str,
    task_id: &str,
    attempt_id: &str,
    effective_request: &Value,
    result: &Value,
) {
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,?4,'{\"private_request\":true}',?5,?6,?7,'settled','{\"native_secret\":\"private\"}',?8,1,1,1,1)",
        params![
            operation_id,
            caller_id,
            format!("request-{operation_id}"),
            method,
            model::canonical(effective_request).unwrap(),
            task_id,
            attempt_id,
            model::canonical(result).unwrap(),
        ],
    )
    .unwrap();
}

fn registration(client_id: &str, scope: &Value) -> Value {
    json!({
        "role":"participant",
        "disabled":false,
        "task_id":scope["task_id"],
        "task_revision":scope["task_revision"],
        "attempt_id":scope["attempt_id"],
        "binding_id":null,
        "binding_generation":null,
        "created_by":"review-manager",
        "review_sponsor_client_id":"review-manager",
        "participation_basis":{
            "kind":"sponsored_reviewer",
            "assignment_id":null,
            "review_scope":scope,
        },
        "display_alias":client_id,
        "inbound_policy":"pull_only",
        "grant_revision":1,
    })
}

fn review_fixture() -> (Connection, Principal, ReviewSlotIdentity, Value) {
    let db = Connection::open_in_memory().unwrap();
    db.pragma_update(None, "foreign_keys", true).unwrap();
    db.execute_batch(SCHEMA).unwrap();

    let identity = ReviewSlotIdentity {
        task_id: TASK_ID.to_owned(),
        attempt_id: ATTEMPT_ID.to_owned(),
        task_revision: 1,
        submission_ref: SUBMISSION_REF.to_owned(),
        candidate_ref: CANDIDATE_REF.to_owned(),
        review_policy_generation: "fixture-generation".to_owned(),
        review_slot: "primary".to_owned(),
    };
    let identity_value = serde_json::to_value(&identity).unwrap();
    let scope = json!({
        "review_assignment_id":ASSIGNMENT_ID,
        "task_id":TASK_ID,
        "attempt_id":ATTEMPT_ID,
        "task_revision":1,
        "submission_ref":SUBMISSION_REF,
        "candidate_ref":CANDIDATE_REF,
    });
    let task_spec = json!({
        "objective":"Exercise exact assigned-review receipt scope",
        "phase":"implementation",
        "requirements":[{"id":"R1","statement":"Keep the receipt bounded"}],
    });
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,'fixture',1,'open',?2,1,1)",
        params![TASK_ID, model::canonical(&task_spec).unwrap()],
    )
    .unwrap();

    let candidate_bytes = b"review candidate fixture";
    let candidate_metadata = json!({"attempt_id":ATTEMPT_ID,"task_revision":1});
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,'artifacts/review-candidate.bin','source_snapshot',?2,?3,1,?4)",
        params![
            CANDIDATE_REF,
            candidate_bytes.len() as i64,
            model::digest(candidate_bytes),
            model::canonical(&candidate_metadata).unwrap(),
        ],
    )
    .unwrap();

    let submission_document = json!({
        "operation_id":SUBMISSION_OPERATION_ID,
        "task_id":TASK_ID,
        "attempt_id":ATTEMPT_ID,
        "task_revision":1,
        "candidate_ref":CANDIDATE_REF,
        "summary":"applied fixture submission",
    });
    let submission_document_raw = model::canonical(&submission_document).unwrap();
    let submission_metadata = json!({"operation_id":SUBMISSION_OPERATION_ID});
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,'artifacts/review-submission.bin','task_submission',?2,?3,1,?4)",
        params![
            SUBMISSION_REF,
            submission_document_raw.len() as i64,
            model::digest(submission_document_raw.as_bytes()),
            model::canonical(&submission_metadata).unwrap(),
        ],
    )
    .unwrap();
    let snapshot = json!({"revision":1,"spec":task_spec,"brief":null});
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,submission_ref,candidate_ref,created_at_ms,updated_at_ms) VALUES(?1,?2,1,?3,'submitter','controller','submitted','[]',?4,?5,1,1)",
        params![
            ATTEMPT_ID,
            TASK_ID,
            model::canonical(&snapshot).unwrap(),
            SUBMISSION_REF,
            CANDIDATE_REF,
        ],
    )
    .unwrap();

    let submission_result = json!({
        "operation_id":SUBMISSION_OPERATION_ID,
        "outcome":"applied",
        "attempt_id":ATTEMPT_ID,
        "submission_ref":SUBMISSION_REF,
        "candidate_ref":CANDIDATE_REF,
        "task_accepted":false,
        "private_result":"must not escape the receipt projector",
    });
    insert_operation(
        &db,
        SUBMISSION_OPERATION_ID,
        "submitter",
        "task.submit",
        TASK_ID,
        ATTEMPT_ID,
        &json!({"submission_document":submission_document,"private_contract":"private"}),
        &submission_result,
    );

    let assignment = json!({
        "review_assignment_id":ASSIGNMENT_ID,
        "operation_id":ASSIGNMENT_OPERATION_ID,
        "identity":identity_value,
        "sponsor_client_id":"review-manager",
        "reviewer_client_id":"assigned-reviewer",
        "assignment_state":"assigned",
        "private_assignment_data":"must not escape the receipt projector",
    });
    insert_operation(
        &db,
        ASSIGNMENT_OPERATION_ID,
        "review-manager",
        "review.assign",
        TASK_ID,
        ATTEMPT_ID,
        &json!({"review_assignment":assignment,"private_contract":"private"}),
        &assignment,
    );
    db.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:review',?1,?2,'review.assignment',?3,1)",
        params![
            format!("assignment:{ASSIGNMENT_ID}"),
            ASSIGNMENT_OPERATION_ID,
            model::canonical(&assignment).unwrap(),
        ],
    )
    .unwrap();

    let slot_key = identity.digest().unwrap();
    set_meta(
        &db,
        &format!("review:slot:{slot_key}"),
        &json!({
            "review_assignment_id":ASSIGNMENT_ID,
            "slot_key":slot_key,
            "identity":identity_value,
        }),
    )
    .unwrap();
    set_meta(
        &db,
        "client:assigned-reviewer",
        &registration("assigned-reviewer", &scope),
    )
    .unwrap();
    set_meta(
        &db,
        "client:other-reviewer",
        &registration("other-reviewer", &scope),
    )
    .unwrap();

    (db, principal("assigned-reviewer"), identity, assignment)
}

fn assert_no_grant(db: &Connection, principal: &Principal, operation_id: &str) {
    assert_eq!(
        resolve_operation_read(db, principal, operation_id).unwrap(),
        None
    );
}

#[test]
fn assigned_reviewer_reads_only_closed_assignment_and_applied_submission_receipts() {
    let (db, reviewer, _, _) = review_fixture();
    for operation_id in [ASSIGNMENT_OPERATION_ID, SUBMISSION_OPERATION_ID] {
        let grant = resolve_operation_read(&db, &reviewer, operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(grant.level, OperationReadLevel::Receipt);
        assert_eq!(grant.basis, OperationReadBasis::RetainedAssignedReviewer);
    }

    let assignment_receipt =
        super::super::object_reads::operation(&db, &reviewer, ASSIGNMENT_OPERATION_ID).unwrap();
    assert_eq!(assignment_receipt["method"], "review.assign");
    assert_eq!(assignment_receipt["result_status"], "not_projected");
    assert!(assignment_receipt.get("result").is_none());
    assert!(assignment_receipt.get("diagnostic").is_none());
    assert!(assignment_receipt.get("native_refs").is_none());
    assert!(assignment_receipt.get("caller_id").is_none());
    assert!(assignment_receipt.get("private_contract").is_none());

    let submission_receipt =
        super::super::object_reads::operation(&db, &reviewer, SUBMISSION_OPERATION_ID).unwrap();
    assert_eq!(submission_receipt["method"], "task.submit");
    assert_eq!(submission_receipt["result"]["outcome"], "applied");
    assert_eq!(
        submission_receipt["result"]["submission_ref"],
        SUBMISSION_REF
    );
    assert!(submission_receipt["result"].get("private_result").is_none());
    assert!(submission_receipt.get("diagnostic").is_none());
    assert!(submission_receipt.get("native_refs").is_none());
    assert!(submission_receipt.get("caller_id").is_none());
}

#[test]
fn another_reviewer_assignment_or_task_does_not_match_the_retained_relation() {
    let (db, reviewer, identity, _) = review_fixture();
    assert_no_grant(&db, &principal("other-reviewer"), ASSIGNMENT_OPERATION_ID);

    let other_assignment_id = "other-review-assignment";
    let other_assignment_operation = "other-review-assign-operation";
    let other_assignment = json!({
        "review_assignment_id":other_assignment_id,
        "operation_id":other_assignment_operation,
        "identity":identity,
        "sponsor_client_id":"review-manager",
        "reviewer_client_id":"other-reviewer",
    });
    insert_operation(
        &db,
        other_assignment_operation,
        "review-manager",
        "review.assign",
        TASK_ID,
        ATTEMPT_ID,
        &json!({"review_assignment":other_assignment}),
        &other_assignment,
    );
    assert_no_grant(&db, &reviewer, other_assignment_operation);

    let mismatched_submission_operation = "unlinked-task-submit-operation";
    insert_operation(
        &db,
        mismatched_submission_operation,
        "review-manager",
        "task.submit",
        TASK_ID,
        ATTEMPT_ID,
        &json!({}),
        &json!({
            "operation_id":mismatched_submission_operation,
            "outcome":"applied",
            "attempt_id":ATTEMPT_ID,
            "submission_ref":SUBMISSION_REF,
            "candidate_ref":CANDIDATE_REF,
        }),
    );
    assert_no_grant(&db, &reviewer, mismatched_submission_operation);

    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('other-task','fixture',1,'open','{}',1,1)",
        [],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,submission_ref,candidate_ref,created_at_ms,updated_at_ms) VALUES('other-attempt','other-task',1,'{\"revision\":1}','other-submitter','controller','submitted','[]',?1,?2,1,1)",
        params![SUBMISSION_REF, CANDIDATE_REF],
    )
    .unwrap();
    let other_identity = ReviewSlotIdentity {
        task_id: "other-task".to_owned(),
        attempt_id: "other-attempt".to_owned(),
        task_revision: 1,
        submission_ref: SUBMISSION_REF.to_owned(),
        candidate_ref: CANDIDATE_REF.to_owned(),
        review_policy_generation: "other-generation".to_owned(),
        review_slot: "primary".to_owned(),
    };
    let other_task_operation = "other-task-review-assign-operation";
    let other_task_result = json!({
        "review_assignment_id":"other-task-assignment",
        "operation_id":other_task_operation,
        "identity":other_identity,
        "sponsor_client_id":"review-manager",
        "reviewer_client_id":"other-reviewer",
    });
    insert_operation(
        &db,
        other_task_operation,
        "review-manager",
        "review.assign",
        "other-task",
        "other-attempt",
        &json!({"review_assignment":other_task_result}),
        &other_task_result,
    );
    assert_no_grant(&db, &reviewer, other_task_operation);

    let unrelated_task_operation = "unrelated-task-dispatch-operation";
    insert_operation(
        &db,
        unrelated_task_operation,
        "review-manager",
        "task.dispatch",
        TASK_ID,
        ATTEMPT_ID,
        &json!({}),
        &json!({"operation_id":unrelated_task_operation,"outcome":"applied"}),
    );
    assert_no_grant(&db, &reviewer, unrelated_task_operation);
}

#[test]
fn historical_assignment_receipt_survives_but_applied_submission_requires_current_scope() {
    let (db, reviewer, _, _) = review_fixture();
    db.execute("UPDATE tasks SET revision=2 WHERE task_id=?1", [TASK_ID])
        .unwrap();
    db.execute(
        "UPDATE attempts SET state='superseded',released_at_ms=2 WHERE attempt_id=?1",
        [ATTEMPT_ID],
    )
    .unwrap();

    let grant = resolve_operation_read(&db, &reviewer, ASSIGNMENT_OPERATION_ID)
        .unwrap()
        .unwrap();
    assert_eq!(grant.level, OperationReadLevel::Receipt);
    assert_eq!(grant.basis, OperationReadBasis::RetainedAssignedReviewer);
    assert_no_grant(&db, &reviewer, SUBMISSION_OPERATION_ID);
}

#[test]
fn malformed_assignment_operation_link_fails_closed() {
    let (db, reviewer, _, _) = review_fixture();
    db.execute(
        "UPDATE observations SET operation_id=?1 WHERE source_stream_id='controller:review' AND source_event_key=?2 AND kind='review.assignment'",
        params![SUBMISSION_OPERATION_ID, format!("assignment:{ASSIGNMENT_ID}")],
    )
    .unwrap();

    let error = resolve_operation_read(&db, &reviewer, ASSIGNMENT_OPERATION_ID).unwrap_err();
    assert_eq!(error.code, "REVIEW_RECORD_DAMAGED");
}
