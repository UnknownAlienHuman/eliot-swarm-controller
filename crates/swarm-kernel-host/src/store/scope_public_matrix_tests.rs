//! Focused public auth matrix for PR49/51/52 (Operation / TaskGraph / Artifact).
//!
//! Base: c56f466. No prior scope patch delivered; no test function is reused from
//! carried receipts. This module adds only missing public-boundary rows and reuses
//! the existing Store fixture pattern from object_scope_review_tests.rs.
//!
//! Covered (exact retained relations only, no new IAM):
//! * verified local Operator gets bounded Diagnostic/Evidence; distinct
//!   Role::Observer without a positive relation is filtered (None) even when the
//!   same frontend profile exposes Operation/Task/Artifact tools;
//! * exact caller keeps its bounded Receipt (including taskless rejections) but
//!   gains no Task/Artifact grant from that receipt alone;
//! * current Task manager (owner of the current unreleased Attempt) gets the
//!   documented Detail/Evidence and Operation Receipt; unrelated Manager, former
//!   GM (no designation), stale revision and released old Attempt are denied;
//! * operation.list filtering skips invisible rows without skipping later visible
//!   rows and without leaking payloads;
//! * fleet queue visibility is not a grant: an unrelated Manager with a visible
//!   queue item still gets None for task.get/operation/artifact resolvers;
//! * Task submission artifact inherits the narrowest grant (caller/manager capped,
//!   unrelated denied, unknown kinds fail closed);
//! * malformed bindings, missing Task linkage and unknown artifact kinds fail
//!   closed as OBJECT_SCOPE_DAMAGED, never as method-name fallback.
//!
//! Explicitly NOT covered here (separately assigned):
//! * contract ratify/reject decision relations (PR49 row 6);
//! * exact review-owner/successor lineage cases completed in 4b2ff276.
use super::super::{SCHEMA, SCRIPT_SCHEMA, WORKSPACE_SCHEMA, set_meta};
use super::{
    ArtifactReadBasis, ArtifactReadLevel, OperationReadBasis, OperationReadLevel,
    TaskGraphIdentity, TaskReadBasis, TaskReadLevel, identity_for_attempt,
    resolve_artifact_read, resolve_assembly_sources, resolve_operation_read,
    resolve_task_read,
};
use crate::{
    artifacts::ArtifactRecord,
    model::{self, Principal, Role},
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
const PROJECT: &str = "matrix-project";
fn test_principal(client_id: &str, role: Role) -> Principal {
    Principal {
        link_id: format!("matrix-link-{client_id}"),
        client_id: client_id.to_owned(),
        role,
    }
}
fn empty_db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.pragma_update(None, "foreign_keys", true).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(SCRIPT_SCHEMA).unwrap();
    db.execute_batch(WORKSPACE_SCHEMA).unwrap();
    db
}
fn insert_task(db: &Connection, task_id: &str, revision: i64) {
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,'open','{}',1,1)",
        params![task_id, PROJECT, revision],
    )
    .unwrap();
}
fn insert_attempt_running(
    db: &Connection,
    attempt_id: &str,
    task_id: &str,
    revision: i64,
    owner_id: &str,
    released_at_ms: Option<i64>,
) {
    let snapshot = json!({"revision": revision});
    let snapshot_raw = model::canonical(&snapshot).unwrap();
    match released_at_ms {
        Some(released) => {
            db.execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,released_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,'controller','superseded','[]',?6,1,1)",
                params![attempt_id, task_id, revision, snapshot_raw, owner_id, released],
            )
            .unwrap();
        }
        None => {
            db.execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,'controller','running','[]',1,1)",
                params![attempt_id, task_id, revision, snapshot_raw, owner_id],
            )
            .unwrap();
        }
    }
}
fn insert_operation(
    db: &Connection,
    operation_id: &str,
    caller_id: &str,
    method: &str,
    task_id: Option<&str>,
    attempt_id: Option<&str>,
    effective: &Value,
    result: &Value,
    state: &str,
) {
    let effective_raw = model::canonical(effective).unwrap();
    let result_raw = model::canonical(result).unwrap();
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,'{}',?5,?6,?7,?8,'{}',?9,1,1,1,1)",
        params![
            operation_id,
            caller_id,
            format!("request-{operation_id}"),
            method,
            effective_raw,
            task_id,
            attempt_id,
            state,
            result_raw,
        ],
    )
    .unwrap();
}
fn insert_operation_position(db: &Connection, operation_id: &str, recorded_at_ms: i64) {
    db.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:read-position:operation',?1,?1,'operation.position','{}',?2)",
        params![operation_id, recorded_at_ms],
    )
    .unwrap();
}
fn register_manager(db: &Connection, client_id: &str) {
    set_meta(
        db,
        &format!("client:{client_id}"),
        &json!({"role":"manager","disabled":false}),
    )
    .unwrap();
}
fn pin_local_operator(db: &Connection, operator_id: &str) {
    set_meta(db, "local_operator_client_id", &json!(operator_id)).unwrap();
    set_meta(
        db,
        &format!("client:{operator_id}"),
        &json!({"role":"operator","disabled":false}),
    )
    .unwrap();
}
#[test]
fn scope_public_operator_diagnostic_vs_observer_filtered() {
    let db = empty_db();
    pin_local_operator(&db, "matrix-operator");
    insert_task(&db, "matrix-task-op", 1);
    insert_attempt_running(&db, "matrix-attempt-op", "matrix-task-op", 1, "matrix-owner", None);
    insert_operation(
        &db,
        "matrix-op-op",
        "matrix-owner",
        "task.submit",
        Some("matrix-task-op"),
        Some("matrix-attempt-op"),
        &json!({}),
        &json!({"outcome":"applied"}),
        "settled",
    );
    let operator = test_principal("matrix-operator", Role::Operator);
    let observer = test_principal("matrix-observer", Role::Observer);
    let role_operator_without_credential = test_principal("matrix-other-operator", Role::Operator);
    let grant = resolve_operation_read(&db, &operator, "matrix-op-op")
        .unwrap()
        .unwrap();
    assert_eq!(grant.level, OperationReadLevel::Diagnostic);
    assert_eq!(grant.basis, OperationReadBasis::LocalOperator);
    assert_eq!(
        resolve_operation_read(&db, &observer, "matrix-op-op").unwrap(),
        None
    );
    assert_eq!(
        resolve_operation_read(&db, &role_operator_without_credential, "matrix-op-op").unwrap(),
        None
    );
    let identity = identity_for_attempt(&db, "matrix-attempt-op")
        .unwrap()
        .unwrap();
    let task_grant = resolve_task_read(&db, &operator, &identity, TaskReadLevel::Evidence)
        .unwrap()
        .unwrap();
    assert_eq!(task_grant.level, TaskReadLevel::Evidence);
    assert_eq!(task_grant.basis, TaskReadBasis::LocalOperator);
    assert_eq!(
        resolve_task_read(&db, &observer, &identity, TaskReadLevel::Evidence).unwrap(),
        None
    );
}
#[test]
fn scope_public_exact_caller_receipt_does_not_grant_objects() {
    let db = empty_db();
    insert_task(&db, "matrix-task-caller", 1);
    insert_attempt_running(
        &db,
        "matrix-attempt-caller",
        "matrix-task-caller",
        1,
        "matrix-owner",
        None,
    );
    insert_operation(
        &db,
        "matrix-op-caller",
        "matrix-caller",
        "task.dispatch",
        Some("matrix-task-caller"),
        Some("matrix-attempt-caller"),
        &json!({}),
        &json!({"outcome":"applied"}),
        "settled",
    );
    let caller = test_principal("matrix-caller", Role::Participant);
    let unrelated = test_principal("matrix-unrelated", Role::Participant);
    let grant = resolve_operation_read(&db, &caller, "matrix-op-caller")
        .unwrap()
        .unwrap();
    assert_eq!(grant.level, OperationReadLevel::Receipt);
    assert_eq!(grant.basis, OperationReadBasis::ExactCaller);
    assert_eq!(
        resolve_operation_read(&db, &unrelated, "matrix-op-caller").unwrap(),
        None
    );
    let identity = identity_for_attempt(&db, "matrix-attempt-caller")
        .unwrap()
        .unwrap();
    assert_eq!(
        resolve_task_read(&db, &caller, &identity, TaskReadLevel::Evidence).unwrap(),
        None
    );
}
#[test]
fn scope_public_current_manager_vs_unrelated_stale_released_denied() {
    let db = empty_db();
    insert_task(&db, "matrix-task-mgr", 1);
    insert_attempt_running(
        &db,
        "matrix-attempt-current",
        "matrix-task-mgr",
        1,
        "matrix-manager",
        None,
    );
    insert_attempt_running(
        &db,
        "matrix-attempt-old",
        "matrix-task-mgr",
        1,
        "matrix-old-owner",
        Some(2),
    );
    register_manager(&db, "matrix-manager");
    register_manager(&db, "matrix-unrelated");
    register_manager(&db, "matrix-former-gm");
    let manager = test_principal("matrix-manager", Role::Manager);
    let unrelated = test_principal("matrix-unrelated", Role::Manager);
    let former = test_principal("matrix-former-gm", Role::Manager);
    let current = identity_for_attempt(&db, "matrix-attempt-current")
        .unwrap()
        .unwrap();
    let grant = resolve_task_read(&db, &manager, &current, TaskReadLevel::Detail)
        .unwrap()
        .unwrap();
    assert_eq!(grant.basis, TaskReadBasis::CurrentTaskManager);
    assert_eq!(
        resolve_task_read(&db, &unrelated, &current, TaskReadLevel::Detail).unwrap(),
        None
    );
    assert_eq!(
        resolve_task_read(&db, &former, &current, TaskReadLevel::Detail).unwrap(),
        None
    );
    let old = identity_for_attempt(&db, "matrix-attempt-old")
        .unwrap()
        .unwrap();
    let old_owner = test_principal("matrix-old-owner", Role::Manager);
    register_manager(&db, "matrix-old-owner");
    assert_eq!(
        resolve_task_read(&db, &old_owner, &old, TaskReadLevel::Evidence).unwrap(),
        None
    );
    let stale = TaskGraphIdentity {
        project_id: PROJECT.to_owned(),
        task_id: "matrix-task-mgr".to_owned(),
        task_revision: Some(999),
        attempt_id: Some("matrix-attempt-current".to_owned()),
        binding_id: None,
        binding_generation: None,
    };
    let err = resolve_task_read(&db, &manager, &stale, TaskReadLevel::Evidence).unwrap_err();
    assert_eq!(err.code, "OBJECT_SCOPE_DAMAGED");
    insert_operation(
        &db,
        "matrix-op-mgr",
        "matrix-other-caller",
        "task.submit",
        Some("matrix-task-mgr"),
        Some("matrix-attempt-current"),
        &json!({}),
        &json!({"outcome":"applied"}),
        "settled",
    );
    let op_grant = resolve_operation_read(&db, &manager, "matrix-op-mgr")
        .unwrap()
        .unwrap();
    assert_eq!(op_grant.level, OperationReadLevel::Receipt);
    assert_eq!(op_grant.basis, OperationReadBasis::CurrentTaskManager);
    assert_eq!(
        resolve_operation_read(&db, &unrelated, "matrix-op-mgr").unwrap(),
        None
    );
}
#[test]
fn scope_public_operation_list_filters_without_skipping_later_visible() {
    let db = empty_db();
    for (operation_id, caller) in [
        ("matrix-list-a1", "matrix-caller-a"),
        ("matrix-list-b1", "matrix-caller-b"),
        ("matrix-list-a2", "matrix-caller-a"),
    ] {
        insert_operation(
            &db,
            operation_id,
            caller,
            "task.dispatch",
            None,
            None,
            &json!({}),
            &json!({"ok":false}),
            "rejected",
        );
    }
    insert_operation_position(&db, "matrix-list-a1", 1);
    insert_operation_position(&db, "matrix-list-b1", 2);
    insert_operation_position(&db, "matrix-list-a2", 3);
    let caller_a = test_principal("matrix-caller-a", Role::Participant);
    assert!(resolve_operation_read(&db, &caller_a, "matrix-list-a1")
        .unwrap()
        .is_some());
    assert_eq!(
        resolve_operation_read(&db, &caller_a, "matrix-list-b1").unwrap(),
        None
    );
    assert!(resolve_operation_read(&db, &caller_a, "matrix-list-a2")
        .unwrap()
        .is_some());
    let page = super::super::object_reads::list(
        &db,
        &caller_a,
        super::super::object_reads::ObjectKind::Operation,
        &json!({"limit":10,"state":"rejected"}),
    )
    .unwrap();
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["client_request_id"], "request-matrix-list-a1");
    assert_eq!(items[1]["client_request_id"], "request-matrix-list-a2");
}
#[test]
fn scope_public_queue_visibility_does_not_grant_exact_objects() {
    let db = empty_db();
    insert_task(&db, "matrix-task-queue", 1);
    insert_attempt_running(
        &db,
        "matrix-attempt-queue",
        "matrix-task-queue",
        1,
        "matrix-queue-owner",
        None,
    );
    register_manager(&db, "matrix-queue-owner");
    register_manager(&db, "matrix-queue-viewer");
    insert_operation(
        &db,
        "matrix-op-queue",
        "matrix-queue-owner",
        "task.submit",
        Some("matrix-task-queue"),
        Some("matrix-attempt-queue"),
        &json!({}),
        &json!({"outcome":"applied"}),
        "settled",
    );
    let viewer = test_principal("matrix-queue-viewer", Role::Manager);
    let owner = test_principal("matrix-queue-owner", Role::Manager);
    let identity = identity_for_attempt(&db, "matrix-attempt-queue")
        .unwrap()
        .unwrap();
    assert!(resolve_task_read(&db, &owner, &identity, TaskReadLevel::Detail)
        .unwrap()
        .is_some());
    assert_eq!(
        resolve_task_read(&db, &viewer, &identity, TaskReadLevel::Detail).unwrap(),
        None
    );
    assert_eq!(
        resolve_operation_read(&db, &viewer, "matrix-op-queue").unwrap(),
        None
    );
}
#[test]
fn scope_public_task_submission_artifact_inherits_narrowest_grant() {
    let db = empty_db();
    pin_local_operator(&db, "matrix-art-operator");
    insert_task(&db, "matrix-task-art", 1);
    let candidate_bytes = b"matrix candidate bytes";
    let candidate_digest = model::digest(candidate_bytes);
    let candidate_len = candidate_bytes.len() as u64;
    let candidate_id = "matrix-candidate-1";
    let candidate_kind = "source_snapshot";
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,?3,?4,?5,1,'{}')",
        params![
            candidate_id,
            "artifacts/matrix-candidate-1.bin",
            candidate_kind,
            candidate_len as i64,
            candidate_digest,
        ],
    )
    .unwrap();
    insert_attempt_running(
        &db,
        "matrix-attempt-art",
        "matrix-task-art",
        1,
        "matrix-art-manager",
        None,
    );
    register_manager(&db, "matrix-art-manager");
    register_manager(&db, "matrix-art-unrelated");
    let submit_operation_id = "matrix-submit-op-1";
    let submit_artifact_id = format!("submission-{}", model::digest(submit_operation_id.as_bytes()));
    let document = json!({
        "operation_id": submit_operation_id,
        "task_id": "matrix-task-art",
        "attempt_id": "matrix-attempt-art",
        "task_revision": 1,
        "candidate_ref": candidate_id,
        "candidate_sha256": candidate_digest,
        "candidate_byte_length": candidate_len,
        "candidate_kind": candidate_kind,
    });
    let raw = model::canonical(&document).unwrap();
    let record_digest = model::digest(raw.as_bytes());
    insert_operation(
        &db,
        submit_operation_id,
        "matrix-art-submitter",
        "task.submit",
        Some("matrix-task-art"),
        Some("matrix-attempt-art"),
        &json!({"submission_document": document}),
        &json!({
            "outcome": "applied",
            "submission_ref": submit_artifact_id,
            "attempt_id": "matrix-attempt-art",
            "candidate_ref": candidate_id,
        }),
        "settled",
    );
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'task_submission',?3,?4,1,?5)",
        params![
            submit_artifact_id,
            format!("artifacts/{submit_artifact_id}.bin"),
            raw.len() as i64,
            record_digest,
            raw,
        ],
    )
    .unwrap();
    let record: ArtifactRecord =
        super::super::results::get(&db, &submit_artifact_id).unwrap();
    let submitter = test_principal("matrix-art-submitter", Role::Participant);
    let grant = resolve_artifact_read(&db, &submitter, &record, ArtifactReadLevel::Bytes)
        .unwrap()
        .unwrap();
    assert_eq!(grant.basis, ArtifactReadBasis::ExactTaskEvidenceCaller);
    assert_eq!(grant.level, ArtifactReadLevel::Bytes);
    let capped = resolve_artifact_read(&db, &submitter, &record, ArtifactReadLevel::Assemble)
        .unwrap()
        .unwrap();
    assert_eq!(capped.level, ArtifactReadLevel::Bytes);
    let manager = test_principal("matrix-art-manager", Role::Manager);
    let manager_grant =
        resolve_artifact_read(&db, &manager, &record, ArtifactReadLevel::Bytes)
            .unwrap()
            .unwrap();
    assert_eq!(manager_grant.basis, ArtifactReadBasis::CurrentTaskManager);
    let unrelated = test_principal("matrix-art-unrelated", Role::Manager);
    assert_eq!(
        resolve_artifact_read(&db, &unrelated, &record, ArtifactReadLevel::Bytes).unwrap(),
        None
    );
    let observer = test_principal("matrix-art-observer", Role::Observer);
    assert_eq!(
        resolve_artifact_read(&db, &observer, &record, ArtifactReadLevel::Bytes).unwrap(),
        None
    );
    let operator = test_principal("matrix-art-operator", Role::Operator);
    let operator_grant =
        resolve_artifact_read(&db, &operator, &record, ArtifactReadLevel::Bytes)
            .unwrap()
            .unwrap();
    assert_eq!(operator_grant.basis, ArtifactReadBasis::LocalOperator);
}
#[test]
fn scope_public_malformed_links_and_unknown_kinds_fail_closed() {
    let db = empty_db();
    insert_task(&db, "matrix-task-bad", 1);
    insert_attempt_running(&db, "matrix-attempt-bad", "matrix-task-bad", 1, "matrix-owner", None);
    register_manager(&db, "matrix-unrelated");
    let unrelated = test_principal("matrix-unrelated", Role::Manager);
    insert_operation(
        &db,
        "matrix-op-binding-without-attempt",
        "matrix-owner",
        "task.dispatch",
        Some("matrix-task-bad"),
        None,
        &json!({}),
        &json!({"outcome":"applied"}),
        "settled",
    );
    db.execute(
        "UPDATE operations SET binding_id='matrix-binding',binding_generation=1 WHERE operation_id='matrix-op-binding-without-attempt'",
    )
    .unwrap();
    let err = resolve_operation_read(&db, &unrelated, "matrix-op-binding-without-attempt")
        .unwrap_err();
    assert_eq!(err.code, "OBJECT_SCOPE_DAMAGED");
    insert_operation(
        &db,
        "matrix-op-attempt-without-task",
        "matrix-owner",
        "task.dispatch",
        None,
        Some("matrix-attempt-bad"),
        &json!({}),
        &json!({"outcome":"applied"}),
        "settled",
    );
    let err = resolve_operation_read(&db, &unrelated, "matrix-op-attempt-without-task")
        .unwrap_err();
    assert_eq!(err.code, "OBJECT_SCOPE_DAMAGED");
    let unknown = ArtifactRecord {
        kind: "unknown-kind".to_owned(),
        artifact_id: "matrix-unknown".to_owned(),
        relative_path: "artifacts/matrix-unknown.bin".to_owned(),
        byte_length: 3,
        content_digest: model::digest(b"abc"),
        metadata: json!({}),
    };
    let err = resolve_artifact_read(&db, &unrelated, &unknown, ArtifactReadLevel::Metadata)
        .unwrap_err();
    assert_eq!(err.code, "OBJECT_SCOPE_DAMAGED");
    assert_eq!(resolve_assembly_sources(&db, &unrelated, &[]).unwrap(), None);
}
