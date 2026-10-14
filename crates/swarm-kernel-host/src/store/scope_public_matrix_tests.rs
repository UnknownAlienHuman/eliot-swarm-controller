//! PR49/51/52 scope regressions at two distinct boundaries.
//!
//! The original synchronous tests below use private SQL fixtures and resolver
//! calls. They establish unit coverage only, not public Store/MCP qualification.
//! In particular, their queue test does not call swarm.queue.get, and their
//! submission test is not a native source-page/assembly chain.
//!
//! connected_store uses bootstrap/client.register/authenticate and Store::call
//! for every authoritative admission. A fixture Module consumes Store commands
//! and supplies typed receipts/pages through module.outcome/module.result.
//! Its synthetic executor evidence qualifies neither a vendor nor MCP transport.
//! SQL in that module only inventories retained effects or damages existing links.
//!
//! Missing public rows remain explicit in the scratch handoff; this file does not
//! qualify the full matrix. Contract decisions and the completed 4b2ff276 review
//! lineage cases are separately assigned.
use super::super::{SCHEMA, SCRIPT_SCHEMA, WORKSPACE_SCHEMA, set_meta};
use super::{
    ArtifactReadBasis, ArtifactReadLevel, AssemblyProof, NativeResultSource, OperationReadBasis,
    OperationReadLevel, TaskGraphIdentity, TaskReadBasis, TaskReadLevel, identity_for_attempt,
    resolve_artifact_read, resolve_assembly_sources, resolve_operation_read, resolve_task_read,
};
use crate::{
    artifacts::ArtifactRecord,
    model::{self, Principal, Role},
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::sync::Arc;
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
    scope_and_state: (Option<&str>, Option<&str>, &str),
    effective: &Value,
    result: &Value,
) {
    let (task_id, attempt_id, state) = scope_and_state;
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
    insert_attempt_running(
        &db,
        "matrix-attempt-op",
        "matrix-task-op",
        1,
        "matrix-owner",
        None,
    );
    insert_operation(
        &db,
        "matrix-op-op",
        "matrix-owner",
        "task.submit",
        (Some("matrix-task-op"), Some("matrix-attempt-op"), "settled"),
        &json!({}),
        &json!({"outcome":"applied"}),
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
        (
            Some("matrix-task-caller"),
            Some("matrix-attempt-caller"),
            "settled",
        ),
        &json!({}),
        &json!({"outcome":"applied"}),
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
        (
            Some("matrix-task-mgr"),
            Some("matrix-attempt-current"),
            "settled",
        ),
        &json!({}),
        &json!({"outcome":"applied"}),
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
            (None, None, "rejected"),
            &json!({}),
            &json!({"ok":false}),
        );
    }
    insert_operation_position(&db, "matrix-list-a1", 1);
    insert_operation_position(&db, "matrix-list-b1", 2);
    insert_operation_position(&db, "matrix-list-a2", 3);
    let caller_a = test_principal("matrix-caller-a", Role::Participant);
    assert!(
        resolve_operation_read(&db, &caller_a, "matrix-list-a1")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        resolve_operation_read(&db, &caller_a, "matrix-list-b1").unwrap(),
        None
    );
    assert!(
        resolve_operation_read(&db, &caller_a, "matrix-list-a2")
            .unwrap()
            .is_some()
    );
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
        (
            Some("matrix-task-queue"),
            Some("matrix-attempt-queue"),
            "settled",
        ),
        &json!({}),
        &json!({"outcome":"applied"}),
    );
    let viewer = test_principal("matrix-queue-viewer", Role::Manager);
    let owner = test_principal("matrix-queue-owner", Role::Manager);
    let identity = identity_for_attempt(&db, "matrix-attempt-queue")
        .unwrap()
        .unwrap();
    assert!(
        resolve_task_read(&db, &owner, &identity, TaskReadLevel::Detail)
            .unwrap()
            .is_some()
    );
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
    let submit_artifact_id = format!(
        "submission-{}",
        model::digest(submit_operation_id.as_bytes())
    );
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
        (
            Some("matrix-task-art"),
            Some("matrix-attempt-art"),
            "settled",
        ),
        &json!({"submission_document": document}),
        &json!({
            "outcome": "applied",
            "submission_ref": submit_artifact_id,
            "attempt_id": "matrix-attempt-art",
            "candidate_ref": candidate_id,
        }),
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
    let record: ArtifactRecord = super::super::results::get(&db, &submit_artifact_id).unwrap();
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
    let manager_grant = resolve_artifact_read(&db, &manager, &record, ArtifactReadLevel::Bytes)
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
    let operator_grant = resolve_artifact_read(&db, &operator, &record, ArtifactReadLevel::Bytes)
        .unwrap()
        .unwrap();
    assert_eq!(operator_grant.basis, ArtifactReadBasis::LocalOperator);
}
#[test]
fn scope_public_malformed_links_and_unknown_kinds_fail_closed() {
    let db = empty_db();
    insert_task(&db, "matrix-task-bad", 1);
    insert_attempt_running(
        &db,
        "matrix-attempt-bad",
        "matrix-task-bad",
        1,
        "matrix-owner",
        None,
    );
    register_manager(&db, "matrix-unrelated");
    let unrelated = test_principal("matrix-unrelated", Role::Manager);
    insert_operation(
        &db,
        "matrix-op-binding-without-attempt",
        "matrix-owner",
        "task.dispatch",
        (Some("matrix-task-bad"), None, "settled"),
        &json!({}),
        &json!({"outcome":"applied"}),
    );
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,created_at_ms) VALUES('matrix-binding',1,'matrix-bad-lane','matrix-bad-module','matrix-bad-artifact','ready','{}',1)",
        [],
    )
    .unwrap();
    db.execute(
        "UPDATE operations SET binding_id='matrix-binding',binding_generation=1 WHERE operation_id='matrix-op-binding-without-attempt'",
        [],
    )
    .unwrap();
    let err =
        resolve_operation_read(&db, &unrelated, "matrix-op-binding-without-attempt").unwrap_err();
    assert_eq!(err.code, "OBJECT_SCOPE_DAMAGED");
    insert_operation(
        &db,
        "matrix-op-attempt-without-task",
        "matrix-owner",
        "task.dispatch",
        (None, Some("matrix-attempt-bad"), "settled"),
        &json!({}),
        &json!({"outcome":"applied"}),
    );
    let err =
        resolve_operation_read(&db, &unrelated, "matrix-op-attempt-without-task").unwrap_err();
    assert_eq!(err.code, "OBJECT_SCOPE_DAMAGED");
    let unknown = ArtifactRecord {
        kind: "unknown-kind".to_owned(),
        artifact_id: "matrix-unknown".to_owned(),
        relative_path: "artifacts/matrix-unknown.bin".to_owned(),
        byte_length: 3,
        content_digest: model::digest(b"abc"),
        metadata: json!({}),
    };
    let err =
        resolve_artifact_read(&db, &unrelated, &unknown, ArtifactReadLevel::Metadata).unwrap_err();
    assert_eq!(err.code, "OBJECT_SCOPE_DAMAGED");
    assert_eq!(
        resolve_assembly_sources(&db, &unrelated, &[]).unwrap(),
        None
    );
}

#[test]
fn assembled_record_cache_is_scoped_to_connection_and_reopen() {
    fn seed_cache_db(db: &Connection, artifact_id: &str) -> ArtifactRecord {
        db.execute_batch(
            "CREATE TABLE artifacts(artifact_id TEXT PRIMARY KEY,kind TEXT NOT NULL);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO artifacts(artifact_id,kind) VALUES(?1,'native_result')",
            [artifact_id],
        )
        .unwrap();
        ArtifactRecord {
            kind: "native_result".to_owned(),
            artifact_id: artifact_id.to_owned(),
            relative_path: "artifacts/cache-scope-probe.bin".to_owned(),
            byte_length: 0,
            content_digest: "cache-scope-digest".to_owned(),
            metadata: json!({"assembly_operation_id":"cache-scope-operation","part_count":1}),
        }
    }

    fn seed_cache(
        db: &Connection,
        record: ArtifactRecord,
    ) -> super::super::results::AssemblyReadCacheSnapshot {
        let snapshot = super::super::results::assembly_read_cache_snapshot(db, &record.artifact_id)
            .unwrap()
            .unwrap();
        let proof = Arc::new(AssemblyProof {
            sources: Arc::new(vec![NativeResultSource {
                producer_operation_id: "cache-scope-producer".to_owned(),
                target_operation_id: "cache-scope-target".to_owned(),
                task_identity: None,
                producer_operation_rowid: 0,
                target_operation_rowid: 0,
            }]),
            distinct_tasks: Arc::new(vec![None]),
            cache_dependencies: Arc::new(vec![super::super::results::AssemblyCacheRow::Artifact(
                snapshot.artifact_rowid,
            )]),
        });
        super::cache_assembly_proof(&Arc::new(record), proof, &snapshot).unwrap();
        snapshot
    }

    let artifact_id = "assembly-cache-namespace-probe";
    let first_db = Connection::open_in_memory().unwrap();
    let first = seed_cache_db(&first_db, artifact_id);
    let first_snapshot = seed_cache(&first_db, first);
    assert!(super::cached_assembly_record(&first_snapshot).is_some());

    let second_db = Connection::open_in_memory().unwrap();
    let second = seed_cache_db(&second_db, artifact_id);
    let second_snapshot =
        super::super::results::assembly_read_cache_snapshot(&second_db, &second.artifact_id)
            .unwrap()
            .unwrap();
    assert!(!first_snapshot.same_database_state(&second_snapshot));
    assert!(super::cached_assembly_record(&second_snapshot).is_none());

    let path = std::env::temp_dir().join(format!(
        "swarm-assembly-cache-reopen-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let file_db = Connection::open(&path).unwrap();
    let file_record = seed_cache_db(&file_db, artifact_id);
    let file_snapshot = seed_cache(&file_db, file_record.clone());
    assert!(super::cached_assembly_record(&file_snapshot).is_some());
    let external_db = Connection::open(&path).unwrap();
    external_db
        .execute(
            "INSERT INTO artifacts(artifact_id,kind) VALUES('external-unrelated-row','source_snapshot')",
            [],
        )
        .unwrap();
    drop(external_db);
    let after_external_write =
        super::super::results::assembly_read_cache_snapshot(&file_db, artifact_id)
            .unwrap()
            .unwrap();
    assert!(!file_snapshot.same_database_state(&after_external_write));
    assert!(super::cached_assembly_record(&after_external_write).is_none());
    seed_cache(&file_db, file_record);
    drop(file_db);

    let reopened_db = Connection::open(&path).unwrap();
    let reopened_snapshot =
        super::super::results::assembly_read_cache_snapshot(&reopened_db, artifact_id)
            .unwrap()
            .unwrap();
    assert!(!file_snapshot.same_database_state(&reopened_snapshot));
    assert!(super::cached_assembly_record(&reopened_snapshot).is_none());
    drop(reopened_db);
    std::fs::remove_file(path).unwrap();
}

/// Authenticated Store boundary; the synchronous resolver fixtures above are
/// intentionally kept separate from these publicly admitted facts.
mod connected_store {
    use crate::store::object_scope::{
        assembly_source_load_counts, reset_assembly_source_load_counts,
    };
    use crate::{
        artifacts::ResultPage,
        config::{Config, McpToolProfile, Route},
        model::{self, Credential, Principal, Role},
        platform::{DataRoot, bootstrap_credential},
        store::{Store, StoreOwner},
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use rusqlite::params;
    use serde_json::{Value, json};
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};
    use swarm_contracts::{
        module_catalog::ModuleDescriptor,
        module_contract::{MODULE_PROTOCOL_V1, ModuleContractClaim},
        runtime::{
            ModuleReceiptIdentity, NormalizedResultOriginContext, NormalizedResultPageSource,
            RuntimeCommand, TaskDispatchAdmissionReceipt, TaskDispatchContext,
        },
    };

    const PROJECT: &str = "connected-scope-project";
    const MODULE_ID: &str = "fixture.scope.public";
    const ARTIFACT_ID: &str = "scope-public-module.1";
    const ROUTE: &str = "scope-public";

    struct Fixture {
        owner: StoreOwner,
        directory: PathBuf,
        operator: Principal,
        manager_a: Principal,
        manager_b: Principal,
        observer: Principal,
    }

    async fn register(
        store: &Store,
        operator: &Principal,
        client_id: &str,
        role: Role,
        binding: Option<(&str, i64)>,
    ) -> Principal {
        let token = format!("scope-token-{}", model::new_id());
        let mut request = json!({
            "client_request_id":model::new_id(),
            "client_id":client_id,
            "role":role,
            "token_hash":model::digest(token.as_bytes()),
        });
        if let Some((id, generation)) = binding {
            request["binding_id"] = json!(id);
            request["binding_generation"] = json!(generation);
        }
        store
            .call(operator.clone(), "client.register".into(), request)
            .await
            .unwrap();
        let principal = store
            .authenticate(Credential {
                client_id: client_id.into(),
                token,
            })
            .await
            .unwrap();
        assert_eq!(principal.role, role);
        principal
    }

    // Same DataRoot/bootstrap/StoreOwner lifecycle as program_tests::start_store.
    // No worker/supervisor is launched: module replies below are fixture evidence.
    async fn start() -> Fixture {
        let directory =
            std::env::temp_dir().join(format!("swarm-scope-public-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let credential = bootstrap_credential(&root.path).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = root.path.clone();
        config.routes.push(Route {
            alias: ROUTE.into(),
            runtime: "module".into(),
            module_artifact_id: ARTIFACT_ID.into(),
            enabled: true,
            native_options: json!({}),
            workspace_option: None,
            owned_service: None,
            admission_policy: None,
        });
        let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
            .await
            .unwrap();
        let operator = owner.store.authenticate(credential).await.unwrap();
        assert_eq!(operator.role, Role::Operator);
        let manager_a = register(
            &owner.store,
            &operator,
            "scope-manager-a",
            Role::Manager,
            None,
        )
        .await;
        let manager_b = register(
            &owner.store,
            &operator,
            "scope-manager-b",
            Role::Manager,
            None,
        )
        .await;
        let observer = register(
            &owner.store,
            &operator,
            "scope-observer",
            Role::Observer,
            None,
        )
        .await;
        Fixture {
            owner,
            directory,
            operator,
            manager_a,
            manager_b,
            observer,
        }
    }

    impl Fixture {
        async fn close(self) {
            self.owner.close().await.unwrap();
            std::fs::remove_dir_all(self.directory).unwrap();
        }
    }

    async fn call(store: &Store, principal: &Principal, method: &str, request: Value) -> Value {
        store
            .call(principal.clone(), method.into(), request)
            .await
            .unwrap_or_else(|error| panic!("{method}: {error:?}"))
    }

    async fn write(
        store: &Store,
        principal: &Principal,
        method: &str,
        mut request: Value,
    ) -> Value {
        request["client_request_id"] = json!(model::new_id());
        call(store, principal, method, request).await
    }

    async fn denied(
        store: &Store,
        principal: &Principal,
        method: &str,
        request: Value,
        code: &str,
    ) {
        let error = store
            .call(principal.clone(), method.into(), request)
            .await
            .unwrap_err();
        assert_eq!(error.code, code, "{method}: {error:?}");
    }

    fn spec(label: &str) -> Value {
        // Reuse the current Task example; acceptance is absent, so this never
        // invents independent review/CheckRun evidence or acceptance.
        let mut spec: Value =
            serde_json::from_str(include_str!("../../config/task.example.json")).unwrap();
        spec["objective"] = json!(format!("Public scope fixture {label}"));
        spec
    }

    async fn create(store: &Store, manager: &Principal, label: &str) -> Value {
        write(store, manager, "task.create", json!({
            "project_id":PROJECT, "origin_key":format!("scope-origin-{label}"), "spec":spec(label),
        })).await
    }

    async fn claim(
        store: &Store,
        manager: &Principal,
        task: &Value,
        binding: Option<&Value>,
    ) -> Value {
        let mut request = json!({
            "task_id":task["task_id"], "expected_revision":1,
            "start_owner":if binding.is_some() { "controller" } else { "native_manager" },
        });
        if let Some(binding) = binding {
            request["binding_id"] = binding["binding_id"].clone();
            request["binding_generation"] = binding["generation"].clone();
        }
        write(store, manager, "task.claim", request).await
    }

    fn assert_closed_receipt(receipt: &Value) {
        for field in [
            "diagnostic",
            "native_refs",
            "original_request",
            "effective_request",
            "caller_id",
        ] {
            assert!(
                receipt.get(field).is_none(),
                "{field} escaped Receipt: {receipt}"
            );
        }
    }

    fn assert_closed_artifact(value: &Value) {
        assert!(value.get("relative_path").is_none());
        let metadata = value["metadata"].as_object().unwrap();
        for field in [
            "source",
            "selector",
            "native_root_id",
            "native_scope_key",
            "normalized_result_origin",
        ] {
            assert!(
                !metadata.contains_key(field),
                "{field} escaped artifact projection: {value}"
            );
        }
    }

    async fn assert_profile_is_not_object_authority(f: &Fixture) {
        for method in [
            "task.get",
            "attempt.get",
            "operation.get",
            "operation.list",
            "artifact.read",
        ] {
            // Both callers can use this same frontend profile. The profile is
            // method inventory only; actual Store calls below decide object grants.
            assert!(swarm_contracts::mcp_catalog::exposes_method(
                McpToolProfile::Observer,
                method
            ));
            for principal in [&f.operator, &f.observer] {
                let authorization =
                    call(&f.owner.store, principal, "mcp.authorization", json!({})).await;
                assert!(
                    authorization["allowed_methods"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|entry| entry == method),
                    "{method}: {authorization}"
                );
            }
        }
    }

    #[tokio::test]
    async fn store_authenticated_profiles_fleet_queue_and_exact_object_grants() {
        let f = start().await;
        let store = &f.owner.store;
        assert_profile_is_not_object_authority(&f).await;

        // Interleaving is intentional: the later authorized item must survive
        // an invisible Task/Operation in between, with a caller page limit of 2.
        let a = create(store, &f.manager_a, "a").await;
        let attempt_a = claim(store, &f.manager_a, &a, None).await;
        let b = create(store, &f.manager_b, "b").await;
        let attempt_b = claim(store, &f.manager_b, &b, None).await;
        let later_a = create(store, &f.manager_a, "later-a").await;
        let later_attempt_a = claim(store, &f.manager_a, &later_a, None).await;
        let reused_a = create(store, &f.manager_a, "a").await;
        assert_eq!(reused_a["created"], false);
        assert_eq!(reused_a["task_id"], a["task_id"]);
        let reused_claim = claim(store, &f.manager_a, &a, None).await;
        assert_eq!(reused_claim["attempt_id"], attempt_a["attempt_id"]);

        for (method, request) in [
            ("task.get", json!({"task_id":a["task_id"]})),
            ("attempt.get", json!({"attempt_id":attempt_a["attempt_id"]})),
            ("operation.get", json!({"operation_id":a["operation_id"]})),
        ] {
            call(store, &f.manager_a, method, request.clone()).await;
            call(store, &f.operator, method, request.clone()).await;
            denied(store, &f.manager_b, method, request.clone(), "NOT_FOUND").await;
            denied(store, &f.observer, method, request, "NOT_FOUND").await;
        }
        let receipt = call(
            store,
            &f.manager_a,
            "operation.get",
            json!({"operation_id":reused_claim["operation_id"]}),
        )
        .await;
        assert_eq!(receipt["task_id"], a["task_id"]);
        assert_eq!(receipt["attempt_id"], attempt_a["attempt_id"]);
        assert_closed_receipt(&receipt);
        let diagnostic = call(
            store,
            &f.operator,
            "operation.get",
            json!({"operation_id":a["operation_id"]}),
        )
        .await;
        assert_eq!(diagnostic["diagnostic"]["caller_id"], f.manager_a.client_id);
        denied(
            store,
            &f.manager_b,
            "swarm.agent.inspect",
            json!({"attempt_id":attempt_a["attempt_id"]}),
            "FORBIDDEN",
        )
        .await;

        let first = call(store, &f.manager_a, "operation.list", json!({"limit":2})).await;
        let second = call(
            store,
            &f.manager_a,
            "operation.list",
            json!({"limit":50,"after":first["next_after"]}),
        )
        .await;
        assert!(
            first.get("filtered_count").is_none(),
            "hidden rows are not counted in the public page: {first}"
        );
        assert!(
            second.get("filtered_count").is_none(),
            "hidden rows are not counted in the public page: {second}"
        );
        let ids: Vec<&str> = first["items"]
            .as_array()
            .unwrap()
            .iter()
            .chain(second["items"].as_array().unwrap())
            .map(|entry| {
                assert_closed_receipt(entry);
                entry["operation_id"].as_str().unwrap()
            })
            .collect();
        assert_eq!(
            ids,
            vec![
                a["operation_id"].as_str().unwrap(),
                attempt_a["operation_id"].as_str().unwrap(),
                later_a["operation_id"].as_str().unwrap(),
                later_attempt_a["operation_id"].as_str().unwrap(),
                reused_a["operation_id"].as_str().unwrap(),
                reused_claim["operation_id"].as_str().unwrap(),
            ]
        );
        assert!(!ids.contains(&b["operation_id"].as_str().unwrap()));
        assert!(!ids.contains(&attempt_b["operation_id"].as_str().unwrap()));
        assert_eq!(second["has_newer"], false);
        let observer_list = call(store, &f.observer, "operation.list", json!({"limit":50})).await;
        assert_eq!(observer_list["items"], json!([]));
        let task_page = call(store, &f.manager_a, "task.list", json!({"limit":1})).await;
        let next_task_page = call(
            store,
            &f.manager_a,
            "task.list",
            json!({"limit":1,"after":task_page["next_after"]}),
        )
        .await;
        assert_eq!(task_page["items"][0]["task_id"], a["task_id"]);
        assert_eq!(next_task_page["items"][0]["task_id"], later_a["task_id"]);
        let delta = call(store, &f.manager_a, "report.delta", json!({"limit":50})).await;
        assert!(
            delta["projection"]["range"].get("filtered_count").is_none(),
            "the linked delta cursor must not disclose how many foreign events it skipped: {delta}"
        );
        let delta_json = model::canonical(&delta).unwrap();
        assert!(!delta_json.contains(b["task_id"].as_str().unwrap()));
        assert!(!delta_json.contains(b["operation_id"].as_str().unwrap()));
        assert!(delta_json.contains(later_a["operation_id"].as_str().unwrap()));

        // A genuinely rejected public admission retains only its exact caller's
        // receipt and must not grant that caller the foreign Task/Attempt.
        denied(
            store,
            &f.manager_b,
            "task.claim",
            json!({
                "client_request_id":"scope-b-foreign-claim",
                "task_id":a["task_id"],"expected_revision":1,"start_owner":"native_manager",
            }),
            "TASK_ALREADY_OWNED",
        )
        .await;
        let rejected = call(
            store,
            &f.manager_b,
            "operation.list",
            json!({"state":"rejected","limit":10}),
        )
        .await;
        let rejection = rejected["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["client_request_id"] == "scope-b-foreign-claim")
            .unwrap();
        assert_closed_receipt(rejection);
        assert_eq!(rejection["state"], "rejected");
        assert!(rejection["task_id"].is_null());
        denied(
            store,
            &f.observer,
            "operation.get",
            json!({"operation_id":rejection["operation_id"]}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "task.get",
            json!({"task_id":a["task_id"]}),
            "NOT_FOUND",
        )
        .await;

        let dashboard = call(store, &f.observer, "swarm.dashboard", json!({})).await;
        assert_eq!(dashboard["coverage"], "aggregate_only");
        assert!(dashboard.get("queue").is_none());

        let queue = call(
            store,
            &f.manager_b,
            "swarm.queue.get",
            json!({"limit":10,"project_id":PROJECT}),
        )
        .await;
        let queue_items = queue["items"].as_array().unwrap();
        let queue_has_foreign_task = queue_items
            .iter()
            .any(|item| item["task_id"] == a["task_id"]);
        let queue_has_own_task = queue_items
            .iter()
            .any(|item| item["task_id"] == b["task_id"]);
        assert!(queue_has_own_task, "{queue}");
        // PR51 row 1 requires this fleet visibility without exact object grants.
        // At bfdca09, fleet_read_scope/query_tasks owner-filter this queue; keep
        // this real boundary assertion visible for the parent's scoped gate.
        assert!(
            queue_has_foreign_task,
            "PR51 fleet queue contract unmet: Manager B cannot see Manager A's bounded queue item: {queue}"
        );

        write(
            store,
            &f.operator,
            "gm.handover",
            json!({
                "client_id":f.manager_b.client_id,
            }),
        )
        .await;
        let gm_task = call(
            store,
            &f.manager_b,
            "task.get",
            json!({"task_id":a["task_id"]}),
        )
        .await;
        assert_eq!(gm_task["task_id"], a["task_id"]);
        let gm_attempt = call(
            store,
            &f.manager_b,
            "attempt.get",
            json!({"attempt_id":attempt_a["attempt_id"]}),
        )
        .await;
        assert_eq!(gm_attempt["attempt_id"], attempt_a["attempt_id"]);
        write(
            store,
            &f.operator,
            "gm.handover",
            json!({
                "client_id":f.manager_a.client_id,
            }),
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "task.get",
            json!({"task_id":a["task_id"]}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "attempt.get",
            json!({"attempt_id":attempt_a["attempt_id"]}),
            "NOT_FOUND",
        )
        .await;
        f.close().await;
    }

    #[tokio::test]
    async fn store_authenticated_released_and_stale_attempts_do_not_widen_task_reads() {
        let f = start().await;
        let store = &f.owner.store;
        let task = create(store, &f.manager_a, "released-scope").await;
        let first = claim(store, &f.manager_a, &task, None).await;
        let first_attempt_id = first["attempt_id"].as_str().unwrap().to_owned();
        call(
            store,
            &f.manager_a,
            "attempt.get",
            json!({"attempt_id":first_attempt_id}),
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "attempt.get",
            json!({"attempt_id":first_attempt_id}),
            "NOT_FOUND",
        )
        .await;

        write(
            store,
            &f.manager_a,
            "attempt.release",
            json!({
                "attempt_id":first_attempt_id,
                "outcome":"superseded",
                "reason":"Replace the retained attempt in the scope fixture.",
                "assignment_closed":true,
            }),
        )
        .await;
        denied(
            store,
            &f.manager_a,
            "attempt.get",
            json!({"attempt_id":first_attempt_id}),
            "NOT_FOUND",
        )
        .await;

        let replacement = claim(store, &f.manager_a, &task, None).await;
        let replacement_id = replacement["attempt_id"].as_str().unwrap().to_owned();
        assert_ne!(replacement_id, first_attempt_id);
        call(
            store,
            &f.manager_a,
            "attempt.get",
            json!({"attempt_id":replacement_id}),
        )
        .await;
        for attempt_id in [&first_attempt_id, &replacement_id] {
            denied(
                store,
                &f.manager_b,
                "attempt.get",
                json!({"attempt_id":attempt_id}),
                "NOT_FOUND",
            )
            .await;
        }

        write(
            store,
            &f.manager_a,
            "attempt.release",
            json!({
                "attempt_id":replacement_id,
                "outcome":"superseded",
                "reason":"Close the current attempt before revision change.",
                "assignment_closed":true,
            }),
        )
        .await;
        let revised = write(
            store,
            &f.operator,
            "task.revise",
            json!({
                "task_id":task["task_id"],
                "expected_revision":1,
                "spec":spec("released-scope-revision-two"),
            }),
        )
        .await;
        assert_eq!(revised["revision"], 2);
        for attempt_id in [&first_attempt_id, &replacement_id] {
            let error = store
                .call(
                    f.manager_a.clone(),
                    "attempt.get".into(),
                    json!({"attempt_id":attempt_id}),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error.code.as_str(), "NOT_FOUND" | "OBJECT_SCOPE_DAMAGED"),
                "stale Attempt retained a public grant: {error:?}"
            );
        }
        f.close().await;
    }

    fn fixture_descriptor() -> ModuleDescriptor {
        // Reuse the existing result/dispatch descriptor vocabulary, with a
        // distinct fixture identity so no vendor-specific decoder is selected.
        let mut value: Value = serde_json::from_str(include_str!(
            "../../../swarm-adapter-opencode/registration/descriptor.template.json"
        ))
        .unwrap();
        value["module_id"] = json!(MODULE_ID);
        value["artifact"] = json!({"artifact_id":ARTIFACT_ID,"version":"1","build_id":null});
        value["enabled"] = json!(true);
        value["lifecycle"] = json!("external_attach");
        value["config_schema"] = Value::Null;
        value["workspace_option"] = Value::Null;
        value["launch"]["executable"] = json!(std::env::current_exe().unwrap());
        value["launch"]["argv"] = json!([]);
        value["capabilities"] = json!(["agent.open", "agent.result", "task.dispatch"]);
        // Current production requires the exact TaskPrompt schema alongside the
        // typed dispatch context and admission schemas for dispatch-capable
        // bindings (TASK_PROMPT_CONTRACT_REQUIRED); the fixture declares the
        // maintained producer schema, no bypass.
        value["command_schemas"] = json!([
            swarm_contracts::module_contract::runtime_command_schema(),
            swarm_contracts::module_contract::task_dispatch_context_schema(),
            swarm_contracts::module_contract::task_prompt_schema(),
            swarm_contracts::module_contract::normalized_result_context_schema(),
        ]);
        value["event_schemas"] = json!([
            swarm_contracts::module_contract::runtime_outcome_schema(),
            swarm_contracts::module_contract::task_dispatch_admission_schema(),
            swarm_contracts::module_contract::normalized_result_page_schema(),
        ]);
        let descriptor: ModuleDescriptor = serde_json::from_value(value).unwrap();
        descriptor.validate().unwrap();
        descriptor
    }

    fn module_receipt(
        command: &RuntimeCommand,
        descriptor: &ModuleDescriptor,
    ) -> ModuleReceiptIdentity {
        ModuleReceiptIdentity {
            schema_version: 1,
            module_id: descriptor.module_id.clone(),
            artifact: descriptor.artifact.clone(),
            protocol: MODULE_PROTOCOL_V1,
            binding_id: command.binding_id.clone(),
            binding_generation: command.generation,
            operation_id: command.operation_id.clone(),
            input_sha256: command.input_sha256.clone().unwrap(),
        }
    }

    async fn next_command(store: &Store, module: &Principal, admission: &Value) -> RuntimeCommand {
        // One admitted command per call; no native worker or polling campaign.
        let next = call(store, module, "module.next", json!({})).await;
        let command: RuntimeCommand = serde_json::from_value(next["command"].clone()).unwrap();
        assert_eq!(
            command.operation_id,
            admission["operation_id"].as_str().unwrap()
        );
        command
    }

    async fn open_fixture_module(f: &Fixture, descriptor: &ModuleDescriptor) -> (Value, Principal) {
        let store = &f.owner.store;
        let supervisor = store
            .authenticate(f.owner.module_supervisor_credential())
            .await
            .unwrap();
        call(
            store,
            &supervisor,
            "module.descriptor.register",
            json!({"descriptor":descriptor}),
        )
        .await;
        let catalog = call(store, &f.operator, "module.catalog.get", json!({})).await;
        write(
            store,
            &f.operator,
            "module.route.select",
            json!({
                "route_alias":ROUTE, "module_id":MODULE_ID, "artifact_id":ARTIFACT_ID,
                "version":"1", "expected_catalog_revision":catalog["catalog_revision"],
            }),
        )
        .await;
        let open = write(
            store,
            &f.operator,
            "agent.open",
            json!({"lane_id":"scope-page-lane","route":ROUTE}),
        )
        .await;
        let module = register(
            store,
            &f.operator,
            "scope-page-module",
            Role::Module,
            Some((open["binding_id"].as_str().unwrap(), 1)),
        )
        .await;
        let claim = ModuleContractClaim::from_descriptor(descriptor, MODULE_PROTOCOL_V1).unwrap();
        call(
            store,
            &module,
            "module.hello",
            json!({
                "boot_id":"scope-page-boot", "module_artifact_id":ARTIFACT_ID,
                "module_contract":claim,
            }),
        )
        .await;
        let command = next_command(store, &module, &open).await;
        call(
            store,
            &module,
            "module.outcome",
            json!({
                "operation_id":command.operation_id, "outcome":"applied",
                "native_root_id":"scope-fixture-root", "native_scope_key":"scope-fixture-namespace",
                "details":{"module_receipt":module_receipt(&command, descriptor)},
            }),
        )
        .await;
        (open, module)
    }

    async fn dispatch_fixture(
        f: &Fixture,
        descriptor: &ModuleDescriptor,
        binding: &Value,
        module: &Principal,
        task_label: &str,
    ) -> Value {
        let store = &f.owner.store;
        let task = create(store, &f.manager_a, task_label).await;
        let attempt = claim(store, &f.manager_a, &task, Some(binding)).await;
        let dispatch = write(
            store,
            &f.manager_a,
            "task.dispatch",
            json!({
                "attempt_id":attempt["attempt_id"], "text":DISPATCH_TEXT,
            }),
        )
        .await;
        let command = next_command(store, module, &dispatch).await;
        let context: TaskDispatchContext =
            serde_json::from_value(command.input["task_dispatch_context"].clone()).unwrap();
        let receipt = module_receipt(&command, descriptor);
        // Donor pattern from module_bridge_recovery_tests: derive the TaskPrompt
        // envelope with production's own builder over the retained Attempt row
        // instead of hand-computing digests. The dispatch handler admitted the
        // identical envelope (plain dispatch, no launch packet) in the same
        // transaction, so the receipt below echoes retained truth.
        let attempt_id = attempt["attempt_id"].as_str().unwrap().to_owned();
        let attempt_row = store
            .run(move |db| crate::store::tasks::get_attempt(db, &attempt_id))
            .await
            .unwrap();
        let envelope = crate::store::task_prompt::build(&attempt_row, DISPATCH_TEXT, None).unwrap();
        let admission = TaskDispatchAdmissionReceipt {
            schema_version: context.schema_version,
            module_receipt: receipt.clone(),
            operation_id: context.operation_id,
            binding_id: context.binding_id,
            binding_generation: context.binding_generation,
            worker_boot_id: context.worker_boot_id,
            attempt_id: context.attempt_id,
            task_id: context.task_id,
            task_revision: context.task_revision,
            task_snapshot_sha256: context.task_snapshot_sha256,
            source_text_sha256: context.source_text_sha256.clone(),
            source_text_bytes: context.source_text_bytes,
            native_payload_sha256: envelope.prompt_sha256,
            native_payload_bytes: envelope.prompt_bytes,
            native_input_id: Some("scope-fixture-input".into()),
        };
        admission.validate().unwrap();
        call(
            store,
            module,
            "module.outcome",
            json!({
                "operation_id":command.operation_id,"outcome":"applied",
                "native_root_id":"scope-fixture-root","native_scope_key":"scope-fixture-namespace",
                "native_input_id":"scope-fixture-input",
                "details":{"module_receipt":receipt,"dispatch_admission":admission,
                    "completion_condition":"native_input_admitted","execution_complete":false,
                    "task_completion":"unknown"},
            }),
        )
        .await;
        let reused = write(
            store,
            &f.manager_a,
            "task.dispatch",
            json!({
                "attempt_id":attempt["attempt_id"],"text":DISPATCH_TEXT,
            }),
        )
        .await;
        assert_eq!(reused["semantic_reuse"], true);
        assert_eq!(reused["start_operation_id"], dispatch["operation_id"]);
        let reused_receipt = call(
            store,
            &f.manager_a,
            "operation.get",
            json!({"operation_id":reused["operation_id"]}),
        )
        .await;
        assert_eq!(reused_receipt["task_id"], task["task_id"]);
        assert_eq!(reused_receipt["attempt_id"], attempt["attempt_id"]);
        assert!(reused_receipt["binding_id"].is_null());
        assert!(reused_receipt["binding_generation"].is_null());
        assert_closed_receipt(&reused_receipt);
        let exact_attempt = call(
            store,
            &f.manager_a,
            "attempt.get",
            json!({"attempt_id":attempt["attempt_id"]}),
        )
        .await;
        assert_eq!(
            exact_attempt["start_operation_id"],
            dispatch["operation_id"]
        );
        dispatch
    }

    async fn publish_page(
        f: &Fixture,
        descriptor: &ModuleDescriptor,
        binding: &Value,
        module: &Principal,
        target: &Value,
        payload: &[u8],
        range: std::ops::Range<usize>,
    ) -> String {
        let offset = range.start;
        let end = range.end;
        let store = &f.owner.store;
        let result = write(store, &f.manager_a, "agent.result", json!({
            "binding_id":binding["binding_id"],"generation":binding["generation"],
            "selector":{"kind":"scope_fixture_result","input_operation_id":target["operation_id"]},
            "offset_bytes":offset,"length_bytes":end-offset,
        })).await;
        let command = next_command(store, module, &result).await;
        let origin: NormalizedResultOriginContext =
            serde_json::from_value(command.input["normalized_result_origin"].clone()).unwrap();
        let source = NormalizedResultPageSource {
            schema_id: swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID.into(),
            schema_version: 1,
            origin,
            result_operation_id: command.operation_id.clone(),
            result_input_sha256: command.input_sha256.clone().unwrap(),
            result_module_receipt: module_receipt(&command, descriptor),
            payload_sha256: model::digest(payload),
            payload_bytes: payload.len() as u64,
            native_response_identity: Some("scope-fixture-response".into()),
            execution_complete: false,
            task_completion: "unknown".into(),
            native_replay: false,
        };
        source.validate().unwrap();
        let page = ResultPage {
            source: serde_json::to_value(source).unwrap(),
            offset_bytes: offset as u64,
            byte_length: (end - offset) as u64,
            total_bytes: payload.len() as u64,
            eof: end == payload.len(),
            media_type: "text/plain".into(),
            content_base64: STANDARD.encode(&payload[offset..end]),
            page_sha256: model::digest(&payload[offset..end]),
        };
        page.decode().unwrap();
        let recorded = call(
            store,
            module,
            "module.result",
            json!({
                "operation_id":command.operation_id,"page":page,
            }),
        )
        .await;
        assert_eq!(recorded["recorded"], true);
        recorded["artifact_ref"].as_str().unwrap().to_owned()
    }

    type RetainedInventory = (Vec<(String, String, String)>, Vec<(String, String)>);

    async fn retained_inventory(store: &Store) -> RetainedInventory {
        // Observation only: no SQL seeds or mutation of authoritative objects.
        store
            .run(|db| {
                let operations = db
                    .prepare(
                        "SELECT operation_id,method,state FROM operations ORDER BY operation_id",
                    )?
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let artifacts = db
                    .prepare(
                        "SELECT artifact_id,relative_path FROM artifacts ORDER BY artifact_id",
                    )?
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((operations, artifacts))
            })
            .await
            .unwrap()
    }

    fn artifact_files(directory: &std::path::Path) -> BTreeSet<PathBuf> {
        std::fs::read_dir(directory.join("artifacts"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }

    async fn denied_assembly_has_no_output(
        f: &Fixture,
        principal: &Principal,
        refs: &[String],
        expected_error: &str,
    ) {
        let store = &f.owner.store;
        let before = retained_inventory(store).await;
        let files_before = artifact_files(&f.directory);
        let request_id = format!("denied-assemble-{}", model::new_id());
        denied(
            store,
            principal,
            "artifact.assemble",
            json!({
                "client_request_id":request_id,"page_refs":refs,
            }),
            expected_error,
        )
        .await;
        let after = retained_inventory(store).await;
        assert_eq!(
            after.1, before.1,
            "denied assembly registered an output artifact"
        );
        assert_eq!(
            artifact_files(&f.directory),
            files_before,
            "denied assembly published a file"
        );
        let added: Vec<_> = after
            .0
            .iter()
            .filter(|row| !before.0.contains(row))
            .collect();
        if expected_error == "INVALID_PARAMS" {
            // Duplicate page_refs fail envelope validation before reservation.
            assert!(added.is_empty(), "invalid envelope reserved an Operation");
        } else {
            // Generic mutation policy retains one rejected caller receipt. It
            // never creates a queued/sending/settled assembly effect.
            assert_eq!(added.len(), 1);
            assert_eq!(added[0].1, "artifact.assemble");
            assert_eq!(added[0].2, "rejected");
            let receipt = call(
                store,
                principal,
                "operation.list",
                json!({"state":"rejected","limit":50}),
            )
            .await;
            assert!(
                receipt["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["client_request_id"] == request_id
                        && entry["state"] == "rejected")
            );
        }
    }

    const PAGES_PAYLOAD: &[u8] = b"private result bytes from the exact admitted source";
    const PAGES_SPLIT: usize = 16;
    const DISPATCH_TEXT: &str = "scope fixture dispatch; no vendor work";

    struct PagesSetup {
        fixture: Fixture,
        first: String,
        second: String,
        participant: Principal,
        foreign_page: String,
    }

    // Shared authenticated setup for the result-page subscenarios below. The
    // previous monolithic scenario future overflowed the default Windows
    // test-thread stack in debug builds (0xc00000fd, still red after a
    // heap-pin attempt: the oversized future is constructed on the stack).
    // Each subscenario below runs this setup in its own Store so every
    // scenario future stays small. Assertions and their order are preserved
    // verbatim from the original scenario; no production code is touched.
    // Parent baseline: the fleet/queue profile case passes; these scenarios
    // are NOT_READY until the parent's targeted retry goes green.
    async fn setup_result_pages() -> PagesSetup {
        let f = start().await;
        assert_profile_is_not_object_authority(&f).await;
        let descriptor = fixture_descriptor();
        let (binding, module) = open_fixture_module(&f, &descriptor).await;
        let store = &f.owner.store;
        let target = dispatch_fixture(&f, &descriptor, &binding, &module, "pages").await;
        let exact_family_request = json!({
            "binding_id":binding["binding_id"],
            "generation":binding["generation"],
            "attempt_id":target["attempt_id"],
            "after":0,
            "limit":5,
        });
        let family = call(
            store,
            &f.manager_a,
            "agent.family",
            exact_family_request.clone(),
        )
        .await;
        assert!(family.is_object());
        denied(
            store,
            &f.manager_b,
            "agent.family",
            exact_family_request.clone(),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.observer,
            "agent.family",
            exact_family_request,
            "NOT_FOUND",
        )
        .await;

        let attempt = call(
            store,
            &f.manager_a,
            "attempt.get",
            json!({"attempt_id":target["attempt_id"]}),
        )
        .await;
        let participant_token = "scope-attempt-owner-token".to_owned();
        write(
            store,
            &f.manager_a,
            "coordination.participant.register",
            json!({
                "client_id":"scope-attempt-owner",
                "token_hash":model::digest(participant_token.as_bytes()),
                "task_id":attempt["task_id"],
                "task_revision":attempt["task_revision"],
                "attempt_id":target["attempt_id"],
                // The Attempt was claimed under this binding; a binding-less
                // credential fails generation match and reads nothing.
                "binding_id":binding["binding_id"],
                "binding_generation":binding["generation"],
                "participation_basis":{"kind":"attempt_owner"},
            }),
        )
        .await;
        let participant = store
            .authenticate(crate::model::Credential {
                client_id: "scope-attempt-owner".into(),
                token: participant_token,
            })
            .await
            .unwrap();
        let first = publish_page(
            &f,
            &descriptor,
            &binding,
            &module,
            &target,
            PAGES_PAYLOAD,
            0..PAGES_SPLIT,
        )
        .await;
        let second = publish_page(
            &f,
            &descriptor,
            &binding,
            &module,
            &target,
            PAGES_PAYLOAD,
            PAGES_SPLIT..PAGES_PAYLOAD.len(),
        )
        .await;
        // Both pages below are valid and readable to Manager A, but their
        // retained dispatch/Attempt identities cannot be spliced together.
        let foreign_target =
            dispatch_fixture(&f, &descriptor, &binding, &module, "foreign-source").await;
        let foreign_page = publish_page(
            &f,
            &descriptor,
            &binding,
            &module,
            &foreign_target,
            PAGES_PAYLOAD,
            0..PAGES_PAYLOAD.len(),
        )
        .await;
        PagesSetup {
            fixture: f,
            first,
            second,
            participant,
            foreign_page,
        }
    }

    async fn assemble_pages(f: &Fixture, first: &str, second: &str) -> String {
        let store = &f.owner.store;
        // Operator assembles; Manager A can read through the sources, while the
        // unrelated Manager/Observer still cannot. Assembler is not ownership.
        let assembly = write(
            store,
            &f.operator,
            "artifact.assemble",
            json!({
                "page_refs":[first,second],"expected_sha256":model::digest(PAGES_PAYLOAD),
            }),
        )
        .await;
        format!(
            "assembled-{}",
            model::digest(assembly["operation_id"].as_str().unwrap().as_bytes())
        )
    }

    struct ReadCostPagesSetup {
        fixture: Fixture,
        payload: Vec<u8>,
        page_refs: Vec<String>,
    }

    async fn setup_read_cost_pages(page_count: usize) -> ReadCostPagesSetup {
        assert!(page_count >= 2);
        let fixture = start().await;
        let descriptor = fixture_descriptor();
        let (binding, module) = open_fixture_module(&fixture, &descriptor).await;
        let target =
            dispatch_fixture(&fixture, &descriptor, &binding, &module, "range-read-cost").await;
        let page_bytes = crate::artifacts::MAX_PAGE_BYTES;
        let payload = vec![b'r'; page_count * page_bytes];
        let mut page_refs = Vec::with_capacity(page_count);
        for index in 0..page_count {
            let start = index * page_bytes;
            page_refs.push(
                publish_page(
                    &fixture,
                    &descriptor,
                    &binding,
                    &module,
                    &target,
                    &payload,
                    start..start + page_bytes,
                )
                .await,
            );
        }
        ReadCostPagesSetup {
            fixture,
            payload,
            page_refs,
        }
    }

    async fn measure_repeated_assembled_ranges(page_count: usize) -> (u64, u64, u64, u64) {
        let setup = setup_read_cost_pages(page_count).await;
        let store = &setup.fixture.owner.store;
        let assembly = write(
            store,
            &setup.fixture.operator,
            "artifact.assemble",
            json!({
                "page_refs":setup.page_refs.clone(),
                "expected_sha256":model::digest(&setup.payload),
            }),
        )
        .await;
        let assembled_id = format!(
            "assembled-{}",
            model::digest(assembly["operation_id"].as_str().unwrap().as_bytes())
        );
        reset_assembly_source_load_counts(&assembled_id);
        let first = call(
            store,
            &setup.fixture.manager_a,
            "artifact.read",
            json!({
                "artifact_id":assembled_id,
                "offset_bytes":0,
                "length_bytes":crate::artifacts::MAX_PAGE_BYTES,
            }),
        )
        .await;
        assert_eq!(first["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
        let warm = assembly_source_load_counts(&assembled_id);
        assert_eq!(warm.source_artifact_point_loads, page_count as u64);
        assert_eq!(
            warm.producer_target_operation_point_loads,
            (page_count * 2) as u64
        );
        assert_eq!(warm.source_resolutions, page_count as u64);
        assert_eq!(warm.immutable_proof_cache_hits, 0);
        assert_eq!(warm.current_scope_grant_checks, 1);
        assert_eq!(warm.assembled_manifest_decodes, 1);
        assert!(warm.assembled_manifest_bytes_copied > 0);
        assert_eq!(warm.cache_validation_query_loads, 3);
        assert_eq!(warm.provenance_point_query_loads, page_count as u64 * 3 + 2);
        assert_eq!(warm.part_manifest_decodes, 1);
        assert_eq!(warm.part_manifest_string_bytes_copied, 0);

        reset_assembly_source_load_counts(&assembled_id);
        for _ in 0..64 {
            let read = call(
                store,
                &setup.fixture.manager_a,
                "artifact.read",
                json!({
                    "artifact_id":assembled_id,
                    "offset_bytes":0,
                    "length_bytes":crate::artifacts::MAX_PAGE_BYTES,
                }),
            )
            .await;
            assert_eq!(read["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
            assert_eq!(read["eof"], false);
        }
        let repeated = assembly_source_load_counts(&assembled_id);
        assert_eq!(repeated.source_artifact_point_loads, 0);
        assert_eq!(repeated.producer_target_operation_point_loads, 0);
        assert_eq!(repeated.source_resolutions, 0);
        assert_eq!(repeated.immutable_proof_cache_hits, 64);
        assert_eq!(repeated.current_scope_grant_checks, 64);
        assert_eq!(repeated.assembled_manifest_decodes, 0);
        assert_eq!(repeated.assembled_manifest_bytes_copied, 0);
        assert_eq!(repeated.part_manifest_decodes, 0);
        assert_eq!(repeated.part_manifest_string_bytes_copied, 0);
        assert_eq!(repeated.cache_validation_query_loads, 128);
        assert_eq!(repeated.provenance_point_query_loads, 0);
        setup.fixture.close().await;
        (
            warm.source_artifact_point_loads,
            warm.producer_target_operation_point_loads,
            repeated.source_artifact_point_loads + repeated.producer_target_operation_point_loads,
            repeated.current_scope_grant_checks,
        )
    }

    #[tokio::test]
    async fn store_authenticated_result_pages_grants_and_denied_assemblies() {
        let setup = setup_result_pages().await;
        let f = &setup.fixture;
        let store = &f.owner.store;
        let first = setup.first.clone();
        let second = setup.second.clone();
        let participant = setup.participant.clone();
        let foreign_page = setup.foreign_page.clone();
        for page in [&first, &second] {
            for principal in [&f.operator, &f.manager_a] {
                let metadata = call(
                    store,
                    principal,
                    "artifact.get",
                    json!({"artifact_id":page}),
                )
                .await;
                assert_closed_artifact(&metadata);
                let bytes = call(
                    store,
                    principal,
                    "artifact.read",
                    json!({"artifact_id":page,"length_bytes":65536}),
                )
                .await;
                assert_closed_artifact(&bytes);
                assert!(bytes["content"].as_str().is_some());
            }
            let participant_bytes = call(
                store,
                &participant,
                "artifact.read",
                json!({"artifact_id":page,"length_bytes":65536}),
            )
            .await;
            assert_closed_artifact(&participant_bytes);
            assert!(participant_bytes["content"].as_str().is_some());
            for principal in [&f.manager_b, &f.observer] {
                denied(
                    store,
                    principal,
                    "artifact.get",
                    json!({"artifact_id":page}),
                    "FORBIDDEN",
                )
                .await;
                denied(
                    store,
                    principal,
                    "artifact.read",
                    json!({"artifact_id":page,"length_bytes":65536}),
                    "FORBIDDEN",
                )
                .await;
            }
        }
        denied_assembly_has_no_output(
            f,
            &f.manager_b,
            &[first.clone(), second.clone()],
            "FORBIDDEN",
        )
        .await;
        denied_assembly_has_no_output(
            f,
            &f.manager_a,
            &[first.clone(), first.clone()],
            "INVALID_PARAMS",
        )
        .await;
        denied_assembly_has_no_output(
            f,
            &f.manager_a,
            &[second.clone(), first.clone()],
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        denied_assembly_has_no_output(
            f,
            &f.manager_a,
            std::slice::from_ref(&first),
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;

        let foreign_bytes = call(
            store,
            &f.manager_a,
            "artifact.read",
            json!({"artifact_id":foreign_page,"length_bytes":65536}),
        )
        .await;
        assert_eq!(
            foreign_bytes["content"],
            std::str::from_utf8(PAGES_PAYLOAD).unwrap()
        );
        denied(
            store,
            &participant,
            "artifact.read",
            json!({"artifact_id":foreign_page,"length_bytes":65536}),
            "FORBIDDEN",
        )
        .await;
        denied_assembly_has_no_output(
            f,
            &f.manager_a,
            &[first.clone(), foreign_page],
            "ARTIFACT_SCOPE",
        )
        .await;
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_authenticated_operator_assembly_inherits_sources() {
        let setup = setup_result_pages().await;
        let f = &setup.fixture;
        let store = &f.owner.store;
        let first = setup.first.clone();
        let second = setup.second.clone();
        let assembled_id = assemble_pages(f, &first, &second).await;
        for principal in [&f.operator, &f.manager_a] {
            let metadata = call(
                store,
                principal,
                "artifact.get",
                json!({"artifact_id":assembled_id}),
            )
            .await;
            assert_eq!(metadata["kind"], "native_result");
            assert_eq!(
                metadata["grant"]["basis"],
                if principal.role == Role::Operator {
                    "local_operator"
                } else {
                    "all_source_pages"
                }
            );
            assert_closed_artifact(&metadata);
            let bytes = call(
                store,
                principal,
                "artifact.read",
                json!({
                    "artifact_id":assembled_id,"offset_bytes":0,"length_bytes":65536,
                }),
            )
            .await;
            assert_eq!(
                bytes["content"],
                std::str::from_utf8(PAGES_PAYLOAD).unwrap()
            );
            assert_closed_artifact(&bytes);
        }
        for principal in [&f.manager_b, &f.observer] {
            denied(
                store,
                principal,
                "artifact.parts",
                json!({"artifact_id":assembled_id}),
                "FORBIDDEN",
            )
            .await;
            denied(
                store,
                principal,
                "artifact.read",
                json!({"artifact_id":assembled_id,"length_bytes":65536}),
                "FORBIDDEN",
            )
            .await;
        }
        let parts = call(
            store,
            &f.manager_a,
            "artifact.parts",
            json!({"artifact_id":assembled_id,"limit":1}),
        )
        .await;
        assert_eq!(parts["parts"][0]["artifact_ref"], first);
        assert_eq!(parts["next_after"], 1);
        let remaining = call(
            store,
            &f.manager_a,
            "artifact.parts",
            json!({"artifact_id":assembled_id,"limit":1,"after":parts["next_after"]}),
        )
        .await;
        assert_eq!(remaining["parts"][0]["artifact_ref"], second);
        assert!(remaining["next_after"].is_null());
        let prefix = call(
            store,
            &f.manager_a,
            "artifact.read",
            json!({"artifact_id":assembled_id,"length_bytes":PAGES_SPLIT}),
        )
        .await;
        assert_eq!(prefix["eof"], false);
        assert_eq!(prefix["byte_length"], PAGES_SPLIT);
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_assembled_range_reads_reuse_bounded_source_proof() {
        let small = measure_repeated_assembled_ranges(2).await;
        let large = measure_repeated_assembled_ranges(32).await;
        assert_eq!(small, (2, 4, 0, 64));
        assert_eq!(large, (32, 64, 0, 64));
    }

    #[tokio::test]
    async fn store_assembled_grant_loop_is_distinct_bounded() {
        for page_count in [2usize, 32usize] {
            let setup = setup_read_cost_pages(page_count).await;
            let store = &setup.fixture.owner.store;
            let assembly = write(
                store,
                &setup.fixture.operator,
                "artifact.assemble",
                json!({
                    "page_refs": setup.page_refs.clone(),
                    "expected_sha256": model::digest(&setup.payload),
                }),
            )
            .await;
            let assembled_id = format!(
                "assembled-{}",
                model::digest(assembly["operation_id"].as_str().unwrap().as_bytes())
            );
            reset_assembly_source_load_counts(&assembled_id);
            let first = call(
                store,
                &setup.fixture.manager_a,
                "artifact.read",
                json!({
                    "artifact_id": assembled_id,
                    "offset_bytes": 0,
                    "length_bytes": crate::artifacts::MAX_PAGE_BYTES,
                }),
            )
            .await;
            assert_eq!(first["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
            let warm = assembly_source_load_counts(&assembled_id);
            assert_eq!(warm.grant_loop_iterations, 1);
            reset_assembly_source_load_counts(&assembled_id);
            for _ in 0..64 {
                let read = call(
                    store,
                    &setup.fixture.manager_a,
                    "artifact.read",
                    json!({
                        "artifact_id": assembled_id,
                        "offset_bytes": 0,
                        "length_bytes": crate::artifacts::MAX_PAGE_BYTES,
                    }),
                )
                .await;
                assert_eq!(read["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
            }
            let repeated = assembly_source_load_counts(&assembled_id);
            assert_eq!(repeated.grant_loop_iterations, 64);
            assert_eq!(repeated.source_artifact_point_loads, 0);
            assert_eq!(repeated.producer_target_operation_point_loads, 0);
            assert_eq!(repeated.assembled_manifest_decodes, 0);
            assert_eq!(repeated.assembled_manifest_bytes_copied, 0);
            assert_eq!(repeated.part_manifest_decodes, 0);
            assert_eq!(repeated.part_manifest_string_bytes_copied, 0);
            assert_eq!(repeated.cache_validation_query_loads, 128);
            assert_eq!(repeated.provenance_point_query_loads, 0);
            setup.fixture.close().await;
        }
    }

    #[tokio::test]
    async fn store_assembled_parts_limit_one_on_32() {
        let setup = setup_read_cost_pages(32).await;
        let store = &setup.fixture.owner.store;
        let assembly = write(
            store,
            &setup.fixture.operator,
            "artifact.assemble",
            json!({
                "page_refs": setup.page_refs.clone(),
                "expected_sha256": model::digest(&setup.payload),
            }),
        )
        .await;
        let assembled_id = format!(
            "assembled-{}",
            model::digest(assembly["operation_id"].as_str().unwrap().as_bytes())
        );
        let warm = call(
            store,
            &setup.fixture.manager_a,
            "artifact.parts",
            json!({"artifact_id": assembled_id, "after": 0, "limit": 1}),
        )
        .await;
        assert_eq!(warm["parts"].as_array().map_or(0, Vec::len), 1);
        reset_assembly_source_load_counts(&assembled_id);
        let window = call(
            store,
            &setup.fixture.manager_a,
            "artifact.parts",
            json!({"artifact_id": assembled_id, "after": 0, "limit": 1}),
        )
        .await;
        assert_eq!(window["part_count"], 32);
        assert_eq!(window["parts"].as_array().map_or(0, Vec::len), 1);
        assert_eq!(window["next_after"], json!(1));
        let counts = assembly_source_load_counts(&assembled_id);
        assert_eq!(counts.grant_loop_iterations, 1);
        assert_eq!(counts.immutable_proof_cache_hits, 1);
        assert_eq!(counts.source_artifact_point_loads, 0);
        assert_eq!(counts.producer_target_operation_point_loads, 0);
        assert_eq!(counts.assembled_manifest_decodes, 0);
        assert_eq!(counts.assembled_manifest_bytes_copied, 0);
        assert_eq!(counts.cache_validation_query_loads, 2);
        assert_eq!(counts.provenance_point_query_loads, 3);
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_assembled_unrelated_write_preserves_proof() {
        let setup = setup_read_cost_pages(32).await;
        let store = &setup.fixture.owner.store;
        let assembly = write(
            store,
            &setup.fixture.operator,
            "artifact.assemble",
            json!({
                "page_refs": setup.page_refs.clone(),
                "expected_sha256": model::digest(&setup.payload),
            }),
        )
        .await;
        let assembled_id = format!(
            "assembled-{}",
            model::digest(assembly["operation_id"].as_str().unwrap().as_bytes())
        );
        let first = call(
            store,
            &setup.fixture.manager_a,
            "artifact.read",
            json!({
                "artifact_id": assembled_id,
                "offset_bytes": 0,
                "length_bytes": crate::artifacts::MAX_PAGE_BYTES,
            }),
        )
        .await;
        assert_eq!(first["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
        reset_assembly_source_load_counts(&assembled_id);
        store
            .run(move |db| {
                db.execute(
                    "INSERT INTO observations(source_stream_id, kind, payload_json, recorded_at_ms) VALUES ('unrelated-proof', 'note', '{}', 1)",
                    [],
                )?;
                db.execute(
                    "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES ('unrelated-proof-artifact','artifacts/unrelated-proof.bin','source_snapshot',0,?1,1,'{}')",
                    [model::digest(b"unrelated proof artifact")],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let read = call(
            store,
            &setup.fixture.manager_a,
            "artifact.read",
            json!({
                "artifact_id": assembled_id,
                "offset_bytes": 0,
                "length_bytes": crate::artifacts::MAX_PAGE_BYTES,
            }),
        )
        .await;
        assert_eq!(read["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
        let counts = assembly_source_load_counts(&assembled_id);
        assert_eq!(counts.source_artifact_point_loads, 0);
        assert_eq!(counts.producer_target_operation_point_loads, 0);
        assert_eq!(counts.immutable_proof_cache_hits, 1);
        assert_eq!(counts.grant_loop_iterations, 1);
        assert_eq!(counts.assembled_manifest_decodes, 0);
        assert_eq!(counts.assembled_manifest_bytes_copied, 0);
        assert_eq!(counts.cache_validation_query_loads, 2);
        assert_eq!(counts.provenance_point_query_loads, 0);
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_assembled_warm_manifest_mutation_invalidates_record_and_proof() {
        let setup = setup_read_cost_pages(2).await;
        let store = &setup.fixture.owner.store;
        let assembly = write(
            store,
            &setup.fixture.operator,
            "artifact.assemble",
            json!({
                "page_refs": setup.page_refs.clone(),
                "expected_sha256": model::digest(&setup.payload),
            }),
        )
        .await;
        let assembled_id = format!(
            "assembled-{}",
            model::digest(assembly["operation_id"].as_str().unwrap().as_bytes())
        );
        let warm = call(
            store,
            &setup.fixture.manager_a,
            "artifact.read",
            json!({
                "artifact_id":assembled_id,
                "offset_bytes":0,
                "length_bytes":crate::artifacts::MAX_PAGE_BYTES,
            }),
        )
        .await;
        assert_eq!(warm["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
        reset_assembly_source_load_counts(&assembled_id);

        let damage_id = assembled_id.clone();
        store
            .run(move |db| {
                let changed = db.execute(
                    "UPDATE artifacts SET metadata_json=json_set(metadata_json,'$.parts[0].offset_bytes',1) WHERE artifact_id=?1",
                    [damage_id],
                )?;
                assert_eq!(changed, 1);
                Ok(())
            })
            .await
            .unwrap();
        denied(
            store,
            &setup.fixture.manager_a,
            "artifact.read",
            json!({
                "artifact_id":assembled_id,
                "offset_bytes":0,
                "length_bytes":crate::artifacts::MAX_PAGE_BYTES,
            }),
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        let counts = assembly_source_load_counts(&assembled_id);
        assert_eq!(counts.assembled_manifest_decodes, 1);
        assert!(counts.assembled_manifest_bytes_copied > 0);
        assert_eq!(counts.source_artifact_point_loads, 1);
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_assembled_warm_producer_mutation_invalidates_proof() {
        let setup = setup_read_cost_pages(2).await;
        let store = &setup.fixture.owner.store;
        let assembly = write(
            store,
            &setup.fixture.operator,
            "artifact.assemble",
            json!({
                "page_refs": setup.page_refs.clone(),
                "expected_sha256": model::digest(&setup.payload),
            }),
        )
        .await;
        let assembled_id = format!(
            "assembled-{}",
            model::digest(assembly["operation_id"].as_str().unwrap().as_bytes())
        );
        let warm = call(
            store,
            &setup.fixture.manager_a,
            "artifact.read",
            json!({
                "artifact_id":assembled_id,
                "offset_bytes":0,
                "length_bytes":crate::artifacts::MAX_PAGE_BYTES,
            }),
        )
        .await;
        assert_eq!(warm["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
        reset_assembly_source_load_counts(&assembled_id);

        let source_id = setup.page_refs[0].clone();
        let source_producer = store
            .run(move |db| {
                let producer: String = db.query_row(
                    "SELECT json_extract(metadata_json,'$.operation_id') FROM artifacts WHERE artifact_id=?1",
                    [source_id],
                    |row| row.get(0),
                )?;
                let changed = db.execute(
                    "UPDATE operations SET state='rejected' WHERE operation_id=?1",
                    [&producer],
                )?;
                assert_eq!(changed, 1);
                Ok(producer)
            })
            .await
            .unwrap();
        denied(
            store,
            &setup.fixture.manager_a,
            "artifact.read",
            json!({
                "artifact_id":assembled_id,
                "offset_bytes":0,
                "length_bytes":crate::artifacts::MAX_PAGE_BYTES,
            }),
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        assert!(!source_producer.is_empty());
        let counts = assembly_source_load_counts(&assembled_id);
        assert_eq!(counts.assembled_manifest_decodes, 1);
        assert!(counts.assembled_manifest_bytes_copied > 0);
        assert!(counts.producer_target_operation_point_loads > 0);
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_assembled_warm_revocation_and_tamper_negative() {
        let setup = setup_read_cost_pages(2).await;
        let store = &setup.fixture.owner.store;
        let assembly = write(
            store,
            &setup.fixture.operator,
            "artifact.assemble",
            json!({
                "page_refs": setup.page_refs.clone(),
                "expected_sha256": model::digest(&setup.payload),
            }),
        )
        .await;
        let assembled_id = format!(
            "assembled-{}",
            model::digest(assembly["operation_id"].as_str().unwrap().as_bytes())
        );
        write(
            store,
            &setup.fixture.operator,
            "gm.handover",
            json!({"client_id": setup.fixture.manager_b.client_id}),
        )
        .await;
        let warm = call(
            store,
            &setup.fixture.manager_b,
            "artifact.read",
            json!({
                "artifact_id": assembled_id,
                "offset_bytes": 0,
                "length_bytes": crate::artifacts::MAX_PAGE_BYTES,
            }),
        )
        .await;
        assert_eq!(warm["byte_length"], crate::artifacts::MAX_PAGE_BYTES);
        write(
            store,
            &setup.fixture.operator,
            "gm.handover",
            json!({"client_id": setup.fixture.manager_a.client_id}),
        )
        .await;
        denied(
            store,
            &setup.fixture.manager_b,
            "artifact.read",
            json!({
                "artifact_id": assembled_id,
                "offset_bytes": 0,
                "length_bytes": crate::artifacts::MAX_PAGE_BYTES,
            }),
            "FORBIDDEN",
        )
        .await;
        let tamper_id = assembled_id.clone();
        store
            .run(move |db| {
                let changed = db.execute(
                    "UPDATE artifacts SET byte_length = byte_length + 1 WHERE artifact_id = ?1",
                    params![tamper_id],
                )?;
                assert_eq!(changed, 1);
                Ok(())
            })
            .await
            .unwrap();
        denied(
            store,
            &setup.fixture.manager_a,
            "artifact.read",
            json!({
                "artifact_id": assembled_id,
                "offset_bytes": 0,
                "length_bytes": crate::artifacts::MAX_PAGE_BYTES,
            }),
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_authenticated_gm_assembly_and_former_gm_loss() {
        let setup = setup_result_pages().await;
        let f = &setup.fixture;
        let store = &f.owner.store;
        let first = setup.first.clone();
        let second = setup.second.clone();

        // An unrelated Manager can assemble only while an independent current-GM
        // source grant exists. Losing it leaves the caller receipt readable but
        // never turns the assembly request into ownership of source bytes.
        write(
            store,
            &f.operator,
            "gm.handover",
            json!({
                "client_id":f.manager_b.client_id,
            }),
        )
        .await;
        let gm_assembly = write(
            store,
            &f.manager_b,
            "artifact.assemble",
            json!({
                "page_refs":[first,second],"expected_sha256":model::digest(PAGES_PAYLOAD),
            }),
        )
        .await;
        let gm_assembled_id = format!(
            "assembled-{}",
            model::digest(gm_assembly["operation_id"].as_str().unwrap().as_bytes())
        );
        let gm_bytes = call(
            store,
            &f.manager_b,
            "artifact.read",
            json!({"artifact_id":gm_assembled_id,"length_bytes":65536}),
        )
        .await;
        assert_eq!(
            gm_bytes["content"],
            std::str::from_utf8(PAGES_PAYLOAD).unwrap()
        );
        write(
            store,
            &f.operator,
            "gm.handover",
            json!({
                "client_id":f.manager_a.client_id,
            }),
        )
        .await;
        let former_gm_receipt = call(
            store,
            &f.manager_b,
            "operation.get",
            json!({"operation_id":gm_assembly["operation_id"]}),
        )
        .await;
        assert_closed_receipt(&former_gm_receipt);
        denied(
            store,
            &f.manager_b,
            "artifact.read",
            json!({"artifact_id":gm_assembled_id,"length_bytes":65536}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "artifact.parts",
            json!({"artifact_id":gm_assembled_id}),
            "FORBIDDEN",
        )
        .await;
        let source_owner_bytes = call(
            store,
            &f.manager_a,
            "artifact.read",
            json!({"artifact_id":gm_assembled_id,"length_bytes":65536}),
        )
        .await;
        assert_eq!(
            source_owner_bytes["content"],
            std::str::from_utf8(PAGES_PAYLOAD).unwrap()
        );
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_authenticated_damaged_page_metadata_fail_closed() {
        let setup = setup_result_pages().await;
        let f = &setup.fixture;
        let store = &f.owner.store;
        let first = setup.first.clone();
        let second = setup.second.clone();

        let second_page_id = second.clone();
        let (original_length, original_digest, original_metadata): (i64, String, String) =
            store.run(move |db| {
                Ok(db.query_row(
                    "SELECT byte_length,content_digest,metadata_json FROM artifacts WHERE artifact_id=?1",
                    [second_page_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?)
            }).await.unwrap();

        // A publicly admitted overlapping page remains a damaged source and
        // cannot be combined into a new artifact.
        let overlap_page = second.clone();
        store.run(move |db| {
            let changed = db.execute(
                "UPDATE artifacts SET metadata_json=json_set(metadata_json,'$.offset_bytes',?2) WHERE artifact_id=?1",
                params![overlap_page, PAGES_SPLIT as i64 - 1],
            )?;
            assert_eq!(changed, 1);
            Ok(())
        }).await.unwrap();
        denied_assembly_has_no_output(
            f,
            &f.manager_a,
            &[first.clone(), second.clone()],
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        let restore_page = second.clone();
        let restore_metadata = original_metadata.clone();
        store
            .run(move |db| {
                db.execute(
                    "UPDATE artifacts SET metadata_json=?2 WHERE artifact_id=?1",
                    params![restore_page, restore_metadata],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let digest_page = second.clone();
        let wrong_digest = model::digest(b"scope-fixture-corrupt-page-digest");
        store
            .run(move |db| {
                db.execute(
                    "UPDATE artifacts SET content_digest=?2 WHERE artifact_id=?1",
                    params![digest_page, wrong_digest],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        denied(
            store,
            &f.manager_a,
            "artifact.read",
            json!({
                "artifact_id":second,"length_bytes":65536,
            }),
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        let restore_page = second.clone();
        let restore_digest = original_digest.clone();
        store
            .run(move |db| {
                db.execute(
                    "UPDATE artifacts SET content_digest=?2 WHERE artifact_id=?1",
                    params![restore_page, restore_digest],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let length_page = second.clone();
        store
            .run(move |db| {
                db.execute(
                    "UPDATE artifacts SET byte_length=?2 WHERE artifact_id=?1",
                    params![length_page, original_length + 1],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        denied(
            store,
            &f.manager_a,
            "artifact.read",
            json!({
                "artifact_id":second,"length_bytes":65536,
            }),
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        let restore_page = second.clone();
        store
            .run(move |db| {
                db.execute(
                    "UPDATE artifacts SET byte_length=?2 WHERE artifact_id=?1",
                    params![restore_page, original_length],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let generation_page = second.clone();
        store.run(move |db| {
            let changed = db.execute(
                "UPDATE artifacts SET metadata_json=json_set(metadata_json,'$.normalized_result_origin.binding_generation',2) WHERE artifact_id=?1",
                [generation_page],
            )?;
            assert_eq!(changed, 1);
            Ok(())
        }).await.unwrap();
        denied(
            store,
            &f.manager_a,
            "artifact.read",
            json!({
                "artifact_id":second,"length_bytes":65536,
            }),
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        let restore_page = second.clone();
        let restore_metadata = original_metadata;
        store
            .run(move |db| {
                db.execute(
                    "UPDATE artifacts SET metadata_json=?2 WHERE artifact_id=?1",
                    params![restore_page, restore_metadata],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        setup.fixture.close().await;
    }

    #[tokio::test]
    async fn store_authenticated_damaged_producer_breaks_parts_and_assembly() {
        let setup = setup_result_pages().await;
        let f = &setup.fixture;
        let store = &f.owner.store;
        let first = setup.first.clone();
        let second = setup.second.clone();
        let assembled_id = assemble_pages(f, &first, &second).await;
        let warm = call(
            store,
            &f.operator,
            "artifact.read",
            json!({
                "artifact_id":assembled_id,
                "offset_bytes":0,
                "length_bytes":crate::artifacts::MAX_PAGE_BYTES,
            }),
        )
        .await;
        assert_eq!(warm["byte_length"], PAGES_PAYLOAD.len());
        reset_assembly_source_load_counts(&assembled_id);

        // Replacing a public page changes its rowid. Even the verified Operator
        // must rebuild provenance and fail before disclosing inherited parts.
        let damage_page = second.clone();
        store.run(move |db| {
            let changed = db.execute(
                "INSERT OR REPLACE INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) SELECT artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,json_set(metadata_json,'$.operation_id','missing-producer') FROM artifacts WHERE artifact_id=?1",
                params![damage_page],
            )?;
            assert_eq!(changed, 1);
            Ok(())
        }).await.unwrap();
        denied(
            store,
            &f.operator,
            "artifact.parts",
            json!({"artifact_id":assembled_id}),
            "OBJECT_SCOPE_DAMAGED",
        )
        .await;
        let counts = assembly_source_load_counts(&assembled_id);
        assert_eq!(counts.assembled_manifest_decodes, 1);
        assert!(counts.assembled_manifest_bytes_copied > 0);
        assert!(counts.source_artifact_point_loads > 0);
        denied_assembly_has_no_output(f, &f.operator, &[first, second], "OBJECT_SCOPE_DAMAGED")
            .await;
        setup.fixture.close().await;
    }

    struct ReviewEvidenceSubject {
        submission_ref: String,
        candidate_ref: String,
    }

    // SQL-seeded review subject mirroring program_tests::seed_subject: an open
    // Task with a submitted Attempt, candidate bytes, submission document and
    // settled submit Operation. Every review row below is admitted through
    // public Store calls; no review assignment, result or disposition is seeded.
    async fn seed_review_subject(
        store: &Store,
        owner_id: &str,
        suffix: &str,
    ) -> ReviewEvidenceSubject {
        let owner = owner_id.to_owned();
        let suffix = suffix.to_owned();
        store
            .run(move |db| {
                let task_id = format!("task-review-{suffix}");
                let attempt_id = format!("attempt-review-{suffix}");
                let candidate_ref = format!(
                    "source-{}",
                    model::digest(format!("candidate-{suffix}").as_bytes())
                );
                let submission_ref = format!(
                    "submission-{}",
                    model::digest(format!("submission-{suffix}").as_bytes())
                );
                let submission_operation_id = format!("submit-op-{suffix}");
                let candidate_bytes = b"review evidence candidate fixture";
                let candidate_digest = model::digest(candidate_bytes);
                let spec_json = json!({
                    "objective":format!("Review evidence fixture {suffix}"),
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Preserve the exact review finding scope"}],
                    "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
                });
                let spec: crate::model::TaskSpec =
                    serde_json::from_value(spec_json.clone())?;
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
                        model::canonical(&json!({"attempt_id":attempt_id,"task_revision":1}))?,
                    ],
                )?;
                let submission_document = json!({
                    "schema_version":1,
                    "operation_id":submission_operation_id,
                    "task_id":task_id,
                    "attempt_id":attempt_id,
                    "task_revision":1,
                    "phase":"implementation",
                    "owner_id":owner,
                    "submitted_by":owner,
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
                db.execute(
                    "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'task_submission',?3,?4,1,?5)",
                    params![
                        submission_ref,
                        format!("artifacts/{submission_ref}.bin"),
                        i64::try_from(model::canonical(&submission_document)?.len()).unwrap(),
                        submission_digest,
                        model::canonical(&json!({"operation_id":submission_operation_id}))?,
                    ],
                )?;
                db.execute(
                    "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,submission_ref,candidate_ref,created_at_ms,updated_at_ms) VALUES(?1,?2,1,?3,?4,'controller','submitted','[]',?5,?6,1,1)",
                    params![
                        attempt_id,
                        task_id,
                        model::canonical(&snapshot)?,
                        owner,
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
                        owner,
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
                Ok(ReviewEvidenceSubject {
                    submission_ref,
                    candidate_ref,
                })
            })
            .await
            .unwrap()
    }

    // PR51 row 4 / PR52 evidence rows: a sponsored reviewer reads only the
    // exact assigned submission and review evidence through public calls and
    // gains nothing else of the Task graph. Live CheckRun admission needs a
    // configured check profile and stays an unmet row after this fixture.
    #[tokio::test]
    async fn store_authenticated_reviewer_submission_evidence_is_exact() {
        let f = start().await;
        let store = &f.owner.store;
        let owner_id = f.manager_a.client_id.clone();
        let subject = seed_review_subject(store, &owner_id, "evidence-a").await;
        let foreign = seed_review_subject(store, &owner_id, "evidence-b").await;

        let reviewer_id = "scope-sponsored-reviewer";
        let reviewer_token = "scope-sponsored-reviewer-token".to_owned();
        let pending_scope = json!({
            "review_assignment_id":null,
            "task_id":"task-review-evidence-a",
            "attempt_id":"attempt-review-evidence-a",
            "task_revision":1,
            "submission_ref":subject.submission_ref,
            "candidate_ref":subject.candidate_ref,
        });
        write(
            store,
            &f.manager_a,
            "coordination.participant.register",
            json!({
                "client_request_id":format!("register-{reviewer_id}"),
                "client_id":reviewer_id,
                "token_hash":model::digest(reviewer_token.as_bytes()),
                "task_id":"task-review-evidence-a",
                "task_revision":1,
                "attempt_id":"attempt-review-evidence-a",
                "participation_basis":{"kind":"sponsored_reviewer","review_scope":pending_scope},
            }),
        )
        .await;
        let assignment = write(
            store,
            &f.manager_a,
            "review.assign",
            json!({
                "client_request_id":format!("assign-{reviewer_id}"),
                "attempt_id":"attempt-review-evidence-a",
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "reviewer_client_id":reviewer_id,
            }),
        )
        .await;
        let assignment_id = assignment["review_assignment_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let reviewer = store
            .authenticate(crate::model::Credential {
                client_id: reviewer_id.into(),
                token: reviewer_token,
            })
            .await
            .unwrap();
        let result = write(
            store,
            &reviewer,
            "review.submit",
            json!({
                "client_request_id":format!("submit-{assignment_id}"),
                "review_assignment_id":assignment_id,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "verdict":"pass",
                "coverage":"complete",
                "findings":[],
                "evidence_refs":["evidence://review/r1"],
            }),
        )
        .await;
        assert_eq!(result["verdict"], "pass");

        let view = call(
            store,
            &reviewer,
            "review.get",
            json!({"review_assignment_id":assignment_id}),
        )
        .await;
        assert_eq!(view["current_slot"], true);
        assert_eq!(view["assignment"]["reviewer_client_id"], reviewer_id);
        assert_eq!(view["result"]["verdict"], "pass");
        let context = call(
            store,
            &reviewer,
            "swarm.review.context",
            json!({"review_assignment_id":assignment_id}),
        )
        .await;
        assert_eq!(
            context["context"]["submission"]["candidate_ref"],
            subject.candidate_ref
        );
        assert_eq!(
            context["context"]["candidate"]["artifact_id"],
            subject.candidate_ref
        );

        let evidence = call(
            store,
            &reviewer,
            "task.submission",
            json!({"submission_ref":subject.submission_ref,"after":0,"limit":10}),
        )
        .await;
        let evidence_json = model::canonical(&evidence).unwrap();
        assert!(
            evidence_json.contains(&subject.submission_ref),
            "exact submission missing: {evidence_json}"
        );
        assert!(
            !evidence_json.contains(&foreign.submission_ref),
            "foreign submission leaked: {evidence_json}"
        );

        denied(
            store,
            &reviewer,
            "task.submission",
            json!({"submission_ref":foreign.submission_ref,"after":0,"limit":10}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &f.observer,
            "review.get",
            json!({"review_assignment_id":assignment_id}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "review.get",
            json!({"review_assignment_id":assignment_id}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &reviewer,
            "review.get",
            json!({"review_assignment_id":"review-assignment-unknown"}),
            "REVIEW_ASSIGNMENT_NOT_FOUND",
        )
        .await;
        f.close().await;
    }

    // PR49 acceptance rows / PR51 row 4 CheckRun rows / PR52 evidence rows:
    // the sponsored reviewer reads the exact assigned acceptance decision and
    // CheckRun through public calls, while foreign, observer and unrelated
    // callers stay outside scope. Retained rows are seeded directly like
    // seed_review_subject does; no live admission is involved.
    #[tokio::test]
    async fn store_authenticated_acceptance_and_checkrun_reads_are_exact() {
        let f = start().await;
        let store = &f.owner.store;
        let owner_id = f.manager_a.client_id.clone();
        let subject = seed_review_subject(store, &owner_id, "evidence-a").await;
        let foreign = seed_review_subject(store, &owner_id, "evidence-b").await;
        let task_a = "task-review-evidence-a".to_owned();
        let attempt_a = "attempt-review-evidence-a".to_owned();
        let task_b = "task-review-evidence-b".to_owned();
        let attempt_b = "attempt-review-evidence-b".to_owned();
        let accept_a = "accept-op-evidence-a".to_owned();
        let accept_b = "accept-op-evidence-b".to_owned();
        let check_op_a = "check-op-evidence-a".to_owned();
        let check_op_b = "check-op-evidence-b".to_owned();
        let check_a = model::new_id();
        let check_b = model::new_id();
        let candidate_a = subject.candidate_ref.clone();
        let candidate_b = foreign.candidate_ref.clone();
        store
            .run({
                let accept_a = accept_a.clone();
                let accept_b = accept_b.clone();
                let task_a = task_a.clone();
                let attempt_a = attempt_a.clone();
                let check_a = check_a.clone();
                let check_b = check_b.clone();
                let check_op_a = check_op_a.clone();
                let candidate_a = candidate_a.clone();
                move |db| {
                for (accept_id, task_id, attempt_id) in
                    [(&accept_a, &task_a, &attempt_a), (&accept_b, &task_b, &attempt_b)]
                {
                    let result = json!({
                        "operation_id":accept_id,
                        "outcome":"applied",
                        "acceptance_operation_id":accept_id,
                        "task_id":task_id,
                        "attempt_id":attempt_id,
                        "task_revision":1,
                    });
                    db.execute(
                        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,'task.accept',?4,'{}',?5,?6,'settled',?7,1,1,1,1)",
                        params![
                            accept_id,
                            &owner_id,
                            format!("accept-request-{accept_id}"),
                            model::canonical(&json!({"client_request_id":format!("accept-request-{accept_id}")}))?,
                            task_id,
                            attempt_id,
                            model::canonical(&result)?,
                        ],
                    )?;
                }
                for (check_id, check_op, attempt_id, candidate) in
                    [
                        (&check_a, &check_op_a, &attempt_a, &candidate_a),
                        (&check_b, &check_op_b, &attempt_b, &candidate_b),
                    ]
                {
                    let task_id = if attempt_id == &attempt_a { &task_a } else { &task_b };
                    db.execute(
                        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,'check.run',?4,?7,?5,?6,'queued','{}',1,1,1)",
                        params![
                            check_op,
                            &owner_id,
                            format!("check-request-{check_op}"),
                            model::canonical(&json!({
                                "client_request_id":format!("check-request-{check_op}"),
                                "attempt_id":attempt_id,
                                "candidate_ref":candidate,
                                "profile_id":"scope-check-profile",
                                "profile_revision":"1",
                            }))?,
                            task_id,
                            attempt_id,
                            model::canonical(&json!({"check_id":check_id}))?,
                        ],
                    )?;
                    db.execute(
                        "INSERT INTO check_runs(check_id,operation_id,attempt_id,candidate_ref,cache_key,resource_key,spec_json,state,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,'queued',1)",
                        params![
                            check_id,
                            check_op,
                            attempt_id,
                            candidate,
                            format!("cache-{check_id}"),
                            format!("check-target:{check_id}"),
                            r#"{"profile_id":"scope-check-profile","profile_revision":"1"}"#,
                        ],
                    )?;
                }
                    Ok(())
                }
            })
            .await
            .unwrap();

        let reviewer_id = "scope-check-reviewer";
        let reviewer_token = "scope-check-reviewer-token".to_owned();
        let pending_scope = json!({
            "review_assignment_id":null,
            "task_id":task_a,
            "attempt_id":attempt_a,
            "task_revision":1,
            "submission_ref":subject.submission_ref,
            "candidate_ref":subject.candidate_ref,
        });
        write(
            store,
            &f.manager_a,
            "coordination.participant.register",
            json!({
                "client_request_id":format!("register-{reviewer_id}"),
                "client_id":reviewer_id,
                "token_hash":model::digest(reviewer_token.as_bytes()),
                "task_id":task_a,
                "task_revision":1,
                "attempt_id":attempt_a,
                "participation_basis":{"kind":"sponsored_reviewer","review_scope":pending_scope},
            }),
        )
        .await;
        let assignment = write(
            store,
            &f.manager_a,
            "review.assign",
            json!({
                "client_request_id":format!("assign-{reviewer_id}"),
                "attempt_id":attempt_a,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "reviewer_client_id":reviewer_id,
            }),
        )
        .await;
        let assignment_id = assignment["review_assignment_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let reviewer = store
            .authenticate(crate::model::Credential {
                client_id: reviewer_id.into(),
                token: reviewer_token,
            })
            .await
            .unwrap();
        let result = write(
            store,
            &reviewer,
            "review.submit",
            json!({
                "client_request_id":format!("submit-{assignment_id}"),
                "review_assignment_id":assignment_id,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "verdict":"pass",
                "coverage":"complete",
                "findings":[],
                "evidence_refs":["evidence://review/r1"],
            }),
        )
        .await;
        assert_eq!(result["verdict"], "pass");

        let acceptance = call(
            store,
            &f.manager_a,
            "task.acceptance",
            json!({"acceptance_operation_id":accept_a}),
        )
        .await;
        assert_eq!(acceptance["outcome"], "applied");
        assert_eq!(acceptance["acceptance_operation_id"], accept_a);
        assert_eq!(acceptance["task_id"], task_a);
        assert_eq!(acceptance["attempt_id"], attempt_a);
        assert_eq!(acceptance["invalidated"], false);
        let check = call(store, &reviewer, "check.get", json!({"check_id":check_a})).await;
        assert_eq!(check["check_id"], check_a);
        assert_eq!(check["operation_id"], check_op_a);
        assert_eq!(check["attempt_id"], attempt_a);
        assert_eq!(check["candidate_ref"], candidate_a);
        assert_eq!(check["state"], "queued");

        // The sponsored reviewer may read its assigned CheckRun evidence,
        // but does not acquire the rest of the Task graph (PR51 row 4).
        denied(
            store,
            &reviewer,
            "task.acceptance",
            json!({"acceptance_operation_id":accept_a}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &reviewer,
            "task.acceptance",
            json!({"acceptance_operation_id":accept_b}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &reviewer,
            "check.get",
            json!({"check_id":check_b}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &f.observer,
            "task.acceptance",
            json!({"acceptance_operation_id":accept_a}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.observer,
            "check.get",
            json!({"check_id":check_a}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "task.acceptance",
            json!({"acceptance_operation_id":accept_a}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "check.get",
            json!({"check_id":check_a}),
            "NOT_FOUND",
        )
        .await;
        f.close().await;
    }
    // PR49 row 4 tail: the initial task.dispatch retains its valid binding
    // while the coalesced reuse retains NULL binding. Both grant nothing to
    // foreign readers. dispatch_fixture already proves the owner's closed
    // reuse receipt; this tail proves the initial binding is retained and
    // B/observer gain no grant.
    #[tokio::test]
    async fn store_authenticated_coalesced_dispatch_hides_binding_from_foreign() {
        let f = start().await;
        let descriptor = fixture_descriptor();
        let (binding, module) = open_fixture_module(&f, &descriptor).await;
        let store = &f.owner.store;
        let target = dispatch_fixture(&f, &descriptor, &binding, &module, "coalesced-tail").await;
        let attempt_id = target["attempt_id"].as_str().unwrap().to_owned();
        let attempt = call(
            store,
            &f.manager_a,
            "attempt.get",
            json!({"attempt_id": attempt_id}),
        )
        .await;
        let task_id = attempt["task_id"].clone();
        let reused = write(
            store,
            &f.manager_a,
            "task.dispatch",
            json!({
                "attempt_id": attempt_id,
                "text": DISPATCH_TEXT,
            }),
        )
        .await;
        assert_eq!(reused["semantic_reuse"], true);
        assert_eq!(reused["start_operation_id"], target["operation_id"]);
        // Initial dispatch retains the fixture binding (PR49 actual contract:
        // UPDATE operations SET binding_id/binding_generation on admission).
        let initial = call(
            store,
            &f.manager_a,
            "operation.get",
            json!({"operation_id": target["operation_id"]}),
        )
        .await;
        assert_eq!(initial["task_id"], task_id);
        assert_eq!(initial["attempt_id"], attempt_id);
        assert_eq!(initial["binding_id"], binding["binding_id"]);
        assert_eq!(initial["binding_generation"], binding["generation"]);
        assert_closed_receipt(&initial);
        denied(
            store,
            &f.manager_b,
            "operation.get",
            json!({"operation_id": target["operation_id"]}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.observer,
            "operation.get",
            json!({"operation_id": target["operation_id"]}),
            "NOT_FOUND",
        )
        .await;
        // Coalesced reuse retains NULL (retain_task_dispatch_reuse keeps
        // binding_id/binding_generation NULL, no new native effect).
        let coalesced = call(
            store,
            &f.manager_a,
            "operation.get",
            json!({"operation_id": reused["operation_id"]}),
        )
        .await;
        assert_eq!(coalesced["task_id"], task_id);
        assert_eq!(coalesced["attempt_id"], attempt_id);
        assert!(coalesced["binding_id"].is_null());
        assert!(coalesced["binding_generation"].is_null());
        assert_closed_receipt(&coalesced);
        denied(
            store,
            &f.manager_b,
            "operation.get",
            json!({"operation_id": reused["operation_id"]}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.observer,
            "operation.get",
            json!({"operation_id": reused["operation_id"]}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "task.get",
            json!({"task_id": task_id}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "attempt.get",
            json!({"attempt_id": attempt_id}),
            "NOT_FOUND",
        )
        .await;
        f.close().await;
    }
    // PR51 row 4 tail: the sponsored reviewer keeps only its exact assigned
    // submission/review evidence. task.get/attempt.get/swarm.agent.inspect are
    // method-denied FORBIDDEN for participants (ParticipantAccess::None);
    // operation.get is CoordinationRead so the foreign submit op stays
    // object-denied NOT_FOUND. Observer/B keep object NOT_FOUND.
    // This also closes the unrelated swarm.agent.inspect denial row.
    #[tokio::test]
    async fn store_authenticated_sponsored_reviewer_rest_graph_and_inspect_denied() {
        let f = start().await;
        let store = &f.owner.store;
        let owner_id = f.manager_a.client_id.clone();
        let subject = seed_review_subject(store, &owner_id, "tail-rest").await;
        let reviewer_id = "scope-tail-reviewer";
        let reviewer_token = "scope-tail-reviewer-token".to_owned();
        let pending_scope = json!({
            "review_assignment_id": null,
            "task_id": "task-review-tail-rest",
            "attempt_id": "attempt-review-tail-rest",
            "task_revision": 1,
            "submission_ref": subject.submission_ref,
            "candidate_ref": subject.candidate_ref,
        });
        write(
            store,
            &f.manager_a,
            "coordination.participant.register",
            json!({
                "client_request_id": format!("register-{reviewer_id}"),
                "client_id": reviewer_id,
                "token_hash": model::digest(reviewer_token.as_bytes()),
                "task_id": "task-review-tail-rest",
                "task_revision": 1,
                "attempt_id": "attempt-review-tail-rest",
                "participation_basis": {"kind": "sponsored_reviewer", "review_scope": pending_scope},
            }),
        )
        .await;
        let assignment = write(
            store,
            &f.manager_a,
            "review.assign",
            json!({
                "client_request_id": format!("assign-{reviewer_id}"),
                "attempt_id": "attempt-review-tail-rest",
                "expected_revision": 1,
                "submission_ref": subject.submission_ref,
                "candidate_ref": subject.candidate_ref,
                "reviewer_client_id": reviewer_id,
            }),
        )
        .await;
        let assignment_id = assignment["review_assignment_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let reviewer = store
            .authenticate(Credential {
                client_id: reviewer_id.into(),
                token: reviewer_token,
            })
            .await
            .unwrap();
        let view = call(
            store,
            &reviewer,
            "review.get",
            json!({"review_assignment_id": assignment_id}),
        )
        .await;
        assert_eq!(view["assignment"]["reviewer_client_id"], reviewer_id);
        denied(
            store,
            &reviewer,
            "task.get",
            json!({"task_id": "task-review-tail-rest"}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &reviewer,
            "attempt.get",
            json!({"attempt_id": "attempt-review-tail-rest"}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &reviewer,
            "operation.get",
            json!({"operation_id": "submit-op-tail-rest"}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &reviewer,
            "swarm.agent.inspect",
            json!({"attempt_id": "attempt-review-tail-rest"}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &f.observer,
            "swarm.agent.inspect",
            json!({"attempt_id": "attempt-review-tail-rest"}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "swarm.agent.inspect",
            json!({"attempt_id": "attempt-review-tail-rest"}),
            "FORBIDDEN",
        )
        .await;
        denied(
            store,
            &f.observer,
            "task.get",
            json!({"task_id": "task-review-tail-rest"}),
            "NOT_FOUND",
        )
        .await;
        denied(
            store,
            &f.manager_b,
            "task.get",
            json!({"task_id": "task-review-tail-rest"}),
            "NOT_FOUND",
        )
        .await;
        f.close().await;
    }
    // PR51 row 6 / PR52 row 5 tail: task.list and report.delta filter foreign
    // rows without skipping later visible items, leaking counts, or claiming
    // complete coverage after a bounded gap. Partial pages stay honest.
    #[tokio::test]
    async fn store_authenticated_task_list_delta_filter_and_partial_page() {
        let f = start().await;
        let store = &f.owner.store;
        let a = create(store, &f.manager_a, "tail-list-a").await;
        let _attempt_a = claim(store, &f.manager_a, &a, None).await;
        let b = create(store, &f.manager_b, "tail-list-b").await;
        let _attempt_b = claim(store, &f.manager_b, &b, None).await;
        let later_a = create(store, &f.manager_a, "tail-list-later-a").await;
        let _later_attempt_a = claim(store, &f.manager_a, &later_a, None).await;
        let first = call(store, &f.manager_a, "task.list", json!({"limit": 1})).await;
        assert!(first.get("filtered_count").is_none());
        assert_eq!(first["items"].as_array().unwrap().len(), 1);
        assert_eq!(first["items"][0]["task_id"], a["task_id"]);
        let second = call(
            store,
            &f.manager_a,
            "task.list",
            json!({"limit": 1, "after": first["next_after"]}),
        )
        .await;
        assert!(second.get("filtered_count").is_none());
        assert_eq!(second["items"][0]["task_id"], later_a["task_id"]);
        let second_json = model::canonical(&second).unwrap();
        assert!(!second_json.contains(b["task_id"].as_str().unwrap()));
        let own_b = call(store, &f.manager_b, "task.list", json!({"limit": 10})).await;
        assert!(own_b.get("filtered_count").is_none());
        assert_eq!(own_b["items"].as_array().unwrap().len(), 1);
        assert_eq!(own_b["items"][0]["task_id"], b["task_id"]);
        let observer_tasks = call(store, &f.observer, "task.list", json!({"limit": 10})).await;
        assert!(observer_tasks["items"].as_array().unwrap().is_empty());
        let delta = call(store, &f.manager_a, "report.delta", json!({"limit": 50})).await;
        assert!(delta["projection"]["range"].get("filtered_count").is_none());
        let delta_json = model::canonical(&delta).unwrap();
        assert!(!delta_json.contains(b["task_id"].as_str().unwrap()));
        assert!(delta_json.contains(later_a["operation_id"].as_str().unwrap()));
        let first_ops = call(store, &f.manager_a, "operation.list", json!({"limit": 2})).await;
        assert!(first_ops.get("filtered_count").is_none());
        assert_eq!(first_ops["items"].as_array().unwrap().len(), 2);
        for entry in first_ops["items"].as_array().unwrap() {
            assert_closed_receipt(entry);
        }
        f.close().await;
    }
}
