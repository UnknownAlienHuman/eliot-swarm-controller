//! PR56 public GM epoch recovery + successor continuation matrix.
//!
//! Exact PR56 remainder (epoch monotonicity, damage/recovery, effect fencing,
//! successor continuation without replay, public Store boundary) exercised only
//! through real `StoreOwner` authenticated principals and `Store::call`.
//! No private resolver/parser paths, no native/model calls. SQL appears once,
//! as intentional fault injection (delete the live `gm` meta row) after real
//! handover admissions; every other step is a public mutation or read.
use super::{Store, StoreOwner};
use crate::{
    artifacts::ArtifactRecord,
    config::Config,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
};
use rusqlite::params;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const PROJECT: &str = "gm-public-matrix-project";

async fn start_store() -> (StoreOwner, PathBuf, Principal) {
    let directory =
        std::env::temp_dir().join(format!("swarm-gm-public-matrix-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    (owner, directory, operator)
}

async fn write(store: &Store, principal: &Principal, method: &str, mut value: Value) -> Value {
    value["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
        .unwrap()
}

async fn write_with_id(
    store: &Store,
    principal: &Principal,
    method: &str,
    request_id: &str,
    mut value: Value,
) -> Value {
    value["client_request_id"] = json!(request_id);
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
        .unwrap()
}

async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    let token = format!("gm-public-matrix-{client_id}-{}", model::new_id());
    write(
        store,
        operator,
        "client.register",
        json!({
            "client_id": client_id,
            "role": "manager",
            "token_hash": model::digest(token.as_bytes()),
        }),
    )
    .await;
    store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .unwrap()
}

fn task_spec() -> Value {
    json!({
        "objective": "Exercise GM epoch recovery and successor continuation on public Store paths",
        "phase": "implementation",
        "requirements": [{"id": "R1", "statement": "Keep one dispatch and exact ownership"}],
        "scope": {"initial_paths": ["src/gm-target.rs"]},
        "owner_policy_id": crate::policy::OWNER_POLICY_V1_ID,
    })
}

async fn register_source_candidate(
    store: &Store,
    task_id: &str,
    attempt_id: &str,
    suffix: &str,
) -> String {
    let source = format!(
        "Store GM public-matrix fixture candidate {suffix}; no native execution is required."
    );
    let manifest = json!({
        "schema_version": 1,
        "files": [{
            "path": "src/lib.rs",
            "content": source,
            "content_sha256": model::digest(source.as_bytes()),
            "byte_length": source.len(),
        }],
    });
    let bytes = model::canonical(&manifest).unwrap().into_bytes();
    let artifact_id = format!("source-{}", model::digest(&bytes));
    let metadata = json!({
        "task_id": task_id,
        "attempt_id": attempt_id,
        "task_revision": 1,
        "coverage": "complete",
    });
    let record = ArtifactRecord {
        kind: "source_snapshot".to_owned(),
        artifact_id: artifact_id.clone(),
        relative_path: format!("artifacts/{artifact_id}.bin"),
        byte_length: u64::try_from(bytes.len()).unwrap(),
        content_digest: model::digest(&bytes),
        metadata: metadata.clone(),
    };
    let publish_record = record.clone();
    let publish_bytes = bytes.clone();
    store
        .file_io(move |files| files.publish(&publish_record, &publish_bytes))
        .await
        .unwrap();
    let db_id = record.artifact_id;
    let db_path = record.relative_path;
    let db_length = i64::try_from(record.byte_length).unwrap();
    let db_digest = record.content_digest;
    let db_metadata = model::canonical(&metadata).unwrap();
    let created_at = model::now_ms().unwrap();
    store
        .run(move |db| {
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'source_snapshot',?3,?4,?5,?6)",
                params![db_id, db_path, db_length, db_digest, created_at, db_metadata],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    artifact_id
}

async fn operation_get(store: &Store, reader: &Principal, operation_id: &str) -> Value {
    store
        .call(
            reader.clone(),
            "operation.get".to_owned(),
            json!({"operation_id": operation_id}),
        )
        .await
        .unwrap()
}
#[tokio::test]
async fn gm_epoch_fencing_old_new_unrelated_and_unchanged_conflict() {
    let (owner, _directory, operator) = start_store().await;
    let old = register_manager(&owner.store, &operator, "gm-matrix-old").await;
    let successor = register_manager(&owner.store, &operator, "gm-matrix-successor").await;
    let unrelated = register_manager(&owner.store, &operator, "gm-matrix-unrelated").await;
    let first = write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": old.client_id}),
    )
    .await;
    assert_eq!(first["client_id"], json!("gm-matrix-old"));
    assert_eq!(first["gm_epoch"], json!(1));
    let second = write(
        &owner.store,
        &old,
        "gm.handover",
        json!({"client_id": successor.client_id}),
    )
    .await;
    assert_eq!(second["client_id"], json!("gm-matrix-successor"));
    assert_eq!(second["gm_epoch"], json!(2));
    let stale = owner
        .store
        .call(
            old.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": unrelated.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code, "FORBIDDEN");
    let denied = owner
        .store
        .call(
            unrelated.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": unrelated.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(denied.code, "FORBIDDEN");
    let conflict = owner
        .store
        .call(
            successor.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": successor.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.code, "CONFLICT");
    owner.close().await.unwrap();
}
#[tokio::test]
async fn successor_continues_retained_attempt_without_replay() {
    let (owner, _directory, operator) = start_store().await;
    let old = register_manager(&owner.store, &operator, "gm-matrix-old").await;
    let successor = register_manager(&owner.store, &operator, "gm-matrix-successor").await;
    let unrelated = register_manager(&owner.store, &operator, "gm-matrix-unrelated").await;
    write(&owner.store, &operator, "gm.handover", json!({"client_id": old.client_id})).await;
    let created = write_with_id(
        &owner.store,
        &old,
        "task.create",
        "gm-matrix-create-1",
        json!({"project_id": PROJECT, "spec": task_spec()}),
    )
    .await;
    let task_id = created["task_id"].as_str().unwrap().to_owned();
    let create_operation_id = created["operation_id"].as_str().unwrap().to_owned();
    let claimed = write(
        &owner.store,
        &old,
        "task.claim",
        json!({
            "task_id": task_id,
            "expected_revision": 1,
            "owner_id": old.client_id,
        }),
    )
    .await;
    let attempt_id = claimed["attempt_id"].as_str().unwrap().to_owned();
    let claim_operation_id = claimed["operation_id"].as_str().unwrap().to_owned();
    let candidate = register_source_candidate(&owner.store, &task_id, &attempt_id, "first").await;
    let submitted = write(
        &owner.store,
        &old,
        "task.submit",
        json!({
            "attempt_id": attempt_id,
            "expected_revision": 1,
            "expected_submission_ref": null,
            "candidate_ref": candidate,
            "summary": "Retain one applied TaskSubmission for GM matrix coverage.",
            "claims": [],
        }),
    )
    .await;
    assert_eq!(submitted["attempt_id"], json!(attempt_id));
    let submit_operation_id = submitted["operation_id"].as_str().unwrap().to_owned();
    let handover = write(
        &owner.store,
        &old,
        "gm.handover",
        json!({"client_id": successor.client_id}),
    )
    .await;
    assert_eq!(handover["gm_epoch"], json!(2));
    let reread = write_with_id(
        &owner.store,
        &old,
        "task.create",
        "gm-matrix-create-1",
        json!({"project_id": PROJECT, "spec": task_spec()}),
    )
    .await;
    assert_eq!(reread["operation_id"], json!(create_operation_id));
    assert_eq!(reread["task_id"], json!(task_id));
    let create_readback = operation_get(&owner.store, &successor, &create_operation_id).await;
    assert_eq!(create_readback["method"], json!("task.create"));
    let claim_readback = operation_get(&owner.store, &successor, &claim_operation_id).await;
    assert_eq!(claim_readback["method"], json!("task.claim"));
    let submit_readback = operation_get(&owner.store, &successor, &submit_operation_id).await;
    assert_eq!(submit_readback["method"], json!("task.submit"));
    assert_eq!(submit_readback["state"], json!("settled"));
    assert_eq!(submit_readback["result"]["outcome"], json!("applied"));
    let fresh = write(
        &owner.store,
        &successor,
        "task.create",
        json!({"project_id": PROJECT, "spec": task_spec()}),
    )
    .await;
    assert_ne!(fresh["task_id"], json!(task_id));
    let stale = owner
        .store
        .call(
            old.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": old.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code, "FORBIDDEN");
    let foreign = owner
        .store
        .call(
            unrelated.clone(),
            "task.submit".to_owned(),
            json!({
                "client_request_id": model::new_id(),
                "attempt_id": attempt_id,
                "expected_revision": 1,
                "expected_submission_ref": null,
                "candidate_ref": "unrelated-dummy-candidate",
                "summary": "Unrelated managers cannot touch the retained attempt.",
                "claims": [],
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign.code, "FORBIDDEN");
    let late_token = format!("gm-matrix-late-{}", model::new_id());
    let late = write(
        &owner.store,
        &successor,
        "client.register",
        json!({
            "client_id": "gm-matrix-late",
            "role": "manager",
            "token_hash": model::digest(late_token.as_bytes()),
        }),
    )
    .await;
    assert_eq!(late["client_id"], json!("gm-matrix-late"));
    owner.close().await.unwrap();
}
#[tokio::test]
async fn lost_reply_readback_returns_retained_receipt() {
    let (owner, _directory, operator) = start_store().await;
    let manager = register_manager(&owner.store, &operator, "gm-matrix-manager").await;
    let first = write_with_id(
        &owner.store,
        &manager,
        "task.create",
        "gm-matrix-lost-reply-1",
        json!({"project_id": PROJECT, "spec": task_spec()}),
    )
    .await;
    let second = write_with_id(
        &owner.store,
        &manager,
        "task.create",
        "gm-matrix-lost-reply-1",
        json!({"project_id": PROJECT, "spec": task_spec()}),
    )
    .await;
    assert_eq!(second["operation_id"], first["operation_id"]);
    assert_eq!(second["task_id"], first["task_id"]);
    let readback = operation_get(
        &owner.store,
        &operator,
        first["operation_id"].as_str().unwrap(),
    )
    .await;
    assert_eq!(readback["method"], json!("task.create"));
    owner.close().await.unwrap();
}
#[tokio::test]
async fn missing_designation_reports_high_water_and_operator_recovers() {
    let (owner, _directory, operator) = start_store().await;
    let old = register_manager(&owner.store, &operator, "gm-matrix-old").await;
    let successor = register_manager(&owner.store, &operator, "gm-matrix-successor").await;
    let replacement = register_manager(&owner.store, &operator, "gm-matrix-replacement").await;
    write(&owner.store, &operator, "gm.handover", json!({"client_id": old.client_id})).await;
    let second = write(
        &owner.store,
        &old,
        "gm.handover",
        json!({"client_id": successor.client_id}),
    )
    .await;
    assert_eq!(second["gm_epoch"], json!(2));
    owner
        .store
        .run(|db| {
            db.execute("DELETE FROM meta WHERE key='gm'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let damaged = owner
        .store
        .call(
            successor.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": successor.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(damaged.code, "GM_DESIGNATION_MISSING");
    let recovered = write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": replacement.client_id}),
    )
    .await;
    assert_eq!(recovered["gm_epoch"], json!(3));
    assert_eq!(recovered["designation_recovered"], json!(true));
    assert_eq!(recovered["recovery_reason"], json!("missing_after_history"));
    owner.close().await.unwrap();
}
