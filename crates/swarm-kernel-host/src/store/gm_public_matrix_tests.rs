//! PR56 public GM epoch recovery + successor continuation matrix.
//!
//! Exact PR56 remainder (epoch monotonicity, damage/recovery, effect fencing,
//! successor continuation without replay, public Store boundary) exercised only
//! through real `StoreOwner` authenticated principals and `Store::call`.
//! No private resolver/parser paths, no native/model calls. SQL appears once,
//! as intentional fault injection on already-retained real admissions (delete
//! or rewrite the live `gm` meta row, rewrite one retained handover receipt,
//! drop retained handover history, seed one module identity row in the exact
//! status-reader donor shape, flip one retained registration to disabled);
//! every other step is a public mutation or read.
use super::{Store, StoreOwner};
use crate::{
    artifacts::ArtifactRecord,
    config::Config,
    model::{self, Credential, Principal},
    platform::{bootstrap_credential, DataRoot},
};
use rusqlite::params;
use serde_json::{json, Value};
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

async fn register_role(
    store: &Store,
    operator: &Principal,
    client_id: &str,
    role: &str,
) -> Principal {
    let token = format!("gm-public-matrix-{client_id}-{}", model::new_id());
    write(
        store,
        operator,
        "client.register",
        json!({
            "client_id": client_id,
            "role": role,
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
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": old.client_id}),
    )
    .await;
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
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": old.client_id}),
    )
    .await;
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

#[tokio::test]
async fn malformed_current_designation_recovers_only_by_operator_at_high_water_plus_one() {
    let (owner, _directory, operator) = start_store().await;
    let old = register_manager(&owner.store, &operator, "gm-damage-old").await;
    let successor = register_manager(&owner.store, &operator, "gm-damage-successor").await;
    let replacement = register_manager(&owner.store, &operator, "gm-damage-replacement").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": old.client_id}),
    )
    .await;
    write(
        &owner.store,
        &old,
        "gm.handover",
        json!({"client_id": successor.client_id}),
    )
    .await;
    // Fault injection on the already-retained designation row: the epoch is
    // no longer a positive integer, so gm::parse_designation fails while the
    // two retained successful handovers keep high-water at 2.
    let malformed = model::canonical(&json!({
        "client_id": successor.client_id,
        "epoch": "two",
        "binding_id": null,
        "binding_generation": null,
    }))
    .unwrap();
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE meta SET value_json=?1 WHERE key='gm'",
                params![malformed],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    // The sitting manager cannot turn damage into a grant: gm::current
    // fails closed with GM_DESIGNATION_DAMAGED before any recovery branch.
    let refused = owner
        .store
        .call(
            successor.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": replacement.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(refused.code, "GM_DESIGNATION_DAMAGED");
    // Only the independently verified local Operator recovers, at exactly
    // high_water + 1 with the damaged_current reason.
    let recovered = write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": replacement.client_id}),
    )
    .await;
    assert_eq!(recovered["gm_epoch"], json!(3));
    assert_eq!(recovered["epoch_high_water_before"], json!(2));
    assert_eq!(recovered["designation_recovered"], json!(true));
    assert_eq!(recovered["recovery_reason"], json!("damaged_current"));
    owner.close().await.unwrap();
}

#[tokio::test]
async fn malformed_handover_history_reports_damage_without_grant_or_recovery() {
    let (owner, _directory, operator) = start_store().await;
    let old = register_manager(&owner.store, &operator, "gm-history-old").await;
    let successor = register_manager(&owner.store, &operator, "gm-history-successor").await;
    let replacement = register_manager(&owner.store, &operator, "gm-history-replacement").await;
    let first = write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": old.client_id}),
    )
    .await;
    write(
        &owner.store,
        &old,
        "gm.handover",
        json!({"client_id": successor.client_id}),
    )
    .await;
    // Fault injection on one already-retained successful handover receipt:
    // the epoch stays valid JSON but is no longer a positive integer, so
    // the history validator counts exactly one damaged row.
    let first_operation = first["operation_id"].as_str().unwrap().to_owned();
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE operations SET result_json=json_set(result_json,'$.gm_epoch','two') WHERE operation_id=?1",
                params![first_operation],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    // Even the sitting GM cannot act once the fence history is unreadable.
    let sitting = owner
        .store
        .call(
            successor.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": successor.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(sitting.code, "GM_EPOCH_HISTORY_DAMAGED");
    // The local Operator cannot recover by guessing a successor either.
    let guessing = owner
        .store
        .call(
            operator.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": replacement.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(guessing.code, "GM_EPOCH_HISTORY_DAMAGED");
    owner.close().await.unwrap();
}

#[tokio::test]
async fn damaged_designation_without_high_water_stays_unavailable_not_epoch_one() {
    let (owner, _directory, operator) = start_store().await;
    let old = register_manager(&owner.store, &operator, "gm-nofloor-old").await;
    let replacement = register_manager(&owner.store, &operator, "gm-nofloor-replacement").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": old.client_id}),
    )
    .await;
    // Fault injection on already-retained admissions: drop the only
    // successful handover receipt and damage the surviving pointer, leaving
    // no recoverable high-water behind.
    owner
        .store
        .run(|db| {
            db.execute("DELETE FROM operations WHERE method='gm.handover'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let malformed = model::canonical(&json!({
        "client_id": old.client_id,
        "epoch": "zero",
        "binding_id": null,
        "binding_generation": null,
    }))
    .unwrap();
    let expected = malformed.clone();
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE meta SET value_json=?1 WHERE key='gm'",
                params![malformed],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    // Managers fail closed on the damaged pointer.
    let refused = owner
        .store
        .call(
            old.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": old.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(refused.code, "GM_DESIGNATION_DAMAGED");
    // The Operator gets UNAVAILABLE, never a fresh epoch-1 grant.
    let unavailable = owner
        .store
        .call(
            operator.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": replacement.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(unavailable.code, "GM_EPOCH_HISTORY_UNAVAILABLE");
    // The damaged pointer is untouched: no designation was written.
    let retained = owner
        .store
        .run(|db| {
            let value: String =
                db.query_row("SELECT value_json FROM meta WHERE key='gm'", [], |row| {
                    row.get(0)
                })?;
            Ok(value)
        })
        .await
        .unwrap();
    assert_eq!(retained, expected);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn gm_handover_denies_roles_without_grant() {
    let (owner, _directory, operator) = start_store().await;
    let old = register_manager(&owner.store, &operator, "gm-deny-old").await;
    let unrelated = register_manager(&owner.store, &operator, "gm-deny-unrelated").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": old.client_id}),
    )
    .await;
    // Observer holds a real public registration but no GM authority.
    let observer = register_role(&owner.store, &operator, "gm-deny-observer", "observer").await;
    let watched = owner
        .store
        .call(
            observer.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": observer.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(watched.code, "FORBIDDEN");
    // Participant holds a real attempt-owner registration on old's retained
    // attempt, yet gm.handover is outside the participant surface.
    let created = write(
        &owner.store,
        &old,
        "task.create",
        json!({"project_id": PROJECT, "spec": task_spec()}),
    )
    .await;
    let task_id = created["task_id"].as_str().unwrap().to_owned();
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
    let participant_token = format!("gm-deny-participant-{}", model::new_id());
    write(
        &owner.store,
        &old,
        "coordination.participant.register",
        json!({
            "client_id": "gm-deny-participant",
            "token_hash": model::digest(participant_token.as_bytes()),
            "task_id": task_id,
            "task_revision": 1,
            "attempt_id": attempt_id,
            "participation_basis": {"kind": "attempt_owner"},
        }),
    )
    .await;
    let participant = owner
        .store
        .authenticate(Credential {
            client_id: "gm-deny-participant".into(),
            token: participant_token,
        })
        .await
        .unwrap();
    let scoped = owner
        .store
        .call(
            participant.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": "gm-deny-participant"}),
        )
        .await
        .unwrap_err();
    assert_eq!(scoped.code, "FORBIDDEN");
    // Module identity is seeded in the exact status-reader donor shape and
    // is stopped at the module method-scope fence before GM authority.
    owner
        .store
        .run(|db| {
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES('client:gm-deny-module','{\"role\":\"module\",\"disabled\":false}')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let module = Principal {
        link_id: model::new_id(),
        client_id: "gm-deny-module".into(),
        role: model::Role::Module,
    };
    let fenced = owner
        .store
        .call(
            module,
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": "gm-deny-module"}),
        )
        .await
        .unwrap_err();
    assert_eq!(fenced.code, "FORBIDDEN");
    // Disabled manager: a real registration whose retained row is then
    // disabled fails closed as UNAUTHORIZED on the stale credential.
    let dormant = register_manager(&owner.store, &operator, "gm-deny-dormant").await;
    owner
        .store
        .run(|db| {
            db.execute(
                "UPDATE meta SET value_json=json_set(value_json,'$.disabled',json('true')) WHERE key='client:gm-deny-dormant'",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let revoked = owner
        .store
        .call(
            dormant.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": dormant.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(revoked.code, "UNAUTHORIZED");
    // Enabled but unrelated manager is not the current GM.
    let foreign = owner
        .store
        .call(
            unrelated.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id": model::new_id(), "client_id": unrelated.client_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign.code, "FORBIDDEN");
    owner.close().await.unwrap();
}

fn assert_no_private_keys(value: &Value) {
    const DENIED: &[&str] = &[
        "token_hash",
        "token",
        "credential",
        "secret",
        "password",
        "private_key",
    ];
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                assert!(
                    !DENIED.contains(&key.as_str()),
                    "private projection key {key:?} must stay out of GM projections"
                );
                assert_no_private_keys(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                assert_no_private_keys(item);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn gm_public_projections_expose_no_private_credential_material() {
    let (owner, _directory, operator) = start_store().await;
    let old = register_manager(&owner.store, &operator, "gm-redact-old").await;
    let successor = register_manager(&owner.store, &operator, "gm-redact-successor").await;
    let observer = register_role(&owner.store, &operator, "gm-redact-observer", "observer").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id": old.client_id}),
    )
    .await;
    let handover = write(
        &owner.store,
        &old,
        "gm.handover",
        json!({"client_id": successor.client_id}),
    )
    .await;
    assert_no_private_keys(&handover);
    let readback = operation_get(
        &owner.store,
        &operator,
        handover["operation_id"].as_str().unwrap(),
    )
    .await;
    assert_no_private_keys(&readback);
    // The observer-readable public boundary carries the designation facts
    // and diagnostics, never credential material.
    let status = owner
        .store
        .call(observer.clone(), "host.status".into(), json!({}))
        .await
        .unwrap();
    let gm = &status["gm"];
    assert_eq!(gm["state"], json!("current"));
    assert_eq!(gm["client_id"], json!(successor.client_id));
    assert_eq!(gm["epoch"], json!(2));
    assert_eq!(
        gm["resync"]["watermarks_are_acknowledgements"],
        json!(false)
    );
    assert_eq!(gm["resync"]["historical_mail_reassigned"], json!(false));
    assert_no_private_keys(gm);
    owner.close().await.unwrap();
}
