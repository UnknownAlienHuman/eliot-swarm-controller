use crate::{
    artifacts::ArtifactFiles,
    checks::{
        model::{CheckProfile, Parser},
        source::{SourceFile, SourceManifest},
    },
    config::Config,
    error::{Error, Result},
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};
use rusqlite::params;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc};

use super::Store;

const PROJECT: &str = "manual-run-now-regression";
const AUTOMATION_ID: &str = "disabled-check";

async fn call(
    store: &Store,
    principal: &Principal,
    method: &str,
    mut request: Value,
) -> Result<Value> {
    if request.get("client_request_id").is_none() {
        request["client_request_id"] = json!(model::new_id());
    }
    store
        .call(principal.clone(), method.to_owned(), request)
        .await
}

async fn register_manager(
    store: &Store,
    operator: &Principal,
    client_id: &str,
) -> Result<(Principal, String)> {
    let token = format!("run-now-{client_id}-{}", model::new_id());
    call(
        store,
        operator,
        "client.register",
        json!({
            "client_id":client_id,
            "role":"manager",
            "token_hash":model::digest(token.as_bytes()),
        }),
    )
    .await?;
    let principal = store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token: token.clone(),
        })
        .await?;
    Ok((principal, token))
}

async fn create_source_snapshot(
    store: &Store,
    directory: &Path,
    task_id: &str,
    attempt_id: &str,
) -> Result<String> {
    let candidate_ref = format!("source-{}", model::digest(task_id.as_bytes()));
    let content = b"disabled run-now source fixture";
    let commit = "a".repeat(40);
    let tree = "b".repeat(40);
    let manifest = SourceManifest {
        version: 1,
        commit: commit.clone(),
        tree: tree.clone(),
        files: vec![SourceFile {
            path: "fixture.txt".into(),
            mode: "100644".into(),
            object_id: "c".repeat(40),
            byte_length: content.len() as u64,
            sha256: model::digest(content),
        }],
    };
    let metadata = json!({
        "task_id":task_id,
        "attempt_id":attempt_id,
        "task_revision":1,
        "commit":commit,
        "tree":tree,
        "file_count":1,
        "coverage":"complete",
    });
    let (record, bytes) = ArtifactFiles::document(
        "source_snapshot",
        &candidate_ref,
        &json!(manifest),
        metadata.clone(),
    )?;
    let source_dir = directory.join("sources").join(&candidate_ref);
    std::fs::create_dir_all(&source_dir)?;
    std::fs::write(source_dir.join("fixture.txt"), content)?;
    ArtifactFiles::new(directory)?.publish(&record, &bytes)?;
    store
        .run(move |db| {
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    record.artifact_id,
                    record.relative_path,
                    record.kind,
                    record.byte_length as i64,
                    record.content_digest,
                    model::now_ms()?,
                    model::canonical(&metadata)?,
                ],
            )?;
            Ok(())
        })
        .await?;
    Ok(candidate_ref)
}

async fn cron_snapshot(store: &Store, manager_id: &str) -> Result<(Value, i64)> {
    let manager_id = manager_id.to_owned();
    store
        .run(move |db| {
            let entry =
                crate::automation::config::load_entry(db, &manager_id, PROJECT, AUTOMATION_ID)?
                    .ok_or_else(|| {
                        Error::new("TEST_ENTRY_MISSING", "manual test entry is missing")
                    })?;
            let state = super::automation_cron::state(db, &entry)?;
            let occurrence_count = db.query_row(
                "SELECT count(*) FROM meta WHERE key LIKE 'automation:v1:cron:occurrence:%'",
                [],
                |row| row.get(0),
            )?;
            Ok((state, occurrence_count))
        })
        .await
}

fn assert_manual_did_not_advance_cron(state: &Value, occurrence_count: i64) {
    assert_eq!(state["enabled"], false);
    assert_eq!(state["next_due_at_ms"], Value::Null);
    assert_eq!(state["last_considered_due_at_ms"], Value::Null);
    assert_eq!(state["last_occurrence_id"], Value::Null);
    assert_eq!(state["last_operation"], Value::Null);
    assert_eq!(state["held_occurrence"], false);
    assert_eq!(occurrence_count, 0);
}

#[tokio::test]
async fn disabled_run_now_admits_one_check_and_replays_after_entry_change() {
    let directory = std::env::temp_dir().join(format!("swarm-run-now-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let operator_credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    // StoreOwner does not start the checker pump. Enable normal admission while
    // this test leaves the admitted CheckRun queued and recurrence disabled.
    config.checks.enabled = true;
    config.checks.profiles = vec![CheckProfile {
        profile_id: "strict".into(),
        profile_revision: "v1".into(),
        executable: std::env::current_exe().unwrap(),
        args: Vec::new(),
        parser: Parser::ExitCode,
        resource: "run-now-checks".into(),
        environment: BTreeMap::new(),
        inherit_env: Vec::new(),
        expected_targets: Vec::new(),
        reproducible: false,
        fingerprint_env: Vec::new(),
        versioned_inputs: BTreeMap::new(),
    }];
    let config = Arc::new(config);
    let owner = StoreOwner::start(root, config, operator_credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(operator_credential).await.unwrap();
    let (manager, _) = register_manager(&owner.store, &operator, "run-now-owner")
        .await
        .unwrap();
    let (foreign, _) = register_manager(&owner.store, &operator, "run-now-foreign")
        .await
        .unwrap();
    call(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await
    .unwrap();

    let task = call(
        &owner.store,
        &operator,
        "task.create",
        json!({
            "project_id":PROJECT,
            "spec":{
                "objective":"Run a saved CheckRun while recurrence stays disabled",
                "phase":"verification",
                "owner_policy_id":"owner-policy-v1",
                "requirements":[{"id":"R1","statement":"Retain the exact manual CheckRun receipt"}],
            }
        }),
    )
    .await
    .unwrap();
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let claimed = call(
        &owner.store,
        &manager,
        "task.claim",
        json!({"task_id":task_id,"expected_revision":1}),
    )
    .await
    .unwrap();
    let attempt_id = claimed["attempt_id"].as_str().unwrap().to_owned();
    let candidate_ref =
        create_source_snapshot(&owner.store, &owner.store.data_dir, &task_id, &attempt_id)
            .await
            .unwrap();
    let cron_settings = json!({
        "calendar":{"expression":"0 0 0 1 1 *","timezone":"UTC","anchor_ms":0},
        "action":{
            "kind":"check_run",
            "attempt_id":attempt_id,
            "expected_task_revision":1,
            "candidate_ref":candidate_ref,
            "profile_id":"strict",
            "profile_revision":"v1",
        }
    });
    let configured = call(
        &owner.store,
        &manager,
        "automation.config.apply",
        json!({
            "project_id":PROJECT,
            "changes":[{
                "automation_id":AUTOMATION_ID,
                "expected_revision":0,
                "include_existing":false,
                "patch":{"enabled":false,"steps":["check_run"],"cron":cron_settings},
            }]
        }),
    )
    .await
    .unwrap();
    assert_eq!(configured["applied"], true);

    let (before_state, before_occurrences) = cron_snapshot(&owner.store, &manager.client_id)
        .await
        .unwrap();
    assert_manual_did_not_advance_cron(&before_state, before_occurrences);
    let run_now_request_id = format!("run-now-{}", model::new_id());
    let request = json!({
        "client_request_id":run_now_request_id,
        "project_id":PROJECT,
        "automation_id":AUTOMATION_ID,
    });
    let first = owner
        .store
        .call(manager.clone(), "schedule.run_now".into(), request.clone())
        .await
        .unwrap();
    assert_eq!(first["state"], "queued");
    assert!(
        first["operation_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(
        first["check_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );

    let (after_first_state, after_first_occurrences) =
        cron_snapshot(&owner.store, &manager.client_id)
            .await
            .unwrap();
    assert_manual_did_not_advance_cron(&after_first_state, after_first_occurrences);
    for field in ["activation_cut_ms", "include_existing"] {
        assert_eq!(after_first_state[field], before_state[field]);
    }

    // Removing the selected step changes the committed entry while keeping
    // recurrence disabled. The exact manual request must still read back its
    // original durable receipt instead of attempting a second admission.
    let changed = call(
        &owner.store,
        &manager,
        "automation.config.apply",
        json!({
            "project_id":PROJECT,
            "changes":[{
                "automation_id":AUTOMATION_ID,
                "expected_revision":1,
                "include_existing":false,
                "patch":{"steps":[]},
            }]
        }),
    )
    .await
    .unwrap();
    assert_eq!(changed["applied"], true);

    let replay = owner
        .store
        .call(manager.clone(), "schedule.run_now".into(), request)
        .await
        .unwrap();
    assert_eq!(replay, first);
    let (after_replay_state, after_replay_occurrences) =
        cron_snapshot(&owner.store, &manager.client_id)
            .await
            .unwrap();
    assert_manual_did_not_advance_cron(&after_replay_state, after_replay_occurrences);
    for field in ["activation_cut_ms", "include_existing"] {
        assert_eq!(after_replay_state[field], before_state[field]);
    }

    let foreign_error = owner
        .store
        .call(
            foreign.clone(),
            "schedule.run_now".into(),
            json!({
                "client_request_id":format!("foreign-{}", model::new_id()),
                "project_id":PROJECT,
                "automation_id":AUTOMATION_ID,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign_error.code, "AUTOMATION_NOT_FOUND");

    let manager_id = manager.client_id.clone();
    let operation_id = first["operation_id"].as_str().unwrap().to_owned();
    // The receipt doesn't expose the caller-supplied run-now ID. Read back the
    // single retained manual marker by the stable manager-scoped Operation.
    let (operation_count, check_count, check_id, check_state, effective): (
        i64,
        i64,
        String,
        String,
        String,
    ) = owner
        .store
        .run(move |db| {
            let (operation_count,): (i64,) = db.query_row(
                "SELECT count(*) FROM operations WHERE caller_id=?1 AND method='check.run'",
                [&manager_id],
                |row| Ok((row.get(0)?,)),
            )?;
            let (check_count, check_id, check_state): (i64, String, String) = db.query_row(
                "SELECT count(*),min(check_id),min(state) FROM check_runs WHERE operation_id=?1",
                [&operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let effective: String = db.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| row.get(0),
            )?;
            Ok((
                operation_count,
                check_count,
                check_id,
                check_state,
                effective,
            ))
        })
        .await
        .unwrap();
    let effective: Value = serde_json::from_str(&effective).unwrap();
    assert_eq!(operation_count, 1);
    assert_eq!(check_count, 1);
    assert_eq!(check_id, first["check_id"]);
    assert_eq!(check_state, "queued");
    assert_eq!(effective["manual_run_now"]["manager_id"], manager.client_id);
    assert_eq!(effective["manual_run_now"]["project_id"], PROJECT);
    assert_eq!(effective["manual_run_now"]["automation_id"], AUTOMATION_ID);
    assert_eq!(
        effective["manual_run_now"]["client_request_id"],
        run_now_request_id
    );

    let (final_state, final_occurrences) = cron_snapshot(&owner.store, &manager.client_id)
        .await
        .unwrap();
    assert_manual_did_not_advance_cron(&final_state, final_occurrences);
    for field in ["activation_cut_ms", "include_existing"] {
        assert_eq!(final_state[field], before_state[field]);
    }
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
