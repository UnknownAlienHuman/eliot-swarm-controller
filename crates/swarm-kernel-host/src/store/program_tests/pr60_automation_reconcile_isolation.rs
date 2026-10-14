use super::super::{
    add_submission_source_fact, configure_lineage_review_dispatch, principal,
    register_profile_reviewer, seed_clients, seed_subject, start_store_with_config,
};
use super::{
    PublicationFixture, accepted_publication_fixture, close_fixture, publication_explanation,
    reconcile_publication_fixture, retained_acceptance_observation, retained_forge_operations,
};
use crate::{
    automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
    config::{Config, McpProfileConfig, McpToolProfile, Route},
    model::{self, Principal, Role},
    policy::{OWNER_POLICY_V1_ID, OWNER_POLICY_V2_ID},
    store::Store,
};
use rusqlite::params;
use serde_json::{Value, json};
use std::path::PathBuf;
use swarm_contracts::{
    module_catalog::{
        ActivationPolicy, ArtifactId, ArtifactVersion, LaunchSpec, LifecycleOwnership, ModuleId,
        ProtectedRef, RestartPolicy, Sha256Digest,
    },
    module_contract::ModuleContractTemplate,
};

struct ReviewDispatchSubject {
    automation_id: String,
    task_id: String,
    observation_id: i64,
}

async fn seed_review_dispatch_subject(
    fixture: &PublicationFixture,
    suffix: &str,
    malformed: bool,
) -> ReviewDispatchSubject {
    let store = &fixture.owner.store;
    let subject = seed_subject(store, suffix, "review-owner-v2", OWNER_POLICY_V2_ID, false).await;
    let candidate_ref = subject.candidate_ref.clone();
    let submission_ref = subject.submission_ref.clone();
    let (candidate, submission, submission_bytes) = store
        .run(move |db| {
            let document = super::super::super::submissions::document(db, &submission_ref)?;
            Ok((
                super::super::super::results::get(db, &candidate_ref)?,
                super::super::super::results::get(db, &submission_ref)?,
                model::canonical(&document)?.into_bytes(),
            ))
        })
        .await
        .unwrap();
    store
        .file_io(move |files| {
            files.publish(&candidate, super::CANDIDATE_BYTES)?;
            files.publish(&submission, &submission_bytes)?;
            Ok(())
        })
        .await
        .unwrap();
    let review_owner = principal("review-owner-v2", Role::Manager);
    let automation_id = format!("pr60-review-{suffix}");
    let profile = format!("pr60-profile-{suffix}");
    let reviewer_id = format!("pr60-reviewer-{suffix}");
    let token = format!("pr60-reviewer-token-{suffix}");
    let _reviewer = register_profile_reviewer(
        store,
        &review_owner,
        &subject,
        &reviewer_id,
        &token,
        &profile,
    )
    .await;
    configure_lineage_review_dispatch(store, &review_owner, &[automation_id.as_str()], &profile)
        .await;
    add_submission_source_fact(store, suffix, &subject).await;

    let source_operation_id = format!("submit-op-{suffix}");
    let observation_id = if malformed {
        let operation_id = source_operation_id.clone();
        store
            .run(move |db| {
                let (observation_id, raw): (i64, String) = db.query_row(
                    "SELECT observation_id,payload_json FROM observations \
                     WHERE source_stream_id='controller' AND kind='task.submission' \
                       AND operation_id=?1",
                    [&operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let mut payload: Value = serde_json::from_str(&raw)?;
                if payload
                    .as_object_mut()
                    .and_then(|object| object.remove("submission_ref"))
                    .is_none()
                {
                    return Err(crate::error::Error::new(
                        "PR60_FIXTURE_SOURCE_INVALID",
                        "submission fixture did not contain its source identity",
                    ));
                }
                let changed = db.execute(
                    "UPDATE observations SET payload_json=?1 WHERE observation_id=?2",
                    params![model::canonical(&payload)?, observation_id],
                )?;
                if changed != 1 {
                    return Err(crate::error::Error::new(
                        "PR60_FIXTURE_SOURCE_CHANGED",
                        "submission fixture changed before the damage was retained",
                    ));
                }
                Ok(observation_id)
            })
            .await
            .unwrap()
    } else {
        store
            .run(move |db| {
                Ok(db.query_row(
                    "SELECT observation_id FROM observations \
                     WHERE source_stream_id='controller' AND kind='task.submission' \
                       AND operation_id=?1",
                    [&source_operation_id],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap()
    };

    ReviewDispatchSubject {
        automation_id,
        task_id: subject.task_id,
        observation_id,
    }
}

async fn corrupt_acceptance_event_identity(fixture: &PublicationFixture) -> i64 {
    let (observation_id, _, _) =
        retained_acceptance_observation(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let operation_id = fixture.accepted_operation_id.clone();
    let replacement_key = format!("accept:pr60-damaged-{operation_id}");
    let changed = fixture
        .owner
        .store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE observations SET source_event_key=?1 \
                 WHERE observation_id=?2 AND source_stream_id='controller:acceptance' \
                   AND kind='task.acceptance' AND operation_id=?3",
                params![replacement_key, observation_id, operation_id],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1, "the exact acceptance fact is damaged");
    observation_id
}

async fn review_dispatch_explanation(fixture: &PublicationFixture, automation_id: &str) -> Value {
    fixture
        .owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "automation.config.explain".to_owned(),
            json!({"project_id":"fixture","automation_id":automation_id}),
        )
        .await
        .unwrap()
}

fn assert_forge_preflight_worker_is_staged() {
    let host = std::env::current_exe()
        .expect("the Store test executable is available")
        .canonicalize()
        .expect("the Store test executable path is canonical");
    let worker_name = if cfg!(windows) {
        "swarm-forge-worker.exe"
    } else {
        "swarm-forge-worker"
    };
    let path = host.with_file_name(worker_name);
    let metadata = std::fs::symlink_metadata(&path).expect(
        "the native test harness must stage the built Forge worker beside the test executable",
    );
    assert!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "the staged Forge worker is a regular file"
    );
}

async fn review_assignment_count(store: &Store, task_id: &str) -> i64 {
    let task_id = task_id.to_owned();
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT count(*) FROM operations WHERE method='review.assign' AND task_id=?1",
                [task_id],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

fn publication_entry<'a>(pass: &'a Value, automation_id: &str) -> &'a Value {
    pass["publication"]["entries"]
        .as_array()
        .expect("the current publication domain returns its entry page")
        .iter()
        .find(|entry| entry["automation_id"] == automation_id)
        .expect("the fixture publication entry was reached")
}

#[tokio::test]
async fn earlier_review_commit_survives_later_publication_quarantine_without_replay() {
    assert_forge_preflight_worker_is_staged();
    let fixture = accepted_publication_fixture("pr60-review-commit-pub-quarantine").await;
    let review = seed_review_dispatch_subject(&fixture, "review-commit-survives", false).await;
    let acceptance_observation_id = corrupt_acceptance_event_identity(&fixture).await;

    let first_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(
        publication_entry(&first_pass, super::AUTOMATION_ID)["processed"],
        1
    );
    let dispatch = review_dispatch_explanation(&fixture, &review.automation_id).await;
    assert!(dispatch["dispatch"]["cursor"].as_i64().unwrap() >= review.observation_id);
    assert_eq!(
        review_assignment_count(&fixture.owner.store, &review.task_id).await,
        1,
        "the earlier ReviewDispatch Operation committed before publication quarantined its subject"
    );

    let publication = publication_explanation(&fixture).await;
    let quarantined = publication["publication"]["recent"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["observation_id"] == acceptance_observation_id)
        .expect("the malformed acceptance identity is retained as one subject disposition");
    assert_eq!(quarantined["status"], "quarantined");
    assert_eq!(quarantined["code"], "AUTOMATION_FACT_CORRUPT");
    assert_eq!(publication["publication"]["pending"], json!([]));
    assert!(
        retained_forge_operations(&fixture.owner.store)
            .await
            .is_empty()
    );

    let repeated_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(
        publication_entry(&repeated_pass, super::AUTOMATION_ID)["processed"],
        0
    );
    assert_eq!(
        review_assignment_count(&fixture.owner.store, &review.task_id).await,
        1,
        "the committed review source does not admit a duplicate Operation on the next pass"
    );
    close_fixture(fixture).await;
}

#[tokio::test]
async fn later_publication_commits_after_review_subject_quarantine() {
    assert_forge_preflight_worker_is_staged();
    let fixture = accepted_publication_fixture("pr60-review-quarantine-pub-progress").await;
    let review = seed_review_dispatch_subject(&fixture, "review-poison-isolated", true).await;

    let pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(
        publication_entry(&pass, super::AUTOMATION_ID)["processed"],
        1
    );
    assert_eq!(
        retained_forge_operations(&fixture.owner.store).await.len(),
        1
    );
    assert_eq!(
        review_assignment_count(&fixture.owner.store, &review.task_id).await,
        0,
        "the malformed ReviewDispatch source is quarantined without an assignment Operation"
    );

    let dispatch = review_dispatch_explanation(&fixture, &review.automation_id).await;
    assert!(dispatch["dispatch"]["cursor"].as_i64().unwrap() >= review.observation_id);
    assert!(
        dispatch["dispatch"]["recent"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["observation_id"] == review.observation_id
                    && item["disposition"] == "quarantined"
                    && item["reason"] == "automation_fact_corrupt"
            })
    );
    close_fixture(fixture).await;
}

fn work_dispatch_config() -> Config {
    let mut config = Config::default();
    config.routes.push(Route {
        admission_policy: Default::default(),
        workspace_option: None,
        owned_service: None,
        alias: "pr60-regression-route".to_owned(),
        runtime: "pr60-regression-runtime".to_owned(),
        module_artifact_id: "pr60-regression-artifact".to_owned(),
        enabled: true,
        native_options: json!({}),
    });
    config.mcp.profiles.insert(
        "pr60-work-participant".to_owned(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Participant,
            expected_client_id: "pr60-participant-template".to_owned(),
            surface: Some("participant-core".to_owned()),
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
    config
}

async fn write(store: &Store, principal: &Principal, method: &str, mut value: Value) -> Value {
    value["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
        .unwrap()
}

async fn select_work_dispatch_route(store: &Store, manager: &Principal) {
    let mut template = ModuleContractTemplate::codex_rust_controller_v5().unwrap();
    template.module_id = ModuleId::new("pr60-regression-runtime").unwrap();
    template.artifact.artifact_id = ArtifactId::new("pr60-regression-artifact").unwrap();
    template.artifact.version = ArtifactVersion::new("5").unwrap();
    let descriptor = template
        .descriptor(
            LaunchSpec {
                executable: if cfg!(windows) {
                    PathBuf::from(r"C:\modules\pr60-regression.exe")
                } else {
                    PathBuf::from("/modules/pr60-regression")
                },
                argv: Vec::new(),
                environment: Vec::new(),
                credential_ref: Some(ProtectedRef::new("credential:pr60-regression").unwrap()),
                working_directory: None,
                inherited_environment_allowlist: Default::default(),
                executable_sha256: Some(Sha256Digest::new("d".repeat(64)).unwrap()),
            },
            LifecycleOwnership::OwnedService,
            ActivationPolicy::OnDemand,
            true,
            RestartPolicy::default(),
        )
        .unwrap();
    let registered = store
        .run(move |db| {
            let tx = db.unchecked_transaction()?;
            let registered =
                crate::store::module_handshake::register_trusted_descriptor(&tx, descriptor)?;
            tx.commit()?;
            Ok(registered)
        })
        .await
        .unwrap();
    write(
        store,
        manager,
        "module.route.select",
        json!({
            "route_alias":"pr60-regression-route",
            "module_id":"pr60-regression-runtime",
            "artifact_id":"pr60-regression-artifact",
            "version":"5",
            "expected_catalog_revision":registered["catalog_revision"],
        }),
    )
    .await;
}

async fn retained_work_state(store: &Store, state_key: String) -> Value {
    store
        .run(move |db| {
            crate::automation::config::read_record(db, &state_key, "PR60 WorkDispatch state")?
                .ok_or_else(|| {
                    crate::error::Error::new(
                        "PR60_FIXTURE_STATE_MISSING",
                        "the configured WorkDispatch state disappeared",
                    )
                })
        })
        .await
        .unwrap()
}

async fn launch_operations(store: &Store, task_id: String) -> Vec<(String, String)> {
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT operation_id,caller_id FROM operations \
                 WHERE method='swarm.launch' AND task_id=?1 \
                 ORDER BY created_at_ms,operation_id",
            )?;
            let rows = statement
                .query_map([task_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
        .unwrap()
}

async fn change_attempt_state(
    store: &Store,
    task_id: String,
    attempt_id: String,
    from: &'static str,
    to: &'static str,
) {
    let changed = store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE attempts SET state=?1 WHERE task_id=?2 AND attempt_id=?3 \
                 AND state=?4 AND released_at_ms IS NULL",
                params![to, task_id, attempt_id, from],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1, "the exact fixture Attempt changes readiness");
}

#[tokio::test]
async fn work_dispatch_pending_second_item_preserves_and_resumes_tail() {
    const PROJECT_ID: &str = "pr60-work-tail-project";
    const AUTOMATION_ID: &str = "pr60-work-tail";

    let (owner, directory, bootstrap) =
        start_store_with_config("pr60-work-tail", work_dispatch_config()).await;
    seed_clients(&owner.store).await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    let manager = principal("review-gm", Role::Manager);
    select_work_dispatch_route(&owner.store, &manager).await;

    let mut facts = Vec::with_capacity(10);
    let mut running_attempts = Vec::with_capacity(9);
    for index in 0..10 {
        let task = write(
            &owner.store,
            &operator,
            "task.create",
            json!({
                "project_id":PROJECT_ID,
                "spec":{
                    "objective":format!("PR60 pending-tail subject {index}"),
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Retain this exact WorkDispatch subject"}],
                    "scope":{"initial_paths":[format!("src/pr60-tail-{index}.rs")]},
                    "owner_policy_id":OWNER_POLICY_V1_ID,
                }
            }),
        )
        .await;
        let task_id = task["task_id"].as_str().unwrap().to_owned();
        if index == 0 {
            facts.push((
                task_id,
                "task.create",
                task["operation_id"].as_str().unwrap().to_owned(),
                None,
            ));
        } else {
            let claim = write(
                &owner.store,
                &manager,
                "task.claim",
                json!({"task_id":task_id,"expected_revision":1}),
            )
            .await;
            let attempt_id = claim["attempt_id"].as_str().unwrap().to_owned();
            running_attempts.push((task_id.clone(), attempt_id.clone()));
            facts.push((
                task_id,
                "task.claim",
                claim["operation_id"].as_str().unwrap().to_owned(),
                Some(attempt_id),
            ));
        }
    }
    for (task_id, attempt_id) in &running_attempts {
        change_attempt_state(
            &owner.store,
            task_id.clone(),
            attempt_id.clone(),
            "reserved",
            "running",
        )
        .await;
    }

    let settings = json!({
        "route":"pr60-regression-route",
        "agent_profile":"pr60-regression-agent",
        "mcp_profile":"pr60-work-participant",
        "mcp_surface":"participant-core",
        "workspace_policy":"manager_owned_worktree",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"implementation",
    });
    let changes = json!([{
        "automation_id":AUTOMATION_ID,
        "expected_revision":0,
        "include_existing":false,
        "patch":{
            "enabled":true,
            "steps":["work_dispatch"],
            "work_dispatch":settings,
        }
    }]);
    let preview = owner
        .store
        .call(
            manager.clone(),
            "automation.config.preview".to_owned(),
            json!({"project_id":PROJECT_ID,"changes":changes.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true, "{preview}");
    let applied = write(
        &owner.store,
        &manager,
        "automation.config.apply",
        json!({
            "project_id":PROJECT_ID,
            "changes":changes,
            "preview_digest":preview["plan_sha256"],
        }),
    )
    .await;
    assert_eq!(applied["applied"], true, "{applied}");

    let first_task_id = facts[0].0.clone();
    let facts_for_store = facts;
    let state_key = owner
        .store
        .run(move |db| {
            let tx = db.unchecked_transaction()?;
            let state_key: String = tx.query_row(
                "SELECT key FROM meta WHERE key LIKE 'automation:v1:work-dispatch:state:%' \
                 ORDER BY key LIMIT 1",
                [],
                |row| row.get(0),
            )?;
            let mut state = crate::automation::config::read_record(
                &tx,
                &state_key,
                "PR60 WorkDispatch initial state",
            )?
            .ok_or_else(|| {
                crate::error::Error::new(
                    "PR60_FIXTURE_STATE_MISSING",
                    "configuration did not retain WorkDispatch state",
                )
            })?;
            let mut pending = Vec::with_capacity(facts_for_store.len());
            for (task_id, source_kind, source_operation_id, attempt_id) in facts_for_store {
                let observation_id = tx.query_row(
                    "SELECT observation_id FROM observations \
                     WHERE source_stream_id='controller' AND kind=?1 AND operation_id=?2",
                    params![source_kind, source_operation_id],
                    |row| row.get::<_, i64>(0),
                )?;
                pending.push(json!({
                    "observation_id":observation_id,
                    "source_kind":source_kind,
                    "source_operation_id":source_operation_id,
                    "task_id":task_id,
                    "task_revision":1,
                    "attempt_id":attempt_id,
                    "reason":"fixture_initial_pending",
                    "wake_when":["fixture_prerequisite_changed"],
                    "first_seen_at_ms":0,
                    "last_checked_at_ms":0,
                    "held":false,
                }));
            }
            state["pending"] = json!(pending);
            state["recheck_cursor"] = json!(0);
            crate::automation::config::write_record(&tx, &state_key, &state)?;
            tx.commit()?;
            Ok(state_key)
        })
        .await
        .unwrap();

    let first_pass = owner.store.reconcile_automations_once().await.unwrap();
    assert_eq!(first_pass["work_dispatch"]["entries"][0]["processed"], 8);
    let first_state = retained_work_state(&owner.store, state_key.clone()).await;
    let pending = first_state["pending"].as_array().unwrap();
    assert_eq!(pending.len(), 9);
    assert_eq!(
        pending
            .iter()
            .map(|item| item["task_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        running_attempts
            .iter()
            .map(|(task_id, _)| task_id.as_str())
            .collect::<Vec<_>>(),
        "item 1 settles while item 2 and the complete tail remain ordered"
    );
    assert!(
        pending[..7]
            .iter()
            .all(|item| item["last_checked_at_ms"].as_i64().unwrap() > 0)
    );
    assert_eq!(pending[0]["reason"], "work_subject_not_ready");
    assert_eq!(pending[7]["last_checked_at_ms"], 0);
    assert_eq!(pending[8]["last_checked_at_ms"], 0);
    let first_operations = launch_operations(&owner.store, first_task_id.clone()).await;
    assert_eq!(first_operations.len(), 1);
    assert_eq!(first_operations[0].1, AUTOMATION_TECHNICAL_REQUESTER_ID);

    let (second_task_id, second_attempt_id) = running_attempts[0].clone();
    change_attempt_state(
        &owner.store,
        second_task_id.clone(),
        second_attempt_id.clone(),
        "running",
        "reserved",
    )
    .await;
    let second_pass = owner.store.reconcile_automations_once().await.unwrap();
    assert_eq!(second_pass["work_dispatch"]["entries"][0]["processed"], 8);
    let second_state = retained_work_state(&owner.store, state_key).await;
    assert!(
        second_state["recent"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["task_id"] == second_task_id
                    && item["attempt_id"] == second_attempt_id
                    && item["disposition"] == "admitted"
            })
    );
    assert_eq!(
        second_state["pending"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["task_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        running_attempts[1..]
            .iter()
            .map(|(task_id, _)| task_id.as_str())
            .collect::<Vec<_>>(),
        "the repaired second subject is resumed exactly and the remaining tail stays ordered"
    );
    assert_eq!(
        launch_operations(&owner.store, second_task_id).await.len(),
        1
    );
    assert_eq!(
        launch_operations(&owner.store, first_task_id).await.len(),
        1
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
