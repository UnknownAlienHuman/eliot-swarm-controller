//! Ownership transfer keeps the existing automation journals attached to the
//! exact entry while retiring its former owner.
use super::{Store, StoreOwner};
use crate::{
    automation::config,
    config::{Config, McpProfileConfig, McpToolProfile, Route},
    error::{Error, Result},
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const PROJECT: &str = "automation-transfer-project";
const AUTOMATION_ID: &str = "transfer-preserves-cursors";

async fn start_store() -> (StoreOwner, PathBuf, Principal) {
    let directory =
        std::env::temp_dir().join(format!("swarm-automation-transfer-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.routes.push(Route {
        owned_service: None,
        alias: "transfer-regression-route".to_owned(),
        runtime: "transfer-regression-runtime".to_owned(),
        module_artifact_id: "transfer-regression-artifact".to_owned(),
        enabled: true,
        native_options: json!({}),
    });
    config.mcp.profiles.insert(
        "transfer-participant".to_owned(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Participant,
            expected_client_id: "transfer-participant-template".to_owned(),
            surface: Some("participant-core".to_owned()),
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
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

async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    let token = format!("automation-transfer-{client_id}-{}", model::new_id());
    write(
        store,
        operator,
        "client.register",
        json!({
            "client_id":client_id,
            "role":"manager",
            "token_hash":model::digest(token.as_bytes()),
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

fn entry_state_keys(
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<Vec<(&'static str, String)>> {
    let identity_digest = model::digest(
        model::canonical(&json!({
            "manager_id":owner,
            "project_id":project,
            "automation_id":automation_id
        }))?
        .as_bytes(),
    );
    Ok(vec![
        (
            "review_dispatch",
            config::dispatch_state_key(owner, project, automation_id)?,
        ),
        (
            "work_dispatch",
            format!("automation:v1:work-dispatch:state:{identity_digest}"),
        ),
        (
            "publication",
            format!(
                "automation:v1:publication:state:{}:{automation_id}",
                config::scope_digest(owner, project)?
            ),
        ),
        (
            "review_disposition",
            format!("automation:v1:review-disposition:state:{identity_digest}"),
        ),
    ])
}

fn cron_state_key(
    origin: &str,
    project: &str,
    automation_id: &str,
    generation: &str,
) -> Result<(String, String)> {
    let logical_id = model::digest(
        model::canonical(&json!({
            "cron_logical_entry_schema_version":1,
            "origin_manager_id":origin,
            "project_id":project,
            "automation_id":automation_id,
        }))?
        .as_bytes(),
    );
    Ok((
        logical_id.clone(),
        format!("automation:v1:cron:state:{logical_id}:{generation}"),
    ))
}

async fn read_cron_ledger(
    store: &Store,
    origin: &str,
    project: &str,
    automation_id: &str,
    generation: String,
) -> (Value, Value, i64) {
    let (logical_id, state_key) =
        cron_state_key(origin, project, automation_id, &generation).unwrap();
    store
        .run(move |db| {
            let state_json: String = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [state_key.as_str()],
                |row| row.get(0),
            )?;
            let state: Value = serde_json::from_str(&state_json)?;
            let wake_at_ms = state["indexed_due_at_ms"].as_i64().ok_or_else(|| {
                Error::new("TEST_CRON_STATE_MISSING", "cron state has no indexed wake")
            })?;
            let due_key = format!("automation:v1:cron:due:{wake_at_ms:020}:{logical_id}");
            let due_json: String = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [due_key.as_str()],
                |row| row.get(0),
            )?;
            let due: Value = serde_json::from_str(&due_json)?;
            let due_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key LIKE ?1",
                [format!("automation:v1:cron:due:%:{logical_id}")],
                |row| row.get(0),
            )?;
            Ok((state, due, due_count))
        })
        .await
        .unwrap()
}

async fn seed_pending_journals(
    store: &Store,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Vec<(String, Value, Value)> {
    let keys = entry_state_keys(owner, project, automation_id).unwrap();
    store
        .run(move |db| {
            let mut preserved = Vec::with_capacity(keys.len());
            // These schema-valid journal entries are inert references: this
            // test never reconciles them or grants authority from their JSON.
            for (kind, key) in keys {
                let mut state = config::read_record(db, &key, "automation transfer state")?
                    .ok_or_else(|| {
                        Error::new(
                            "TEST_AUTOMATION_STATE_MISSING",
                            format!("missing initialized {kind} state"),
                        )
                    })?;
                let activation_cut = state["activation_cut"].as_i64().unwrap_or(0);
                let observation_id = activation_cut.max(1);
                let pending = match kind {
                    "review_dispatch" => json!([{
                        "cause":{
                            "kind":"applied_submission",
                            "observation_id":observation_id,
                            "operation_id":"retained-submission-operation",
                            "id":"retained-submission-artifact"
                        },
                        "reason":"reviewer_unavailable",
                        "wake_when":["reviewer_registration"],
                        "first_seen_at_ms":1,
                        "last_checked_at_ms":1,
                        "held":true
                    }]),
                    "work_dispatch" => json!([{
                        "observation_id":observation_id,
                        "source_kind":"task.create",
                        "source_operation_id":"retained-task-operation",
                        "task_id":"retained-task-reference",
                        "task_revision":1,
                        "attempt_id":null,
                        "reason":"workspace_unavailable",
                        "wake_when":["workspace"],
                        "first_seen_at_ms":1,
                        "last_checked_at_ms":1,
                        "held":true
                    }]),
                    "publication" => json!([{
                        "observation_id":observation_id,
                        "accepted_operation_id":"retained-acceptance-operation",
                        "historical_replay_authorized":observation_id <= activation_cut,
                        "retries":2,
                        "next_retry_at_ms":2000
                    }]),
                    "review_disposition" => json!([{
                        "observation_id":observation_id,
                        "review_assignment_id":"retained-review-assignment",
                        "review_result_operation_id":"retained-review-result-operation",
                        "retries":2,
                        "next_retry_at_ms":2000
                    }]),
                    _ => unreachable!("state key list has a closed set of namespaces"),
                };
                state["pending"] = pending.clone();
                if kind == "review_dispatch" {
                    state["pending_after_observation_id"] = json!(observation_id);
                }
                config::write_record(db, &key, &state)?;
                preserved.push((kind.to_owned(), state["cursor"].clone(), pending));
            }
            Ok(preserved)
        })
        .await
        .unwrap()
}

async fn read_state_records(
    store: &Store,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Vec<(&'static str, Option<Value>)> {
    let keys = entry_state_keys(owner, project, automation_id).unwrap();
    store
        .run(move |db| {
            keys.into_iter()
                .map(|(kind, key)| {
                    Ok((
                        kind,
                        config::read_record(db, &key, "automation transfer state")?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })
        .await
        .unwrap()
}

async fn operation_snapshot(
    store: &Store,
    operation_id: &str,
) -> (String, String, String, Option<String>) {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            db.query_row(
                "SELECT state,original_request_json,effective_request_json,result_json \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn current_gm_transfer_preserves_all_entry_journals_and_retires_old_owner() {
    let (owner, _directory, operator) = start_store().await;
    let former = register_manager(&owner.store, &operator, "transfer-former").await;
    let successor = register_manager(&owner.store, &operator, "transfer-successor").await;
    let unrelated = register_manager(&owner.store, &operator, "transfer-unrelated").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":former.client_id}),
    )
    .await;

    // Establish a real observation before activation so each consumer retains
    // an exact, nonzero source watermark without dispatching native work.
    write(
        &owner.store,
        &operator,
        "task.create",
        json!({
            "project_id":PROJECT,
            "spec":{
                "objective":"Exercise durable automation ownership transfer",
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Keep consumer journals exact"}],
                "scope":{"initial_paths":["src/transfer-target.rs"]},
                "owner_policy_id":crate::policy::OWNER_POLICY_V1_ID
            }
        }),
    )
    .await;

    let work_dispatch = json!({
        "route":"transfer-regression-route",
        "agent_profile":"transfer-agent",
        "mcp_profile":"transfer-participant",
        "mcp_surface":"participant-core",
        "workspace_policy":"manager_owned_worktree",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"implementation"
    });
    // Keep a real enabled cron generation in this fixture. The four owner-
    // scoped journals below are separate from cron's stable logical-entry
    // state, which transfer relocates by updating its current-owner index.
    let cron_anchor_ms = model::now_ms().unwrap().saturating_add(86_400_000);
    let cron_calendar = crate::scheduler::calendar::CalendarDefinition {
        expression: "0 0 0 * * *".to_owned(),
        timezone: "UTC".to_owned(),
        anchor_ms: cron_anchor_ms,
    };
    let cron_generation = crate::scheduler::calendar::generation_digest(&cron_calendar).unwrap();
    let changes = json!([{
        "automation_id":AUTOMATION_ID,
        "expected_revision":0,
        "include_existing":false,
        "patch":{
            "enabled":true,
            "scope":{"work_pool_id":null},
            "steps":[
                "review_dispatch",
                "review_disposition",
                "work_dispatch",
                "publication",
                "check_run"
            ],
            "review":{"profile":"transfer-auditor","required_reviewers":1},
            "work_dispatch":work_dispatch,
            "publication":{
                "target_ref":"refs/heads/main",
                "expected_old_ref":null,
                "expected_create":true
            },
            "cron":{
                "calendar":cron_calendar.clone(),
                "action":{
                    "kind":"check_run",
                    "attempt_id":"00000000-0000-4000-8000-000000000001",
                    "expected_task_revision":1,
                    "candidate_ref":"transfer-cron-candidate",
                    "profile_id":"strict",
                    "profile_revision":"v1"
                }
            }
        }
    }]);
    let preview = owner
        .store
        .call(
            former.clone(),
            "automation.config.preview".to_owned(),
            json!({"project_id":PROJECT,"changes":changes}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true);
    let applied = owner
        .store
        .call(
            former.clone(),
            "automation.config.apply".to_owned(),
            json!({
                "client_request_id":"enable-transfer-regression",
                "project_id":PROJECT,
                "changes":changes,
                "preview_digest":preview["plan_sha256"]
            }),
        )
        .await
        .unwrap();
    assert_eq!(applied["applied"], true);
    let config_operation_id = applied["operation_id"].as_str().unwrap().to_owned();
    let old_operation_snapshot = operation_snapshot(&owner.store, &config_operation_id).await;
    let old_journals =
        seed_pending_journals(&owner.store, &former.client_id, PROJECT, AUTOMATION_ID).await;
    assert_eq!(old_journals.len(), 4);
    let (old_cron_state, old_cron_due, old_cron_due_count) = read_cron_ledger(
        &owner.store,
        &former.client_id,
        PROJECT,
        AUTOMATION_ID,
        cron_generation.clone(),
    )
    .await;
    assert_eq!(old_cron_due_count, 1);
    assert_eq!(old_cron_state["origin_manager_id"], former.client_id);
    assert_eq!(
        old_cron_state["indexed_due_at_ms"],
        old_cron_due["wake_at_ms"]
    );
    assert_eq!(old_cron_due["current_owner_manager_id"], former.client_id);

    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":successor.client_id}),
    )
    .await;

    let unrelated_result = owner
        .store
        .call(
            unrelated.clone(),
            "automation.config.transfer".to_owned(),
            json!({
                "client_request_id":"unrelated-transfer-attempt",
                "project_id":PROJECT,
                "former_owner_manager_id":former.client_id,
                "automation_id":AUTOMATION_ID,
                "expected_revision":1
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(unrelated_result.code, "FORBIDDEN");

    let stale_revision = owner
        .store
        .call(
            successor.clone(),
            "automation.config.transfer".to_owned(),
            json!({
                "client_request_id":"stale-transfer-attempt",
                "project_id":PROJECT,
                "former_owner_manager_id":former.client_id,
                "automation_id":AUTOMATION_ID,
                "expected_revision":99
            }),
        )
        .await;
    assert_eq!(
        stale_revision.unwrap_err().code,
        "AUTOMATION_TRANSFER_STALE"
    );
    let source_before_transfer: Value = owner
        .store
        .run({
            let former_id = former.client_id.clone();
            move |db| {
                config::read_record(
                    db,
                    &config::entry_key(&former_id, PROJECT, AUTOMATION_ID)?,
                    "automation transfer entry",
                )?
                .ok_or_else(|| Error::new("TEST_AUTOMATION_ENTRY_MISSING", "source entry missing"))
            }
        })
        .await
        .unwrap();
    assert_eq!(source_before_transfer["revision"], 1);
    assert_eq!(source_before_transfer["enabled"], true);

    let transfer = owner
        .store
        .call(
            successor.clone(),
            "automation.config.transfer".to_owned(),
            json!({
                "client_request_id":"transfer-to-successor",
                "project_id":PROJECT,
                "former_owner_manager_id":former.client_id,
                "automation_id":AUTOMATION_ID,
                "expected_revision":1
            }),
        )
        .await
        .unwrap();
    assert_eq!(transfer["status"], "transferred");
    assert_eq!(transfer["state_ledgers_relocated"], 6);
    assert_eq!(transfer["new_owner_manager_id"], successor.client_id);
    let transfer_operation_id = transfer["transfer_operation_id"].as_str().unwrap();

    let target_journals =
        read_state_records(&owner.store, &successor.client_id, PROJECT, AUTOMATION_ID).await;
    assert_eq!(target_journals.len(), 4);
    for (kind, cursor, pending) in old_journals {
        let record = target_journals
            .iter()
            .find(|(target_kind, _)| *target_kind == kind.as_str())
            .and_then(|(_, value)| value.as_ref())
            .unwrap_or_else(|| panic!("target {kind} journal is missing"));
        assert_eq!(record["owner_manager_id"], successor.client_id);
        assert_eq!(record["project_id"], PROJECT);
        assert_eq!(record["automation_id"], AUTOMATION_ID);
        assert_eq!(record["cursor"], cursor, "{kind} cursor changed");
        assert_eq!(record["pending"], pending, "{kind} pending refs changed");
    }
    let retired_journals =
        read_state_records(&owner.store, &former.client_id, PROJECT, AUTOMATION_ID).await;
    assert!(retired_journals.iter().all(|(_, value)| value.is_none()));

    let (target_cron_state, target_cron_due, target_cron_due_count) = read_cron_ledger(
        &owner.store,
        &former.client_id,
        PROJECT,
        AUTOMATION_ID,
        cron_generation,
    )
    .await;
    assert_eq!(
        target_cron_due_count, 1,
        "cron due index was duplicated or lost"
    );
    let mut expected_cron_state = old_cron_state.clone();
    expected_cron_state["updated_at_ms"] = target_cron_state["updated_at_ms"].clone();
    assert_eq!(target_cron_state, expected_cron_state);
    let mut expected_cron_due = old_cron_due.clone();
    expected_cron_due["current_owner_manager_id"] = json!(successor.client_id);
    assert_eq!(target_cron_due, expected_cron_due);
    assert_eq!(target_cron_state["origin_manager_id"], former.client_id);

    let (source, target, lineage, successors) = {
        let former_id = former.client_id.clone();
        let successor_id = successor.client_id.clone();
        owner
            .store
            .run(move |db| {
                let source = config::read_record(
                    db,
                    &config::entry_key(&former_id, PROJECT, AUTOMATION_ID)?,
                    "automation transfer entry",
                )?
                .ok_or_else(|| {
                    Error::new("TEST_AUTOMATION_ENTRY_MISSING", "source entry missing")
                })?;
                let target = config::read_record(
                    db,
                    &config::entry_key(&successor_id, PROJECT, AUTOMATION_ID)?,
                    "automation transfer entry",
                )?
                .ok_or_else(|| {
                    Error::new("TEST_AUTOMATION_ENTRY_MISSING", "target entry missing")
                })?;
                let lineage = serde_json::to_value(config::transfer_lineage(
                    db,
                    &successor_id,
                    PROJECT,
                    AUTOMATION_ID,
                )?)?;
                let successors = serde_json::to_value(config::transfer_successors(
                    db,
                    &former_id,
                    PROJECT,
                    AUTOMATION_ID,
                )?)?;
                Ok((source, target, lineage, successors))
            })
            .await
            .unwrap()
    };
    assert_eq!(source["enabled"], false);
    assert_eq!(target["enabled"], true);
    assert_eq!(target["owner_manager_id"], successor.client_id);
    assert_eq!(target["revision"], 2);
    assert_eq!(
        target["cron"], source["cron"],
        "transfer changed cron configuration"
    );
    assert_eq!(lineage[0]["transfer_operation_id"], transfer_operation_id);
    assert_eq!(lineage[0]["former_owner_manager_id"], former.client_id);
    assert_eq!(lineage[0]["new_owner_manager_id"], successor.client_id);
    assert_eq!(
        successors[0]["transfer_operation_id"],
        transfer_operation_id
    );
    assert_eq!(
        operation_snapshot(&owner.store, &config_operation_id).await,
        old_operation_snapshot,
        "transfer rewrote the former owner's original config history"
    );

    let retired_reenable = owner
        .store
        .call(
            former.clone(),
            "automation.config.apply".to_owned(),
            json!({
                "client_request_id":"retired-owner-reenable-attempt",
                "project_id":PROJECT,
                "changes":[{
                    "automation_id":AUTOMATION_ID,
                    "expected_revision":1,
                    "include_existing":false,
                    "patch":{"enabled":true}
                }]
            }),
        )
        .await;
    assert_eq!(
        retired_reenable.unwrap_err().code,
        "AUTOMATION_TRANSFER_RETIRED"
    );
    let current_owner = owner
        .store
        .call(
            successor.clone(),
            "automation.config.get".to_owned(),
            json!({"project_id":PROJECT}),
        )
        .await
        .unwrap();
    assert_eq!(current_owner["owner_manager_id"], successor.client_id);
    assert_eq!(current_owner["items"][0]["automation_id"], AUTOMATION_ID);
    assert_eq!(current_owner["items"][0]["enabled"], true);
    let retired_owner = owner
        .store
        .call(
            former,
            "automation.config.get".to_owned(),
            json!({"project_id":PROJECT}),
        )
        .await
        .unwrap();
    assert_eq!(retired_owner["items"][0]["enabled"], false);
    let _ = unrelated;
}
