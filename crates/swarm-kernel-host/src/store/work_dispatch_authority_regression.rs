//! Public Store regressions for the remaining PR74 WorkDispatch, minimum-size,
//! and retained-history boundaries.

use super::{
    Store, StoreOwner, register_manager, regression_config, select_regression_descriptor,
    start_store, write,
};
use crate::{
    automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
    model::{self, Principal},
    policy::OWNER_POLICY_V1_ID,
};
use rusqlite::OptionalExtension;
use serde_json::{Value, json};

#[cfg(not(debug_assertions))]
use super::super::projection;
use super::super::{automation_work_dispatch as work_dispatch, launcher as store_launcher};

fn public_preview_request(task_id: &str, expected_task_revision: i64) -> Value {
    json!({
        "task_id":task_id,
        "expected_task_revision":expected_task_revision,
        "route":"regression-route",
        "agent_profile":"workspace-admission-agent",
        "mcp_profile":"work-participant",
        "mcp_surface":"participant-core",
        "workspace_policy":"manager_owned_worktree",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"implementation",
    })
}

async fn public_preview(store: &Store, manager: &Principal, request: Value) -> Value {
    store
        .call(manager.clone(), "swarm.launch.preview".to_owned(), request)
        .await
        .unwrap()
}

fn work_dispatch_settings(preview_request: &Value) -> Value {
    let mut settings = preview_request.clone();
    let object = settings
        .as_object_mut()
        .expect("preview request is an object");
    object.remove("task_id");
    object.remove("expected_task_revision");
    Value::Object(object.clone())
}

async fn apply_work_dispatch(
    store: &Store,
    manager: &Principal,
    project_id: &str,
    automation_id: &str,
    include_existing: bool,
    settings: &Value,
) -> Value {
    let changes = json!([{
        "automation_id":automation_id,
        "expected_revision":0,
        "include_existing":include_existing,
        "patch":{
            "enabled":true,
            "steps":["work_dispatch"],
            "work_dispatch":settings,
        }
    }]);
    let preview = store
        .call(
            manager.clone(),
            "automation.config.preview".to_owned(),
            json!({"project_id":project_id,"changes":changes}),
        )
        .await
        .expect("preview WorkDispatch automation");
    assert_eq!(preview["valid"], true, "{preview}");
    let applied = write(
        store,
        manager,
        "automation.config.apply",
        json!({
            "project_id":project_id,
            "changes":changes,
            "preview_digest":preview["plan_sha256"],
        }),
    )
    .await;
    assert_eq!(applied["applied"], true, "{applied}");
    applied
}

async fn seed_oversized_authority_dependencies(
    owner: &StoreOwner,
    operator: &Principal,
) -> Vec<Value> {
    let mut dependencies = Vec::new();
    for index in 0..64 {
        let phase = format!("phase-{index}-{}", "p".repeat(4_096));
        let dependency = write(
            &owner.store,
            operator,
            "task.create",
            json!({"project_id":"preview-authority-project","spec":{
                "objective":"Retained accepted dependency for the preview fixture",
                "phase":phase,
                "requirements":[{"id":"R1","statement":"Accepted fixture dependency"}],
                "owner_policy_id":OWNER_POLICY_V1_ID,
            }}),
        )
        .await;
        let dependency_task_id = dependency["task_id"].as_str().unwrap().to_owned();
        dependencies.push(json!({
            "task_id":dependency_task_id,
            "required_revision":1,
            "required_phase":phase,
        }));

        // Pin actual public Task IDs to original acceptance facts while their
        // current revisions advance, matching the existing PR74 A fixture.
        let caller_id = operator.client_id.clone();
        owner
            .store
            .run(move |db| {
                let acceptance_id = model::new_id();
                let result = json!({
                    "outcome":"applied",
                    "acceptance_operation_id":acceptance_id,
                    "task_id":dependency_task_id,
                    "task_revision":1,
                    "phase":phase,
                });
                db.execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) \
                     VALUES(?1,?2,?1,'task.accept','{}','{}',?3,'settled',?4,1,1,1,1)",
                    rusqlite::params![
                        acceptance_id,
                        caller_id,
                        dependency_task_id,
                        model::canonical(&result)?
                    ],
                )?;
                db.execute(
                    "UPDATE tasks SET revision=2,updated_at_ms=2 WHERE task_id=?1",
                    [dependency_task_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    dependencies
}

async fn create_oversized_authority_task(
    owner: &StoreOwner,
    operator: &Principal,
    dependencies: Vec<Value>,
) -> (String, String) {
    let task = write(
        &owner.store,
        operator,
        "task.create",
        json!({"project_id":"preview-authority-project","spec":{
            "objective":"Keep private launch authority independent of preview size",
            "phase":"implementation",
            "requirements":[{"id":"R1","statement":"Retain exact Task authority"}],
            "dependencies":dependencies,
            "scope":{"initial_paths":["src/launch-authority.rs"]},
            "owner_policy_id":OWNER_POLICY_V1_ID,
        }}),
    )
    .await;
    (
        task["task_id"].as_str().unwrap().to_owned(),
        task["operation_id"].as_str().unwrap().to_owned(),
    )
}

#[cfg(not(debug_assertions))]
fn require_release_build() {}

#[tokio::test]
async fn pr74_remaining_b_work_dispatch_preserves_preimage_manager_scope_and_slot() {
    let (owner, directory, operator, _) = start_store().await;
    let manager = register_manager(&owner.store, &operator, "pr74-work-dispatch-manager").await;
    let outsider = register_manager(&owner.store, &operator, "pr74-work-dispatch-outsider").await;
    select_regression_descriptor(&owner.store, &manager, "regression-route").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;

    let dependencies = seed_oversized_authority_dependencies(&owner, &operator).await;
    let settings = work_dispatch_settings(&public_preview_request("work-dispatch-settings", 1));
    let configured = apply_work_dispatch(
        &owner.store,
        &manager,
        "preview-authority-project",
        "pr74-oversized-work-dispatch",
        false,
        &settings,
    )
    .await;
    assert_eq!(configured["applied"], true, "{configured}");

    // Activation starts after the 64 pinned dependency Tasks. The actual
    // target is created afterward and reaches the real Store WorkDispatch pass.
    let (task_id, task_source_operation_id) =
        create_oversized_authority_task(&owner, &operator, dependencies).await;
    let request = public_preview_request(&task_id, 1);
    let preview = public_preview(&owner.store, &manager, request.clone()).await;
    assert_eq!(preview["hard_blocks"], json!([]), "{preview}");
    assert_eq!(preview["authority_projection"], "detached", "{preview}");

    let changed_request = {
        let mut changed = request.clone();
        changed["purpose"] = json!("drifted-purpose");
        changed
    };
    let changed_preview = public_preview(&owner.store, &manager, changed_request.clone()).await;
    assert_ne!(changed_preview["plan_digest"], preview["plan_digest"]);

    let _reconciliation = owner.store.reconcile_automations_once().await.unwrap();
    let task_id_for_operation = task_id.clone();
    let operation_id = owner
        .store
        .run(move |db| {
            db.query_row(
                "SELECT operation_id FROM operations WHERE method='swarm.launch' AND caller_id=?1 AND task_id=?2 AND state='queued'",
                rusqlite::params![AUTOMATION_TECHNICAL_REQUESTER_ID, task_id_for_operation],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();

    let manager_id = manager.client_id.clone();
    let operation_id_for_read = operation_id.clone();
    let (operation, link, slot_before) = owner
        .store
        .run(move |db| {
            let operation = db.query_row(
                "SELECT caller_id,state,task_id,attempt_id,binding_id,binding_generation,effective_request_json \
                 FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
                [&operation_id_for_read],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )?;
            let link = work_dispatch::operation_link(db, &operation_id_for_read)?
                .ok_or_else(|| crate::error::Error::new("TEST_LINK_MISSING", "WorkDispatch link missing"))?;
            let key = format!("launch:v1:semantic-slot:{}", link.semantic_slot_id);
            let slot = db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                row.get::<_, String>(0)
            })?;
            Ok((operation, link, slot))
        })
        .await
        .unwrap();

    assert_eq!(operation.0, AUTOMATION_TECHNICAL_REQUESTER_ID);
    assert_eq!(operation.1, "queued");
    assert_eq!(operation.2.as_deref(), Some(task_id.as_str()));
    assert_eq!(operation.3, None);
    assert_eq!(operation.4, None);
    assert_eq!(operation.5, None);
    assert_eq!(
        link.technical_requester_id,
        AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(link.effective_manager_id, manager_id);
    assert_eq!(link.task_id, task_id);
    assert_eq!(link.project_id, "preview-authority-project");
    assert_eq!(link.task_revision, 1);
    assert_eq!(link.attempt_id, None);
    assert_eq!(link.semantic_cause_kind, "initial_task");
    assert_eq!(link.source["operation_id"], task_source_operation_id);

    let effective: Value = serde_json::from_str(&operation.6).unwrap();
    let authority = &effective["launch_plan_authority"];
    let manifest = &effective["launch_manifest"];
    let authority_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(authority).unwrap().as_bytes())
    );
    assert_eq!(authority_digest, preview["plan_digest"]);
    assert_eq!(manifest["plan_digest"], preview["plan_digest"]);
    assert_eq!(manifest["request"], authority["requested_plan_facts"]);
    assert_eq!(manifest["runtime"]["route"], authority["route"]);
    assert_eq!(manifest["mcp"], authority["mcp"]);
    assert_eq!(
        manifest["task"]["candidate_scope"],
        authority["candidate_scope"]
    );
    assert_eq!(manifest["task"]["task_id"], task_id);
    assert_eq!(manifest["task"]["observed_revision"], 1);
    assert_eq!(manifest["task"]["expected_revision"], 1);
    assert_eq!(authority["task"]["task_id"], task_id);
    assert_eq!(authority["task"]["revision"], 1);
    assert_eq!(authority["current_attempt"]["attempt_id"], Value::Null);
    assert_eq!(
        manifest["task"]["attempt_id"],
        authority["current_attempt"]["attempt_id"]
    );
    assert_eq!(
        manifest["actor"]["client_id"],
        AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(manifest["actor"]["effective_manager_id"], manager_id);
    assert_eq!(manifest["actor"]["semantic_slot_id"], link.semantic_slot_id);
    assert_eq!(manifest["runtime"]["native_effect"], "not_attempted");

    let visible = owner
        .store
        .call(
            manager.clone(),
            "operation.get".to_owned(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(visible["task_id"], task_id);
    assert!(visible.get("caller_id").is_none());
    let hidden = owner
        .store
        .call(
            outsider,
            "operation.get".to_owned(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(hidden.code, "NOT_FOUND");

    let mut alias_request = request.clone();
    alias_request["client_request_id"] =
        json!(format!("pr74-work-dispatch-alias-{}", model::new_id()));
    alias_request["plan_digest"] = preview["plan_digest"].clone();
    let alias = owner
        .store
        .call(manager.clone(), "swarm.launch".to_owned(), alias_request)
        .await
        .unwrap();
    assert_eq!(alias["semantic_reuse"], true);
    assert_eq!(alias["operation_id"], operation_id);

    let changed_request_id = format!("pr74-work-dispatch-drift-{}", model::new_id());
    let mut changed_launch = changed_request;
    changed_launch["client_request_id"] = json!(changed_request_id.clone());
    changed_launch["plan_digest"] = changed_preview["plan_digest"].clone();
    let conflict = owner
        .store
        .call(manager.clone(), "swarm.launch".to_owned(), changed_launch)
        .await
        .unwrap_err();
    assert_eq!(conflict.code, "LAUNCH_SLOT_CONFLICT");

    let operation_id_for_after = operation_id.clone();
    let caller_for_after = manager.client_id.clone();
    let task_for_after = task_id.clone();
    let changed_request_id_for_after = changed_request_id.clone();
    let (slot_after, admitted_for_task, workspace_leases, changed_operation) = owner
        .store
        .run(move |db| {
            let key = format!("launch:v1:semantic-slot:{}", link.semantic_slot_id);
            let slot: String = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [&key],
                |row| row.get(0),
            )?;
            let admitted: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method='swarm.launch' AND task_id=?1 AND state='queued'",
                [&task_for_after],
                |row| row.get(0),
            )?;
            let leases: i64 = db.query_row(
                "SELECT COUNT(*) FROM workspace_leases WHERE operation_id=?1",
                [&operation_id_for_after],
                |row| row.get(0),
            )?;
            let changed: Option<(String, Option<String>, Option<String>, String)> = db
                .query_row(
                    "SELECT state,task_id,attempt_id,effective_request_json FROM operations \
                     WHERE caller_id=?1 AND client_request_id=?2 AND method='swarm.launch'",
                    rusqlite::params![caller_for_after, changed_request_id_for_after],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            Ok((slot, admitted, leases, changed))
        })
        .await
        .unwrap();
    assert_eq!(
        slot_after, slot_before,
        "changed authority altered the retained slot"
    );
    assert_eq!(
        admitted_for_task, 1,
        "changed authority created a shadow launch"
    );
    assert_eq!(
        workspace_leases, 0,
        "WorkDispatch did not start workspace work"
    );
    if let Some((state, task_id, attempt_id, effective)) = changed_operation {
        assert_eq!(state, "rejected");
        assert_eq!(task_id, None);
        assert_eq!(attempt_id, None);
        let effective: Value = serde_json::from_str(&effective).unwrap();
        assert!(effective.get("launch_manifest").is_none());
        assert!(effective.get("launch_plan_authority").is_none());
    }

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(not(debug_assertions))]
#[tokio::test]
async fn pr74_remaining_c_irreducible_minimum_is_rejected_in_release_before_effects() {
    require_release_build();

    let (owner, directory, operator, _) = start_store().await;
    let manager = register_manager(&owner.store, &operator, "pr74-minimum-manager").await;
    select_regression_descriptor(&owner.store, &manager, "regression-route").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;

    // project_id is accepted as a nonempty Task identity string. Its exact
    // retained read reference makes even the terminal detached projection too
    // large, while task_id remains a real ID returned by task.create.
    let project_id = "x".repeat(projection::MAX_SERIALIZED_BYTES + 1);
    let task = write(
        &owner.store,
        &operator,
        "task.create",
        json!({"project_id":project_id,"spec":{
            "objective":"Reject an irreducible launch preview",
            "phase":"implementation",
            "requirements":[{"id":"R1","statement":"The minimum response remains bounded"}],
            "scope":{"initial_paths":["src/minimum-projection.rs"]},
            "owner_policy_id":OWNER_POLICY_V1_ID,
        }}),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let mut request = public_preview_request(&task_id, 1);
    let preview_error = owner
        .store
        .call(
            manager.clone(),
            "swarm.launch.preview".to_owned(),
            request.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(preview_error.code, "PAYLOAD_TOO_LARGE", "{preview_error}");

    let request_id = format!("pr74-minimum-rejected-{}", model::new_id());
    request["client_request_id"] = json!(request_id);
    request["plan_digest"] = json!(format!("sha256:{}", "0".repeat(64)));
    let launch_error = owner
        .store
        .call(manager.clone(), "swarm.launch".to_owned(), request.clone())
        .await
        .unwrap_err();
    assert_eq!(launch_error.code, "PAYLOAD_TOO_LARGE", "{launch_error}");

    let slot_preview = public_preview_request(&task_id, 1);
    let parsed = crate::launcher::LaunchPreviewRequest::parse(&slot_preview).unwrap();
    let (slot_id, _) =
        work_dispatch::slot_identity_for_assignment(&manager.client_id, &task_id, 1, None, &parsed)
            .unwrap();
    let caller_id = manager.client_id.clone();
    let request_id_for_read = request["client_request_id"].as_str().unwrap().to_owned();
    let task_id_for_read = task_id.clone();
    let slot_key = format!("launch:v1:semantic-slot:{slot_id}");
    let retained = owner
        .store
        .run(move |db| {
            let row = db.query_row(
                "SELECT operation_id,state,task_id,attempt_id,binding_id,binding_generation,effective_request_json,result_json \
                 FROM operations WHERE caller_id=?1 AND client_request_id=?2 AND method='swarm.launch'",
                rusqlite::params![caller_id, request_id_for_read],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )?;
            let slot_exists: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
                [&slot_key],
                |row| row.get(0),
            )?;
            let leases: i64 = db.query_row(
                "SELECT COUNT(*) FROM workspace_leases WHERE operation_id=?1",
                [&row.0],
                |row| row.get(0),
            )?;
            let child_effects: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE task_id=?1 AND method IN ('agent.open','task.dispatch')",
                [&task_id_for_read],
                |row| row.get(0),
            )?;
            Ok((row, slot_exists, leases, child_effects))
        })
        .await
        .unwrap();

    let row = retained.0;
    assert_eq!(row.1, "rejected");
    assert_eq!(row.2, None);
    assert_eq!(row.3, None);
    assert_eq!(row.4, None);
    assert_eq!(row.5, None);
    assert!(!retained.1, "rejected preview retained a semantic slot");
    assert_eq!(retained.2, 0, "rejected preview retained a workspace lease");
    assert_eq!(
        retained.3, 0,
        "rejected preview created a native child Operation"
    );
    let effective: Value = serde_json::from_str(&row.6).unwrap();
    assert_eq!(effective["receipt"]["ok"], false);
    assert!(effective.get("launch_manifest").is_none());
    assert!(effective.get("launch_plan_authority").is_none());
    let result: Value = serde_json::from_str(&row.7).unwrap();
    assert_eq!(result["code"], "PAYLOAD_TOO_LARGE");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn pr74_remaining_d_malformed_retained_history_stays_damaged_at_effect_gate() {
    let (owner, directory, operator, _) = start_store().await;
    let manager = register_manager(&owner.store, &operator, "pr74-history-manager").await;
    select_regression_descriptor(&owner.store, &manager, "regression-route").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;
    let task = write(
        &owner.store,
        &operator,
        "task.create",
        json!({"project_id":"pr74-history-project","spec":{
            "objective":"Retain a valid launch before damaging its history",
            "phase":"implementation",
            "requirements":[{"id":"R1","statement":"Malformed history must remain diagnostic"}],
            "scope":{"initial_paths":["src/retained-history.rs"]},
            "owner_policy_id":OWNER_POLICY_V1_ID,
        }}),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let applied = apply_work_dispatch(
        &owner.store,
        &manager,
        "pr74-history-project",
        "pr74-malformed-history",
        true,
        &work_dispatch_settings(&public_preview_request(&task_id, 1)),
    )
    .await;
    let recent = applied["work_dispatch"][0]["recent"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["task_id"] == task_id)
        .unwrap_or_else(|| panic!("the exact retained Task is admitted: {applied}"));
    assert_eq!(recent["disposition"], "admitted", "{recent}");
    let operation_id = recent["operation_id"].as_str().unwrap().to_owned();

    let operation_id_for_damage = operation_id.clone();
    let task_id_for_damage = task_id.clone();
    let damaged_json = owner
        .store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
                [&operation_id_for_damage],
                |row| row.get(0),
            )?;
            let mut effective: Value = serde_json::from_str(&raw)?;
            assert_eq!(
                effective["launch_manifest"]["task"]["task_id"].as_str(),
                Some(task_id_for_damage.as_str())
            );
            assert_eq!(effective["launch_manifest"]["runtime"]["route"]["alias"], "regression-route");
            assert_eq!(effective["launch_manifest"]["mcp"]["profile_name"], "work-participant");
            effective
                .as_object_mut()
                .expect("retained Operation is an object")
                .remove("launch_plan_authority");
            effective["launch_manifest"]
                .as_object_mut()
                .expect("retained launch manifest is an object")
                .remove("mcp");
            effective["launch_manifest"]["request"]
                .as_object_mut()
                .expect("retained launch request is an object")
                .remove("route");
            let damaged_json = model::canonical(&effective)?;
            db.execute(
                "UPDATE operations SET effective_request_json=?2 WHERE operation_id=?1 AND method='swarm.launch'",
                rusqlite::params![operation_id_for_damage, &damaged_json],
            )?;
            Ok(damaged_json)
        })
        .await
        .unwrap();
    let current_config = regression_config(&directory);
    assert!(
        current_config
            .routes
            .iter()
            .any(|route| route.alias == "regression-route")
    );
    let operation_id_for_gate = operation_id.clone();
    let diagnostic = owner
        .store
        .run(move |db| {
            Ok(store_launcher::current_launch_admission(
                db,
                &operation_id_for_gate,
                &current_config,
            ))
        })
        .await
        .unwrap()
        .expect_err("retained history must not reconstruct a missing route from current config");
    assert_eq!(diagnostic.code, "INVALID_PARAMS", "{diagnostic}");

    let operation_id_for_read = operation_id.clone();
    let task_id_for_read = task_id.clone();
    let ((state, task_scope, attempt_scope, binding_scope, effective_after), leases, child_effects) =
        owner
            .store
            .run(move |db| {
                let row = db.query_row(
                    "SELECT state,task_id,attempt_id,binding_id,effective_request_json \
                     FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
                    [&operation_id_for_read],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )?;
                let leases: i64 = db.query_row(
                    "SELECT COUNT(*) FROM workspace_leases WHERE operation_id=?1",
                    [&operation_id_for_read],
                    |row| row.get(0),
                )?;
                let child_effects: i64 = db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE task_id=?1 AND method IN ('agent.open','task.dispatch')",
                    [&task_id_for_read],
                    |row| row.get(0),
                )?;
                Ok((row, leases, child_effects))
            })
            .await
            .unwrap();
    assert_eq!(state, "queued");
    assert_eq!(task_scope.as_deref(), Some(task_id.as_str()));
    assert_eq!(attempt_scope, None);
    assert_eq!(binding_scope, None);
    assert_eq!(
        effective_after, damaged_json,
        "the reader repaired malformed history"
    );
    let damaged: Value = serde_json::from_str(&effective_after).unwrap();
    assert!(damaged.get("launch_plan_authority").is_none());
    assert!(damaged["launch_manifest"].get("mcp").is_none());
    assert!(damaged["launch_manifest"]["request"].get("route").is_none());
    assert_eq!(leases, 0, "malformed history advanced to workspace effect");
    assert_eq!(
        child_effects, 0,
        "malformed history replayed a native child"
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
