use super::{SCHEMA, automation, set_meta};
use crate::{
    automation::{actions::AutomationStep, config},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, params};
use serde_json::json;

fn database() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(SCHEMA).unwrap();
    for client_id in ["former-manager", "current-manager", "other-manager"] {
        set_meta(
            &db,
            &format!("client:{client_id}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
    }
    set_meta(&db, "gm", &json!({"client_id":"current-manager","epoch":4})).unwrap();
    db
}

fn principal(client_id: &str, role: Role) -> Principal {
    Principal {
        link_id: format!("link-{client_id}"),
        client_id: client_id.to_owned(),
        role,
    }
}

#[test]
fn current_gm_can_recover_former_owner_settings_cursors_and_linked_history() {
    let db = database();
    let owner = "former-manager";
    let project = "recovery-project";
    let automation_id = "retained-review";

    let mut entry = config::AutomationEntry::new(owner, project, automation_id, 1);
    entry.enabled = true;
    entry.steps.push(AutomationStep::ReviewDispatch);
    config::write_record(
        &db,
        &config::entry_key(owner, project, automation_id).unwrap(),
        &entry.value().unwrap(),
    )
    .unwrap();

    config::write_record(
        &db,
        &config::dispatch_state_key(owner, project, automation_id).unwrap(),
        &json!({
            "schema_version":1,
            "owner_manager_id":owner,
            "project_id":project,
            "automation_id":automation_id,
            "step":"review_dispatch",
            "cursor":17,
            "activation_cut":3,
            "catch_up_until":null,
            "pending_after_observation_id":16,
            "pending":[{
                "cause":{"id":"submission-pending","observation_id":17},
                "reason":"awaiting_review_assignment",
                "wake_when":["review_assignment_available"],
                "first_seen_at_ms":10,
                "last_checked_at_ms":11,
                "held":false
            }],
            "recent":[],
            "updated_at_ms":12
        }),
    )
    .unwrap();

    let operation_id = "review-op-retained";
    let technical_requester = crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID;
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,'request-1','review.assign','{}','{}','outcome_unknown',1,1,1)",
        params![operation_id, technical_requester],
    )
    .unwrap();
    let link = json!({
        "schema_version":1,
        "operation_id":operation_id,
        "technical_requester_id":technical_requester,
        "effective_manager_id":owner,
        "automation_id":automation_id,
        "automation_revision":entry.revision,
        "project_id":project,
        "action":"review.assign",
        "cause":{
            "kind":"applied_submission",
            "observation_id":17,
            "operation_id":"submission-source-op",
            "id":"submission-original"
        },
        "linked_at_ms":13
    });
    config::write_record(
        &db,
        &config::operation_link_key(operation_id).unwrap(),
        &link,
    )
    .unwrap();
    config::write_record(
        &db,
        &config::entry_operation_key(owner, project, automation_id, operation_id).unwrap(),
        &link,
    )
    .unwrap();

    let previous_manager = principal(owner, Role::Manager);
    let current_manager = principal("current-manager", Role::Manager);
    let ordinary_manager = principal("other-manager", Role::Manager);

    // The previous owner keeps the default self-scoped read after handover.
    let self_page =
        automation::get(&db, &previous_manager, &json!({"project_id":project})).unwrap();
    assert_eq!(self_page["owner_manager_id"], owner);
    assert_eq!(self_page["items"][0]["enabled"], true);

    // The live current GM can select the former owner, while all projections
    // retain the former owner and the existing cursor/pending/history facts.
    let recovered = automation::get(
        &db,
        &current_manager,
        &json!({"project_id":project,"owner_manager_id":owner}),
    )
    .unwrap();
    assert_eq!(recovered["owner_manager_id"], owner);
    assert_eq!(recovered["items"][0]["owner_manager_id"], owner);

    let explained = automation::explain(
        &db,
        &current_manager,
        &json!({
            "project_id":project,
            "automation_id":automation_id,
            "owner_manager_id":owner
        }),
    )
    .unwrap();
    assert_eq!(explained["owner_manager_id"], owner);
    assert_eq!(explained["entry"]["owner_manager_id"], owner);
    assert_eq!(explained["dispatch"]["cursor"], 17);
    assert_eq!(
        explained["dispatch"]["pending"][0]["cause"]["id"],
        "submission-pending"
    );
    assert_eq!(
        explained["linked_operation_history"]["items"][0]["operation_id"],
        operation_id
    );
    assert_eq!(
        explained["linked_operations"]["uncertain"][0]["operation_id"],
        operation_id
    );

    let denied = automation::get(
        &db,
        &ordinary_manager,
        &json!({"project_id":project,"owner_manager_id":owner}),
    )
    .unwrap_err();
    assert_eq!(denied.code, "FORBIDDEN");
    let observer_denied = automation::get(
        &db,
        &principal("observer", Role::Observer),
        &json!({"project_id":project,"owner_manager_id":owner}),
    )
    .unwrap_err();
    assert_eq!(observer_denied.code, "FORBIDDEN");

    // Recovery is projection only: the retained entry and its original scope
    // remain in place, with no copied entry under the new GM.
    assert_eq!(
        config::load_entry(&db, owner, project, automation_id).unwrap(),
        Some(entry)
    );
    assert!(
        config::load_entry(&db, "current-manager", project, automation_id)
            .unwrap()
            .is_none()
    );
    model::validate_automation_config_read(
        "automation.config.get",
        &json!({"project_id":project,"owner_manager_id":owner}),
    )
    .unwrap();
}
