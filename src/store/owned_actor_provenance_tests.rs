use super::*;
const DIRECT_MANAGER: &str = "provenance-direct-manager";
const SUCCESSOR_MANAGER: &str = "provenance-successor-manager";
const TECHNICAL_REQUESTER: &str = "provenance-technical-requester";
fn start_row(technical_requester_id: &str, effective_manager_id: &str) -> OwnedStartRow {
    OwnedStartRow {
        launch_operation_id: "provenance-launch".to_owned(),
        open_operation_id: "provenance-open".to_owned(),
        binding_id: "provenance-binding".to_owned(),
        binding_generation: 1,
        task_id: "provenance-task".to_owned(),
        task_revision: 1,
        attempt_id: "provenance-attempt".to_owned(),
        lease_id: "provenance-lease".to_owned(),
        lease_generation: 1,
        technical_requester_id: technical_requester_id.to_owned(),
        effective_manager_id: effective_manager_id.to_owned(),
        service_id: "opencode".to_owned(),
        service_version: "2.0.7".to_owned(),
        route_digest: "route-digest".to_owned(),
        binding_digest: "binding-digest".to_owned(),
        owner_nonce: "owner-nonce".to_owned(),
        intent_digest: "intent-digest".to_owned(),
        state: "reserved".to_owned(),
        process_id: None,
        process_birth_token: None,
        executable_sha256: None,
        proof_json: "{}".to_owned(),
        updated_at_ms: 1,
    }
}
fn parent(caller_id: &str) -> OperationRow {
    OperationRow {
        operation_id: "provenance-launch".to_owned(),
        caller_id: caller_id.to_owned(),
        method: "swarm.launch".to_owned(),
        state: "queued".to_owned(),
        task_id: Some("provenance-task".to_owned()),
        attempt_id: Some("provenance-attempt".to_owned()),
        binding_id: Some("provenance-binding".to_owned()),
        binding_generation: Some(1),
        prerequisite_operation_id: None,
        original_request_json: "{}".to_owned(),
        effective_request_json: "{}".to_owned(),
    }
}
fn assert_corrupt(error: Error) {
    assert_eq!(error.code, "OWNED_SERVICE_RECEIPT_CORRUPT");
}

#[test]
fn direct_legacy_and_work_dispatch_actor_attribution_are_strict() {
    let db = Connection::open_in_memory().unwrap();
    let validate = |parent: &OperationRow, manifest: &Value, row: &OwnedStartRow| {
        validate_actor_link(&db, parent, manifest, row)
    };
    let reject = |parent: &OperationRow, manifest: &Value, row: &OwnedStartRow| {
        assert_corrupt(validate(parent, manifest, row).unwrap_err());
    };
    let direct_manifest: Value = json!({
        "actor":{
            "kind":"direct",
            "client_id":DIRECT_MANAGER,
            "role":"manager",
            "link_id":"ephemeral-direct-link",
        },
    });
    assert_eq!(direct_manifest["actor"].as_object().unwrap().len(), 4);
    let direct_parent = parent(DIRECT_MANAGER);
    validate(
        &direct_parent,
        &direct_manifest,
        &start_row(DIRECT_MANAGER, DIRECT_MANAGER),
    )
    .unwrap();

    let mut wrong_direct = direct_manifest;
    wrong_direct["actor"]["effective_manager_id"] = json!(SUCCESSOR_MANAGER);
    reject(
        &direct_parent,
        &wrong_direct,
        &start_row(DIRECT_MANAGER, SUCCESSOR_MANAGER),
    );

    let work_dispatch_parent = parent(TECHNICAL_REQUESTER);
    let work_dispatch_row = start_row(TECHNICAL_REQUESTER, DIRECT_MANAGER);

    let mut work_dispatch = json!({
        "actor":{
            "kind":"work_dispatch",
            "client_id":TECHNICAL_REQUESTER,
        },
    });
    reject(&work_dispatch_parent, &work_dispatch, &work_dispatch_row);
    work_dispatch["actor"]["effective_manager_id"] = json!(SUCCESSOR_MANAGER);
    reject(&work_dispatch_parent, &work_dispatch, &work_dispatch_row);
}
