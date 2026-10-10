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

fn module_open_scope() -> OpeningScope {
    let row = start_row(DIRECT_MANAGER, DIRECT_MANAGER);
    let actor = super::super::launcher::LaunchActor::Direct(crate::model::Principal {
        link_id: "provenance-link".into(),
        client_id: DIRECT_MANAGER.into(),
        role: crate::model::Role::Manager,
    });
    let root = std::env::temp_dir().join("owned-module-open-admission");
    let server_program = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../modules/opencode/serve.mjs")
        .canonicalize()
        .expect("repository-pinned serve.mjs test prerequisite");
    let server_program_sha256 = model::digest(&std::fs::read(&server_program).unwrap());
    OpeningScope {
        actor,
        launch_operation_id: row.launch_operation_id.clone(),
        open_operation_id: row.open_operation_id.clone(),
        open_operation_state: "queued".into(),
        task_id: row.task_id.clone(),
        task_revision: row.task_revision,
        attempt_id: row.attempt_id.clone(),
        lease: LeaseAuthorityRef {
            lease_id: row.lease_id.clone(),
            registration_id: "provenance-registration".into(),
            registration_generation: 1,
            project_id: "provenance-project".into(),
            task_id: row.task_id.clone(),
            task_revision: row.task_revision,
            operation_id: row.launch_operation_id.clone(),
            plan_digest: model::digest(b"provenance-plan"),
            owner_client_id: DIRECT_MANAGER.into(),
            attempt_id: Some(row.attempt_id.clone()),
            generation: row.lease_generation,
            baseline_commit: "baseline".into(),
            branch_ref: "refs/heads/main".into(),
            worktree_handle: "provenance-worktree".into(),
            binding_digest: model::digest(b"provenance-binding"),
            state: "held".into(),
        },
        workspace_directory: root.join("workspace"),
        binding_id: row.binding_id,
        binding_generation: row.binding_generation,
        binding_digest: model::digest(b"provenance-binding"),
        service_config: OwnedOpenCodeServiceConfig {
            origin: "fresh_owned_service".into(),
            service_id: "provenance-opencode".into(),
            model: crate::runtime::opencode_v2::ModelRef {
                id: "step-5-preview-free".into(),
                provider_id: "opencode".into(),
                variant: "high".into(),
            },
            model_catalog: "refresh".into(),
            credential_ref: None,
            bun_executable: root.join("bun.exe"),
            bun_sha256: "a".repeat(64),
            server_program,
            server_program_sha256,
            state_root: root.join("owned-state"),
            port: 0,
        },
        actor_manifest: json!({"kind":"direct","client_id":DIRECT_MANAGER,"role":"manager","link_id":"provenance-link"}),
    }
}

#[test]
fn module_first_open_accepts_only_an_unused_exact_reservation() {
    let scope = module_open_scope();
    let admission = make_admission(&scope, model::new_id()).unwrap();
    validate_module_start_reservation(&scope, &admission.row).unwrap();

    for state in [
        "service_observed",
        "outcome_unknown",
        "failed_no_effect",
        "service_departed",
    ] {
        let mut used = admission.row.clone();
        used.state = state.into();
        assert_eq!(
            validate_module_start_reservation(&scope, &used)
                .unwrap_err()
                .code,
            "OWNED_SERVICE_RECOVERY_REQUIRED",
            "{state}"
        );
    }
    // Even a row still labelled reserved must carry no prior process/proof.
    let mutations: [fn(&mut OwnedStartRow); 4] = [
        |row| row.process_id = Some(42),
        |row| row.process_birth_token = Some("c".repeat(64)),
        |row| row.executable_sha256 = Some("a".repeat(64)),
        |row| row.proof_json = "{\"spawn_attempted\":true}".into(),
    ];
    for mutate in mutations {
        let mut used = admission.row.clone();
        mutate(&mut used);
        assert_eq!(
            validate_module_start_reservation(&scope, &used)
                .unwrap_err()
                .code,
            "OWNED_SERVICE_RECOVERY_REQUIRED"
        );
    }
}

#[test]
fn module_first_open_rejects_changed_authority_scope_and_route() {
    let mut scope = module_open_scope();
    let admission = make_admission(&scope, model::new_id()).unwrap();
    let mutations: [fn(&mut OwnedStartRow); 14] = [
        |row| row.launch_operation_id.push_str("-other"),
        |row| row.open_operation_id.push_str("-other"),
        |row| row.binding_id.push_str("-other"),
        |row| row.binding_generation += 1,
        |row| row.task_id.push_str("-other"),
        |row| row.task_revision += 1,
        |row| row.attempt_id.push_str("-other"),
        |row| row.lease_id.push_str("-other"),
        |row| row.lease_generation += 1,
        |row| row.technical_requester_id.push_str("-other"),
        |row| row.effective_manager_id.push_str("-other"),
        |row| row.binding_digest = "d".repeat(64),
        |row| row.owner_nonce = model::new_id(),
        |row| row.intent_digest = "e".repeat(64),
    ];
    for mutate in mutations {
        let mut changed = admission.row.clone();
        mutate(&mut changed);
        assert!(validate_module_start_reservation(&scope, &changed).is_err());
    }
    scope.service_config.model.variant = "low".into();
    assert!(validate_module_start_reservation(&scope, &admission.row).is_err());
    scope.service_config.model.variant = "high".into();
    scope.open_operation_state = "sending".into();
    assert!(validate_module_start_reservation(&scope, &admission.row).is_err());
}
