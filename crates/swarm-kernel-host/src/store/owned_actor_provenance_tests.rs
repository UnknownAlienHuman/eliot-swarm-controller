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
fn direct_and_unknown_actor_attribution_are_strict() {
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

    let mut wrong_direct = direct_manifest.clone();
    wrong_direct["actor"]["effective_manager_id"] = json!(SUCCESSOR_MANAGER);
    reject(
        &direct_parent,
        &wrong_direct,
        &start_row(DIRECT_MANAGER, SUCCESSOR_MANAGER),
    );

    // Current production accepts only the direct and work_dispatch variants.
    // Keep legacy/unknown spellings rejected instead of inventing a positive
    // route that is not part of the current manifest contract.
    let mut legacy_actor = direct_manifest.clone();
    legacy_actor["actor"]["kind"] = json!("legacy");
    reject(
        &direct_parent,
        &legacy_actor,
        &start_row(DIRECT_MANAGER, DIRECT_MANAGER),
    );
    let mut missing_actor_kind = direct_manifest;
    assert!(
        missing_actor_kind["actor"]
            .as_object_mut()
            .unwrap()
            .remove("kind")
            .is_some()
    );
    reject(
        &direct_parent,
        &missing_actor_kind,
        &start_row(DIRECT_MANAGER, DIRECT_MANAGER),
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

fn work_dispatch_fixture_config(directory: &Path) -> crate::config::Config {
    let mut config = crate::config::Config::default();
    config.storage.data_dir = directory.to_path_buf();
    config.routes.push(crate::config::Route {
        admission_policy: Default::default(),
        workspace_option: None,
        owned_service: None,
        alias: "provenance-route".to_owned(),
        runtime: "provenance-runtime".to_owned(),
        module_artifact_id: "provenance-artifact".to_owned(),
        enabled: true,
        native_options: json!({}),
    });
    config.mcp.profiles.insert(
        "provenance-participant".to_owned(),
        crate::config::McpProfileConfig {
            tool_profile: crate::config::McpToolProfile::Participant,
            expected_client_id: "participant-template".to_owned(),
            surface: Some("participant-core".to_owned()),
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
    config
}

async fn start_work_dispatch_fixture_store()
-> (super::super::StoreOwner, PathBuf, crate::model::Principal) {
    let directory =
        std::env::temp_dir().join(format!("swarm-owned-actor-provenance-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = crate::platform::DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    let owner = super::super::StoreOwner::start(
        root,
        std::sync::Arc::new(work_dispatch_fixture_config(&directory)),
        credential.clone(),
    )
    .await
    .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    (owner, directory, operator)
}

async fn work_dispatch_fixture_write(
    store: &super::Store,
    principal: &crate::model::Principal,
    method: &str,
    mut value: Value,
) -> Value {
    value["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
        .unwrap()
}

async fn register_work_dispatch_fixture_manager(
    store: &super::Store,
    operator: &crate::model::Principal,
) -> crate::model::Principal {
    let credential = crate::model::Credential {
        client_id: SUCCESSOR_MANAGER.to_owned(),
        token: format!("owned-actor-test-{}", model::new_id()),
    };
    work_dispatch_fixture_write(
        store,
        operator,
        "client.register",
        json!({
            "client_id":SUCCESSOR_MANAGER,
            "role":"manager",
            "token_hash":model::digest(credential.token.as_bytes()),
        }),
    )
    .await;
    store.authenticate(credential).await.unwrap()
}

async fn select_work_dispatch_fixture_route(
    store: &super::Store,
    manager: &crate::model::Principal,
) {
    let mut template =
        swarm_contracts::module_contract::ModuleContractTemplate::codex_rust_controller_v5()
            .unwrap();
    template.module_id =
        swarm_contracts::module_catalog::ModuleId::new("provenance-runtime").unwrap();
    template.artifact.artifact_id =
        swarm_contracts::module_catalog::ArtifactId::new("provenance-artifact").unwrap();
    template.artifact.version = swarm_contracts::module_catalog::ArtifactVersion::new("5").unwrap();
    let descriptor = template
        .descriptor(
            swarm_contracts::module_catalog::LaunchSpec {
                executable: if cfg!(windows) {
                    PathBuf::from(r"C:\modules\provenance.exe")
                } else {
                    PathBuf::from("/modules/provenance")
                },
                argv: Vec::new(),
                environment: Vec::new(),
                credential_ref: Some(
                    swarm_contracts::module_catalog::ProtectedRef::new(
                        "credential:provenance-fixture",
                    )
                    .unwrap(),
                ),
                working_directory: None,
                inherited_environment_allowlist: Default::default(),
                executable_sha256: Some(
                    swarm_contracts::module_catalog::Sha256Digest::new("d".repeat(64)).unwrap(),
                ),
            },
            swarm_contracts::module_catalog::LifecycleOwnership::OwnedService,
            swarm_contracts::module_catalog::ActivationPolicy::OnDemand,
            true,
            swarm_contracts::module_catalog::RestartPolicy::default(),
        )
        .unwrap();
    let registered = store
        .run(move |db| {
            let tx = db.unchecked_transaction()?;
            let registered =
                super::super::module_handshake::register_trusted_descriptor(&tx, descriptor)?;
            tx.commit()?;
            Ok(registered)
        })
        .await
        .unwrap();
    work_dispatch_fixture_write(
        store,
        manager,
        "module.route.select",
        json!({
            "route_alias":"provenance-route",
            "module_id":"provenance-runtime",
            "artifact_id":"provenance-artifact",
            "version":"5",
            "expected_catalog_revision":registered["catalog_revision"],
        }),
    )
    .await;
}

#[tokio::test]
async fn work_dispatch_actor_attribution_accepts_real_store_link_and_rejects_tampering() {
    let (owner, directory, operator) = start_work_dispatch_fixture_store().await;
    let manager = register_work_dispatch_fixture_manager(&owner.store, &operator).await;
    select_work_dispatch_fixture_route(&owner.store, &manager).await;
    work_dispatch_fixture_write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":SUCCESSOR_MANAGER}),
    )
    .await;

    let task = work_dispatch_fixture_write(
        &owner.store,
        &operator,
        "task.create",
        json!({
            "project_id":"provenance-project",
            "spec":{
                "objective":"Exercise retained owned actor WorkDispatch attribution",
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Preserve exact manager and task identity"}],
                "scope":{"initial_paths":["src/target.rs"]},
                "owner_policy_id":crate::policy::OWNER_POLICY_V1_ID,
            }
        }),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let settings = json!({
        "route":"provenance-route",
        "agent_profile":"provenance-agent",
        "mcp_profile":"provenance-participant",
        "mcp_surface":"participant-core",
        "workspace_policy":"manager_owned_worktree",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"implementation",
    });
    let changes = json!([{
        "automation_id":"owned-actor-provenance",
        "expected_revision":0,
        "include_existing":true,
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
            json!({"project_id":"provenance-project","changes":changes.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true, "{preview}");
    let applied = work_dispatch_fixture_write(
        &owner.store,
        &manager,
        "automation.config.apply",
        json!({
            "project_id":"provenance-project",
            "changes":changes,
            "preview_digest":preview["plan_sha256"],
        }),
    )
    .await;
    assert_eq!(applied["applied"], true, "{applied}");
    let recent = applied["work_dispatch"][0]["recent"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["task_id"] == task_id)
        .unwrap_or_else(|| panic!("committed task.create source was not consumed: {applied}"));
    assert_eq!(recent["disposition"], "admitted");
    let operation_id = recent["operation_id"].as_str().unwrap().to_owned();
    let task_source_operation_id = task["operation_id"].as_str().unwrap().to_owned();
    let visible = owner
        .store
        .call(
            manager.clone(),
            "operation.get".to_owned(),
            json!({"operation_id":operation_id.clone()}),
        )
        .await
        .unwrap();
    assert!(visible.get("caller_id").is_none());
    assert_eq!(visible["task_id"], task_id);

    let evidence = {
        let operation_id = operation_id.clone();
        let task_id_for_validation = task_id.clone();
        let task_source_operation_id_for_validation = task_source_operation_id.clone();
        owner
            .store
            .run(move |db| {
                let parent = super::operation_row(db, &operation_id)?.ok_or_else(|| {
                    Error::new("STORE_ERROR", "WorkDispatch parent Operation disappeared")
                })?;
                let effective: Value = serde_json::from_str(&parent.effective_request_json)?;
                let manifest = effective["launch_manifest"].clone();
                let link =
                    super::super::automation_work_dispatch::operation_link(db, &operation_id)?
                        .ok_or_else(|| {
                            Error::new("STORE_ERROR", "typed WorkDispatch link disappeared")
                        })?;
                let mut row = start_row(&link.technical_requester_id, &link.effective_manager_id);
                row.launch_operation_id = link.operation_id.clone();
                row.task_id = link.task_id.clone();
                row.task_revision = link.task_revision;
                row.attempt_id = link
                    .attempt_id
                    .clone()
                    .or_else(|| parent.attempt_id.clone())
                    .unwrap_or_else(|| "claim-new-attempt".to_owned());

                assert_eq!(manifest["actor"]["kind"], "work_dispatch");
                assert_eq!(parent.caller_id, link.technical_requester_id);
                assert_eq!(parent.method, "swarm.launch");
                assert_eq!(link.effective_manager_id, SUCCESSOR_MANAGER);
                assert_eq!(link.action, "swarm.launch");
                assert_eq!(link.automation_id, "owned-actor-provenance");
                assert_eq!(link.task_id, task_id_for_validation);
                assert_eq!(link.source["event_kind"], "task.create");
                assert_eq!(
                    link.source["operation_id"],
                    task_source_operation_id_for_validation
                );

                super::validate_actor_link(db, &parent, &manifest, &row)?;

                let mismatched_requester = format!("{}-tampered", row.technical_requester_id);
                let mut wrong_manifest_requester = manifest.clone();
                wrong_manifest_requester["actor"]["client_id"] =
                    json!(mismatched_requester.as_str());
                assert_corrupt(
                    super::validate_actor_link(db, &parent, &wrong_manifest_requester, &row)
                        .unwrap_err(),
                );

                // Keep manifest and row mutually consistent so the retained typed link
                // itself must reject the altered requester attribution.
                let mut wrong_row_requester = row.clone();
                wrong_row_requester.technical_requester_id = mismatched_requester.clone();
                let mut matching_wrong_manifest_requester = manifest.clone();
                matching_wrong_manifest_requester["actor"]["client_id"] =
                    json!(mismatched_requester.as_str());
                assert_corrupt(
                    super::validate_actor_link(
                        db,
                        &parent,
                        &matching_wrong_manifest_requester,
                        &wrong_row_requester,
                    )
                    .unwrap_err(),
                );

                let mut wrong_manifest_manager = manifest.clone();
                wrong_manifest_manager["actor"]["effective_manager_id"] = json!(DIRECT_MANAGER);
                assert_corrupt(
                    super::validate_actor_link(db, &parent, &wrong_manifest_manager, &row)
                        .unwrap_err(),
                );

                let mut wrong_row_manager = row.clone();
                wrong_row_manager.effective_manager_id = DIRECT_MANAGER.to_owned();
                assert_corrupt(
                    super::validate_actor_link(db, &parent, &manifest, &wrong_row_manager)
                        .unwrap_err(),
                );

                let mut wrong_row_task = row.clone();
                wrong_row_task.task_id.push_str("-tampered");
                assert_corrupt(
                    super::validate_actor_link(db, &parent, &manifest, &wrong_row_task)
                        .unwrap_err(),
                );

                Ok(json!({
                    "actor":manifest["actor"],
                    "technical_requester_id":link.technical_requester_id,
                    "effective_manager_id":link.effective_manager_id,
                    "operation_id":link.operation_id,
                    "task_id":link.task_id,
                    "task_revision":link.task_revision,
                    "automation_id":link.automation_id,
                    "automation_revision":link.automation_revision,
                    "semantic_slot_id":link.semantic_slot_id,
                    "source":link.source,
                }))
            })
            .await
            .unwrap()
    };
    assert_eq!(
        evidence["actor"]["client_id"],
        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(evidence["actor"]["effective_manager_id"], SUCCESSOR_MANAGER);
    assert_eq!(
        evidence["technical_requester_id"],
        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(evidence["effective_manager_id"], SUCCESSOR_MANAGER);
    assert_eq!(evidence["operation_id"], operation_id);
    assert_eq!(evidence["task_id"], task_id);
    assert_eq!(evidence["automation_id"], "owned-actor-provenance");
    assert_eq!(evidence["source"]["operation_id"], task_source_operation_id);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
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
