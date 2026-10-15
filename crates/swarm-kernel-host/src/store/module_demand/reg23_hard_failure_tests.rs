//! Actual demand consumer regressions over retained Store fixtures.
//! No native process, credential, model or workspace effect is started.
use super::{LaunchActor, final_workspace_manifest_digest, launch_actor_manifest};
use crate::{
    config::{Config, OwnedOpenCodeServiceConfig, Route},
    launcher::LaunchRequest,
    model::{self, Principal, Role},
    runtime::opencode_v2::ModelRef,
    store::{module_demand, module_handshake, workspace},
};
use rusqlite::{Connection, OpenFlags, params};
use serde_json::{Value, json};
use std::path::PathBuf;
use swarm_contracts::{
    module_catalog::{
        ActivationPolicy, ArtifactId, ArtifactVersion, LaunchSpec, LifecycleOwnership, ModuleId,
        ProtectedRef, RestartPolicy, Sha256Digest,
    },
    module_contract::ModuleContractTemplate,
};

const CATALOG: &str = "module_catalog:trusted_descriptors:v1";
const OPERATOR: &str = "reg23-demand-operator";

struct Fixture {
    db: Connection,
    config: Config,
    selector: Value,
    directory: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        Self::with_database(Connection::open_in_memory().unwrap())
    }

    fn with_database(db: Connection) -> Self {
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(crate::store::SCHEMA).unwrap();
        db.execute_batch(crate::store::WORKSPACE_SCHEMA).unwrap();
        db.execute_batch(crate::store::OWNED_SERVICE_SCHEMA)
            .unwrap();
        crate::store::set_meta(
            &db,
            &format!("client:{OPERATOR}"),
            &json!({"role":"operator","disabled":false}),
        )
        .unwrap();
        let directory = std::env::temp_dir().join(format!("reg23-demand-{}", model::new_id()));
        crate::store::set_meta(&db, "local_operator_client_id", &json!(OPERATOR)).unwrap();
        let mut template = ModuleContractTemplate::codex_rust_controller_v5().unwrap();
        template.module_id = ModuleId::new("reg23.demand").unwrap();
        template.artifact.artifact_id =
            ArtifactId::new(crate::config::OPENCODE_RUST_ARTIFACT_ID).unwrap();
        template.artifact.version = ArtifactVersion::new("5").unwrap();
        let descriptor = template
            .descriptor(
                LaunchSpec {
                    executable: directory.join("unused-module"),
                    argv: Vec::new(),
                    environment: Vec::new(),
                    credential_ref: Some(ProtectedRef::new("credential:reg23-demand").unwrap()),
                    working_directory: None,
                    inherited_environment_allowlist: Default::default(),
                    executable_sha256: Some(Sha256Digest::new("a".repeat(64)).unwrap()),
                },
                LifecycleOwnership::OwnedService,
                ActivationPolicy::OnDemand,
                true,
                RestartPolicy::default(),
            )
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let registered =
            module_handshake::register_trusted_descriptor(&tx, descriptor.clone()).unwrap();
        tx.commit().unwrap();
        let selector = json!({
            "schema_version":1,
            "module_id":descriptor.module_id,
            "artifact":descriptor.artifact,
            "registered_revision":registered["catalog_revision"],
            "selected_revision":registered["catalog_revision"],
        });
        Self {
            db,
            config: Config::default(),
            selector,
            directory,
        }
    }

    fn add_binding(&mut self, name: &str, owned: bool) {
        let workspace_directory = self.directory.join(name);
        let service = owned.then(|| {
            let server_program = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../modules/opencode/serve.mjs")
                .canonicalize()
                .unwrap();
            OwnedOpenCodeServiceConfig {
                origin: "fresh_owned_service".into(),
                service_id: format!("reg23-{name}"),
                model: ModelRef {
                    id: "step-5-preview-free".into(),
                    provider_id: "opencode".into(),
                    variant: "high".into(),
                },
                model_catalog: "offline".into(),
                bun_executable: self.directory.join("unused-bun"),
                bun_sha256: "a".repeat(64),
                server_program_sha256: model::digest(&std::fs::read(&server_program).unwrap()),
                server_program,
                state_root: self.directory.join("unused-owned-state"),
                port: 0,
            }
        });
        let route = Route {
            admission_policy: Default::default(),
            workspace_option: None,
            owned_service: service,
            alias: name.into(),
            runtime: "module".into(),
            module_artifact_id: crate::config::OPENCODE_RUST_ARTIFACT_ID.into(),
            enabled: true,
            native_options: json!({"directory":workspace_directory}),
        };
        self.config.routes.push(route.clone());
        let lease_id = format!("{name}-lease");
        let route_value = serde_json::to_value(&route).unwrap();
        self.db
            .execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,
                    module_artifact_id,state,route_json,state_json,created_at_ms)
                 VALUES(?1,1,?2,?3,?4,'opening',?5,?6,1)",
                params![
                    name,
                    format!("launch-{lease_id}"),
                    format!("{name}-instance"),
                    route.module_artifact_id,
                    model::canonical(&route_value).unwrap(),
                    model::canonical(&json!({"module_contract_selector":self.selector})).unwrap(),
                ],
            )
            .unwrap();
        if owned {
            self.add_owned_launch(name, &lease_id, &workspace_directory, route_value);
        } else {
            self.db
                .execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,
                        original_request_json,effective_request_json,binding_id,binding_generation,
                        state,due_at_ms,created_at_ms,updated_at_ms)
                     VALUES(?1,?2,?1,'agent.open','{}','{}',?3,1,'queued',1,1,1)",
                    params![format!("{name}-open"), OPERATOR, name],
                )
                .unwrap();
        }
    }

    fn add_owned_launch(
        &self,
        name: &str,
        lease_id: &str,
        workspace_directory: &std::path::Path,
        route: Value,
    ) {
        let task_id = format!("{name}-task");
        let attempt_id = format!("{name}-attempt");
        let launch_id = format!("{name}-launch");
        let open_id = format!("{name}-open");
        let registration_id = format!("{name}-registration");
        let project_id = format!("{name}-project");
        let plan_digest = format!("sha256:{}", model::digest(name.as_bytes()));
        let binding_digest = model::digest(name.as_bytes());
        self.db
            .execute(
                "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms)
                 VALUES(?1,?2,1,'open','{}',1,1)",
                params![task_id, project_id],
            )
            .unwrap();
        self.db
            .execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,
                    start_owner,binding_id,binding_generation,state,created_at_ms,updated_at_ms)
                 VALUES(?1,?2,1,'{}',?3,'controller',?4,1,'reserved',1,1)",
                params![attempt_id, task_id, OPERATOR, name],
            )
            .unwrap();
        self.db
            .execute(
                "INSERT INTO workspace_registrations(registration_id,project_id,trusted_repository,
                    repository_path,allowed_roots_json,registration_digest,generation,authorized_by,
                    state,created_at_ms,updated_at_ms)
                 VALUES(?1,?2,'reg23-fixture',?3,?4,?5,1,?6,'active',1,1)",
                params![
                    registration_id,
                    project_id,
                    workspace_directory.to_str().unwrap(),
                    model::canonical(&json!([workspace_directory])).unwrap(),
                    binding_digest,
                    OPERATOR,
                ],
            )
            .unwrap();
        let request_value = json!({
            "client_request_id":format!("{name}-request"),
            "plan_digest":plan_digest,
            "task_id":task_id,
            "expected_task_revision":1,
            "route":name,
            "agent_profile":"reg23-agent",
            "mcp_profile":"reg23-participant",
            "mcp_surface":"participant-core",
            "workspace_policy":"manager_owned_worktree",
            "requested_model":null,
            "requested_effort":null,
            "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
            "stop_conditions":[],
            "purpose":"implementation",
        });
        let request = LaunchRequest::parse(&request_value).unwrap();
        self.db
            .execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,
                    original_request_json,effective_request_json,task_id,attempt_id,binding_id,
                    binding_generation,state,due_at_ms,created_at_ms,updated_at_ms)
                 VALUES(?1,?2,?3,'swarm.launch',?4,'{}',?5,?6,?7,1,'queued',1,1,1)",
                params![
                    launch_id,
                    OPERATOR,
                    request.client_request_id,
                    model::canonical(&request_value).unwrap(),
                    task_id,
                    attempt_id,
                    name,
                ],
            )
            .unwrap();
        self.db
            .execute(
                "INSERT INTO workspace_leases(lease_id,registration_id,registration_generation,
                    project_id,task_id,task_revision,operation_id,plan_digest,owner_client_id,
                    attempt_id,allowed_paths_json,allowed_symbols_json,baseline_commit,branch_ref,
                    worktree_handle,workspace_path,clean_state_json,generation,binding_digest,
                    state,created_at_ms,updated_at_ms)
                 VALUES(?1,?2,1,?3,?4,1,?5,?6,?7,?8,'[]','[]',?9,'refs/heads/main',
                    'reg23-worktree',?10,'{}',1,?11,'held',1,1)",
                params![
                    lease_id,
                    registration_id,
                    project_id,
                    task_id,
                    launch_id,
                    plan_digest,
                    OPERATOR,
                    attempt_id,
                    "a".repeat(40),
                    workspace_directory.to_str().unwrap(),
                    binding_digest,
                ],
            )
            .unwrap();
        let lease = workspace::held_lease_for_operation(&self.db, &launch_id)
            .unwrap()
            .unwrap();
        let lease_view = workspace::get_lease_view(&self.db, &lease).unwrap();
        let actor = LaunchActor::Direct(Principal {
            link_id: "reg23-demand-link".into(),
            client_id: OPERATOR.into(),
            role: Role::Operator,
        });
        let manifest = json!({
            "manifest_version":"eliot-launch-manifest-v1",
            "state":"awaiting_binding",
            "actor":launch_actor_manifest(&actor),
            "client_request_id":request.client_request_id,
            "plan_digest":request.plan_digest,
            "request":request.preview_params(),
            "task":{
                "task_id":task_id,"project_id":project_id,"expected_revision":1,
                "observed_revision":1,"attempt_id":attempt_id,"attempt_action":"use_existing",
            },
            "attempt":{"state":"reserved","action":"use_existing","claim_operation_id":null,"start_owner":"controller"},
            "workspace":{
                "policy":"manager_owned_worktree","lease_state":"held","dirty_state":"clean_verified",
                "filesystem_inspected":true,"lease_authority":lease,"lease":lease_view,
                "manifest_digest":final_workspace_manifest_digest(&request.plan_digest,&lease,&lease_view).unwrap(),
            },
            "runtime":{
                "route":route,"agent_profile":request.preview.agent_profile,
                "requested_model":null,"requested_effort":null,"budget":request.preview.budget,
                "stop_conditions":[],"purpose":"implementation","state":"opening","native_effect":"not_observed",
            },
            "progress":{"binding_open":"queued","task_dispatch":"not_started"},
            "binding":{"binding_id":name,"generation":1,"state":"queued","operation_id":open_id},
        });
        let effective_parent = json!({
            "operation_contract":{
                "effect_scope":"one_exact_launch_plan",
                "completion_condition":"workspace_binding_dispatch_and_capability_readback",
                "replay_policy":"same_request_id_returns_retained_launch_receipt; unknown_effects_require_readback",
                "contract_revision":"swarm-launch-v1",
            },
            "launch_manifest":manifest,
        });
        self.db
            .execute(
                "UPDATE operations SET effective_request_json=?1 WHERE operation_id=?2",
                params![model::canonical(&effective_parent).unwrap(), launch_id],
            )
            .unwrap();
        let child_request_id = super::launch_child_request_id(&launch_id, "open");
        let child_original = json!({"client_request_id":child_request_id,"lane_id":format!("launch-{lease_id}"),"route":name});
        let child_result =
            json!({"operation_id":open_id,"binding_id":name,"generation":1,"state":"queued"});
        let child_effective = json!({
            "route":route,"module_instance_id":format!("{name}-instance"),
            "operation_contract":{"parent_launch_operation_id":launch_id},
            "workspace_lease":lease,
            "receipt":{"ok":true,"value":child_result},
        });
        self.db
            .execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,
                    original_request_json,effective_request_json,task_id,attempt_id,binding_id,
                    binding_generation,prerequisite_operation_id,state,result_json,
                    due_at_ms,created_at_ms,updated_at_ms)
                 VALUES(?1,?2,?3,'agent.open',?4,?5,?6,?7,?8,1,?9,'queued',?10,1,1,1)",
                params![
                    open_id,
                    OPERATOR,
                    child_request_id,
                    model::canonical(&child_original).unwrap(),
                    model::canonical(&child_effective).unwrap(),
                    task_id,
                    attempt_id,
                    name,
                    launch_id,
                    model::canonical(&child_result).unwrap(),
                ],
            )
            .unwrap();
    }

    fn pending(&mut self) -> crate::error::Result<module_demand::ModuleDemandSnapshot> {
        module_demand::pending(&mut self.db, &self.config, None)
    }

    fn intent_count(&self) -> i64 {
        self.db
            .query_row("SELECT count(*) FROM owned_service_starts", [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn catalog_bytes(&self) -> String {
        self.db
            .query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [CATALOG],
                |row| row.get(0),
            )
            .unwrap()
    }
}

#[test]
fn semantic_binding_error_keeps_healthy_neighbor_demand() {
    let mut f = Fixture::new();
    f.add_binding("a-poison", false);
    f.add_binding("b-healthy", false);
    f.db.execute(
        "UPDATE bindings SET state_json=?1 WHERE binding_id='a-poison'",
        [r#"{"module_contract_selector":{"schema_version":999}}"#],
    )
    .unwrap();
    let snapshot = f.pending().unwrap();
    assert_eq!(snapshot.blocked.len(), 1);
    assert_eq!(snapshot.blocked[0].binding_id, "a-poison");
    assert_eq!(snapshot.blocked[0].error_code, "MODULE_ROUTE_CORRUPT");
    assert_eq!(snapshot.demands.len(), 1);
    assert_eq!(snapshot.demands[0].binding_id, "b-healthy");
    assert_eq!(snapshot.demands[0].generation, 1);
}

#[test]
fn real_owned_intents_commit_only_with_complete_projection() {
    let mut f = Fixture::new();
    f.add_binding("a-owned", true);
    f.add_binding("b-owned", true);
    let snapshot = f.pending().unwrap();
    assert!(snapshot.blocked.is_empty(), "{:?}", snapshot.blocked);
    assert_eq!(snapshot.demands.len(), 2);
    assert_eq!(f.intent_count(), 2);
    let nonces = snapshot
        .demands
        .iter()
        .map(|d| d.route_native_options["__eliot_owner_nonce"].clone())
        .collect::<Vec<_>>();
    let repeated = f.pending().unwrap();
    assert_eq!(f.intent_count(), 2);
    assert_eq!(
        nonces,
        repeated
            .demands
            .iter()
            .map(|d| d.route_native_options["__eliot_owner_nonce"].clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn repeated_existing_owned_intents_do_not_reserve_the_store_writer() {
    let database_uri = format!(
        "file:reg23-demand-{}?mode=memory&cache=shared",
        model::new_id()
    );
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI;
    let db = Connection::open_with_flags(&database_uri, flags).unwrap();
    let mut f = Fixture::with_database(db);
    f.add_binding("a-owned", true);
    let first = f.pending().unwrap();
    assert_eq!(first.demands.len(), 1);
    assert!(first.blocked.is_empty(), "{:?}", first.blocked);
    let nonce = first.demands[0].route_native_options["__eliot_owner_nonce"].clone();

    f.db.busy_timeout(std::time::Duration::ZERO).unwrap();
    let writer = Connection::open_with_flags(&database_uri, flags).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE;").unwrap();
    let repeated = f.pending().unwrap();
    writer.execute_batch("ROLLBACK;").unwrap();

    assert_eq!(repeated.demands.len(), 1);
    assert!(repeated.blocked.is_empty(), "{:?}", repeated.blocked);
    assert_eq!(
        repeated.demands[0].route_native_options["__eliot_owner_nonce"],
        nonce
    );
    assert_eq!(f.intent_count(), 1);
    assert!(f.db.is_autocommit());
}

#[test]
fn intent_write_failure_is_hard_and_rolls_back_earlier_real_intent() {
    let mut f = Fixture::new();
    f.add_binding("a-owned", true);
    f.add_binding("b-owned", true);
    f.db.execute_batch(
        "CREATE TRIGGER reg23_fail_second_intent BEFORE INSERT ON owned_service_starts
             WHEN (SELECT count(*) FROM owned_service_starts)=1
             BEGIN SELECT RAISE(ABORT,'reg23_second_real_intent_failure'); END;",
    )
    .unwrap();
    let error = f.pending().unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    assert!(error.message.contains("reg23_second_real_intent_failure"));
    assert_eq!(f.intent_count(), 0);
    assert!(f.db.is_autocommit());
    f.db.execute_batch("DROP TRIGGER reg23_fail_second_intent;")
        .unwrap();
    assert_eq!(f.pending().unwrap().demands.len(), 2);
    assert_eq!(f.intent_count(), 2);
}

#[test]
fn global_catalog_damage_after_real_reservation_rolls_back_domain() {
    let mut f = Fixture::new();
    f.add_binding("a-owned", true);
    let catalog_before = f.catalog_bytes();
    f.db.execute_batch(
        "CREATE TRIGGER reg23_damage_catalog AFTER INSERT ON owned_service_starts
             BEGIN UPDATE meta SET value_json='{\"schema_version\":999}'
               WHERE key='module_catalog:trusted_descriptors:v1'; END;",
    )
    .unwrap();
    let error = f.pending().unwrap_err();
    assert_eq!(error.code, "MODULE_CATALOG_CORRUPT");
    assert_eq!(f.intent_count(), 0);
    assert_eq!(f.catalog_bytes(), catalog_before);
    assert!(f.db.is_autocommit());
}

#[test]
fn readback_store_failure_after_real_reservation_rolls_back_domain() {
    let mut f = Fixture::new();
    f.add_binding("a-owned", true);
    f.db.execute_batch("DROP INDEX unresolved_target_operations;")
        .unwrap();
    let error = f.pending().unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    assert_eq!(f.intent_count(), 0);
    assert!(f.db.is_autocommit());
}

#[test]
fn known_owned_scope_block_does_not_suppress_healthy_intent() {
    let mut f = Fixture::new();
    f.add_binding("a-stale", true);
    f.add_binding("b-healthy", true);
    f.db.execute(
        "UPDATE bindings SET state='ready' WHERE binding_id='a-stale'",
        [],
    )
    .unwrap();
    let snapshot = f.pending().unwrap();
    assert_eq!(snapshot.blocked.len(), 1);
    assert_eq!(snapshot.blocked[0].binding_id, "a-stale");
    assert_eq!(snapshot.blocked[0].error_code, "OWNED_SERVICE_SCOPE_STALE");
    assert_eq!(snapshot.demands.len(), 1);
    assert_eq!(snapshot.demands[0].binding_id, "b-healthy");
    assert_eq!(f.intent_count(), 1);
}

#[test]
fn catalog_store_read_failure_stays_hard() {
    let mut f = Fixture::new();
    f.add_binding("a-plain", false);
    f.db.execute_batch("ALTER TABLE meta RENAME TO reg23_unavailable_meta;")
        .unwrap();
    let error = f.pending().unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    assert!(f.db.is_autocommit());
}

#[test]
fn empty_demand_returns_empty_snapshot_without_intents() {
    let mut f = Fixture::new();
    let snapshot = f.pending().unwrap();
    assert!(snapshot.demands.is_empty());
    assert!(snapshot.blocked.is_empty());
    assert!(!snapshot.truncated);
    assert!(snapshot.next_cursor.is_none());
    assert_eq!(f.intent_count(), 0);
    assert!(f.db.is_autocommit());
}

#[test]
fn cursor_page_ignores_poisoned_future_page_scope() {
    let mut f = Fixture::new();
    f.add_binding("a-poison", true);
    f.add_binding("z-healthy", true);
    f.db.execute_batch(
        "CREATE TRIGGER reg23_fail_poison_intent BEFORE INSERT ON owned_service_starts
             WHEN NEW.binding_id='a-poison'
             BEGIN SELECT RAISE(ABORT,'reg23_poison_intent_failure'); END;",
    )
    .unwrap();
    // The cursor excludes the poisoned open from the requested page, so the
    // page-local preparation must not touch that future-page subject.
    let cursor = module_demand::ModuleDemandCursor {
        created_at_ms: 1,
        operation_id: "a-poison-open".to_owned(),
    };
    let snapshot = module_demand::pending(&mut f.db, &f.config, Some(&cursor)).unwrap();
    assert!(snapshot.blocked.is_empty(), "{:?}", snapshot.blocked);
    assert_eq!(snapshot.demands.len(), 1);
    assert_eq!(snapshot.demands[0].binding_id, "z-healthy");
    assert_eq!(f.intent_count(), 1);
    assert!(f.db.is_autocommit());
    f.db.execute_batch("DROP TRIGGER reg23_fail_poison_intent;")
        .unwrap();
    let full = f.pending().unwrap();
    assert_eq!(full.demands.len(), 2);
    assert_eq!(f.intent_count(), 2);
}

#[test]
fn current_page_poison_aborts_page_and_rolls_back_earlier_intent() {
    let mut f = Fixture::new();
    f.add_binding("a-healthy", true);
    f.add_binding("z-poison", true);
    f.db.execute_batch(
        "CREATE TRIGGER reg23_fail_poison_intent BEFORE INSERT ON owned_service_starts
             WHEN NEW.binding_id='z-poison'
             BEGIN SELECT RAISE(ABORT,'reg23_poison_intent_failure'); END;",
    )
    .unwrap();
    let error = f.pending().unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    assert!(error.message.contains("reg23_poison_intent_failure"));
    assert_eq!(f.intent_count(), 0);
    assert!(f.db.is_autocommit());
    f.db.execute_batch("DROP TRIGGER reg23_fail_poison_intent;")
        .unwrap();
    let snapshot = f.pending().unwrap();
    assert_eq!(snapshot.demands.len(), 2);
    assert_eq!(f.intent_count(), 2);
}
