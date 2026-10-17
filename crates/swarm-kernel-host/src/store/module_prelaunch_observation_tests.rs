//! Connected prelaunch reporting regression. Wire as a child of `store`.
//! Requires Sagan's prelaunch producer and the same-source swarm-module-owner
//! binary beside the lib test executable, or ELIOT_PRELAUNCH_TEST_OWNER_HELPER.
//! The adapter is an inert child of this test executable; no provider is used.

use super::*;
use crate::{ipc, platform::bootstrap_credential};
use futures_util::{SinkExt, StreamExt};
use std::{collections::BTreeMap, fs, path::PathBuf, sync::Mutex};
use swarm_supervisor::{
    AdmissionState, LaunchValue, LifecycleState, ModuleDescriptor, ModuleEffectCertainty,
    ModuleFailureStage, ModuleSupervisorObservation, ModuleSupervisorPhase, ServiceScope,
    Sha256Digest, StandaloneRouteConfigMapper, StandaloneSupervisor, StandaloneSupervisorConfig,
    SupervisorBootstrap, SupervisorControlClient, SupervisorStatus,
    control::ObservationDeliveryDisposition,
};
use tokio::{task::JoinSet, time};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

const ADAPTER_CHILD: &str = "store::module_prelaunch_observation_tests::prelaunch_adapter_child";
const CONFIG_CANARY: &str = "PRIVATE_PRELAUNCH_OPTIONS_MUST_NOT_ESCAPE";
const UNSUPPORTED: &str = "MODULE_CONFIG_SCHEMA_UNSUPPORTED";
const BOUND: Duration = Duration::from_secs(20);

fn compatible_status_diagnostic(status: Option<&SupervisorStatus>) -> Value {
    let Some(status) = status else {
        return json!({"snapshot_available":false});
    };
    let lifecycle = serde_json::to_value(&status.lifecycle).unwrap();
    let closed_code = |code: Option<&str>| {
        code.filter(|code| {
            !code.is_empty()
                && code.len() <= 128
                && code
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        })
        .map(str::to_owned)
    };
    let retained_reason = match &status.lifecycle {
        LifecycleState::OwnerGroupRetained { reason, .. } => Some(*reason),
        _ => None,
    };
    // Whitelist bounded metadata only. Never print Isolated.reason,
    // last_failure.detail, process image/path/birth, configuration or input.
    json!({
        "snapshot_available":true,
        "phase":lifecycle["state"],
        "exit_code":lifecycle["exit_code"],
        "exit_proven":lifecycle["exit_proven"],
        "retained_reason":retained_reason,
        "boot_present":status.worker_boot_id.is_some(),
        "boot_id":status.worker_boot_id.as_deref().filter(|id| uuid::Uuid::parse_str(id).is_ok()),
        "owner_pid":status.owner.as_ref().map(|owner| owner.pid),
        "worker_pid":status.worker.as_ref().map(|worker| worker.pid),
        "admission":status.admission,
        "readback_required":status.readback_required,
        "unknown_operation_count":status.unknown_operation_count,
        "unknown_operation_ids_truncated":status.unknown_operation_ids_truncated,
        "restart":status.restart,
        "effect_certainty":status.effect_certainty,
        "failure_stage":status.failure_stage,
        "failure_code":closed_code(status.last_failure.as_ref().map(|failure| failure.code.as_str())),
        "worker_diagnostic_code":closed_code(status.worker_diagnostic_code.as_deref()),
    })
}

#[derive(Clone)]
struct Delivery {
    observation: Value,
    reply: Value,
    withheld: bool,
    forwarding: Arc<Mutex<Forwarding>>,
}

#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Forwarding {
    Pending,
    Withheld,
    Completed,
    FrameLimit,
    PeerClosed,
    ConnectionReset,
    TimedOut,
    OtherIo,
}

fn forwarding_failure(error: &LinesCodecError) -> Forwarding {
    match error {
        LinesCodecError::MaxLineLengthExceeded => Forwarding::FrameLimit,
        LinesCodecError::Io(error) => match error.kind() {
            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::NotConnected => {
                Forwarding::PeerClosed
            }
            std::io::ErrorKind::ConnectionReset => Forwarding::ConnectionReset,
            std::io::ErrorKind::TimedOut => Forwarding::TimedOut,
            _ => Forwarding::OtherIo,
        },
    }
}

#[derive(Default, serde::Serialize)]
struct StartupGateDiagnostic {
    polls: u64,
    sampled_at_ms: Option<u128>,
    pages: usize,
    acknowledged_at_page: Option<usize>,
    duplicate_ack_seen: bool,
    stable_pages: bool,
    scoped_rejection_seen: bool,
    entry: bool,
    returned: bool,
    release: bool,
    runner_finished: bool,
    admission_open: bool,
    lookup_started: u64,
    lookup_completed: u64,
    lookup_started_at_ms: Option<u128>,
    lookup_completed_at_ms: Option<u128>,
    lookup_elapsed_ms: Option<u128>,
    requested_module_id: String,
    requested_binding_id: String,
    requested_generation: u64,
    compatible: Value,
    wire: Value,
}

#[derive(Default)]
struct WireTrace {
    demand_pages: usize,
    credential_calls: BTreeMap<String, usize>,
    deliveries: Vec<Delivery>,
    drop_for_binding: Option<String>,
}

impl WireTrace {
    fn delivery_diagnostic(&self, binding: &str) -> Value {
        let matching = || {
            self.deliveries.iter()
                .filter(|item| item.observation["scope"]["binding_id"] == binding)
        };
        let total = matching().count();
        let rows: Vec<_> = matching().rev().take(8)
            .map(|item| {
                let disposition = match item.reply["disposition"].as_str() {
                    Some("committed") => "committed",
                    Some("exact_duplicate") => "exact_duplicate",
                    Some("scoped_permanent_rejection") => "scoped_permanent_rejection",
                    _ => "other",
                };
                json!({"disposition":disposition,"withheld":item.withheld,
                    "forwarding":*item.forwarding.lock().unwrap()})
            })
            .collect();
        json!({"count":total,"omitted":total.saturating_sub(rows.len()),"last_reverse_order":rows})
    }

    fn deliveries_for(&self, binding: &str) -> Vec<Delivery> {
        self.deliveries
            .iter()
            .filter(|item| item.observation["scope"]["binding_id"] == binding)
            .cloned()
            .collect()
    }
}

// This relay changes no request or response bytes. Only the first committed
// callback reply for the armed binding is consumed instead of forwarded.
// Store authentication, dispatch, transaction and disconnection are real.
async fn relay(
    stream: ipc::Stream,
    bridge: tokio::io::DuplexStream,
    trace: Arc<Mutex<WireTrace>>,
    limit: usize,
    mut stopping: watch::Receiver<bool>,
) {
    let mut client = Framed::new(stream, LinesCodec::new_with_max_length(limit));
    let mut host = Framed::new(bridge, LinesCodec::new_with_max_length(limit));
    loop {
        let incoming = tokio::select! {
            _ = stopping.changed() => break,
            incoming = client.next() => incoming,
        };
        let Some(Ok(line)) = incoming else { break };
        let request: Value = serde_json::from_str(&line).unwrap();
        {
            let mut trace = trace.lock().unwrap();
            match request["method"].as_str() {
                Some("module.supervisor.demand.page") => trace.demand_pages += 1,
                Some(
                    "module.supervisor.credential.ensure" | "module.supervisor.credential.ready",
                ) => {
                    let binding = request["params"]["binding_id"].as_str().unwrap();
                    *trace
                        .credential_calls
                        .entry(binding.to_owned())
                        .or_default() += 1;
                }
                _ => {}
            }
        }
        if host.send(line).await.is_err() {
            break;
        }
        let Some(Ok(reply)) = host.next().await else {
            break;
        };
        let response: Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(request["id"], response["id"]);
        let mut withhold = false;
        let mut forwarding = None;
        if request["method"] == "module.supervisor.observation.record" {
            let observation = request["params"]["observation"].clone();
            let mut trace = trace.lock().unwrap();
            withhold = trace.drop_for_binding.as_deref()
                == observation["scope"]["binding_id"].as_str()
                && response["result"]["disposition"] == "committed";
            if withhold {
                trace.drop_for_binding = None;
            }
            let state = Arc::new(Mutex::new(if withhold {
                Forwarding::Withheld
            } else {
                Forwarding::Pending
            }));
            trace.deliveries.push(Delivery {
                observation,
                reply: response["result"].clone(),
                withheld: withhold,
                forwarding: state.clone(),
            });
            // Retain this entry directly: the pre-run trace clear cannot make
            // a late forwarding completion update a different delivery.
            forwarding = Some(state);
        }
        if withhold {
            break;
        }
        let forwarded = client.send(reply).await;
        if let Some(state) = forwarding {
            *state.lock().unwrap() = match &forwarded {
                Ok(()) => Forwarding::Completed,
                Err(error) => forwarding_failure(error),
            };
        }
        if forwarded.is_err() {
            break;
        }
    }
}

struct ConnectedStore {
    owner: StoreOwner,
    operator: Credential,
    config: Arc<Config>,
    trace: Arc<Mutex<WireTrace>>,
    stop: watch::Sender<bool>,
    accept: tokio::task::JoinHandle<()>,
}

impl ConnectedStore {
    async fn start(root: &Path, routes: Vec<crate::config::Route>) -> Self {
        let data_root = DataRoot::acquire(root).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = data_root.path.clone();
        config.routes = routes;
        let config = Arc::new(config);
        let operator = bootstrap_credential(&data_root.path).unwrap();
        let owner = StoreOwner::start(data_root, config.clone(), operator.clone())
            .await
            .unwrap();
        let mut listener = ipc::Listener::bind(root).unwrap();
        let (stop, mut stopping) = watch::channel(false);
        let trace = Arc::new(Mutex::new(WireTrace::default()));
        let accept_trace = trace.clone();
        let store = owner.store.clone();
        let ipc_config = Arc::new(config.ipc.clone());
        let accept = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopping.changed() => break,
                    joined = connections.join_next(), if !connections.is_empty() => {
                        joined.unwrap().unwrap();
                    }
                    accepted = listener.accept() => {
                        let stream = accepted.unwrap();
                        let store = store.clone();
                        let config = ipc_config.clone();
                        let trace = accept_trace.clone();
                        let stopping = stopping.clone();
                        connections.spawn(async move {
                            let (server, bridge) = tokio::io::duplex(65_536);
                            let serve = ipc::serve(
                                Box::new(server), store, config.clone(), stopping.clone(),
                            );
                            let forward = relay(stream, bridge, trace, config.max_frame_bytes, stopping);
                            let _ = tokio::join!(serve, forward);
                        });
                    }
                }
            }
            while let Some(joined) = connections.join_next().await {
                joined.unwrap();
            }
        });
        Self {
            owner,
            operator,
            config,
            trace,
            stop,
            accept,
        }
    }

    fn control(&self) -> SupervisorControlClient {
        SupervisorControlClient::new(
            self.config.storage.data_dir.clone(),
            self.owner.module_supervisor_credential(),
            self.config.ipc.clone(),
        )
        .unwrap()
    }

    async fn call(&self, credential: &Credential, method: &str, params: Value) -> Value {
        swarm_client::call(
            &self.config.storage.data_dir,
            credential,
            method,
            params,
            &self.config.ipc,
        )
        .await
        .unwrap_or_else(|error| panic!("fixture IPC {method}: {}", error.code))
    }

    async fn open(&self, manager: &Credential, route: &str) -> Value {
        let catalog = self.call(manager, "module.catalog.get", json!({})).await;
        let descriptor = catalog["descriptors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["module_id"] == format!("fixture.prelaunch.{route}"))
            .unwrap();
        self.call(
            manager,
            "module.route.select",
            json!({
                "client_request_id":model::new_id(), "route_alias":route,
                "module_id":descriptor["module_id"],
                "artifact_id":descriptor["artifact"]["artifact_id"],
                "version":descriptor["artifact"]["version"],
                "expected_catalog_revision":catalog["catalog_revision"],
            }),
        )
        .await;
        self.call(
            manager,
            "agent.open",
            json!({
                "client_request_id":model::new_id(), "lane_id":model::new_id(), "route":route,
            }),
        )
        .await
    }

    async fn close(self) {
        self.stop.send(true).unwrap();
        time::timeout(Duration::from_secs(3), self.accept)
            .await
            .unwrap()
            .unwrap();
        self.owner.close().await.unwrap();
    }
}

struct FixtureFiles {
    root: PathBuf,
    release: PathBuf,
}

impl FixtureFiles {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("swarm-prelaunch-{}", model::new_id()));
        fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        Self {
            release: root.join("release-adapter"),
            root,
        }
    }

    fn descriptor(&self, name: &str, blocked: bool) -> (PathBuf, ModuleDescriptor) {
        let directory = self.root.join("install").join(name);
        fs::create_dir_all(&directory).unwrap();
        let executable = directory.join(if cfg!(windows) {
            "adapter.exe"
        } else {
            "adapter"
        });
        fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
        let executable_hash = model::digest(&fs::read(&executable).unwrap());
        let mut descriptor: Value = serde_json::from_str(include_str!(
            "../../../swarm-adapter-opencode/registration/descriptor.template.json"
        ))
        .unwrap();
        descriptor["module_id"] = json!(format!("fixture.prelaunch.{name}"));
        descriptor["artifact"] = json!({
            "artifact_id":format!("fixture.prelaunch.{name}.artifact"),
            "version":"91.0.0", "build_id":null,
        });
        descriptor["enabled"] = json!(true);
        descriptor["capabilities"] = json!(["agent.open", "agent.send/next_turn"]);
        descriptor["command_schemas"] = json!([]);
        descriptor["event_schemas"] = json!([]);
        descriptor["workspace_option"] = Value::Null;
        descriptor["config_schema"] = if blocked {
            json!({"schema_id":"fixture.deliberately.unmapped", "version":"91", "sha256":"a".repeat(64)})
        } else {
            Value::Null
        };
        descriptor["launch"]["executable"] = json!(executable);
        descriptor["launch"]["executable_sha256"] = json!(executable_hash);
        descriptor["launch"]["argv"] = json!([
            {"kind":"literal","value":"--exact"}, {"kind":"literal","value":ADAPTER_CHILD},
            {"kind":"literal","value":"--ignored"}, {"kind":"literal","value":"--nocapture"},
            {"kind":"literal","value":"--skip"}, LaunchValue::ModuleHostConfigPath { schema_version:1 },
        ]);
        descriptor["launch"]["environment"] = json!([
            {"name":"ELIOT_PRELAUNCH_MARKER","value":{"kind":"literal","value":self.root.join(format!("{name}-adapter-started"))}},
            {"name":"ELIOT_PRELAUNCH_RELEASE","value":{"kind":"literal","value":self.release}},
        ]);
        descriptor["restart"]["max_starts"] = json!(1);
        let typed: ModuleDescriptor = serde_json::from_value(descriptor.clone()).unwrap();
        typed.validate().unwrap();
        let descriptor_file = directory.join("module-descriptor.json");
        let bytes = serde_json::to_vec(&descriptor).unwrap();
        fs::write(&descriptor_file, &bytes).unwrap();
        fs::write(
            directory.join("install-receipt.json"),
            serde_json::to_vec(&json!({
                "schema_version":1, "format":"eliot.module_install_receipt.v1",
                "module_id":typed.module_id, "artifact_id":typed.artifact.artifact_id,
                "version":typed.artifact.version, "build_id":typed.artifact.build_id,
                "source_file":std::env::current_exe().unwrap(), "installed_file":executable,
                "source_sha256":executable_hash, "staged_sha256":executable_hash,
                "installed_sha256":executable_hash, "descriptor_file":descriptor_file,
                "descriptor_sha256":model::digest(&bytes),
            }))
            .unwrap(),
        )
        .unwrap();
        (descriptor_file, typed)
    }
}

impl Drop for FixtureFiles {
    fn drop(&mut self) {
        // Also releases the bounded inert child if an assertion fails. Keep
        // failed-run receipts for diagnosis; successful runs remove the root.
        if self.root.exists() {
            let _ = fs::write(&self.release, b"release");
        }
    }
}

fn route(name: &str) -> crate::config::Route {
    serde_json::from_value(json!({
        "alias":name, "runtime":"module", "enabled":true,
        "module_artifact_id":format!("fixture.prelaunch.{name}.artifact"),
        "native_options":{"private_fixture_option":CONFIG_CANARY},
    }))
    .unwrap()
}

async fn operation_snapshot(store: &Store) -> Value {
    store
        .run_fixture(|db| {
            let mut statement =
                db.prepare("SELECT operation_id FROM operations ORDER BY operation_id")?;
            let ids = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(statement);
            let rows = ids
                .iter()
                .map(|id| operations::get_operation(db, id))
                .collect::<Result<Vec<_>>>()?;
            Ok(json!(rows))
        })
        .await
        .unwrap()
}

async fn observation_count(store: &Store, binding: &str, kind: &str) -> i64 {
    let binding = binding.to_owned();
    let kind = kind.to_owned();
    store
        .run_fixture(move |db| {
            db.query_row(
                "SELECT count(*) FROM observations WHERE binding_id=?1 AND kind=?2",
                params![binding, kind],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap()
}

#[test]
#[ignore = "inert adapter child selected only by the connected prelaunch fixture"]
fn prelaunch_adapter_child() {
    let marker = PathBuf::from(std::env::var_os("ELIOT_PRELAUNCH_MARKER").unwrap());
    let release = PathBuf::from(std::env::var_os("ELIOT_PRELAUNCH_RELEASE").unwrap());
    fs::write(&marker, b"actual adapter child entered").unwrap();
    let deadline = std::time::Instant::now() + BOUND;
    while !release.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "fixture child release deadline"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // Test-body return is diagnostic only, never OS/family departure proof.
    fs::write(
        marker.with_extension("returned"),
        b"adapter test body returning",
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connected_prelaunch_observation_is_scoped_idempotent_and_status_only() {
    let case_started = std::time::Instant::now();
    let helper = std::env::var_os("ELIOT_PRELAUNCH_TEST_OWNER_HELPER")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .unwrap()
                .with_file_name(if cfg!(windows) {
                    "swarm-module-owner.exe"
                } else {
                    "swarm-module-owner"
                })
        });
    assert!(
        helper.is_absolute() && helper.is_file(),
        "stage the same-source swarm-module-owner beside the lib test executable, or set ELIOT_PRELAUNCH_TEST_OWNER_HELPER to its absolute path"
    );
    let helper = helper.canonicalize().unwrap();
    let helper_hash = Sha256Digest::new(model::digest(&fs::read(&helper).unwrap())).unwrap();
    let files = FixtureFiles::new();
    let (blocked_file, blocked_descriptor) = files.descriptor("blocked", true);
    let (compatible_file, compatible_descriptor) = files.descriptor("compatible", false);
    let connected = ConnectedStore::start(
        &files.root.join("store"),
        vec![route("blocked"), route("compatible")],
    )
    .await;
    let actor = model::new_id();
    connected
        .owner
        .store
        .admit_module_supervisor_actor(&actor)
        .await
        .unwrap();
    let supervisor = Arc::new(
        StandaloneSupervisor::start_from_bootstrap(SupervisorBootstrap {
            actor_instance_id: Some(actor.clone()),
            config: StandaloneSupervisorConfig {
                schema_version: 1,
                root: connected.config.storage.data_dir.clone(),
                ipc: connected.config.ipc.clone(),
                supervisor_credential: connected.owner.module_supervisor_credential(),
                install_root: files.root.join("install"),
                descriptor_files: vec![blocked_file, compatible_file],
                state_root: files.root.join("supervisor-state"),
                resolver_root: files.root.join("resolver"),
                owner_helper: helper,
                owner_helper_sha256: helper_hash,
                protected_files: BTreeMap::new(),
                launch_configs: BTreeMap::new(),
                route_config_mapper: StandaloneRouteConfigMapper::DescriptorSchema,
            },
        })
        .await
        .unwrap(),
    );
    let manager = Credential {
        client_id: "prelaunch-manager".into(),
        token: format!("{}{}", model::new_id(), model::new_id()),
    };
    connected
        .call(
            &connected.operator,
            "client.register",
            json!({
                "client_request_id":model::new_id(), "client_id":manager.client_id,
                "role":"manager", "token_hash":model::digest(manager.token.as_bytes()),
            }),
        )
        .await;
    connected
        .call(
            &connected.operator,
            "host.mode",
            json!({
                "client_request_id":model::new_id(), "new_work":"enabled",
            }),
        )
        .await;
    let blocked = connected.open(&manager, "blocked").await;
    let held = connected.open(&manager, "blocked").await;
    let compatible = connected.open(&manager, "compatible").await;
    let blocked_id = blocked["binding_id"].as_str().unwrap().to_owned();
    let held_id = held["binding_id"].as_str().unwrap().to_owned();
    let compatible_id = compatible["binding_id"].as_str().unwrap().to_owned();
    let compatible_scope = ServiceScope {
        binding_id: compatible_id.clone(),
        generation: 1,
    };
    assert_eq!(blocked["state"], "queued");

    // Negative authority case, not a launch receipt: bootless/Unknown and the
    // wrong module/artifact for the held binding. Store itself retains the
    // permanent rejection and quarantine before the genuine producer runs.
    let foreign = ModuleSupervisorObservation {
        schema_version: 1,
        actor_instance_id: actor.clone(),
        event_id: format!("{actor}:9000"),
        sequence: 9000,
        module_id: compatible_descriptor.module_id.to_string(),
        artifact_id: compatible_descriptor.artifact.artifact_id.to_string(),
        artifact_version: compatible_descriptor.artifact.version.to_string(),
        build_id: None,
        scope: ServiceScope {
            binding_id: held_id.clone(),
            generation: 1,
        },
        boot_id: None,
        phase: ModuleSupervisorPhase::Isolated,
        effect_certainty: ModuleEffectCertainty::Unknown,
        stage: Some(ModuleFailureStage::ValidateLaunch),
        error_code: Some(UNSUPPORTED.into()),
        worker_diagnostic_code: None,
        unknown_operation_ids: Vec::new(),
        unknown_operation_count: 0,
        unknown_operation_ids_truncated: false,
    };
    let control = connected.control();
    assert_eq!(
        control.record_observation(&foreign).await,
        ObservationDeliveryDisposition::ScopedPermanentRejection {
            error_code: "MODULE_OBSERVATION_IDENTITY_MISMATCH".into(),
        }
    );
    assert_eq!(
        observation_count(
            &connected.owner.store,
            &held_id,
            "module.supervisor_observation_rejected"
        )
        .await,
        1
    );

    // Retained uncertainty is synthetic fixture input, never an inferred
    // native outcome. All three real unresolved states must survive; neither
    // queued opening nor terminal work belongs in unknown_operation_ids.
    let seed_binding = blocked_id.clone();
    connected
        .owner
        .store
        .run_fixture(move |db| {
            for (id, state) in [
                ("unknown-sending", "sending"),
                ("unknown-native", "native_accepted"),
                ("unknown-outcome", "outcome_unknown"),
                ("terminal-settled", "settled"),
            ] {
                db.execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,\
                    original_request_json,effective_request_json,binding_id,binding_generation,\
                    state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) \
                 VALUES(?1,'prelaunch-manager',?1,'agent.send',\
                    '{\"delivery\":\"next_turn\"}','{}',?2,1,?3,?4,1,?5,1,1)",
                    params![
                        id,
                        seed_binding,
                        state,
                        (state == "settled").then_some("{}"),
                        (state == "settled").then_some(2_i64)
                    ],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    let before = operation_snapshot(&connected.owner.store).await;
    let unknown_before = serde_json::to_value(
        control
            .scope_readback(blocked_descriptor.module_id.as_str(), &blocked_id, 1)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(unknown_before["operations"].as_array().unwrap().len(), 3);
    let page = control.demand_page(None).await.unwrap();
    assert!(
        page.blocked.is_empty(),
        "the mapper, not Store demand admission, must fail"
    );
    assert!(
        page.demands
            .iter()
            .any(|demand| demand.binding_id == compatible_id)
    );
    {
        let mut trace = connected.trace.lock().unwrap();
        trace.demand_pages = 0;
        trace.deliveries.clear();
        trace.drop_for_binding = Some(blocked_id.clone());
    }
    let (stop, stopping) = watch::channel(false);
    let runner_supervisor = supervisor.clone();
    let setup_finished_ms = case_started.elapsed().as_millis();
    let mut runner = tokio::spawn(async move { runner_supervisor.run(stopping).await });
    let mut compatible_start_status = None;
    let mut gate_diagnostic = StartupGateDiagnostic {
        requested_module_id: compatible_descriptor.module_id.to_string(),
        requested_binding_id: compatible_scope.binding_id.clone(),
        requested_generation: compatible_scope.generation,
        compatible: compatible_status_diagnostic(None),
        ..StartupGateDiagnostic::default()
    };
    let progress_started_ms = case_started.elapsed().as_millis();
    let progress = time::timeout(BOUND, async {
        let mut acknowledged_at = None;
        loop {
            gate_diagnostic.polls = gate_diagnostic.polls.saturating_add(1);
            gate_diagnostic.runner_finished = runner.is_finished();
            gate_diagnostic.admission_open = matches!(
                supervisor.registry().admission_state(), AdmissionState::Open
            );
            // Only marker metadata is sampled here. No receipt/config reads
            // or new status awaits are added before the original AND gate.
            let marker = files.root.join("compatible-adapter-started");
            gate_diagnostic.entry = marker.is_file();
            gate_diagnostic.returned = marker.with_extension("returned").is_file();
            gate_diagnostic.release = files.release.is_file();
            if runner.is_finished() {
                gate_diagnostic.runner_finished = true;
                return Err("supervisor stopped before scoped recovery/progress");
            }
            if acknowledged_at.is_some()
                && !matches!(
                    supervisor.registry().admission_state(),
                    AdmissionState::Open
                )
            {
                gate_diagnostic.admission_open = false;
                return Err("a scoped rejection closed global admission");
            }
            let (pages, acknowledged, rejected) = {
                let trace = connected.trace.lock().unwrap();
                gate_diagnostic.wire = json!({
                    "blocked":trace.delivery_diagnostic(&blocked_id),
                    "held":trace.delivery_diagnostic(&held_id),
                    "compatible":trace.delivery_diagnostic(&compatible_id),
                    "drop_armed":trace.drop_for_binding.is_some(),
                });
                (
                    trace.demand_pages,
                    trace
                        .deliveries_for(&blocked_id)
                        .iter()
                        .any(|item| item.reply["disposition"] == "exact_duplicate"),
                    trace
                        .deliveries_for(&held_id)
                        .iter()
                        .any(|item| item.reply["disposition"] == "scoped_permanent_rejection"),
                )
            };
            if acknowledged && acknowledged_at.is_none() {
                acknowledged_at = Some(pages);
            }
            gate_diagnostic.pages = pages;
            gate_diagnostic.acknowledged_at_page = acknowledged_at;
            gate_diagnostic.duplicate_ack_seen = acknowledged;
            gate_diagnostic.stable_pages = acknowledged_at.is_some_and(|at| pages >= at + 2);
            gate_diagnostic.scoped_rejection_seen = rejected;
            gate_diagnostic.sampled_at_ms = Some(case_started.elapsed().as_millis());
            if acknowledged_at.is_some_and(|at| pages >= at + 2)
                && rejected
                && files.root.join("compatible-adapter-started").is_file()
            {
                // The original entry predicate just proved this marker in the
                // snapshot too; no metadata field substitutes for the gate.
                gate_diagnostic.entry = true;
                // The child's entry marker can precede the owner's image
                // capture and worker.json publication (module_owner.rs:572).
                // Keep the child live until the real supervisor has validated
                // that receipt; early release can make Linux /proc image
                // capture fail and correctly leave retirement unprovable.
                gate_diagnostic.lookup_started = gate_diagnostic.lookup_started.saturating_add(1);
                gate_diagnostic.lookup_started_at_ms = Some(case_started.elapsed().as_millis());
                gate_diagnostic.lookup_elapsed_ms = None;
                let lookup_started = std::time::Instant::now();
                compatible_start_status = supervisor
                    .registry()
                    .status(compatible_descriptor.module_id.as_str(), &compatible_scope)
                    .await;
                gate_diagnostic.lookup_completed = gate_diagnostic.lookup_completed
                    .saturating_add(1);
                gate_diagnostic.lookup_completed_at_ms = Some(case_started.elapsed().as_millis());
                gate_diagnostic.lookup_elapsed_ms = Some(lookup_started.elapsed().as_millis());
                gate_diagnostic.compatible =
                    compatible_status_diagnostic(compatible_start_status.as_ref());
                if compatible_start_status.as_ref().is_some_and(|status| {
                    status.owner.is_some()
                        && status.worker.is_some()
                        && status.worker_boot_id.is_some()
                }) {
                    return Ok(());
                }
            }
            time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    let progress_finished_ms = case_started.elapsed().as_millis();
    let lookup_pending_elapsed_ms = gate_diagnostic.lookup_started_at_ms
        .filter(|_| gate_diagnostic.lookup_started > gate_diagnostic.lookup_completed)
        .map(|started| progress_finished_ms.saturating_sub(started));
    // Freeze the last in-bound sample BEFORE stop/release changes any markers.
    // Pending lookup time is cancellation-time metadata, not a late status read.
    let before_cleanup = json!({"last_in_bound":gate_diagnostic,
        "lookup_pending_elapsed_ms":lookup_pending_elapsed_ms});
    // Stop first so assertion failures cannot leave the reconciliation loop
    // manufacturing further evidence while the test unwinds.
    let _ = stop.send(true);
    let stop_requested_ms = case_started.elapsed().as_millis();
    fs::write(&files.release, b"release").unwrap();
    let release_written_ms = case_started.elapsed().as_millis();
    let cleanup = time::timeout(Duration::from_secs(3), &mut runner).await;
    let phase_times = json!({
        "setup_finished_ms":setup_finished_ms,"progress_started_ms":progress_started_ms,
        "progress_finished_ms":progress_finished_ms,"stop_requested_ms":stop_requested_ms,
        "release_written_ms":release_written_ms,
        "runner_join_wait_finished_ms":case_started.elapsed().as_millis(),
        "runner_join_state":match &cleanup {
            Ok(Ok(Ok(()))) => "ok",
            Ok(Ok(Err(_))) => "runner_error",
            Ok(Err(_)) => "join_error",
            Err(_) => "timeout",
        },
    });
    // Logging cannot delay the child's release: only the frozen pre-cleanup
    // sample is printed, even when cleanup itself times out or fails.
    eprintln!("prelaunch startup diagnostic: {}", json!({
        "before_cleanup":before_cleanup,"phase_times":phase_times,
    }));
    cleanup
        .unwrap()
        .unwrap()
        .unwrap();
    progress.unwrap_or_else(|error| panic!(
        "no connected duplicate ACK, scoped rejection, stable passes or receipt-validated compatible adapter start within the bound: {error:?}; compatible={}; gate={before_cleanup}; phases={phase_times}",
        compatible_status_diagnostic(compatible_start_status.as_ref()),
    ))
        .expect("scoped observation delivery disrupted the supervisor");

    let deliveries = connected.trace.lock().unwrap().deliveries_for(&blocked_id);
    assert_eq!(
        deliveries.len(),
        2,
        "stable preparation failure must not create new event IDs"
    );
    assert!(deliveries[0].withheld);
    assert_eq!(deliveries[0].reply, json!({"disposition":"committed"}));
    assert!(!deliveries[1].withheld);
    assert_eq!(
        deliveries[1].reply,
        json!({"disposition":"exact_duplicate"})
    );
    assert_eq!(
        deliveries[0].observation, deliveries[1].observation,
        "lost completion reply must resend the exact retained callback"
    );
    let observation = &deliveries[0].observation;
    assert_eq!(observation["boot_id"], Value::Null);
    assert_eq!(observation["phase"], "isolated");
    assert_eq!(observation["effect_certainty"], "unknown");
    assert_eq!(observation["stage"], "validate_launch");
    assert_eq!(observation["error_code"], UNSUPPORTED);
    assert_eq!(
        observation["unknown_operation_ids"],
        json!(["unknown-native", "unknown-outcome", "unknown-sending"])
    );
    assert_eq!(observation["unknown_operation_count"], 3);
    assert_eq!(observation["unknown_operation_ids_truncated"], false);
    let held_deliveries = connected.trace.lock().unwrap().deliveries_for(&held_id);
    assert_eq!(
        held_deliveries.len(),
        1,
        "permanent ACK must remain scoped and stop callback retries"
    );
    assert_eq!(
        held_deliveries[0].reply["error_code"],
        "MODULE_OBSERVATION_SCOPE_QUARANTINED"
    );
    assert_eq!(held_deliveries[0].reply["retained"], true);
    for binding in [&blocked_id, &held_id] {
        assert_eq!(
            connected
                .trace
                .lock()
                .unwrap()
                .credential_calls
                .get(binding),
            None
        );
        assert!(
            supervisor
                .registry()
                .status(
                    blocked_descriptor.module_id.as_str(),
                    &ServiceScope {
                        binding_id: binding.clone(),
                        generation: 1
                    }
                )
                .await
                .is_none()
        );
    }
    assert!(!files.root.join("blocked-adapter-started").exists());
    let blocked_state = files.root.join("supervisor-state").join(model::digest(
        blocked_descriptor.module_id.as_str().as_bytes(),
    ));
    assert!(
        !blocked_state.exists(),
        "no blocked owner plan, helper launch or worker receipt"
    );
    assert!(
        connected
            .trace
            .lock()
            .unwrap()
            .credential_calls
            .get(&compatible_id)
            .is_some_and(|count| *count >= 2)
    );
    assert_eq!(operation_snapshot(&connected.owner.store).await, before);
    assert_eq!(
        serde_json::to_value(
            control
                .scope_readback(blocked_descriptor.module_id.as_str(), &blocked_id, 1,)
                .await
                .unwrap()
        )
        .unwrap(),
        unknown_before
    );
    assert_eq!(
        observation_count(
            &connected.owner.store,
            &blocked_id,
            "module.supervisor_observation"
        )
        .await,
        1
    );
    assert_eq!(
        observation_count(
            &connected.owner.store,
            &held_id,
            "module.supervisor_observation"
        )
        .await,
        0
    );
    assert_eq!(
        observation_count(
            &connected.owner.store,
            &held_id,
            "module.supervisor_observation_rejected"
        )
        .await,
        2
    );

    let state = connected
        .call(
            &manager,
            "agent.state",
            json!({
                "binding_id":blocked_id, "generation":1,
            }),
        )
        .await;
    assert_eq!(state["state"], "opening");
    let diagnostic = &state["observation"]["module_supervisor"];
    for key in [
        "event_id",
        "module_id",
        "artifact_id",
        "artifact_version",
        "boot_id",
        "phase",
        "effect_certainty",
        "stage",
        "error_code",
        "unknown_operation_ids",
        "unknown_operation_count",
        "unknown_operation_ids_truncated",
    ] {
        assert_eq!(diagnostic[key], observation[key], "agent.state field {key}");
    }
    assert_eq!(diagnostic["retry_authorized"], false);
    let attention = connected
        .call(&manager, "report.attention", json!({"limit":100}))
        .await;
    let item = attention["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| {
            item["kind"] == "module_supervisor_failure" && item["binding_id"] == blocked_id
        })
        .expect("Manager attention must expose the blocked opening");
    assert_eq!(item["address"]["error_code"], UNSUPPORTED);
    assert_eq!(item["address"]["stage"], "validate_launch");
    assert_eq!(item["address"]["retry_authorized"], false);
    assert_eq!(item["suggested_action"]["method"], "agent.state");
    for public in [&state, &attention, observation, &held_deliveries[0].reply] {
        let text = serde_json::to_string(public).unwrap();
        for private in [
            CONFIG_CANARY,
            "native_options",
            "credential_file",
            "the retained descriptor names a launch config schema with no supervisor mapper",
        ] {
            assert!(
                !text.contains(private),
                "private configuration/raw error escaped: {private}"
            );
        }
    }
    for bounded in [
        diagnostic,
        &item["address"],
        observation,
        &held_deliveries[0].reply,
    ] {
        for raw in ["detail", "message", "error", "route_native_options"] {
            assert!(bounded.get(raw).is_none(), "raw error field escaped: {raw}");
        }
    }
    assert!(matches!(
        control.admission().await.unwrap(),
        AdmissionState::Open
    ));
    // Wait for actual helper-family retirement before closing Store/removing
    // the fixture root. Neither the marker nor direct child exit is proof.
    let mut last_retirement_status = None;
    let retirement = time::timeout(BOUND, async {
        loop {
            let status = supervisor
                .registry()
                .status(compatible_descriptor.module_id.as_str(), &compatible_scope)
                .await;
            let terminal = status.as_ref().is_some_and(|status| {
                matches!(
                    &status.lifecycle,
                    LifecycleState::ProcessExited {
                        exit_proven: true,
                        ..
                    } | LifecycleState::Completed { .. }
                )
            });
            last_retirement_status = status;
            if terminal {
                break;
            }
            time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    retirement.unwrap_or_else(|error| panic!(
        "actual compatible owner family did not retire: {error:?}; start={}; last={}; adapter_started={}; release_written={}; adapter_body_returned={}",
        compatible_status_diagnostic(compatible_start_status.as_ref()),
        compatible_status_diagnostic(last_retirement_status.as_ref()),
        files.root.join("compatible-adapter-started").is_file(),
        files.release.is_file(),
        files.root.join("compatible-adapter-started").with_extension("returned").is_file(),
    ));
    drop(supervisor);
    connected.close().await;
    fs::remove_dir_all(&files.root).unwrap();
}
