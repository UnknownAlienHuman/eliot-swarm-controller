use super::*;
use agent_client_protocol::{
    Channel, Client, RawJsonRpcMessage, RawJsonRpcParams, TransportFrame,
    schema::{
        ProtocolVersion,
        v1::{
            InitializeResponse, NewSessionResponse, PromptResponse, SessionConfigOption,
            SessionConfigOptionCategory, SessionConfigSelectOption, StopReason,
        },
    },
};

use serde_json::{Value, json};
use std::{
    future::Future,
    io,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use swarm_contracts::{
    EffectOutcome,
    module_catalog::ModuleDescriptor,
    module_contract::{MODULE_PROTOCOL_V1, ModuleContractClaim},
};
use swarm_kernel_host::{
    config::{Config as HostConfig, Route},
    ipc,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{Notify, mpsc, oneshot, watch},
    task::JoinHandle,
};
use uuid::Uuid;

const ROUTE_ALIAS: &str = "pr44-acp-runtime-ipc";
const SESSION_ID: &str = "pr44-runtime-ipc-session";
const DISPATCH_TEXT: &str = "Admitted by Store; keep this exact prompt across IPC reconnect.";

#[derive(Clone)]
struct ActivePrompt {
    binding_id: String,
    generation: i64,
    session_id: String,
    operation_id: String,
    prompt: String,
    command: RuntimeCommand,
}

#[derive(Clone, Copy)]
enum ReleaseAction {
    BreakConnection,
    ForwardResponse,
}

struct ResponseGate {
    request_method: &'static str,
    action: ReleaseAction,
    release: oneshot::Receiver<()>,
    observed: Option<mpsc::UnboundedSender<()>>,
    held: bool,
}

#[derive(Default)]
struct GateSlots {
    initial_next: Option<ResponseGate>,
    reconnect_hello: Option<ResponseGate>,
}

#[derive(Clone)]
struct IpcFaultPlan {
    fail_next_armed: Arc<AtomicBool>,
    gates: Arc<Mutex<GateSlots>>,
}

struct ObservedIpcStream {
    inner: ipc::Stream,
    incoming: Vec<u8>,
    fail_next_armed: Arc<AtomicBool>,
    gate: Option<ResponseGate>,
}

impl ObservedIpcStream {
    fn observe_requests(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.incoming.push(*byte);
            if *byte != b'\n' {
                continue;
            }
            let request = serde_json::from_slice::<Value>(&self.incoming).ok();
            self.incoming.clear();
            let Some(method) = request
                .as_ref()
                .and_then(|request| request["method"].as_str())
            else {
                continue;
            };
            let Some(gate) = self.gate.as_mut() else {
                continue;
            };
            if gate.held || method != gate.request_method {
                continue;
            }
            let armed = match gate.action {
                ReleaseAction::BreakConnection => {
                    self.fail_next_armed.swap(false, Ordering::AcqRel)
                }
                ReleaseAction::ForwardResponse => true,
            };
            if armed {
                gate.held = true;
                if matches!(gate.action, ReleaseAction::BreakConnection)
                    && let Some(observed) = gate.observed.take()
                {
                    let _ = observed.send(());
                }
            }
        }
    }

    fn poll_gate(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let action = {
            let Some(gate) = self.gate.as_mut().filter(|gate| gate.held) else {
                return Poll::Ready(Ok(()));
            };
            if matches!(gate.action, ReleaseAction::ForwardResponse)
                && let Some(observed) = gate.observed.take()
            {
                let _ = observed.send(());
            }
            match Pin::new(&mut gate.release).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(_) => gate.action,
            }
        };
        match action {
            ReleaseAction::BreakConnection => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "qualification fixture severed the actual Store IPC connection",
            ))),
            ReleaseAction::ForwardResponse => {
                self.gate = None;
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl AsyncRead for ObservedIpcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_before = buffer.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buffer) {
            Poll::Ready(Ok(())) => {
                let filled_after = buffer.filled().len();
                this.observe_requests(&buffer.filled()[filled_before..filled_after]);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl AsyncWrite for ObservedIpcStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match this.poll_gate(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => Pin::new(&mut this.inner).poll_write(cx, bytes),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

fn scripted_model_option() -> SessionConfigOption {
    SessionConfigOption::select(
        "model",
        "Model",
        "scripted/local",
        vec![SessionConfigSelectOption::new(
            "scripted/local",
            "Scripted Local",
        )],
    )
    .category(SessionConfigOptionCategory::Model)
}

fn make_descriptor() -> ModuleDescriptor {
    let mut value: Value =
        serde_json::from_str(include_str!("../../../module-descriptor-acp.template.json"))
            .expect("maintained ACP descriptor parses");
    value["enabled"] = json!(true);
    value["launch"]["executable"] = json!(std::env::current_exe().expect("test executable path"));
    value["launch"]["argv"] = json!([]);
    value["launch"]["credential_ref"] = json!("qualification:pr44-runtime-ipc");
    let descriptor: ModuleDescriptor =
        serde_json::from_value(value).expect("ACP descriptor is typed");
    descriptor.validate().expect("ACP descriptor is valid");
    descriptor
}

async fn store_call(
    store: &swarm_kernel_host::store::Store,
    principal: &Principal,
    method: &str,
    params: Value,
) -> std::result::Result<Value, swarm_kernel_host::error::Error> {
    store
        .call(principal.clone(), method.to_owned(), params)
        .await
}

async fn register_manager(
    store: &swarm_kernel_host::store::Store,
    operator: &Principal,
) -> std::result::Result<Principal, swarm_kernel_host::error::Error> {
    let client_id = format!("pr44-manager-{}", Uuid::new_v4());
    let token = format!("pr44-manager-token-{}", Uuid::new_v4());
    store_call(
        store,
        operator,
        "client.register",
        json!({
            "client_request_id":Uuid::new_v4().to_string(),
            "client_id":client_id,
            "role":"manager",
            "token_hash":model::digest(token.as_bytes()),
        }),
    )
    .await?;
    store.authenticate(Credential { client_id, token }).await
}

async fn serve_listener(
    mut listener: ipc::Listener,
    store: swarm_kernel_host::store::Store,
    ipc_config: swarm_client::IpcConfig,
    mut stopping: watch::Receiver<bool>,
    faults: IpcFaultPlan,
    connections: mpsc::UnboundedSender<
        JoinHandle<std::result::Result<(), swarm_kernel_host::error::Error>>,
    >,
) {
    loop {
        tokio::select! {
            changed = stopping.changed() => {
                let _ = changed;
                break;
            }
            accepted = listener.accept() => {
                let Ok(stream) = accepted else {
                    break;
                };
                let gate = {
                    let mut slots = faults.gates.lock().expect("IPC gate slots are healthy");
                    slots.initial_next.take().or_else(|| slots.reconnect_hello.take())
                };
                let observed: ipc::Stream = Box::new(ObservedIpcStream {
                    inner: stream,
                    incoming: Vec::new(),
                    fail_next_armed: Arc::clone(&faults.fail_next_armed),
                    gate,
                });
                let task = tokio::spawn(ipc::serve(
                    observed,
                    store.clone(),
                    Arc::new(ipc_config.clone()),
                    stopping.clone(),
                ));
                if connections.send(task).is_err() {
                    break;
                }
            }
        }
    }
}

async fn manager_write(
    store: &swarm_kernel_host::store::Store,
    manager: &Principal,
    method: &str,
    mut params: Value,
) -> std::result::Result<Value, swarm_kernel_host::error::Error> {
    params["client_request_id"] = json!(Uuid::new_v4().to_string());
    store_call(store, manager, method, params).await
}

fn task_spec() -> Value {
    let mut spec: Value = serde_json::from_str(include_str!(
        "../../../../swarm-kernel-host/config/task.example.json"
    ))
    .expect("maintained Task specification parses");
    spec["objective"] = json!("Exercise the real ACP module IPC reconnect path");
    spec
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture proves the connected Store-to-ACP prompt recovery contract"
)]
async fn pr44_acp_runtime_ipc_loss_keeps_the_admitted_prompt_and_client()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let fixture_root =
        std::env::temp_dir().join(format!("pr44-acp-runtime-ipc-{}", Uuid::new_v4()));
    let host_path = fixture_root.join("host");
    let workspace = fixture_root.join("workspace");
    let module_state_dir = fixture_root.join("module");
    std::fs::create_dir_all(&host_path)?;
    std::fs::create_dir_all(&workspace)?;
    std::fs::create_dir_all(&module_state_dir)?;
    swarm_process::private_permissions(&module_state_dir, true)?;

    let data_root = DataRoot::acquire(&host_path)?;
    let bootstrap = bootstrap_credential(&data_root.path)?;
    let descriptor = make_descriptor();
    let mut host_config = HostConfig::default();
    host_config.storage.data_dir = data_root.path.clone();
    host_config.routes.push(Route {
        alias: ROUTE_ALIAS.to_owned(),
        runtime: crate::RUNTIME.to_owned(),
        module_artifact_id: crate::ACP_ARTIFACT_ID.to_owned(),
        enabled: true,
        native_options: json!({
            "modelId":"scripted/local",
            "workspaceRoot":workspace.clone(),
        }),
        workspace_option: Some("workspaceRoot".to_owned()),
        owned_service: None,
        admission_policy: None,
    });
    let store_owner =
        StoreOwner::start(data_root, Arc::new(host_config), bootstrap.clone()).await?;
    let store = store_owner.store.clone();
    let operator = store.authenticate(bootstrap).await?;
    let manager = register_manager(&store, &operator).await?;
    let supervisor = store
        .authenticate(store_owner.module_supervisor_credential())
        .await?;

    store_call(
        &store,
        &supervisor,
        "module.descriptor.register",
        json!({"descriptor":descriptor.clone()}),
    )
    .await?;
    let catalog = store_call(&store, &operator, "module.catalog.get", json!({})).await?;
    store_call(
        &store,
        &operator,
        "module.route.select",
        json!({
            "client_request_id":Uuid::new_v4().to_string(),
            "route_alias":ROUTE_ALIAS,
            "module_id":"runtime.command",
            "artifact_id":crate::ACP_ARTIFACT_ID,
            "version":crate::ACP_ARTIFACT_VERSION,
            "expected_catalog_revision":catalog["catalog_revision"],
        }),
    )
    .await?;
    let open = manager_write(
        &store,
        &operator,
        "agent.open",
        json!({"lane_id":"pr44-acp-runtime-ipc","route":ROUTE_ALIAS}),
    )
    .await?;
    let binding_id = open["binding_id"]
        .as_str()
        .expect("Store agent.open returns its binding")
        .to_owned();
    let generation = open["generation"].as_i64().unwrap_or(1);
    let credential_value = store_call(
        &store,
        &supervisor,
        "module.supervisor.credential.ensure",
        json!({
            "operation_id":open["operation_id"],
            "binding_id":binding_id.clone(),
            "generation":generation,
        }),
    )
    .await?;
    let credential_path = PathBuf::from(
        credential_value["credential_file"]
            .as_str()
            .expect("Store provisions the exact private module credential file"),
    );
    let provisioned_credential = swarm_kernel_host::platform::load_credential(&credential_path)?;
    assert_eq!(
        credential_value["module_client_id"],
        provisioned_credential.client_id
    );
    let module_credential: swarm_contracts::Credential =
        serde_json::from_value(serde_json::to_value(provisioned_credential)?)?;
    let claim = ModuleContractClaim::from_descriptor(&descriptor, MODULE_PROTOCOL_V1)
        .expect("registered ACP descriptor yields its exact claim");

    let owner_token = Uuid::new_v4().to_string();
    let process_group = swarm_process::Group::enter_module(&owner_token)?;
    let owner_record = json!({
        "version":1,
        "token":owner_token,
        "process":process_group.identity.clone(),
    });
    assert_eq!(owner_record["process"]["pid"], std::process::id());

    swarm_process::write_private_new(
        &module_state_dir.join("owner.json"),
        &serde_json::to_vec(&owner_record)?,
    )?;
    let module_host = module_host::ModuleHostIdentity {
        claim,
        profile: crate::Profile::AcpV2,
        boot_id: Uuid::new_v4().to_string(),
        binding_id: binding_id.clone(),
        generation,
        module_client_id: module_credential.client_id.clone(),
        credential_file: credential_path,
    };
    let owner = Owner {
        state_dir: module_state_dir,
        token: owner_record["token"].as_str().unwrap().to_owned(),
        record: owner_record,
        host: module_host,
    };
    let config = Config {
        module_artifact_id: crate::ACP_ARTIFACT_ID.to_owned(),
        command: PathBuf::from("unused-scripted-peer"),
        command_args: Vec::new(),
        mod_path: None,
        run_timeout_ms: None,
        host_connection: Some(swarm_client::HostConnectionConfig {
            schema_version: 1,
            host_data_dir: host_path.clone(),
            ipc: swarm_client::IpcConfig::default(),
        }),
    };
    config
        .host_connection
        .as_ref()
        .expect("Store IPC config exists")
        .validate()?;

    let faults = IpcFaultPlan {
        fail_next_armed: Arc::new(AtomicBool::new(false)),
        gates: Arc::new(Mutex::new(GateSlots::default())),
    };
    let (fail_release, fail_release_rx) = oneshot::channel();
    let (next_waiting_tx, mut next_waiting_rx) = mpsc::unbounded_channel();
    let (hello_release, hello_release_rx) = oneshot::channel();
    let (hello_waiting_tx, mut hello_waiting_rx) = mpsc::unbounded_channel();
    {
        let mut gates = faults.gates.lock().expect("IPC gate slots are healthy");
        gates.initial_next = Some(ResponseGate {
            request_method: "module.next",
            action: ReleaseAction::BreakConnection,
            release: fail_release_rx,
            observed: Some(next_waiting_tx),
            held: false,
        });
        gates.reconnect_hello = Some(ResponseGate {
            request_method: "module.hello",
            action: ReleaseAction::ForwardResponse,
            release: hello_release_rx,
            observed: Some(hello_waiting_tx),
            held: false,
        });
    }
    let listener = ipc::Listener::bind(&host_path)?;
    let (stopping, stopping_rx) = watch::channel(false);
    let (connection_tx, mut connection_rx) = mpsc::unbounded_channel();
    let server = tokio::spawn(serve_listener(
        listener,
        store.clone(),
        swarm_client::IpcConfig::default(),
        stopping_rx,
        faults.clone(),
        connection_tx,
    ));
    let mut link = crate::adapter::connect_module_with_native_state(
        &config,
        &module_credential,
        &owner,
        None,
        None,
        false,
    )
    .await?;
    validate_acp_route_with_model_policy(&link.route, owner.host.profile.model_optional())?;
    let open_command: RuntimeCommand =
        serde_json::from_value(link.client.next().await?["command"].clone())?;
    assert_eq!(open_command.operation_id, open["operation_id"]);
    crate::adapter::validate_command(&open_command, &link, None)?;

    let journal = AcpJournal::new_for_profile(&owner.state_dir, owner.host.profile)?;
    let capture = Arc::new(Mutex::new(Capture::default()));
    let (transport, peer) = Channel::duplex();
    let (active_tx, active_rx) = oneshot::channel();
    let (expected_prompt_tx, expected_prompt_rx) = oneshot::channel();
    let (prompt_seen_tx, prompt_seen_rx) = oneshot::channel();
    let expected_workspace = workspace.clone();
    let peer_workspace = workspace.clone();
    let store_for_adapter = store.clone();
    let manager_for_adapter = manager.clone();
    let mut adapter_link = link;
    let adapter_config = config.clone();
    let adapter_owner = owner.clone();
    let adapter_credential = module_credential.clone();
    let adapter_journal = journal.clone();
    let adapter_capture = Arc::clone(&capture);

    let fail_next_armed = Arc::clone(&faults.fail_next_armed);

    let client = Client
        .builder()
        .connect_with(transport, async move |connection| {
            let observation_sequence = Arc::new(Mutex::new(2_u64));
            let adapter_recovery = ModuleLinkRecovery {
                config: &adapter_config,
                credential: &adapter_credential,
                owner: &adapter_owner,
                journal: &adapter_journal,
                capture: &adapter_capture,
            };

            adapter_journal
                .record_command(&open_command)
                .map_err(to_acp_error)?;
            let features = initialize(&connection).await?;
            let open_workspace = expected_workspace.as_path();
            let route = SessionRouteConfiguration {
                model_id: command_model(&adapter_link.route).map(str::to_owned),
                effort: command_effort(&adapter_link.route).map(str::to_owned),
                mode: command_mode(&adapter_link.route).map(str::to_owned),
            };
            let opened_session = open_session(
                SessionOpenContext {
                    connection: &connection,
                    journal: &adapter_journal,
                    command: &open_command,
                    workspace: open_workspace,
                    owner: &adapter_owner,
                    features: &features,
                    capture: &adapter_capture,
                },
                &route,
            )
            .await
            .map_err(to_acp_error)?;
            let open_outcome = applied_in_session(
                &open_command,
                &opened_session,
                json!({
                    "execution_shape":crate::ACP_EXECUTION_SHAPE,
                    "native_session_state":"open",
                    "family_departure_claimed":false,
                }),
            );
            adapter_recovery
                .deliver_outcome(&mut adapter_link, open_command.clone(), open_outcome)
                .await
                .map_err(to_acp_error)?;
            let retained_session = adapter_journal.read_session().map_err(to_acp_error)?;
            let observation = acp_state(
                &adapter_link,
                &adapter_owner,
                &json!({
                    "operation_id":open_command.operation_id,
                    "method":"agent.open",
                    "state":"applied",
                }),
                retained_session.as_ref(),
                &[],
                None,
            );
            adapter_recovery
                .send_observation(&mut adapter_link, 1, observation)
                .await
                .map_err(to_acp_error)?;

            let task = manager_write(
                &store_for_adapter,
                &manager_for_adapter,
                "task.create",
                json!({
                    "project_id":"pr44-acp-runtime-ipc",
                    "origin_key":format!("pr44-acp-runtime-ipc-{}",Uuid::new_v4()),
                    "spec":task_spec(),
                }),
            )
            .await
            .map_err(|error| {
                agent_client_protocol::Error::internal_error().data(error.to_string())
            })?;
            let attempt = manager_write(
                &store_for_adapter,
                &manager_for_adapter,
                "task.claim",
                json!({
                    "task_id":task["task_id"],
                    "expected_revision":1,
                    "start_owner":"controller",
                    "binding_id":adapter_link.binding_id,
                    "binding_generation":adapter_link.generation,
                }),
            )
            .await
            .map_err(|error| {
                agent_client_protocol::Error::internal_error().data(error.to_string())
            })?;
            let dispatch_admission = manager_write(
                &store_for_adapter,
                &manager_for_adapter,
                "task.dispatch",
                json!({
                    "attempt_id":attempt["attempt_id"],
                    "text":DISPATCH_TEXT,
                }),
            )
            .await
            .map_err(|error| {
                agent_client_protocol::Error::internal_error().data(error.to_string())
            })?;
            let dispatch_command: RuntimeCommand = serde_json::from_value(
                adapter_link.client.next().await.map_err(to_acp_error)?["command"].clone(),
            )
            .map_err(|error| {
                agent_client_protocol::Error::internal_error().data(error.to_string())
            })?;
            assert_eq!(
                dispatch_command.operation_id,
                dispatch_admission["operation_id"]
            );
            let opened_session_id = opened_session.id.to_string();
            crate::adapter::validate_command(
                &dispatch_command,
                &adapter_link,
                Some(&opened_session_id),
            )
            .map_err(to_acp_error)?;
            adapter_journal
                .record_command(&dispatch_command)
                .map_err(to_acp_error)?;
            let prepared =
                acp_prompt::prepare(&dispatch_command, adapter_link.profile.model_optional())
                    .map_err(to_acp_error)?;
            assert!(prepared.envelope.prompt.starts_with(DISPATCH_TEXT));
            assert!(prepared.envelope.prompt.contains("ELIOT Task identity v1:"));
            let active_prompt = ActivePrompt {
                binding_id: adapter_link.binding_id.clone(),
                generation: adapter_link.generation,
                session_id: opened_session.id.to_string(),
                operation_id: dispatch_command.operation_id.clone(),
                prompt: prepared.envelope.prompt.clone(),
                command: dispatch_command.clone(),
            };
            assert!(
                active_tx.send(active_prompt.clone()).is_ok(),
                "test observes the admitted operation identity"
            );
            assert!(
                expected_prompt_tx.send(active_prompt.clone()).is_ok(),
                "scripted ACP peer observes the same admitted prompt"
            );
            fail_next_armed.store(true, Ordering::Release);

            let mut session = Some(opened_session);
            let permissions: PermissionBroker =
                Arc::new(Mutex::new(std::collections::HashMap::new()));
            let permission_wakeup = Arc::new(Notify::new());
            let mut context = CommandContext {
                connection: &connection,
                link: &mut adapter_link,
                recovery: &adapter_recovery,
                owner: &adapter_owner,
                journal: &adapter_journal,
                features: &features,
                session: &mut session,
                workspace: Some(open_workspace),
                model: route.model_id.as_deref(),
                capture: &adapter_capture,
                permissions: &permissions,
                permission_wakeup: &permission_wakeup,
                observation_sequence: &observation_sequence,
            };
            let dispatch_outcomes = process_command(&mut context, &dispatch_command)
                .await
                .map_err(to_acp_error)?;
            let dispatch_outcome = dispatch_outcomes
                .into_iter()
                .next()
                .expect("one outcome belongs to the active dispatch");
            assert_eq!(dispatch_outcome.operation_id, dispatch_command.operation_id);
            assert_eq!(dispatch_outcome.outcome, EffectOutcome::Applied);
            adapter_recovery
                .deliver_outcome(
                    &mut adapter_link,
                    dispatch_command.clone(),
                    dispatch_outcome,
                )
                .await
                .map_err(to_acp_error)?;
            assert_eq!(adapter_link.binding_id, active_prompt.binding_id);
            assert_eq!(adapter_link.generation, active_prompt.generation);
            Ok::<(), agent_client_protocol::Error>(())
        });

    let peer = async move {
        let mut peer = peer;
        let Some(TransportFrame::Single(RawJsonRpcMessage::Request(initialize))) =
            peer.rx.next().await
        else {
            panic!("scripted peer receives ACP initialize");
        };
        assert_eq!(initialize.method.as_ref(), "initialize");
        let initialize_response = InitializeResponse::new(ProtocolVersion::V1);
        peer.tx
            .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                initialize.id,
                Ok(serde_json::to_value(initialize_response)
                    .expect("initialize response serializes")),
            )))
            .expect("ACP client accepts initialize response");

        let Some(TransportFrame::Single(RawJsonRpcMessage::Request(new_session))) =
            peer.rx.next().await
        else {
            panic!("same ACP client opens one session");
        };
        assert_eq!(new_session.method.as_ref(), "session/new");
        let new_session_params = match new_session.params {
            Some(RawJsonRpcParams::Object(params)) => params,
            _ => panic!("session/new parameters are an object"),
        };
        assert_eq!(
            new_session_params["cwd"],
            peer_workspace.to_string_lossy().as_ref()
        );
        peer.tx
            .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                new_session.id,
                Ok(
                    serde_json::to_value(NewSessionResponse::new(SESSION_ID).config_options(vec![
                    SessionConfigOption::select(
                        "model",
                        "Model",
                        "scripted/local",
                        vec![SessionConfigSelectOption::new(
                            "scripted/local",
                            "Scripted Local",
                        )],
                    )
                    .category(SessionConfigOptionCategory::Model),
                ]))
                    .expect("typed session response serializes"),
                ),
            )))
            .expect("ACP client accepts the single session/new response");

        let Some(TransportFrame::Single(RawJsonRpcMessage::Request(set_config))) =
            peer.rx.next().await
        else {
            panic!("session/new model readback uses session/set_config_option");
        };
        assert_eq!(set_config.method.as_ref(), "session/set_config_option");
        let set_config_params = match set_config.params {
            Some(RawJsonRpcParams::Object(params)) => params,
            _ => panic!("session/set_config_option parameters are an object"),
        };
        assert_eq!(set_config_params["sessionId"], SESSION_ID);
        assert_eq!(set_config_params["configId"], "model");
        assert_eq!(set_config_params["value"], "scripted/local");
        peer.tx
            .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                set_config.id,
                Ok(
                    serde_json::to_value(SetSessionConfigOptionResponse::new(vec![
                        scripted_model_option(),
                    ]))
                    .expect("typed model readback serializes"),
                ),
            )))
            .expect("model readback response reaches the adapter");

        let expected = expected_prompt_rx
            .await
            .expect("Store dispatch supplies the exact prompt identity");
        let Some(TransportFrame::Single(RawJsonRpcMessage::Request(prompt))) = peer.rx.next().await
        else {
            panic!("the admitted prompt reaches the same ACP connection");
        };
        assert_eq!(prompt.method.as_ref(), "session/prompt");
        let prompt_params = match prompt.params {
            Some(RawJsonRpcParams::Object(params)) => params,
            _ => panic!("session/prompt parameters are an object"),
        };
        assert_eq!(prompt_params["sessionId"], expected.session_id);
        assert_eq!(prompt_params["prompt"][0]["text"], expected.prompt);
        prompt_seen_tx
            .send(())
            .expect("host IPC fault waits until the prompt is active");
        let prompt_id = prompt.id;

        let Some(TransportFrame::Single(RawJsonRpcMessage::Notification(cancel))) =
            peer.rx.next().await
        else {
            panic!("reconnected Store control uses session/cancel");
        };
        assert_eq!(cancel.method.as_ref(), "session/cancel");
        let cancel_params =
            serde_json::to_value(cancel.params).expect("session/cancel parameters serialize");
        assert_eq!(cancel_params["sessionId"], expected.session_id);

        peer.tx
            .unbounded_send(TransportFrame::Single(RawJsonRpcMessage::response(
                prompt_id,
                Ok(
                    serde_json::to_value(PromptResponse::new(StopReason::Cancelled))
                        .expect("cancelled prompt response serializes"),
                ),
            )))
            .expect("the original prompt receives its cancellation result");
        assert!(
            tokio::time::timeout(Duration::from_secs(3), peer.rx.next())
                .await
                .expect("ACP client closes after the one prompt")
                .is_none(),
            "no prompt replay, session/load fallback, or $/cancel_request is sent"
        );
        Ok::<(), agent_client_protocol::Error>(())
    };

    let store_for_cancel = store.clone();
    let manager_for_cancel = manager.clone();
    let orchestrate = async move {
        let active_prompt = tokio::time::timeout(Duration::from_secs(10), active_rx)
            .await
            .map_err(|_| {
                agent_client_protocol::Error::internal_error()
                    .data("admitted dispatch identity wait timed out")
            })?
            .map_err(|_| {
                agent_client_protocol::Error::internal_error()
                    .data("adapter dropped admitted dispatch identity")
            })?;
        tokio::time::timeout(Duration::from_secs(10), prompt_seen_rx)
            .await
            .map_err(|_| {
                agent_client_protocol::Error::internal_error().data("prompt peer wait timed out")
            })?
            .map_err(|_| {
                agent_client_protocol::Error::internal_error()
                    .data("prompt peer dropped its admission signal")
            })?;
        tokio::time::timeout(Duration::from_secs(10), next_waiting_rx.recv())
            .await
            .map_err(|_| {
                agent_client_protocol::Error::internal_error()
                    .data("old module.next wait timed out")
            })?
            .ok_or_else(|| {
                agent_client_protocol::Error::internal_error()
                    .data("old module.next gate channel closed")
            })?;
        fail_release.send(()).map_err(|_| {
            agent_client_protocol::Error::internal_error()
                .data("old module.next gate release failed")
        })?;
        tokio::time::timeout(Duration::from_secs(20), hello_waiting_rx.recv())
            .await
            .map_err(|_| {
                agent_client_protocol::Error::internal_error()
                    .data("reconnect hello wait timed out")
            })?
            .ok_or_else(|| {
                agent_client_protocol::Error::internal_error()
                    .data("reconnect hello gate channel closed")
            })?;

        let cancel_result = store_call(
            &store_for_cancel,
            &manager_for_cancel,
            "native.command.cancel_turn",
            json!({
                "client_request_id":Uuid::new_v4().to_string(),
                "binding_id":active_prompt.binding_id.clone(),
                "generation":active_prompt.generation,
                "session_id":active_prompt.session_id.clone(),
                "target_operation_id":active_prompt.operation_id.clone(),
                "turn_id":active_prompt.operation_id.clone(),
            }),
        )
        .await;
        hello_release.send(()).map_err(|_| {
            agent_client_protocol::Error::internal_error()
                .data("reconnect hello release gate failed")
        })?;
        let cancel_admission = cancel_result.map_err(|error| {
            agent_client_protocol::Error::internal_error().data(error.to_string())
        })?;
        if cancel_admission["operation_id"].as_str().is_none() {
            return Err(agent_client_protocol::Error::internal_error());
        }
        Ok::<ActivePrompt, agent_client_protocol::Error>(active_prompt)
    };
    let (_, _, active_prompt) = tokio::time::timeout(Duration::from_secs(30), async {
        futures::try_join!(client, peer, orchestrate)
    })
    .await??;

    {
        let capture = capture
            .lock()
            .map_err(|_| invalid("fixture capture mutex is poisoned"))?;
        assert!(capture.module_ipc_disconnected);
        assert_eq!(capture.native_session_id.as_deref(), Some(SESSION_ID));
    }

    let dispatch_dir = journal.operation_dir(&active_prompt.operation_id)?;
    let prompt_marker: Value = read_json(&dispatch_dir.join("prompt-state.json"))?;
    assert_eq!(prompt_marker["state"], "native_prompt_started");
    assert_eq!(prompt_marker["session_id"], active_prompt.session_id);
    assert_eq!(prompt_marker["turn_id"], active_prompt.operation_id);
    assert_eq!(
        prompt_marker["prompt_sha256"],
        digest(active_prompt.prompt.as_bytes())
    );
    let prompt_intent = journal.read_effect(&active_prompt.command, "session/prompt", "intent")?;
    assert_eq!(
        prompt_intent
            .as_ref()
            .map(|value| value["turn_id"].as_str()),
        Some(Some(active_prompt.operation_id.as_str()))
    );

    stopping
        .send(true)
        .expect("IPC listener stops after verification");
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("IPC listener task stops")
        .expect("IPC listener task joins");
    while let Ok(connection) = connection_rx.try_recv() {
        let _ = tokio::time::timeout(Duration::from_secs(5), connection).await;
    }
    drop(store);
    store_owner.close().await?;
    std::fs::remove_dir_all(fixture_root)?;
    Ok(())
}
