//! Command-to-native-to-Store-ack consumers for PR43's remaining controls.

use super::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};
use swarm_contracts::{
    module_catalog::{ModuleDescriptor, ProtocolVersion},
    module_contract::ModuleContractClaim,
    runtime::RuntimeCommand,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
};

#[derive(Debug, Clone, Copy)]
enum NativeMode {
    Questions,
    PermissionCascade,
    Refresh,
}

#[derive(Debug, Clone)]
struct NativeRequest {
    method: String,
    path: String,
    body: Value,
}

struct NativeState {
    mode: NativeMode,
    root: String,
    binding_id: String,
    generation: i64,
    directory: String,
    model: ModelRef,
    requests: Mutex<Vec<NativeRequest>>,
}

struct NativeFixture {
    state: Arc<NativeState>,
    stop: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}

impl NativeFixture {
    async fn start(
        connection_file: &Path,
        options: &NativeOptions,
        binding_id: &str,
        generation: i64,
        mode: NativeMode,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let root = root_id(binding_id, generation);
        fs::write(
            connection_file,
            serde_json::to_vec(&json!({
                "schema_version":1,"endpoint":endpoint,"pid":43117,
                "username":"fixture-user","password":"fixture-password"
            }))
            .unwrap(),
        )
        .unwrap();
        let state = Arc::new(NativeState {
            mode,
            root,
            binding_id: binding_id.to_owned(),
            generation,
            directory: options.directory.to_string_lossy().into_owned(),
            model: options.model.clone(),
            requests: Mutex::new(Vec::new()),
        });
        let (stop, mut stopped) = watch::channel(false);
        let task_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    changed = stopped.changed() => {
                        if changed.is_err() || *stopped.borrow() { break; }
                        continue;
                    }
                    accepted = listener.accept() => accepted?,
                };
                serve_native_request(accepted.0, Arc::clone(&task_state)).await?;
            }
            Ok(())
        });
        Self { state, stop, task }
    }

    fn requests(&self) -> Vec<NativeRequest> {
        self.state.requests.lock().unwrap().clone()
    }

    async fn close(self) {
        let _ = self.stop.send(true);
        self.task.abort();
        let _ = self.task.await;
    }
}

#[derive(Debug, Clone)]
struct RpcCall {
    method: String,
    params: Value,
    recorded: Option<bool>,
    cursor_at_call: Option<u64>,
}

struct RpcState {
    calls: Mutex<Vec<RpcCall>>,
    observe_count: AtomicUsize,
    observe_ack_after: usize,
    acknowledged_cursor: AtomicU64,
    premature_cursor_outcome: AtomicBool,
}

struct ModuleRpcFixture {
    state: Arc<RpcState>,
    stop: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}

struct ControlFixture {
    root_dir: PathBuf,
    config: AdapterConfig,
    credential: Credential,
    claim: ModuleContractClaim,
    journal: Journal,
    native: NativeFixture,
    rpc: ModuleRpcFixture,
    outcomes: Arc<Mutex<Vec<Value>>>,
}

impl ControlFixture {
    async fn new(mode: NativeMode, observe_ack_after: usize) -> Self {
        let root_dir =
            std::env::temp_dir().join(format!("eliot-pr43-control-{}", uuid::Uuid::new_v4()));
        let host_data_dir = root_dir.join("host");
        let state_dir = root_dir.join("adapter-state");
        let workspace = root_dir.join("workspace");
        fs::create_dir_all(&host_data_dir).unwrap();
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        let binding_id = "pr43-control-fixture".to_owned();
        let generation = 1;
        let native_options = NativeOptions {
            service_id: "pr43-loopback-fixture".to_owned(),
            connection_file: root_dir.join("connection.json"),
            directory: workspace,
            model: ModelRef {
                id: "fixture-model".to_owned(),
                provider_id: "fixture-provider".to_owned(),
                variant: "fixture-variant".to_owned(),
            },
        };
        let native = NativeFixture::start(
            &native_options.connection_file,
            &native_options,
            &binding_id,
            generation,
            mode,
        )
        .await;
        let config = AdapterConfig {
            schema_version: 1,
            host_data_dir: host_data_dir.clone(),
            credential_file: root_dir.join("credential.json"),
            state_dir: state_dir.clone(),
            binding_id: binding_id.clone(),
            generation,
            module_artifact_id: ARTIFACT_ID.to_owned(),
            native_options,
            owned_native: None,
            ipc: swarm_client::IpcConfig::default(),
        };
        config.validate().unwrap();
        let credential = Credential {
            client_id: "pr43-control-fixture".to_owned(),
            token: "fixture-token".to_owned(),
        };
        let claim = fixture_claim();
        let scope = config.native_options.scope_key();
        let route_digest =
            digest_json(&serde_json::to_value(&config.native_options).unwrap()).unwrap();
        let journal =
            Journal::open(&state_dir, &binding_id, generation, &scope, &route_digest).unwrap();
        let root = native.state.root.clone();
        journal
            .remember_native_root(&binding_id, generation, &scope, &root)
            .unwrap();
        let rpc = module_rpc_fixture(&config, &claim, observe_ack_after);
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        Self {
            root_dir,
            config,
            credential,
            claim,
            journal,
            native,
            rpc,
            outcomes,
        }
    }

    fn root(&self) -> &str {
        &self.native.state.root
    }

    fn host(&self) -> HostSession<'_> {
        HostSession {
            config: &self.config,
            credential: &self.credential,
            journal: &self.journal,
            claim: &self.claim,
            boot_id: "pr43-control-boot".to_owned(),
            hello_base: json!({
                "boot_id":"pr43-control-boot",
                "module_artifact_id":ARTIFACT_ID,
                "native_ready":false,
                "managed_owner":{"token":"fixture-owner","process":{"pid":std::process::id()}}
            }),
            root_hint: Mutex::new(Some(self.root().to_owned())),
            ipc_link: AsyncMutex::new(None),
            outcome_observer: Some(Arc::clone(&self.outcomes)),
            retention_retry_pending: AtomicBool::new(false),
        }
    }

    fn command(
        &self,
        operation_id: &str,
        method: &str,
        input: Value,
        target_input_sha256: Option<String>,
    ) -> RuntimeCommand {
        let input_sha256 = sha256_json(&input);
        RuntimeCommand {
            operation_id: operation_id.to_owned(),
            method: method.to_owned(),
            created_at_ms: 1,
            binding_id: self.config.binding_id.clone(),
            generation: self.config.generation,
            native_root_id: Some(self.root().to_owned()),
            route: json!({
                "runtime":RUNTIME,
                "module_artifact_id":ARTIFACT_ID,
                "workspace_option":"directory",
                "native_options":self.config.native_options
            }),
            input,
            input_sha256: Some(input_sha256),
            target_input_sha256,
        }
    }

    async fn close(self) {
        self.native.close().await;
        let _ = self.rpc.stop.send(true);
        self.rpc.task.abort();
        let _ = self.rpc.task.await;
        let _ = tokio::fs::remove_dir_all(self.root_dir).await;
    }
}

fn fixture_claim() -> ModuleContractClaim {
    let mut value: Value =
        serde_json::from_str(include_str!("../registration/descriptor.template.json")).unwrap();
    value["launch"]["executable"] = json!(std::env::current_exe().unwrap());
    let descriptor: ModuleDescriptor = serde_json::from_value(value).unwrap();
    ModuleContractClaim::from_descriptor(&descriptor, ProtocolVersion { major: 1, minor: 0 })
        .unwrap()
}

fn sha256_json(value: &Value) -> String {
    let bytes = canonical_json(value).unwrap();
    format!("{:x}", Sha256::digest(bytes.as_bytes()))
}

fn native_question(root: &str, id: &str) -> Value {
    json!({
        "id":id,
        "sessionID":root,
        "questions":[{
            "question":"Choose one",
            "header":"Choice",
            "options":[{"label":"yes","description":"Accept"}],
            "multiple":false,
            "custom":false
        }]
    })
}

fn native_permission(root: &str, id: &str) -> Value {
    json!({"id":id,"sessionID":root,"action":"shell","resources":["fixture"]})
}

fn durable_event(root: &str, id: &str, event_type: &str, sequence: u64, data: Value) -> Value {
    json!({
        "id":id,
        "type":event_type,
        "version":1,
        "created":sequence as f64,
        "durable":{"aggregateID":root,"seq":sequence,"version":1},
        "data":data
    })
}

fn root_created_event(state: &NativeState) -> Value {
    durable_event(
        &state.root,
        "evt_pr43_root_created",
        "session.created",
        1,
        json!({
            "sessionID":state.root,
            "parentID":null,
            "model":{
                "id":state.model.id,
                "providerID":state.model.provider_id,
                "variant":state.model.variant
            },
            "metadata":{"eliot":{"binding":state.binding_id,"generation":state.generation}}
        }),
    )
}

fn history_for(state: &NativeState, after: u64) -> Vec<Value> {
    let root_event = root_created_event(state);
    match state.mode {
        NativeMode::Questions => vec![root_event]
            .into_iter()
            .filter(|event| event["durable"]["seq"].as_u64().unwrap() > after)
            .collect(),
        NativeMode::Refresh => {
            let question = native_question(&state.root, "que_refresh");
            [
                root_event,
                durable_event(
                    &state.root,
                    "evt_pr43_question_asked",
                    "question.v2.asked",
                    2,
                    question,
                ),
            ]
            .into_iter()
            .filter(|event| event["durable"]["seq"].as_u64().unwrap() > after)
            .collect()
        }
        NativeMode::PermissionCascade => vec![root_event]
            .into_iter()
            .filter(|event| event["durable"]["seq"].as_u64().unwrap() > after)
            .collect(),
    }
}

#[tokio::test]
async fn question_reply_and_reject_cross_native_and_outcome_ack() {
    let fixture = ControlFixture::new(NativeMode::Questions, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();

    let reply_request = native_question(fixture.root(), "que_reply");
    let reply_command_input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "question",
        fixture.root(),
        &reply_request,
        json!({"answers":[["yes"]]}),
    );
    let reply = fixture.command(
        "pr43-question-reply",
        "agent.reply",
        reply_command_input,
        None,
    );
    execute(&host, &reply).await.unwrap();

    let reject_request = native_question(fixture.root(), "que_reject");
    let reject_input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "question",
        fixture.root(),
        &reject_request,
        json!({"decision":"reject"}),
    );
    let reject = fixture.command("pr43-question-reject", "agent.reply", reject_input, None);
    execute(&host, &reject).await.unwrap();

    let posts = fixture
        .native
        .requests()
        .into_iter()
        .filter(|request| request.method == "POST")
        .collect::<Vec<_>>();
    assert_eq!(posts.len(), 2);
    assert_eq!(
        posts[0].path,
        format!("/api/session/{}/question/que_reply/reply", fixture.root())
    );
    assert_eq!(posts[0].body, json!({"answers":[["yes"]]}));
    assert_eq!(
        posts[1].path,
        format!("/api/session/{}/question/que_reject/reject", fixture.root())
    );
    assert_eq!(posts[1].body, Value::Null);

    let outcomes = fixture.outcomes.lock().unwrap().clone();
    assert_eq!(outcomes.len(), 2);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome["outcome"] == "applied")
    );
    assert_eq!(outcomes[0]["details"]["action"], "reply");
    assert_eq!(outcomes[1]["details"]["action"], "reject");
    let calls = fixture.rpc.calls();
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.method == "module.outcome")
            .count(),
        2
    );
    drop(host);
    fixture.close().await;
}

#[tokio::test]
async fn permission_reject_cascade_reaches_native_and_outcome_ack() {
    let fixture = ControlFixture::new(NativeMode::PermissionCascade, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();
    let target_request = native_permission(fixture.root(), "per_primary");
    let input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "permission",
        fixture.root(),
        &target_request,
        json!({"decision":"reject"}),
    );
    let target_id = "pr43-permission-reject";
    let command = fixture.command(target_id, "agent.reply", input, None);
    execute(&host, &command).await.unwrap();
    let outcome = fixture.journal.load(target_id).unwrap().outcome.unwrap();
    assert_eq!(outcome["outcome"], "applied");
    assert_eq!(
        outcome["details"]["affected_request_ids"],
        json!(["per_primary", "per_secondary"])
    );
    let posts = fixture
        .native
        .requests()
        .into_iter()
        .filter(|request| request.method == "POST")
        .collect::<Vec<_>>();
    assert_eq!(posts.len(), 1);
    assert_eq!(
        posts[0].path,
        format!(
            "/api/session/{}/permission/per_primary/reply",
            fixture.root()
        )
    );
    assert_eq!(posts[0].body, json!({"reply":"reject"}));
    let calls = fixture.rpc.calls();
    assert!(calls.iter().any(|call| {
        call.method == "module.outcome"
            && call.recorded == Some(true)
            && call.params["operation_id"] == target_id
            && call.params["details"]["affected_request_ids"]
                == json!(["per_primary", "per_secondary"])
    }));
    drop(host);
    fixture.close().await;
}

#[tokio::test]
async fn refresh_cursor_is_published_only_after_recorded_observation_ack() {
    let fixture = ControlFixture::new(NativeMode::Refresh, 2).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();
    let command = fixture.command(
        "pr43-refresh-ack-cursor",
        "agent.refresh",
        json!({"session_id":fixture.root(),"after_sequence":1}),
        None,
    );
    execute(&host, &command).await.unwrap();

    let calls = fixture.rpc.calls();
    let observations = calls
        .iter()
        .filter(|call| call.method == "module.observe")
        .collect::<Vec<_>>();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].recorded, Some(false));
    assert_eq!(observations[1].recorded, Some(true));
    assert_eq!(observations[0].params, observations[1].params);
    assert_eq!(observations[1].params["state"]["after_sequence"], 1);
    assert_eq!(observations[1].params["state"]["through_sequence"], 2);
    assert_eq!(fixture.rpc.acknowledged_cursor(), 2);
    assert!(!fixture.rpc.premature_cursor_outcome());
    let outcome = calls
        .iter()
        .find(|call| call.method == "module.outcome")
        .unwrap();
    assert_eq!(outcome.params["operation_id"], "pr43-refresh-ack-cursor");
    assert_eq!(outcome.params["details"]["next_after_sequence"], 2);
    assert_eq!(outcome.cursor_at_call, Some(2));
    drop(host);
    fixture.close().await;
}
async fn serve_native_request(stream: TcpStream, state: Arc<NativeState>) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).await? == 0 {
        return Ok(());
    }
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default().to_owned();
    let target = request_parts.next().unwrap_or_default().to_owned();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body_bytes = vec![0; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body_bytes).await?;
    }
    let body = if body_bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body_bytes).unwrap_or(Value::Null)
    };
    let path = target.split('?').next().unwrap_or_default().to_owned();
    state.requests.lock().unwrap().push(NativeRequest {
        method: method.clone(),
        path: path.clone(),
        body: body.clone(),
    });
    let Some(response) = native_response(&state, &method, &target, &path, body) else {
        return Ok(());
    };
    let status_text = match response.status {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        _ => "Fixture Response",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.status,
        status_text,
        response.content_type,
        response.body.len()
    );
    reader.get_mut().write_all(header.as_bytes()).await?;
    reader.get_mut().write_all(&response.body).await?;
    reader.get_mut().flush().await
}

struct NativeResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

fn json_response(status: u16, value: Value) -> Option<NativeResponse> {
    Some(NativeResponse {
        status,
        content_type: "application/json",
        body: serde_json::to_vec(&value).unwrap(),
    })
}

fn native_response(
    state: &NativeState,
    method: &str,
    target: &str,
    path: &str,
    _body: Value,
) -> Option<NativeResponse> {
    let location = json!({
        "directory":state.directory,
        "project":{"id":"pr43-fixture-project"}
    });
    let session = json!({
        "id":state.root,
        "parentID":null,
        "fork":null,
        "location":{"directory":state.directory},
        "model":{
            "id":state.model.id,
            "providerID":state.model.provider_id,
            "variant":state.model.variant
        },
        "metadata":{"eliot":{"binding":state.binding_id,"generation":state.generation}}
    });
    if method == "GET" && path == "/api/info" {
        return json_response(
            200,
            json!({
                "version":"pr43-fixture-version",
                "pid":43117,
                "urls":["http://127.0.0.1/"],
                "paths":{"tmp":"/tmp/pr43-fixture"}
            }),
        );
    }
    if method == "GET" && path == "/api/location" {
        return json_response(200, location);
    }
    if method == "GET" && path == "/api/model" {
        return json_response(
            200,
            json!({
                "location":{"directory":state.directory},
                "data":[{
                    "id":state.model.id,
                    "providerID":state.model.provider_id,
                    "enabled":true,
                    "variants":[{"id":state.model.variant}]
                }]
            }),
        );
    }
    if method == "GET" && path == format!("/api/session/{}", state.root) {
        return json_response(200, json!({"data":session}));
    }
    if method == "GET" && path == format!("/api/experimental/session/{}/log", state.root) {
        let event = root_created_event(state);
        let body = format!(
            "event: message\ndata: {}\n\nevent: message\ndata: {}\n\n",
            serde_json::to_string(&event).unwrap(),
            serde_json::to_string(&json!({
                "type":"log.synced",
                "aggregateID":state.root,
                "seq":1
            }))
            .unwrap()
        );
        return Some(NativeResponse {
            status: 200,
            content_type: "text/event-stream; charset=utf-8",
            body: body.into_bytes(),
        });
    }
    if method == "GET" && path == format!("/api/session/{}/history", state.root) {
        let after = target
            .split('?')
            .nth(1)
            .and_then(|query| {
                query
                    .split('&')
                    .find_map(|pair| pair.strip_prefix("after="))
            })
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        return json_response(
            200,
            json!({
                "data":history_for(state, after),
                "hasMore":false
            }),
        );
    }
    if method == "GET" && path == format!("/api/session/{}/question", state.root) {
        let questions = match state.mode {
            NativeMode::Questions => vec![
                native_question(&state.root, "que_reply"),
                native_question(&state.root, "que_reject"),
            ],
            NativeMode::Refresh => vec![native_question(&state.root, "que_refresh")],
            NativeMode::PermissionCascade => Vec::new(),
        };
        return json_response(200, json!({"data":questions}));
    }
    if method == "GET" && path == format!("/api/session/{}/permission", state.root) {
        let permissions = if matches!(state.mode, NativeMode::PermissionCascade) {
            vec![
                native_permission(&state.root, "per_primary"),
                native_permission(&state.root, "per_secondary"),
            ]
        } else {
            Vec::new()
        };
        return json_response(200, json!({"data":permissions}));
    }
    if method == "POST"
        && (path == format!("/api/session/{}/question/que_reply/reply", state.root)
            || path == format!("/api/session/{}/question/que_reject/reject", state.root)
            || path == format!("/api/session/{}/permission/per_primary/reply", state.root))
    {
        return Some(NativeResponse {
            status: 204,
            content_type: "application/json",
            body: Vec::new(),
        });
    }
    json_response(
        404,
        json!({"error":"unhandled PR43 fixture route","path":path}),
    )
}

async fn serve_module_rpc<S>(
    stream: S,
    config: AdapterConfig,
    claim: ModuleContractClaim,
    state: Arc<RpcState>,
    mut stopped: watch::Receiver<bool>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (read, mut write) = tokio::io::split(stream);
    let mut reader = BufReader::new(read);
    loop {
        let mut line = String::new();
        let count = tokio::select! {
            changed = stopped.changed() => {
                let _ = changed;
                break;
            }
            count = reader.read_line(&mut line) => count?,
        };
        if count == 0 {
            break;
        }
        let request: Value = serde_json::from_str(line.trim_end())
            .map_err(|error| io::Error::other(error.to_string()))?;
        let id = request["id"].clone();
        let method = request["method"].as_str().unwrap_or_default();
        let params = request.get("params").cloned().unwrap_or(Value::Null);
        let result = match method {
            "client.hello" => json!({}),
            "module.hello" => module_hello_result(&config, &claim, &params),
            "module.observe" => {
                let ordinal = state.observe_count.fetch_add(1, Ordering::AcqRel) + 1;
                let recorded = ordinal >= state.observe_ack_after;
                let cursor = params["state"]["through_sequence"].as_u64();
                if recorded && let Some(cursor) = cursor {
                    state.acknowledged_cursor.store(cursor, Ordering::Release);
                }
                state.calls.lock().unwrap().push(RpcCall {
                    method: method.to_owned(),
                    params,
                    recorded: Some(recorded),
                    cursor_at_call: Some(state.acknowledged_cursor.load(Ordering::Acquire)),
                });
                json!({"recorded":recorded})
            }
            "module.outcome" => {
                let next = params["details"]["next_after_sequence"].as_u64();
                let cursor = state.acknowledged_cursor.load(Ordering::Acquire);
                if next.is_some_and(|next| next > cursor) {
                    state
                        .premature_cursor_outcome
                        .store(true, Ordering::Release);
                }
                state.calls.lock().unwrap().push(RpcCall {
                    method: method.to_owned(),
                    params,
                    recorded: Some(true),
                    cursor_at_call: Some(cursor),
                });
                json!({"recorded":true})
            }
            _ => json!({"recorded":false}),
        };
        let response = json!({"jsonrpc":"2.0","id":id,"result":result});
        write
            .write_all(serde_json::to_string(&response).unwrap().as_bytes())
            .await?;
        write.write_all(b"\n").await?;
        write.flush().await?;
    }
    Ok(())
}

fn module_hello_result(
    config: &AdapterConfig,
    claim: &ModuleContractClaim,
    params: &Value,
) -> Value {
    let claim_value = serde_json::to_value(claim).unwrap();
    json!({
        "binding_id":config.binding_id,
        "generation":config.generation,
        "route":{
            "runtime":RUNTIME,
            "module_artifact_id":ARTIFACT_ID,
            "workspace_option":"directory",
            "native_options":config.native_options
        },
        "native_root_id":params.get("native_root_id").cloned().unwrap_or(Value::Null),
        "native_scope_key":params.get("native_scope_key").cloned().unwrap_or(Value::Null),
        "module_contract_negotiation":{
            "status":"negotiated",
            "module_id":claim_value["module_id"],
            "artifact":claim_value["artifact"],
            "protocol":claim_value["protocol"],
            "capabilities":claim_value["capabilities"],
            "config_schema":claim_value["config_schema"],
            "command_schemas":claim_value["command_schemas"],
            "event_schemas":claim_value["event_schemas"],
            "effects_authorized_by_descriptor":false
        }
    })
}

#[cfg(unix)]
fn start_module_ipc(
    host_data_dir: PathBuf,
    config: AdapterConfig,
    claim: ModuleContractClaim,
    state: Arc<RpcState>,
) -> (watch::Sender<bool>, JoinHandle<io::Result<()>>) {
    let endpoint = swarm_client::ipc_endpoint(&host_data_dir).unwrap();
    let listener = tokio::net::UnixListener::bind(endpoint).unwrap();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        serve_module_rpc(stream, config, claim, state, stopped).await
    });
    (stop, task)
}

#[cfg(windows)]
fn start_module_ipc(
    host_data_dir: PathBuf,
    config: AdapterConfig,
    claim: ModuleContractClaim,
    state: Arc<RpcState>,
) -> (watch::Sender<bool>, JoinHandle<io::Result<()>>) {
    use tokio::net::windows::named_pipe::ServerOptions;
    let endpoint = swarm_client::ipc_endpoint(&host_data_dir).unwrap();
    let mut options = ServerOptions::new();
    options.first_pipe_instance(true);
    let server = options.create(endpoint).unwrap();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut stopped = stopped;
        tokio::select! {
            changed = stopped.changed() => {
                let _ = changed;
                return Ok(());
            }
            connected = server.connect() => connected?,
        }
        serve_module_rpc(server, config, claim, state, stopped).await
    });
    (stop, task)
}

fn module_rpc_fixture(
    config: &AdapterConfig,
    claim: &ModuleContractClaim,
    observe_ack_after: usize,
) -> ModuleRpcFixture {
    let state = Arc::new(RpcState {
        calls: Mutex::new(Vec::new()),
        observe_count: AtomicUsize::new(0),
        observe_ack_after,
        acknowledged_cursor: AtomicU64::new(0),
        premature_cursor_outcome: AtomicBool::new(false),
    });
    let (stop, task) = start_module_ipc(
        config.host_data_dir.clone(),
        config.clone(),
        claim.clone(),
        Arc::clone(&state),
    );
    ModuleRpcFixture { state, stop, task }
}

impl ModuleRpcFixture {
    fn calls(&self) -> Vec<RpcCall> {
        self.state.calls.lock().unwrap().clone()
    }

    fn acknowledged_cursor(&self) -> u64 {
        self.state.acknowledged_cursor.load(Ordering::Acquire)
    }

    fn premature_cursor_outcome(&self) -> bool {
        self.state.premature_cursor_outcome.load(Ordering::Acquire)
    }
}

async fn execute(host: &HostSession<'_>, command: &RuntimeCommand) -> Result<()> {
    let mut owner = native_owner::NativeOwnerController::new(None);
    handle_command(host, command, &mut owner).await
}

fn reply_input(
    binding_id: &str,
    generation: i64,
    kind: &str,
    session_id: &str,
    request: &Value,
    body: Value,
) -> Value {
    json!({
        "client_request_id":uuid::Uuid::new_v4().to_string(),
        "binding_id":binding_id,
        "generation":generation,
        "reply":{
            "kind":kind,
            "session_id":session_id,
            "request_id":request["id"],
            "fingerprint":sha256_json(request),
            "body":body
        }
    })
}
