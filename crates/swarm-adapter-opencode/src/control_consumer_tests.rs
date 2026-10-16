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
    PermissionCascadeUncertain,
    Refresh,
    QuestionDetailAnswered,
    QuestionDetailCancelled,
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
            model: options.model.clone().unwrap(),
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
            model: Some(ModelRef {
                id: "fixture-model".to_owned(),
                provider_id: "fixture-provider".to_owned(),
                variant: "fixture-variant".to_owned(),
            }),
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
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../modules/opencode/fixtures/native-controls-2.0.7.json"
    ))
    .unwrap();
    let mut question = fixture["question"].clone();
    question["id"] = json!(id);
    question["sessionID"] = json!(root);
    question
}

fn native_permission(root: &str, id: &str) -> Value {
    json!({"id":id,"sessionID":root,"action":"shell","resources":["fixture"]})
}

fn root_created_event(state: &NativeState) -> Value {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../modules/opencode/fixtures/native-inbox-2.0.7.json"
    ))
    .unwrap();
    let mut event = fixture["events"]["rootCreated"].clone();
    event["durable"]["aggregateID"] = json!(state.root);
    event["data"]["sessionID"] = json!(state.root);
    event["data"]["location"]["directory"] = json!(state.directory);
    event["data"]["model"] = serde_json::to_value(&state.model).unwrap();
    event["data"]["metadata"] =
        json!({"eliot":{"binding":state.binding_id,"generation":state.generation}});
    event
}

#[tokio::test]
async fn question_answered_detail_proves_exact_form_reply() {
    let fixture = ControlFixture::new(NativeMode::QuestionDetailAnswered, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();
    let form = native_question(fixture.root(), "frm_proven");
    let input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "question",
        fixture.root(),
        &form,
        json!({"answers":[["yes"]]}),
    );
    let target_id = "pr43-question-detail-proven";
    let command = fixture.command(target_id, "agent.reply", input, None);
    execute(&host, &command).await.unwrap();
    let saved = fixture.journal.load(target_id).unwrap();
    assert_eq!(saved.outcome.as_ref().unwrap()["outcome"], "unknown");
    let intent = saved.intent.unwrap();
    let (native, _) = NativeClient::connect(&fixture.config.native_options)
        .await
        .unwrap();
    native_interactions::reconcile_reply(
        &native,
        &command,
        &fixture.config.native_options,
        &intent,
    )
    .await
    .unwrap();
    drop(host);
    fixture.close().await;
}

#[tokio::test]
async fn question_cancelled_detail_proves_exact_form_reject() {
    let fixture = ControlFixture::new(NativeMode::QuestionDetailCancelled, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();
    let form = native_question(fixture.root(), "frm_cancelled");
    let input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "question",
        fixture.root(),
        &form,
        json!({"decision":"reject"}),
    );
    let target_id = "pr43-question-detail-cancelled";
    let command = fixture.command(target_id, "agent.reply", input, None);
    execute(&host, &command).await.unwrap();
    let saved = fixture.journal.load(target_id).unwrap();
    assert_eq!(saved.outcome.as_ref().unwrap()["outcome"], "unknown");
    let intent = saved.intent.unwrap();
    let (native, _) = NativeClient::connect(&fixture.config.native_options)
        .await
        .unwrap();
    native_interactions::reconcile_reply(
        &native,
        &command,
        &fixture.config.native_options,
        &intent,
    )
    .await
    .unwrap();
    drop(host);
    fixture.close().await;
}

#[tokio::test]
async fn unknown_schema_state_never_fabricates_form_or_history_proof() {
    let fixture = ControlFixture::new(NativeMode::Questions, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();
    let stale = json!({
        "id":"frm_stale",
        "sessionID":fixture.root(),
        "title":"Questions",
        "metadata":{"kind":"question","tool":{"messageID":"msg_stale","id":"tool_stale"}},
        "fields":[{
            "key":"q0",
            "title":"Choice",
            "description":"Choose one",
            "type":"string",
            "options":[{"value":"no","label":"no","description":"Decline"}],
            "custom":true
        }],
        "bogus":true
    });
    let input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "question",
        fixture.root(),
        &stale,
        json!({"answers":[["no"]]}),
    );
    let command = fixture.command("pr43-question-stale", "agent.reply", input, None);
    let (native, _) = NativeClient::connect(&fixture.config.native_options)
        .await
        .unwrap();
    assert!(
        native_interactions::prepare_reply(&native, &command, &fixture.config.native_options)
            .await
            .is_err()
    );
    let foreign = json!({
        "id":"frm_foreign",
        "sessionID":fixture.root(),
        "title":"Other",
        "metadata":{"kind":"question","tool":{"messageID":"msg_f","id":"tool_f"}},
        "fields":[{
            "key":"q0",
            "title":"Choice",
            "description":"Choose one",
            "type":"string",
            "options":[{"value":"yes","label":"yes","description":"Accept"}],
            "custom":true
        }]
    });
    let foreign_input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "question",
        fixture.root(),
        &foreign,
        json!({"answers":[["yes"]]}),
    );
    let foreign_command =
        fixture.command("pr43-question-foreign", "agent.reply", foreign_input, None);
    assert!(
        native_interactions::prepare_reply(
            &native,
            &foreign_command,
            &fixture.config.native_options
        )
        .await
        .is_err()
    );
    drop(host);
    fixture.close().await;
}

#[tokio::test]

async fn question_reply_and_reject_cross_native_and_outcome_ack() {
    let fixture = ControlFixture::new(NativeMode::Questions, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();

    let reply_request = native_question(fixture.root(), "frm_reply");
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

    let reject_request = native_question(fixture.root(), "frm_reject");
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

    let requests = fixture.native.requests();
    let posts = requests
        .iter()
        .filter(|request| request.method == "POST")
        .collect::<Vec<_>>();
    let deletes = requests
        .iter()
        .filter(|request| request.method == "DELETE")
        .collect::<Vec<_>>();
    assert_eq!(posts.len(), 1);
    assert_eq!(deletes.len(), 1);
    assert_eq!(
        posts[0].path,
        format!("/api/session/{}/form/frm_reply/reply", fixture.root())
    );
    assert_eq!(posts[0].body, json!({"answer":{"q0":"yes"}}));
    assert_eq!(
        deletes[0].path,
        format!("/api/session/{}/form/frm_reject", fixture.root())
    );
    assert_eq!(deletes[0].body, Value::Null);

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
    assert!(
        fixture
            .native
            .requests()
            .iter()
            .all(|request| !request.path.contains("/question/")
                && !request.path.contains("/history"))
    );
    drop(host);
    fixture.close().await;
}

#[tokio::test]
async fn permission_reject_cascade_reaches_native_and_outcome_ack() {
    let fixture = ControlFixture::new(NativeMode::PermissionCascade, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();
    let target_request = native_permission(fixture.root(), "percustom");
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
        json!(["per_secondary", "percustom"])
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
        format!("/api/session/{}/permission/percustom/reply", fixture.root())
    );
    assert_eq!(posts[0].body, json!({"decision":"reject"}));
    let calls = fixture.rpc.calls();
    assert!(calls.iter().any(|call| {
        call.method == "module.outcome"
            && call.recorded == Some(true)
            && call.params["operation_id"] == target_id
            && call.params["details"]["affected_request_ids"]
                == json!(["per_secondary", "percustom"])
    }));
    drop(host);
    fixture.close().await;
}

#[tokio::test]
async fn ambiguous_permission_post_stays_unknown_without_replay() {
    let fixture = ControlFixture::new(NativeMode::PermissionCascadeUncertain, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();
    let target_request = native_permission(fixture.root(), "percustom");
    let input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "permission",
        fixture.root(),
        &target_request,
        json!({"decision":"reject"}),
    );
    let target_id = "pr43-permission-ambiguous-unknown";
    let command = fixture.command(target_id, "agent.reply", input, None);
    execute(&host, &command).await.unwrap();

    let unknown = fixture.journal.load(target_id).unwrap();
    assert_eq!(unknown.outcome.as_ref().unwrap()["outcome"], "unknown");
    let target_intent = unknown.intent.as_ref().unwrap().clone();

    let reconcile = fixture.command(
        "pr43-permission-ambiguous-reconcile",
        "agent.reconcile",
        json!({"operation_id":target_id}),
        Some(target_intent.module_receipt.input_sha256.clone()),
    );
    let reconcile_intent = intent_for(
        &reconcile,
        &fixture.claim,
        &fixture.config.native_options,
        None,
        None,
    )
    .unwrap();
    fixture.journal.write_intent(&reconcile_intent).unwrap();
    execute(&host, &reconcile).await.unwrap();

    let settled = fixture.journal.load(target_id).unwrap();
    assert_eq!(settled.outcome.as_ref().unwrap()["outcome"], "unknown");
    let posts = fixture
        .native
        .requests()
        .into_iter()
        .filter(|request| request.method == "POST")
        .collect::<Vec<_>>();
    assert_eq!(posts.len(), 1);
    assert_eq!(
        posts[0].path,
        format!("/api/session/{}/permission/percustom/reply", fixture.root())
    );
    assert_eq!(posts[0].body, json!({"decision":"reject"}));
    drop(host);
    fixture.close().await;
}

#[tokio::test]
async fn permission_always_is_never_authorized() {
    let fixture = ControlFixture::new(NativeMode::PermissionCascade, 1).await;
    let host = fixture.host();
    host.hello_retry().await.unwrap();
    let target_request = native_permission(fixture.root(), "percustom");
    let input = reply_input(
        &fixture.config.binding_id,
        fixture.config.generation,
        "permission",
        fixture.root(),
        &target_request,
        json!({"decision":"always"}),
    );
    let command = fixture.command("pr43-permission-always", "agent.reply", input, None);
    let (native, _) = NativeClient::connect(&fixture.config.native_options)
        .await
        .unwrap();
    let err = native_interactions::prepare_reply(&native, &command, &fixture.config.native_options)
        .await
        .unwrap_err();
    assert_eq!(err.code, "INVALID_NATIVE_INTERACTION");
    assert!(
        fixture
            .native
            .requests()
            .iter()
            .all(|request| !["POST", "PUT", "DELETE", "PATCH"].contains(&request.method.as_str()))
    );
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
        json!({"session_id":fixture.root(),"after_sequence":0}),
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
    assert_eq!(observations[1].params["state"]["after_sequence"], 0);
    assert_eq!(observations[1].params["state"]["through_sequence"], 1);
    assert_eq!(fixture.rpc.acknowledged_cursor(), 1);
    assert!(!fixture.rpc.premature_cursor_outcome());
    let outcome = calls
        .iter()
        .find(|call| call.method == "module.outcome")
        .unwrap();
    assert_eq!(outcome.params["operation_id"], "pr43-refresh-ack-cursor");
    assert_eq!(outcome.params["details"]["next_after_sequence"], 1);
    assert_eq!(outcome.cursor_at_call, Some(1));
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
                "version":"2.0.7",
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
        let renamed = json!({"id":"evt_pr43_renamed","type":"session.renamed","created":123,
            "durable":{"aggregateID":state.root,"version":1,"seq":1},
            "data":{"sessionID":state.root,"title":"fixture-refresh"}});
        let after = reqwest::Url::parse(&format!("http://fixture{target}"))
            .unwrap()
            .query_pairs()
            .find_map(|(key, value)| (key == "after").then(|| value.parse::<u64>().unwrap()));
        let mut body = [root_created_event(state), renamed]
            .into_iter()
            .filter(|event| {
                after.is_none_or(|after| event["durable"]["seq"].as_u64().unwrap() > after)
            })
            .map(|event| format!("event: message\ndata: {event}\n\n"))
            .collect::<String>();
        let sync = json!({"type":"log.synced","aggregateID":state.root,"seq":1});
        body.push_str(&format!("event: message\ndata: {sync}\n\n"));
        return Some(NativeResponse {
            status: 200,
            content_type: "text/event-stream; charset=utf-8",
            body: body.into_bytes(),
        });
    }
    if method == "GET" && path == format!("/api/session/{}/form", state.root) {
        let questions = match state.mode {
            NativeMode::Questions => vec![
                serde_json::from_str::<Value>(include_str!(
                    "../../../modules/opencode/fixtures/native-controls-2.0.7.json"
                ))
                .unwrap()["authForm"]
                    .clone(),
                native_question(&state.root, "frm_reply"),
                native_question(&state.root, "frm_reject"),
            ],
            NativeMode::Refresh => vec![native_question(&state.root, "frm_refresh")],
            NativeMode::PermissionCascade | NativeMode::PermissionCascadeUncertain => Vec::new(),
            NativeMode::QuestionDetailAnswered => vec![native_question(&state.root, "frm_proven")],
            NativeMode::QuestionDetailCancelled => {
                vec![native_question(&state.root, "frm_cancelled")]
            }
        };
        return json_response(200, json!({"data":questions}));
    }
    if method == "GET"
        && path.starts_with(&format!("/api/session/{}/form/", state.root))
        && !path.ends_with("/reply")
    {
        let form_id = path.rsplit('/').next().unwrap_or_default();
        let known = match state.mode {
            NativeMode::QuestionDetailAnswered => native_question(&state.root, form_id),
            NativeMode::QuestionDetailCancelled => native_question(&state.root, form_id),
            _ => Value::Null,
        };
        if known.is_null() {
            return json_response(404, json!({"error":"form not found","path":path}));
        }
        let status = match state.mode {
            NativeMode::QuestionDetailAnswered => "answered",
            NativeMode::QuestionDetailCancelled => "cancelled",
            _ => "pending",
        };
        let mut detail = known;
        if status == "answered" {
            detail["state"] = json!({"status":"answered","answer":{"q0":"yes"}});
        } else if status == "cancelled" {
            detail["state"] = json!({"status":"cancelled"});
        } else {
            detail["state"] = json!({"status":"pending"});
        }
        return json_response(200, json!({"data":detail}));
    }
    if method == "GET" && path == format!("/api/session/{}/permission", state.root) {
        let permission_reject_posted = state.requests.lock().unwrap().iter().any(|request| {
            request.method == "POST"
                && request.path == format!("/api/session/{}/permission/percustom/reply", state.root)
        });
        let permissions = if matches!(
            state.mode,
            NativeMode::PermissionCascade | NativeMode::PermissionCascadeUncertain
        ) && !permission_reject_posted
        {
            vec![
                native_permission(&state.root, "percustom"),
                native_permission(&state.root, "per_secondary"),
            ]
        } else {
            Vec::new()
        };
        return json_response(200, json!({"data":permissions}));
    }
    if method == "GET" && path == format!("/api/session/{}/permission/percustom", state.root) {
        return json_response(
            200,
            json!({"data":native_permission(&state.root, "percustom")}),
        );
    }
    if method == "GET" && path == format!("/api/session/{}/log", state.root) {
        return json_response(
            404,
            json!({"error":"stale history route is gone","path":path}),
        );
    }
    if matches!(
        state.mode,
        NativeMode::QuestionDetailAnswered | NativeMode::QuestionDetailCancelled
    ) && ((method == "POST" && path.ends_with("/reply")) || method == "DELETE")
    {
        return json_response(500, json!({"error":"reply committed before lost response"}));
    }
    if method == "POST" && path == format!("/api/session/{}/form/frm_reply/reply", state.root) {
        return Some(NativeResponse {
            status: 204,
            content_type: "application/json",
            body: Vec::new(),
        });
    }
    if method == "DELETE" && path == format!("/api/session/{}/form/frm_reject", state.root) {
        return Some(NativeResponse {
            status: 204,
            content_type: "application/json",
            body: Vec::new(),
        });
    }
    if method == "POST" && path == format!("/api/session/{}/permission/percustom/reply", state.root)
    {
        if matches!(state.mode, NativeMode::PermissionCascadeUncertain) {
            return json_response(500, json!({"error":"reply committed before lost response"}));
        }
        return Some(NativeResponse {
            status: 204,
            content_type: "application/json",
            body: Vec::new(),
        });
    }
    if method == "POST" && path.contains("/question/") {
        return json_response(
            404,
            json!({"error":"stale question route is gone","path":path}),
        );
    }
    if method == "GET" && path.contains("/history") {
        return json_response(
            404,
            json!({"error":"stale history route is gone","path":path}),
        );
    }
    json_response(
        404,
        json!({"error":"unhandled native fixture route","path":path}),
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
