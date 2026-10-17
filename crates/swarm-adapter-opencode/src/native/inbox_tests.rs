use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

// The same fixture is decoded by the actual SDK schemas in
// modules/opencode/native-inbox-contract.test.mjs.
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../modules/opencode/fixtures/native-inbox-2.0.7.json"
    ))
    .unwrap()
}

#[tokio::test]
#[ignore = "requires the existing native service registration; GET-only, never starts a service"]
async fn live_registered_native_service_connects_without_provider_bootstrap() {
    let path = std::env::var_os("ELIOT_OPENCODE_REGISTERED_CONNECTION")
        .expect("explicit native service registration is required");
    let directory = std::env::current_dir().unwrap();
    let options = NativeOptions {
        service_id: "native-registered-read-only".into(),
        connection_file: path.into(),
        directory,
        model: None,
    };
    let (_, observation) = NativeClient::connect(&options).await.unwrap();
    assert!(observation.pid > 0);
    assert!(!observation.version.is_empty());
}

#[test]
fn native_registration_preserves_service_identity_and_optional_http_auth() {
    let no_auth =
        decode_connection_record(br#"{"url":"http://127.0.0.1:12345","pid":42,"version":"2.0.7"}"#)
            .unwrap();
    assert_eq!(no_auth.pid, 42);
    assert_eq!(no_auth.registered_version.as_deref(), Some("2.0.7"));
    assert!(no_auth.auth.is_none());
    let protected = decode_connection_record(
        br#"{"url":"http://127.0.0.1:12345","pid":42,"password":"local-service-only"}"#,
    )
    .unwrap();
    assert_eq!(
        protected.auth,
        Some(("opencode".into(), "local-service-only".into()))
    );
    for bad in [
        br#"{"url":"http://example.com","pid":42}"#.as_slice(),
        br#"{"url":"http://127.0.0.1:12345","pid":0}"#.as_slice(),
        br#"{"url":"http://127.0.0.1:12345","pid":42,"password":""}"#.as_slice(),
        br#"{"url":"http://127.0.0.1:12345","pid":42,"provider_auth":{}}"#.as_slice(),
    ] {
        assert!(decode_connection_record(bad).is_err());
    }
}

#[tokio::test]
#[ignore = "requires an explicit owned live session and admitted input; GET-only, no model input"]
async fn live_existing_input_log_readback_is_exact_and_read_only() {
    let path = std::env::var_os("ELIOT_OPENCODE_EXISTING_LOG_OPTIONS")
        .expect("explicit private live-log options file is required");
    let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let options: NativeOptions = serde_json::from_value(value["native"].clone()).unwrap();
    let binding = value["binding_id"].as_str().unwrap();
    let generation = value["generation"].as_i64().unwrap();
    let operation = value["operation_id"].as_str().unwrap();
    let session_id = root_id(binding, generation);
    let input = input_id(operation);
    let (client, _) = NativeClient::connect(&options).await.unwrap();
    let session = client.session(&session_id).await.unwrap();
    assert_eq!(session["metadata"]["eliot"]["binding"], binding);
    assert_eq!(session["metadata"]["eliot"]["generation"], generation);
    client
        .require_durable_root_creation_inner(
            &session_id,
            &options,
            binding,
            generation,
            &serde_json::to_value(&options.model).unwrap(),
            None,
        )
        .await
        .unwrap();
    let page = client.session_log_page(&session_id, 0, 100).await.unwrap();
    assert_eq!(
        page["hasMore"], false,
        "this qualification requires a complete bounded cut"
    );
    let events = page["data"].as_array().unwrap();
    let admitted = events
        .iter()
        .find(|event| {
            event["type"] == "session.inbox.enqueued" && event["data"]["inboxID"] == input
        })
        .expect("exact admission");
    assert_eq!(
        admitted["data"]["item"]["payload"]["metadata"]["eliot"]["operation"],
        operation
    );
    let delivered = events
        .iter()
        .find(|event| {
            event["type"] == "session.inbox.delivered" && event["data"]["inboxID"] == input
        })
        .expect("exact input delivery");
    assert!(
        delivered["durable"]["seq"].as_u64().unwrap()
            > admitted["durable"]["seq"].as_u64().unwrap()
    );
}

fn command() -> RuntimeCommand {
    RuntimeCommand {
        operation_id: "op207-input".into(),
        method: "task.dispatch".into(),
        created_at_ms: 1,
        binding_id: "fixture-binding".into(),
        generation: 1,
        native_root_id: Some("ses_swarm_fixture".into()),
        route: json!({}),
        input: json!({}),
        input_sha256: Some("a".repeat(64)),
        target_input_sha256: None,
    }
}

fn intent() -> OperationIntent {
    serde_json::from_value(json!({
        "version":2,
        "operation_id":"op207-input",
        "module_receipt":{
            "schema_version":1,
            "module_id":"eliot.opencode",
            "artifact":{
                "artifact_id":"eliot-opencode-v2.rust-http.1",
                "version":crate::config::ARTIFACT_VERSION
            },
            "protocol":{"major":1,"minor":0},
            "binding_id":"fixture-binding",
            "binding_generation":1,
            "operation_id":"op207-input",
            "input_sha256":"a".repeat(64)
        },
        "method":"task.dispatch",
        "binding_id":"fixture-binding",
        "generation":1,
        "native_scope_key":"fixture-scope",
        "native_root_id":"ses_swarm_fixture",
        "native_input_id":"msg_swarm_fixture",
        "prompt_sha256":sha256(b"Documented task input"),
        "prompt_bytes":21,
        "route_sha256":"b".repeat(64),
        "model":{},
        "marker":marker(&command())
    }))
    .unwrap()
}

async fn http_fixture(
    responses: Vec<Value>,
    method: &'static str,
) -> (NativeClient, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for response in responses {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            let header_end = loop {
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0 && request.len() + count <= 8192);
                request.extend_from_slice(&buffer[..count]);
                if let Some(position) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    break position + 4;
                }
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let mut line = headers.lines().next().unwrap().split_whitespace();
            let expected_method = response["expected_method"].as_str().unwrap_or(method);
            assert_eq!(line.next(), Some(expected_method));
            let path = line.next().unwrap();
            if let Some(expected_path) = response["json_path"].as_str() {
                assert_eq!(path, expected_path);
            } else if let Some(prefix) = response["json_path_prefix"].as_str() {
                assert!(path.starts_with(prefix));
            } else if expected_method == "POST" {
                assert_eq!(path, "/api/session/ses_swarm_fixture/prompt");
            } else {
                let session = response["log_session"]
                    .as_str()
                    .unwrap_or("ses_swarm_fixture");
                assert!(path.starts_with(&format!("/api/experimental/session/{session}/log?")));
                assert!(path.contains("follow=false"));
                if let Some(expected_after) = response["expected_after"].as_u64() {
                    let request_url = Url::parse(&format!("http://fixture{path}")).unwrap();
                    assert!(
                        request_url
                            .query_pairs()
                            .any(|(key, value)| key == "after"
                                && value == expected_after.to_string())
                    );
                }
            }
            let length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                .unwrap_or(0);
            while request.len() < header_end + length {
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0 && request.len() + count <= 8192);
                request.extend_from_slice(&buffer[..count]);
            }
            bodies.push(if length == 0 {
                Value::Null
            } else {
                serde_json::from_slice(&request[header_end..header_end + length]).unwrap()
            });
            let (content_type, body) = if let Some(raw) = response["raw_stream"].as_str() {
                ("text/event-stream", raw.as_bytes().to_vec())
            } else if response.get("json_payload").is_some() {
                (
                    "application/json",
                    serde_json::to_vec(&response["json_payload"]).unwrap(),
                )
            } else if expected_method == "POST" {
                ("application/json", serde_json::to_vec(&response).unwrap())
            } else {
                let events = response["events"].as_array().unwrap();
                let frame = response["frame_type"].as_str().unwrap_or("message");
                let mut stream_body = events
                    .iter()
                    .filter(|event| {
                        response["expected_after"].as_u64().is_none_or(|after| {
                            event["durable"]["seq"]
                                .as_u64()
                                .is_some_and(|sequence| sequence > after)
                        })
                    })
                    .map(|event| format!("event: {frame}\ndata: {event}\n\n"))
                    .collect::<String>();
                if response["omit_sync"] != true {
                    let sequence = response["sync_sequence"].as_u64().unwrap_or_else(|| {
                        events
                            .last()
                            .and_then(|event| event["durable"]["seq"].as_u64())
                            .unwrap_or(0)
                    });
                    let aggregate = response
                        .get("sync_aggregate")
                        .cloned()
                        .unwrap_or(json!("ses_swarm_fixture"));
                    let mut synced =
                        json!({"type":"log.synced","aggregateID":aggregate,"seq":sequence});
                    if response["extra_sync_field"] == true {
                        synced["unexpected"] = json!(true);
                    }
                    stream_body.push_str(&format!("event: {frame}\ndata: {synced}\n\n"));
                }
                ("text/event-stream", stream_body.into_bytes())
            };
            let content_length = body.len()
                + if response["truncate_body"] == true {
                    1
                } else {
                    0
                };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                content_length
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        }
        bodies
    });
    let client = NativeClient {
        client: Client::builder().no_proxy().build().unwrap(),
        endpoint,
        pid: 1,
        version: "2.0.7".into(),
    };
    (client, server)
}

fn log_response(events: Vec<Value>, expected_after: u64) -> Value {
    json!({"events":events,"expected_after":expected_after})
}

fn root_event() -> Value {
    fixture()["events"]["rootCreated"].clone()
}

#[tokio::test]
async fn native_defaults_create_a_root_without_model_catalog_or_model_override() {
    let mut command = command();
    command.method = "agent.open".into();
    command.native_root_id = None;
    let root = root_id(&command.binding_id, command.generation);
    let directory = std::env::current_dir().unwrap();
    let location = json!({"directory":directory});
    let native_model =
        json!({"id":"native-default","providerID":"native-configured","variant":"native-saved"});
    let session = json!({"id":root,"parentID":null,"fork":null,"location":location,"model":native_model,"metadata":{"eliot":marker(&command)}});
    let mut created = root_event();
    created["durable"]["aggregateID"] = json!(root);
    created["data"]["sessionID"] = json!(root);
    created["data"]["location"] = location.clone();
    created["data"]["model"] = native_model;
    created["data"]["metadata"] = session["metadata"].clone();
    let (client, server) = http_fixture(vec![
        json!({"expected_method":"GET","json_path_prefix":"/api/location?","json_payload":{"directory":directory,"project":{"id":"global"}}}),
        json!({"expected_method":"POST","json_path":"/api/session","json_payload":{"data":session}}),
        json!({"expected_method":"GET","log_session":root,"sync_aggregate":root,"events":[created]})
    ], "GET").await;
    let options = NativeOptions {
        service_id: "native-defaults".into(),
        connection_file: directory.join("unused.json"),
        directory,
        model: None,
    };
    let resolved = client
        .preflight_open(&options)
        .await
        .unwrap_or_else(|failure| panic!("{}", failure.error.code));
    client
        .create_root(&command, &options, &root, resolved)
        .await
        .unwrap();
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert!(!requests[1].as_object().unwrap().contains_key("model"));
    assert_eq!(requests[1]["id"], root);
    assert_eq!(requests[1]["metadata"]["eliot"], marker(&command));
}

#[tokio::test]
async fn native_207_root_durability_requires_valid_complete_message_log() {
    let binding = "fixture-binding";
    let root = root_id(binding, 1);
    let model = json!({"id":"fixture-model","providerID":"fixture","variant":"fixture-variant"});
    let directory = std::env::current_dir().unwrap();
    let options = NativeOptions {
        service_id: "durable-log-fixture".into(),
        connection_file: directory.join("unused.json"),
        directory,
        model: Some(serde_json::from_value(model.clone()).unwrap()),
    };
    let mut created = root_event();
    created["durable"]["aggregateID"] = json!(root);
    created["data"]["sessionID"] = json!(root);
    created["data"]["model"] = model.clone();
    created["data"]["metadata"] = json!({"eliot":{"binding":binding,"generation":1}});
    let valid = json!({"events":[created.clone()],"log_session":root,"sync_aggregate":root});
    let mut filler = created;
    filler["id"] = json!("evt_invalid_filler");
    filler["type"] = json!("session.renamed");
    filler["durable"]["seq"] = json!(2);
    filler["data"] = json!({"sessionID":root});
    let mut malformed = valid.clone();
    malformed["events"].as_array_mut().unwrap().push(filler);
    let mut wrong_frame = valid.clone();
    wrong_frame["frame_type"] = json!("not-message");
    let mut ahead = valid.clone();
    ahead["sync_sequence"] = json!(999);
    let mut extra_field = valid.clone();
    extra_field["extra_sync_field"] = json!(true);
    let mut gap = malformed.clone();
    gap["events"][1]["durable"]["seq"] = json!(3);
    gap["events"][1]["data"]["title"] = json!("Valid rename after a gap");
    for (response, expected_error) in [
        (valid, None),
        (malformed, Some("NATIVE_HISTORY_SCHEMA")),
        (wrong_frame, Some("NATIVE_LOG_SCHEMA")),
        (ahead, None),
        (extra_field, Some("NATIVE_LOG_WATERMARK_REGRESSED")),
        (gap, None),
    ] {
        let (client, server) = http_fixture(vec![response], "GET").await;
        let result = client
            .require_durable_root_creation_inner(&root, &options, binding, 1, &model, None)
            .await;
        if let Some(code) = expected_error {
            assert_eq!(result.unwrap_err().code, code);
        } else {
            result.unwrap();
        }
        server.await.unwrap();
    }
}

// Native 2.0.31 may retain no session events. Root readback still needs the
// exact HTTP projections; an empty log supplies no inbox admission or delivery.
struct RootReadbackFixture {
    command: RuntimeCommand,
    options: NativeOptions,
    root: String,
    location: Value,
    session: Value,
}

impl RootReadbackFixture {
    fn new(pinned_model: bool) -> Self {
        let mut command = command();
        command.operation_id = "op071-open".into();
        command.method = "agent.open".into();
        command.native_root_id = None;
        let root = root_id(&command.binding_id, command.generation);
        let directory = std::env::current_dir().unwrap();
        let location = json!({"directory":directory});
        let model =
            json!({"id":"native-model","providerID":"native-provider","variant":"native-variant"});
        let session = json!({
            "id":root,"parentID":null,"fork":null,"location":location,
            "model":model,"metadata":{"eliot":marker(&command)}
        });
        let options = NativeOptions {
            service_id: "empty-root-readback".into(),
            connection_file: directory.join("unused.json"),
            directory,
            model: pinned_model.then(|| serde_json::from_value(model).unwrap()),
        };
        Self {
            command,
            options,
            root,
            location,
            session,
        }
    }

    fn session_response(&self, session: &Value) -> Value {
        json!({"json_path":format!("/api/session/{}", self.root),"json_payload":{"data":session}})
    }

    fn location_response(&self) -> Value {
        let mut url = Url::parse("http://fixture/api/location").unwrap();
        url.query_pairs_mut().append_pair(
            "location[directory]",
            &self.options.directory.to_string_lossy(),
        );
        json!({
            "json_path":format!("{}?{}", url.path(), url.query().unwrap()),
            "json_payload":{"directory":self.options.directory,"project":{"id":"global"}}
        })
    }

    fn raw_root_log(&self, stream: String) -> Value {
        json!({
            "json_path":format!("/api/experimental/session/{}/log?follow=false", self.root),
            "raw_stream":stream
        })
    }

    fn empty_log(&self, sequence: bool) -> Value {
        let mut sync = json!({"type":"log.synced","aggregateID":self.root});
        if sequence {
            sync["seq"] = json!(0);
        }
        self.raw_root_log(format!("event: message\ndata: {sync}\n\n"))
    }

    fn creation_response(&self) -> Value {
        json!({"expected_method":"POST","json_path":"/api/session","json_payload":{"data":self.session}})
    }

    fn model_response(&self) -> Value {
        let mut url = Url::parse("http://fixture/api/model").unwrap();
        url.query_pairs_mut().append_pair(
            "location[directory]",
            &self.options.directory.to_string_lossy(),
        );
        json!({
            "json_path":format!("{}?{}", url.path(), url.query().unwrap()),
            "json_payload":{
                "location":self.location,
                "data":[{
                    "id":self.session["model"]["id"],
                    "providerID":self.session["model"]["providerID"],
                    "enabled":true,
                    "variants":[{"id":self.session["model"]["variant"]}]
                }]
            }
        })
    }

    fn input_status_prefix(&self, sync_sequence: bool) -> Vec<Value> {
        let mut responses = vec![
            self.session_response(&self.session),
            self.location_response(),
        ];
        if self.options.model.is_some() {
            responses.push(self.model_response());
        }
        responses.extend([
            self.empty_log(sync_sequence),
            self.session_response(&self.session),
            self.location_response(),
        ]);
        responses
    }

    fn user_message(&self, intent: &OperationIntent) -> Value {
        assert_eq!(intent.native_root_id.as_deref(), Some(self.root.as_str()));
        json!({
            "id":intent.native_input_id,
            "type":"user",
            "time":{"created":1},
            "text":"Documented task input",
            "metadata":{"eliot":intent.marker}
        })
    }

    fn input_message_response(&self, intent: &OperationIntent, message: &Value) -> Value {
        json!({
            "json_path":format!(
                "/api/session/{}/message/{}", self.root, intent.native_input_id.as_deref().unwrap()
            ),
            "json_payload":{"data":message}
        })
    }

    fn saved_intent(&self, method: &str) -> OperationIntent {
        let mut command = self.command.clone();
        command.method = method.into();
        command.operation_id = match method {
            "task.dispatch" => "op071-dispatch",
            "agent.send" => "op071-send",
            _ => self.command.operation_id.as_str(),
        }
        .into();
        let mut intent = intent();
        intent.operation_id = command.operation_id.clone();
        intent.module_receipt.operation_id = command.operation_id.clone();
        intent.method = method.into();
        intent.native_root_id = Some(self.root.clone());
        intent.native_input_id =
            native_delivery_for_method(method).map(|_| input_id(&command.operation_id));
        intent.native_scope_key = self.options.scope_key();
        intent.route_sha256 = digest_json(&serde_json::to_value(&self.options).unwrap()).unwrap();
        intent.model = serde_json::to_value(&self.options.model).unwrap();
        intent.marker = marker(&command);
        if method == "agent.open" {
            intent.prompt_sha256 = None;
            intent.prompt_bytes = None;
        }
        intent
    }
}

#[tokio::test]
async fn native_071_empty_root_sync_requires_exact_session_location_marker_and_model_readback() {
    for pinned_model in [false, true] {
        for sequence in [false, true] {
            let fixture = RootReadbackFixture::new(pinned_model);
            let (client, server) = http_fixture(
                vec![
                    fixture.creation_response(),
                    fixture.empty_log(sequence),
                    fixture.session_response(&fixture.session),
                    fixture.location_response(),
                ],
                "GET",
            )
            .await;
            client
                .create_root(
                    &fixture.command,
                    &fixture.options,
                    &fixture.root,
                    fixture.location.clone(),
                )
                .await
                .unwrap();
            let mut expected = json!({
                "id":fixture.root,"location":fixture.location,
                "metadata":{"eliot":marker(&fixture.command)}
            });
            if pinned_model {
                expected["model"] = fixture.session["model"].clone();
            }
            assert_eq!(
                server.await.unwrap(),
                vec![expected, Value::Null, Value::Null, Value::Null]
            );
        }
    }
}

#[tokio::test]
async fn native_071_empty_root_sync_reconciles_the_exact_saved_open_without_post() {
    for sequence in [false, true] {
        let fixture = RootReadbackFixture::new(true);
        let (client, server) = http_fixture(
            vec![
                fixture.session_response(&fixture.session),
                fixture.location_response(),
                fixture.empty_log(sequence),
                fixture.session_response(&fixture.session),
                fixture.location_response(),
            ],
            "GET",
        )
        .await;
        client
            .reconcile_open(&fixture.saved_intent("agent.open"), &fixture.options)
            .await
            .unwrap();
        assert_eq!(server.await.unwrap(), vec![Value::Null; 5]);
    }
}

#[tokio::test]
async fn native_071_empty_root_sync_rejects_foreign_or_incomplete_readback() {
    let fixture = RootReadbackFixture::new(true);
    let mut extra_marker = marker(&fixture.command);
    extra_marker["foreign"] = json!(true);
    for (pointer, replacement) in [
        ("/id", json!("ses_foreign")),
        ("/parentID", json!("ses_parent")),
        ("/fork", json!({"sessionID":"ses_foreign"})),
        (
            "/location/directory",
            json!(fixture.options.directory.join("foreign")),
        ),
        ("/model/id", json!("other-model")),
        ("/model/providerID", json!("other-provider")),
        ("/model/variant", json!("other-variant")),
        ("/model", Value::Null),
        ("/metadata/eliot/binding", json!("foreign-binding")),
        ("/metadata/eliot/generation", json!(2)),
        ("/metadata/eliot/operation", json!("other-open")),
        ("/metadata/eliot/input_sha256", json!("b".repeat(64))),
        (
            "/metadata/eliot",
            json!({"binding":fixture.command.binding_id,"generation":1}),
        ),
        ("/metadata/eliot", extra_marker),
    ] {
        let mut session = fixture.session.clone();
        *session.pointer_mut(pointer).unwrap() = replacement;
        let mut responses = vec![
            fixture.creation_response(),
            fixture.empty_log(true),
            fixture.session_response(&session),
        ];
        if pointer != "/id" {
            responses.push(fixture.location_response());
        }
        let expected_requests = responses.len();
        let (client, server) = http_fixture(responses, "GET").await;
        let error = client
            .create_root(
                &fixture.command,
                &fixture.options,
                &fixture.root,
                fixture.location.clone(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            if pointer == "/id" {
                "NATIVE_SCHEMA_ERROR"
            } else {
                "DURABLE_EVENT_PERSISTENCE_UNAVAILABLE"
            },
            "readback mutation {pointer}"
        );
        assert_eq!(server.await.unwrap().len(), expected_requests);
    }
    for foreign_directory in [false, true] {
        let mut location = fixture.location_response();
        let expected = if foreign_directory {
            location["json_payload"]["directory"] =
                json!(fixture.options.directory.parent().unwrap());
            "NATIVE_LOCATION_MISMATCH"
        } else {
            location["json_payload"]["project"] = json!({});
            "NATIVE_LOCATION_UNAVAILABLE"
        };
        let (client, server) = http_fixture(
            vec![
                fixture.empty_log(true),
                fixture.session_response(&fixture.session),
                location,
            ],
            "GET",
        )
        .await;
        let error = client
            .require_durable_root_creation_inner(
                &fixture.root,
                &fixture.options,
                &fixture.command.binding_id,
                1,
                &serde_json::to_value(&fixture.options.model).unwrap(),
                Some(&marker(&fixture.command)),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, expected);
        assert_eq!(server.await.unwrap(), vec![Value::Null; 3]);
    }
}

#[tokio::test]
async fn native_071_open_reconciliation_rejects_a_foreign_full_marker_before_log_readback() {
    let fixture = RootReadbackFixture::new(true);
    for field in ["operation", "input_sha256"] {
        let mut changed = fixture.session.clone();
        changed["metadata"]["eliot"][field] = if field == "operation" {
            json!("another-open")
        } else {
            json!("b".repeat(64))
        };
        let (client, server) = http_fixture(
            vec![
                fixture.session_response(&changed),
                fixture.location_response(),
            ],
            "GET",
        )
        .await;
        let error = client
            .reconcile_open(&fixture.saved_intent("agent.open"), &fixture.options)
            .await
            .unwrap_err();
        assert_eq!(error.code, "NATIVE_EVIDENCE_UNAVAILABLE", "marker {field}");
        assert_eq!(server.await.unwrap(), vec![Value::Null; 2]);
    }
}

#[tokio::test]
async fn native_071_empty_root_fallback_requires_a_complete_scoped_well_formed_sync() {
    let fixture = RootReadbackFixture::new(true);
    let mut invalid = vec![
        (fixture.raw_root_log(String::new()), "NATIVE_LOG_NOT_SYNCED"),
        (
            fixture.raw_root_log("event: message\ndata: {\n\n".into()),
            "NATIVE_LOG_SCHEMA",
        ),
        (
            fixture.raw_root_log(format!(
                "event: message\ndata: {}\n\n",
                json!({"type":"log.synced","aggregateID":"ses_foreign","seq":0})
            )),
            "NATIVE_LOG_WATERMARK_SCOPE",
        ),
    ];
    for sequence in [
        Value::Null,
        json!(-1),
        json!(0.5),
        json!("0"),
        json!(9_007_199_254_740_992_u64),
    ] {
        invalid.push((
            fixture.raw_root_log(format!(
                "event: message\ndata: {}\n\n",
                json!({"type":"log.synced","aggregateID":fixture.root,"seq":sequence})
            )),
            "NATIVE_LOG_WATERMARK_REGRESSED",
        ));
    }
    // A watermark cannot hide a truncated HTTP tail, even after it was parsed.
    let mut truncated = fixture.empty_log(true);
    truncated["truncate_body"] = json!(true);
    invalid.push((truncated, "NATIVE_LOG_STREAM_INCOMPLETE"));
    let mut truncated_sync =
        fixture.raw_root_log("event: message\ndata: {\"type\":\"log.synced\"".into());
    truncated_sync["truncate_body"] = json!(true);
    invalid.push((truncated_sync, "NATIVE_LOG_STREAM_INCOMPLETE"));
    let sync = json!({"type":"log.synced","aggregateID":fixture.root,"seq":0});
    invalid.push((
        fixture.raw_root_log(format!(
            "event: message\ndata: {sync}\n\nevent: message\ndata: {sync}\n\n"
        )),
        "NATIVE_LOG_AFTER_WATERMARK",
    ));
    let mut foreign = root_event();
    foreign["data"]["sessionID"] = json!("ses_foreign");
    foreign["durable"]["aggregateID"] = json!("ses_foreign");
    invalid.push((
        fixture.raw_root_log(format!("event: message\ndata: {foreign}\n\n")),
        "NATIVE_LOG_SCOPE",
    ));
    let mut renamed = root_event();
    renamed["type"] = json!("session.renamed");
    renamed["durable"]["aggregateID"] = json!(fixture.root);
    renamed["data"] = json!({"sessionID":fixture.root,"title":"Retained non-creation event"});
    invalid.push((
        fixture.raw_root_log(format!(
            "event: message\ndata: {renamed}\n\nevent: message\ndata: {sync}\n\n"
        )),
        "NATIVE_LOG_ORIGIN",
    ));
    for (response, expected) in invalid {
        let (client, server) = http_fixture(vec![response], "GET").await;
        let error = client
            .require_durable_root_creation_inner(
                &fixture.root,
                &fixture.options,
                &fixture.command.binding_id,
                1,
                &serde_json::to_value(&fixture.options.model).unwrap(),
                Some(&marker(&fixture.command)),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, expected);
        assert_eq!(server.await.unwrap(), vec![Value::Null]);
    }
}

#[tokio::test]
async fn native_071_nonempty_root_log_still_requires_a_numeric_sync_sequence() {
    let fixture = RootReadbackFixture::new(true);
    let mut created = root_event();
    created["durable"]["aggregateID"] = json!(fixture.root);
    created["data"]["sessionID"] = json!(fixture.root);
    created["data"]["metadata"] = fixture.session["metadata"].clone();
    created["data"]["model"] = fixture.session["model"].clone();
    created["data"]["location"] = fixture.location.clone();
    let sync = json!({"type":"log.synced","aggregateID":fixture.root});
    let response = fixture.raw_root_log(format!(
        "event: message\ndata: {created}\n\nevent: message\ndata: {sync}\n\n"
    ));
    let (client, server) = http_fixture(vec![response], "GET").await;
    let error = client
        .require_durable_root_creation_inner(
            &fixture.root,
            &fixture.options,
            &fixture.command.binding_id,
            1,
            &serde_json::to_value(&fixture.options.model).unwrap(),
            Some(&marker(&fixture.command)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "NATIVE_LOG_WATERMARK_REGRESSED");
    assert_eq!(server.await.unwrap(), vec![Value::Null]);
}

#[tokio::test]
async fn native_071_history_pages_never_accept_a_missing_sync_sequence() {
    for (events, after) in [(vec![], 0), (vec![], 7), (vec![root_event()], 0)] {
        let mut stream = events
            .iter()
            .map(|event| format!("event: message\ndata: {event}\n\n"))
            .collect::<String>();
        stream.push_str(
            "event: message\ndata: {\"type\":\"log.synced\",\"aggregateID\":\"ses_swarm_fixture\"}\n\n",
        );
        let (client, server) = http_fixture(
            vec![json!({
                "json_path":format!("/api/experimental/session/ses_swarm_fixture/log?after={after}&follow=false"),
                "raw_stream":stream
            })],
            "GET",
        )
        .await;
        let error = client
            .session_log_page("ses_swarm_fixture", after, 100)
            .await
            .unwrap_err();
        assert_eq!(error.code, "NATIVE_HISTORY_GAP");
        assert_eq!(server.await.unwrap(), vec![Value::Null]);
    }
}

#[tokio::test]
async fn native_071_empty_root_readback_never_proves_input_admission_or_send_completion() {
    let fixture = RootReadbackFixture::new(false);
    for method in ["task.dispatch", "agent.send"] {
        let intent = fixture.saved_intent(method);
        let mut history = log_response(vec![], 0);
        history["log_session"] = json!(fixture.root);
        history["sync_aggregate"] = json!(fixture.root);
        let (client, server) = http_fixture(vec![history.clone()], "GET").await;
        let evidence = client
            .read_input_history(
                &fixture.root,
                intent.native_input_id.as_deref().unwrap(),
                &intent,
                None,
            )
            .await
            .unwrap();
        assert!(evidence.admitted_sequence.is_none());
        assert!(evidence.prompted_sequence.is_none());
        assert_eq!(server.await.unwrap(), vec![Value::Null]);
        for input_status in [false, true] {
            let mut responses = fixture.input_status_prefix(true);
            responses.push(if input_status {
                fixture.input_message_response(&intent, &Value::Null)
            } else {
                history.clone()
            });
            let (client, server) = http_fixture(responses, "GET").await;
            let result = if input_status {
                client
                    .read_input_status(&intent, &fixture.options)
                    .await
                    .map(|_| ())
            } else {
                client
                    .reconcile_input(&intent, &fixture.options)
                    .await
                    .map(|_| ())
            };
            assert_eq!(
                result.unwrap_err().code,
                "NATIVE_EVIDENCE_UNAVAILABLE",
                "{method}, input_status={input_status}"
            );
            assert_eq!(server.await.unwrap(), vec![Value::Null; 6]);
        }
    }
}

#[tokio::test]
async fn native_072_input_status_reads_exact_user_without_input_history_events() {
    for pinned_model in [false, true] {
        for sync_sequence in [false, true] {
            for method in ["task.dispatch", "agent.send"] {
                let fixture = RootReadbackFixture::new(pinned_model);
                let intent = fixture.saved_intent(method);
                let message = fixture.user_message(&intent);
                let mut responses = fixture.input_status_prefix(sync_sequence);
                responses.extend([
                    fixture.input_message_response(&intent, &message),
                    fixture.input_message_response(&intent, &message),
                ]);
                let requests = responses.len();
                let (mut client, server) = http_fixture(responses, "GET").await;
                client.version = "2.0.31".into();
                let evidence = client
                    .read_input_status(&intent, &fixture.options)
                    .await
                    .unwrap();
                assert_eq!(
                    evidence.input_message_sha256,
                    digest_json(&message).unwrap()
                );
                // Each request has an exact path: only root-proof log sync is
                // expected, never an after-cursor input-history query or POST.
                assert_eq!(server.await.unwrap(), vec![Value::Null; requests]);
            }
        }
    }
}

#[tokio::test]
async fn native_072_input_status_rejects_foreign_user_identity_marker_and_text() {
    for (pointer, replacement) in [
        ("/id", json!("msg_swarm_foreign")),
        ("/sessionID", json!("ses_swarm_foreign")),
        ("/type", json!("assistant")),
        ("/text", json!("documented task input")),
        ("/metadata", Value::Null),
        ("/metadata/eliot/binding", json!("foreign-binding")),
        ("/metadata/eliot/generation", json!(2)),
        ("/metadata/eliot/operation", json!("foreign-operation")),
        ("/metadata/eliot/input_sha256", json!("f".repeat(64))),
        ("/metadata/eliot/native_delivery", json!("steer")),
    ] {
        let fixture = RootReadbackFixture::new(false);
        let intent = fixture.saved_intent("agent.send");
        let mut message = fixture.user_message(&intent);
        message["sessionID"] = json!(fixture.root);
        *message.pointer_mut(pointer).unwrap() = replacement;
        let mut responses = fixture.input_status_prefix(false);
        responses.push(fixture.input_message_response(&intent, &message));
        let requests = responses.len();
        let (client, server) = http_fixture(responses, "GET").await;
        assert_eq!(
            client
                .read_input_status(&intent, &fixture.options)
                .await
                .unwrap_err()
                .code,
            "NATIVE_EVIDENCE_UNAVAILABLE",
            "user field {pointer}"
        );
        assert_eq!(server.await.unwrap(), vec![Value::Null; requests]);
    }
}

#[tokio::test]
async fn native_072_input_status_rejects_changed_immutable_intent_before_gets() {
    for field in [
        "scope", "route", "root", "input", "delivery", "marker", "receipt", "model", "method",
    ] {
        let fixture = RootReadbackFixture::new(false);
        let mut intent = fixture.saved_intent("agent.send");
        match field {
            "scope" => intent.native_scope_key = "foreign-scope".into(),
            "route" => intent.route_sha256 = "f".repeat(64),
            "root" => intent.native_root_id = Some("ses_swarm_foreign".into()),
            "input" => intent.native_input_id = Some("msg_swarm_foreign".into()),
            "delivery" => intent.marker["native_delivery"] = json!("steer"),
            "marker" => intent.marker["unexpected"] = json!(true),
            "receipt" => intent.module_receipt.binding_generation = 2,
            "model" => intent.model = json!({}),
            "method" => intent.method = "native.opencode.loop_step".into(),
            _ => unreachable!(),
        }
        let (client, server) = http_fixture(Vec::new(), "GET").await;
        assert_eq!(
            client
                .read_input_status(&intent, &fixture.options)
                .await
                .unwrap_err()
                .code,
            "NATIVE_IDENTITY_MISMATCH",
            "intent field {field}"
        );
        assert!(server.await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn native_072_input_status_rejects_foreign_root_scope_and_pinned_model() {
    for (pointer, replacement) in [
        ("/id", json!("ses_swarm_foreign")),
        ("/parentID", json!("ses_swarm_foreign")),
        ("/fork", json!({"sessionID":"ses_swarm_foreign"})),
        ("/metadata/eliot/binding", json!("foreign-binding")),
        ("/metadata/eliot/generation", json!(2)),
        ("/location/directory", json!("foreign-directory")),
        ("/model/id", json!("foreign-model")),
        ("/model/providerID", json!("foreign-provider")),
        ("/model/variant", json!("foreign-variant")),
    ] {
        let fixture = RootReadbackFixture::new(true);
        let intent = fixture.saved_intent("agent.send");
        let mut session = fixture.session.clone();
        *session.pointer_mut(pointer).unwrap() = replacement;
        let (client, server) = http_fixture(
            vec![
                fixture.session_response(&session),
                fixture.location_response(),
            ],
            "GET",
        )
        .await;
        assert_eq!(
            client
                .read_input_status(&intent, &fixture.options)
                .await
                .unwrap_err()
                .code,
            "NATIVE_EVIDENCE_UNAVAILABLE",
            "root field {pointer}"
        );
        assert_eq!(server.await.unwrap(), vec![Value::Null; 2]);
    }
}

#[tokio::test]
async fn native_072_input_status_requires_root_proof_and_selected_location() {
    for second_root in [false, true] {
        let fixture = RootReadbackFixture::new(false);
        let intent = fixture.saved_intent("agent.send");
        let (responses, code) = if second_root {
            let mut responses = fixture.input_status_prefix(false);
            responses[3]["json_payload"]["data"]["metadata"]["eliot"]["generation"] = json!(2);
            (responses, "DURABLE_EVENT_PERSISTENCE_UNAVAILABLE")
        } else {
            let mut location = fixture.location_response();
            location["json_payload"]["directory"] = json!("relative-location");
            (
                vec![fixture.session_response(&fixture.session), location],
                "NATIVE_LOCATION_MISMATCH",
            )
        };
        let requests = responses.len();
        let (client, server) = http_fixture(responses, "GET").await;
        assert_eq!(
            client
                .read_input_status(&intent, &fixture.options)
                .await
                .unwrap_err()
                .code,
            code
        );
        assert_eq!(server.await.unwrap(), vec![Value::Null; requests]);
    }
}

#[tokio::test]
async fn native_072_input_status_rejects_transient_user_projection() {
    let fixture = RootReadbackFixture::new(false);
    let intent = fixture.saved_intent("agent.send");
    let first = fixture.user_message(&intent);
    let mut second = first.clone();
    second["time"]["created"] = json!(2);
    let mut responses = fixture.input_status_prefix(false);
    responses.extend([
        fixture.input_message_response(&intent, &first),
        fixture.input_message_response(&intent, &second),
    ]);
    let requests = responses.len();
    let (client, server) = http_fixture(responses, "GET").await;
    assert_eq!(
        client
            .read_input_status(&intent, &fixture.options)
            .await
            .unwrap_err()
            .code,
        "NATIVE_EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(server.await.unwrap(), vec![Value::Null; requests]);
}

#[test]
fn native_207_unrelated_forms_are_validated_before_filtering_and_detail_credit() {
    let controls: Value = serde_json::from_str(include_str!(
        "../../../../modules/opencode/fixtures/native-controls-2.0.7.json"
    ))
    .unwrap();
    let mut auth = controls["authForm"].clone();
    NativeClient::validate_form_info(&auth).unwrap();
    auth["fields"] = json!("damaged");
    assert!(NativeClient::validate_form_info(&auth).is_err());
    let mut detail = controls["question"].clone();
    detail["state"] = json!({"status":"cancelled"});
    NativeClient::validate_form_detail(&detail).unwrap();
    detail["state"]["answer"] = json!({"q0":"yes"});
    assert!(NativeClient::validate_form_detail(&detail).is_err());
}

#[tokio::test]
async fn native_207_malformed_visible_filler_never_receives_history_credit() {
    let fixture = fixture();
    let mut missing_payload = root_event();
    missing_payload["data"] = json!({"sessionID":"ses_swarm_fixture"});
    let mut unknown_type = root_event();
    unknown_type["type"] = json!("session.invented");
    let mut wrong_variant = root_event();
    wrong_variant["type"] = json!("session.renamed");
    let mut unsupported_version = root_event();
    unsupported_version["durable"]["version"] = json!(2);
    for mut filler in [
        missing_payload,
        unknown_type,
        wrong_variant,
        unsupported_version,
    ] {
        filler["durable"]["seq"] = json!(1);
        let (client, server) = http_fixture(
            vec![log_response(
                vec![
                    filler,
                    fixture["events"]["enqueued"].clone(),
                    fixture["events"]["delivered"].clone(),
                ],
                0,
            )],
            "GET",
        )
        .await;
        let error = client
            .read_input_history("ses_swarm_fixture", "msg_swarm_fixture", &intent(), None)
            .await
            .unwrap_err();
        assert_eq!(error.code, "NATIVE_HISTORY_SCHEMA");
        server.await.unwrap();
    }
}

#[tokio::test]
async fn native_207_admission_uses_flat_payload_and_does_not_invent_sequences() {
    let fixture = fixture();
    let (client, server) = http_fixture(vec![fixture["admission"].clone()], "POST").await;
    let evidence = client
        .admit_input(
            &command(),
            "ses_swarm_fixture",
            "msg_swarm_fixture",
            "Documented task input",
        )
        .await
        .unwrap();
    assert!(evidence.admitted_sequence.is_none());
    assert!(evidence.promoted_sequence.is_none());
    assert_eq!(server.await.unwrap(), vec![fixture["request"].clone()]);
}

#[test]
fn native_207_admission_rejects_wrong_identity_payload_and_legacy_shape() {
    let original = fixture()["admission"]["data"].clone();
    let mut cases = Vec::new();
    let mut value = original.clone();
    value["sessionID"] = json!("ses_foreign");
    cases.push(value);
    let mut value = original.clone();
    value["payload"]["text"] = json!("other task");
    cases.push(value);
    let mut value = original.clone();
    value["payload"]["metadata"]["eliot"]["operation"] = json!("other-operation");
    cases.push(value);
    let mut value = original;
    value["payload"]["skills"] = json!(["extra-skill"]);
    cases.push(value);
    cases.push(json!({
        "id":"msg_swarm_fixture","sessionID":"ses_swarm_fixture",
        "admittedSeq":2,"timeCreated":123,"prompt":{"text":"Documented task input"},
        "delivery":"queue"
    }));
    for item in cases {
        assert!(
            admitted_input_matches(
                &item,
                "ses_swarm_fixture",
                "msg_swarm_fixture",
                "Documented task input",
                &command()
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn native_207_history_reads_exact_enqueued_and_delivered_across_pages_without_post() {
    let fixture = fixture();
    let mut first_page = vec![root_event(), fixture["events"]["enqueued"].clone()];
    for sequence in 3..=101 {
        first_page.push(json!({
            "id":format!("evt_fixture_{sequence}"),"type":"session.renamed","created":sequence,
            "durable":{"aggregateID":"ses_swarm_fixture","seq":sequence,"version":1},
            "data":{"sessionID":"ses_swarm_fixture","title":"coverage"}
        }));
    }
    let mut delivered = fixture["events"]["delivered"].clone();
    delivered["durable"]["seq"] = json!(102);
    first_page.push(delivered.clone());
    let (client, server) = http_fixture(
        vec![
            log_response(first_page, 0),
            log_response(vec![delivered], 101),
        ],
        "GET",
    )
    .await;
    let evidence = client
        .read_input_history("ses_swarm_fixture", "msg_swarm_fixture", &intent(), None)
        .await
        .unwrap();
    assert_eq!(evidence.admitted_sequence, Some(2));
    assert_eq!(evidence.prompted_sequence, Some(102));
    assert_eq!(server.await.unwrap(), vec![Value::Null, Value::Null]);
}

#[tokio::test]
async fn native_207_delivery_without_exact_admission_never_proves_input() {
    let mut delivered = fixture()["events"]["delivered"].clone();
    delivered["durable"]["seq"] = json!(2);
    let (client, server) =
        http_fixture(vec![log_response(vec![root_event(), delivered], 0)], "GET").await;
    let error = client
        .read_input_history("ses_swarm_fixture", "msg_swarm_fixture", &intent(), None)
        .await
        .unwrap_err();
    assert_eq!(error.code, "NATIVE_HISTORY_GAP");
    server.await.unwrap();
}

#[tokio::test]
async fn native_207_log_eof_and_foreign_watermarks_never_prove_complete_history() {
    for field in ["omit_sync", "sync_aggregate"] {
        let mut response = log_response(vec![root_event()], 0);
        response[field] = if field == "omit_sync" {
            json!(true)
        } else {
            json!("ses_foreign")
        };
        let (client, server) = http_fixture(vec![response], "GET").await;
        let error = client
            .session_log_page("ses_swarm_fixture", 0, 100)
            .await
            .unwrap_err();
        assert!(matches!(
            error.code.as_str(),
            "NATIVE_HISTORY_INCOMPLETE" | "NATIVE_HISTORY_GAP"
        ));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn native_207_filtered_bus_sequences_preserve_exact_admission_and_delivery() {
    let fixture = fixture();
    let mut later = fixture["events"]["delivered"].clone();
    later["durable"]["seq"] = json!(4);
    let response = log_response(
        vec![root_event(), fixture["events"]["enqueued"].clone(), later],
        0,
    );
    let (client, server) = http_fixture(vec![response], "GET").await;
    let evidence = client
        .read_input_history("ses_swarm_fixture", "msg_swarm_fixture", &intent(), None)
        .await
        .unwrap();
    assert_eq!(evidence.admitted_sequence, Some(2));
    assert_eq!(evidence.prompted_sequence, Some(4));
    server.await.unwrap();
}

#[tokio::test]
async fn native_207_selected_message_get_does_not_infer_an_assistant_parent() {
    let fixture = fixture();
    let (client, server) = http_fixture(vec![
        json!({"json_path":"/api/session/ses_swarm_fixture/message/msg_swarm_fixture","json_payload":{"data":fixture["userMessage"]}}),
        json!({"json_path":"/api/session/ses_swarm_fixture/message/msg_fixture_assistant","json_payload":{"data":fixture["assistantMessage"]}}),
    ], "GET").await;
    let error = client
        .read_selected_assistant_result(
            "ses_swarm_fixture",
            "msg_swarm_fixture",
            "msg_fixture_assistant",
            &intent(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "NATIVE_ASSISTANT_PARENT_UNAVAILABLE");
    assert_eq!(server.await.unwrap(), vec![Value::Null, Value::Null]);
}

#[test]
fn native_207_durable_admission_and_user_projection_require_exact_marker() {
    let fixture = fixture();
    let intent = intent();
    validate_prompt_history_event(
        "session.inbox.enqueued",
        &fixture["events"]["enqueued"]["data"],
        "ses_swarm_fixture",
        "msg_swarm_fixture",
        "queue",
        &intent,
    )
    .unwrap();
    assert!(saved_message_matches(
        &fixture["userMessage"],
        "ses_swarm_fixture",
        "msg_swarm_fixture",
        &intent
    ));
    let mut event = fixture["events"]["enqueued"]["data"].clone();
    event["item"]["payload"]["metadata"]["eliot"]["operation"] = json!("foreign");
    assert!(
        validate_prompt_history_event(
            "session.inbox.enqueued",
            &event,
            "ses_swarm_fixture",
            "msg_swarm_fixture",
            "queue",
            &intent
        )
        .is_err()
    );
    let mut message = fixture["userMessage"].clone();
    message.as_object_mut().unwrap().remove("metadata");
    assert!(!saved_message_matches(
        &message,
        "ses_swarm_fixture",
        "msg_swarm_fixture",
        &intent
    ));
}
