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
        // Connection health does not select or execute a model.
        model: crate::config::ModelRef {
            id: "not-requested".into(),
            provider_id: "not-requested".into(),
            variant: "not-requested".into(),
        },
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
            binding,
            generation,
            &serde_json::to_value(&options.model).unwrap(),
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
            assert_eq!(line.next(), Some(method));
            let path = line.next().unwrap();
            if method == "POST" {
                assert_eq!(path, "/api/session/ses_swarm_fixture/prompt");
            } else if let Some(expected_path) = response["json_path"].as_str() {
                assert_eq!(path, expected_path);
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
            let (content_type, body) = if response.get("json_payload").is_some() {
                (
                    "application/json",
                    serde_json::to_vec(&response["json_payload"]).unwrap(),
                )
            } else if method == "POST" {
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
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
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
async fn native_207_root_durability_requires_valid_complete_message_log() {
    let binding = "fixture-binding";
    let root = root_id(binding, 1);
    let model = json!({"id":"fixture-model","providerID":"fixture","variant":"fixture-variant"});
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
            .require_durable_root_creation_inner(&root, binding, 1, &model)
            .await;
        if let Some(code) = expected_error {
            assert_eq!(result.unwrap_err().code, code);
        } else {
            result.unwrap();
        }
        server.await.unwrap();
    }
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
