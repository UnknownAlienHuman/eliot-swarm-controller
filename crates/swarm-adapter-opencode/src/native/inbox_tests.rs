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
            } else {
                assert!(path.starts_with("/api/session/ses_swarm_fixture/history?"));
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
            let body = serde_json::to_vec(&response).unwrap();
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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

fn history_page(events: Vec<Value>, has_more: bool) -> Value {
    json!({"data":events,"hasMore":has_more})
}

fn root_event() -> Value {
    json!({
        "id":"evt_fixture_root",
        "type":"session.created",
        "created":122,
        "durable":{"aggregateID":"ses_swarm_fixture","seq":1,"version":1},
        "data":{"sessionID":"ses_swarm_fixture"}
    })
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
    let (client, server) = http_fixture(
        vec![
            history_page(
                vec![root_event(), fixture["events"]["enqueued"].clone()],
                true,
            ),
            history_page(vec![fixture["events"]["delivered"].clone()], false),
        ],
        "GET",
    )
    .await;
    let evidence = client
        .read_input_history("ses_swarm_fixture", "msg_swarm_fixture", &intent(), None)
        .await
        .unwrap();
    assert_eq!(evidence.admitted_sequence, Some(2));
    assert_eq!(evidence.prompted_sequence, Some(3));
    assert_eq!(server.await.unwrap(), vec![Value::Null, Value::Null]);
}

#[tokio::test]
async fn native_207_delivery_without_exact_admission_never_proves_input() {
    let mut delivered = fixture()["events"]["delivered"].clone();
    delivered["durable"]["seq"] = json!(2);
    let (client, server) = http_fixture(
        vec![history_page(vec![root_event(), delivered], false)],
        "GET",
    )
    .await;
    let error = client
        .read_input_history("ses_swarm_fixture", "msg_swarm_fixture", &intent(), None)
        .await
        .unwrap_err();
    assert_eq!(error.code, "NATIVE_HISTORY_GAP");
    server.await.unwrap();
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
