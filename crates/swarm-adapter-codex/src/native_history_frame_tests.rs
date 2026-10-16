use super::*;

#[tokio::test]
async fn large_tool_history_stays_within_native_frames_and_preserves_exact_results() {
    let home = std::env::temp_dir().join(format!("codex-history-frame-{}", Uuid::new_v4()));
    fs::create_dir(&home).expect("fixture native home");
    let home = fs::canonicalize(home).expect("canonical native home");
    let mut entries = vec![json!({"turnId":"turn", "item":{
        "type":"userMessage", "id":"input", "clientId":"operation",
        "content":[{"type":"text", "text":"exact retained input"}]
    }})];
    for index in 0..20 {
        entries.push(json!({"turnId":"turn", "item":{
            "type":"commandExecution", "id":format!("tool-{index}"),
            "aggregatedOutput":"x".repeat(64 * 1024), "status":"completed"
        }}));
    }
    entries.push(json!({"turnId":"turn", "item":{
        "type":"agentMessage", "id":"answer", "phase":"final_answer",
        "text":"exact terminal result"
    }}));
    assert!(serde_json::to_vec(&entries).unwrap().len() > MAX_FRAME_BYTES);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server_home = home.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let packet: Value = serde_json::from_str(&text).unwrap();
            let Some(id) = packet.get("id") else { continue };
            let params = &packet["params"];
            let result = match packet["method"].as_str().unwrap() {
                "initialize" => json!({"userAgent":"Codex CLI/0.161.0",
                    "codexHome":server_home, "platformFamily":std::env::consts::FAMILY,
                    "platformOs":std::env::consts::OS}),
                "thread/turns/list" => {
                    assert_eq!(params["itemsView"], "notLoaded");
                    json!({"data":[{"id":"turn", "status":"completed", "error":null}],
                        "nextCursor":null})
                }
                "thread/items/list" => {
                    let offset = params["cursor"]
                        .as_str()
                        .map(|value| value.parse::<usize>().unwrap())
                        .unwrap_or(0);
                    let limit = params["limit"].as_u64().unwrap() as usize;
                    let mut ordered = entries.clone();
                    if params["sortDirection"] == "desc" {
                        ordered.reverse();
                    }
                    let end = (offset + limit).min(ordered.len());
                    json!({"data":ordered[offset..end],
                        "nextCursor":if end < ordered.len() {Some(end.to_string())} else {None}})
                }
                method => panic!("unexpected native method, no input may be replayed: {method}"),
            };
            let response = json!({"id":id, "result":result}).to_string();
            socket.send(Message::Text(response.into())).await.unwrap();
        }
    });
    let mut native =
        NativeClient::attach_inner(&endpoint, None, false, NativeEventSink::new(), false)
            .await
            .expect("attach loopback native client");
    let HistoryRead::Complete(matches) = native.read_history("thread", "operation").await else {
        panic!("bounded pages must read the full large tool history");
    };
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].item_id, "input");
    assert_eq!(matches[0].turn_id, "turn");
    assert_eq!(matches[0].text, "exact retained input");
    assert!(
        matches!(native.read_turn("thread", "turn").await, TurnRead::Found(turn)
        if turn["status"] == "completed")
    );
    let AssistantRead::Found(answer) = native.read_final_assistant_response("thread", "turn").await
    else {
        panic!("bounded pages must preserve the final assistant result")
    };
    assert_eq!(answer.item_id, "answer");
    assert_eq!(answer.text, "exact terminal result");
    drop(native);
    server.abort();
    let _ = server.await;
    fs::remove_dir(&home).unwrap();
}
