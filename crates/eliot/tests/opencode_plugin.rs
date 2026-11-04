//! OpenCode plugin protocol tests with a scripted fake HTTP/SSE server.
//!
//! This target runs with `harness = false` (see `[[test]]` in Cargo.toml) so
//! the test binary can re-execute itself as the fake `opencode serve`: the
//! plugin spawns `cmd` with serve arguments, and a libtest harness would
//! corrupt that process model. When `ELIOT_FAKE_OPENCODE_SCRIPT` is set,
//! `main` runs the fixture server instead of the scenarios.
//!
//! Fixture payloads follow the installed OpenCode v2 API generation
//! (`opencode serve` 2.0.32, routes under `/api/...`, `{"data": ...}`
//! success bodies, `{"_tag": ...}` errors, Basic auth as
//! `opencode:OPENCODE_PASSWORD`, SSE envelopes with durable sequences);
//! the plugin under test is the real `eliot plugin opencode` binary driven
//! over the Eliot plugin protocol, with this file emulating the host side
//! (`process.put`, `events.put`, `source.gap`). No model is ever called.

use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};

const CLI: &str = env!("CARGO_BIN_EXE_eliot");
const MODEL: &str = "test-provider/test-model";
const OP_TIMEOUT: Duration = Duration::from_secs(15);
const HANG_TIMEOUT: Duration = Duration::from_secs(45);

fn main() {
    if let Ok(script) = std::env::var("ELIOT_FAKE_OPENCODE_SCRIPT") {
        fake_server_main(&script);
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        scenario_launch_send_snapshot_stop().await;
        println!("ok - launch/send/snapshot/stop");
        scenario_failed_turn_reports_native_error().await;
        println!("ok - failed turn reports its native error");
        scenario_model_mismatch_is_rejected_before_ready().await;
        println!("ok - model mismatch is rejected before session.ready");
        scenario_location_mismatch_is_rejected_before_ready().await;
        println!("ok - location mismatch is rejected before session.ready");
        scenario_prompt_400_is_rejected_without_rewrite().await;
        println!("ok - prompt 400 is rejected without a second write");
        scenario_lost_prompt_reply_is_unknown_without_rewrite().await;
        println!("ok - lost prompt reply is unknown without a second write");
        scenario_stop_of_gone_session_applies().await;
        println!("ok - stop of a gone session applies");
        scenario_invalid_profile_rejected_before_spawn().await;
        println!("ok - invalid profile rejected before spawn");
        scenario_sse_loss_during_send_gaps().await;
        println!("ok - SSE loss during send gaps the source");
    });
    println!("opencode_plugin: all scenarios passed");
}

// ---------------------------------------------------------------------------
// Fake OpenCode server fixture (runs as a child of the plugin under test)
// ---------------------------------------------------------------------------

/// Script knobs as JSON in `ELIOT_FAKE_OPENCODE_SCRIPT`. Defaults describe
/// the happy path; scenarios override single fields.
#[derive(Debug, Clone)]
struct Script {
    session_id: String,
    create_model: Option<Value>,
    create_dir: Option<String>,
    prompt_status: u16,
    prompt_hang: bool,
    terminal: String,
    assistant_text: Option<String>,
    error: Option<Value>,
    delete_status: u16,
    drop_sse_on_prompt: bool,
}

impl Script {
    fn parse(raw: &str) -> Self {
        let value: Value = serde_json::from_str(raw).expect("script JSON");
        let get = |key: &str| value.get(key).cloned().unwrap_or(Value::Null);
        let text = |key: &str, default: &str| get(key).as_str().unwrap_or(default).to_owned();
        Script {
            session_id: text("session_id", "ses_test00000000000000000001"),
            create_model: match get("create_model") {
                Value::Null => None,
                model => Some(model),
            },
            create_dir: match get("create_dir") {
                Value::Null => None,
                Value::String(dir) => Some(dir),
                _ => None,
            },
            prompt_status: get("prompt_status").as_u64().unwrap_or(200) as u16,
            prompt_hang: get("prompt_hang").as_bool().unwrap_or(false),
            terminal: text("terminal", "succeeded"),
            assistant_text: match get("assistant_text") {
                Value::Null => None,
                Value::String(text) => Some(text),
                _ => None,
            },
            error: match get("error") {
                Value::Null => None,
                error => Some(error),
            },
            delete_status: get("delete_status").as_u64().unwrap_or(204) as u16,
            drop_sse_on_prompt: get("drop_sse_on_prompt").as_bool().unwrap_or(false),
        }
    }
}

struct Fixture {
    script: Script,
    expected_auth: String,
    log_path: PathBuf,
    sessions: Mutex<HashMap<String, FixtureSession>>,
    deleted: Mutex<std::collections::HashSet<String>>,
    inbox_counter: Mutex<u64>,
    events: broadcast::Sender<String>,
    sse_close: broadcast::Sender<()>,
    config_path: String,
}

struct FixtureSession {
    model: Value,
    directory: String,
    seq: u64,
    inbox_id: Option<String>,
    terminal: Option<String>,
}

fn fake_server_main(script_raw: &str) {
    let script = Script::parse(script_raw);
    let mut port: Option<u16> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--port"
            && let Some(value) = args.next()
        {
            port = value.parse().ok();
        }
    }
    let port = port.expect("fixture --port");
    let password = std::env::var("OPENCODE_PASSWORD").expect("fixture password");
    assert!(!password.is_empty(), "fixture password is empty");
    let config_home = std::env::var("XDG_CONFIG_HOME").expect("fixture XDG_CONFIG_HOME");
    let data_home = std::env::var("XDG_DATA_HOME").expect("fixture XDG_DATA_HOME");
    let log_path = PathBuf::from(std::env::var("ELIOT_FAKE_OPENCODE_LOG").expect("fixture log"));
    let report_path =
        PathBuf::from(std::env::var("ELIOT_FAKE_OPENCODE_REPORT").expect("fixture report"));
    // The plugin generates this file; the server only reads it. Its presence
    // and content prove the per-Agent config isolation end to end.
    let config_path = Path::new(&config_home)
        .join("opencode")
        .join("opencode.jsonc");
    let config_text = std::fs::read_to_string(&config_path).expect("fixture reads its config");
    let config_json: Value = serde_json::from_str(&config_text).expect("fixture config JSON");
    let config_model = config_json
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    assert!(
        !config_model.is_empty(),
        "fixture config names the profile model"
    );
    std::fs::write(
        &report_path,
        serde_json::to_string(&json!({
            "config_path": config_path.to_string_lossy(),
            "config_model": config_model,
            "config_mcp": config_json.get("mcp").cloned().unwrap_or(Value::Null),
            "has_password": true,
            "data_home": data_home,
            "port": port,
        }))
        .expect("report JSON"),
    )
    .expect("write fixture report");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("fixture runtime");
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("fixture bind");
        let (events, _) = broadcast::channel::<String>(64);
        let (sse_close, _) = broadcast::channel::<()>(1);
        let fixture = Arc::new(Fixture {
            script,
            expected_auth: format!(
                "Basic {}",
                base64_encode(format!("opencode:{password}").as_bytes())
            ),
            log_path,
            sessions: Mutex::new(HashMap::new()),
            deleted: Mutex::new(std::collections::HashSet::new()),
            inbox_counter: Mutex::new(0),
            events,
            sse_close,
            config_path: config_path.to_string_lossy().into_owned(),
        });
        // The installed server prints its listening line on stdout.
        println!("server listening on http://127.0.0.1:{port}");
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let fixture = Arc::clone(&fixture);
            tokio::spawn(async move {
                serve_connection(fixture, stream).await;
            });
        }
    });
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let mut value: u32 = 0;
        for (index, byte) in chunk.iter().enumerate() {
            value |= (*byte as u32) << (16 - 8 * index);
        }
        out.push(ALPHABET[(value >> 18) as usize & 63] as char);
        out.push(ALPHABET[(value >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(value >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[value as usize & 63] as char);
        } else {
            out.push('=');
        }
    }
    out
}

async fn serve_connection(fixture: Arc<Fixture>, stream: tokio::net::TcpStream) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = AsyncBufReader::new(reader);
    let mut head = Vec::new();
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => return,
            Ok(_) => {}
            Err(_) => return,
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        head.push(line);
        if head.len() > 64 || head.iter().map(|line| line.len()).sum::<usize>() > 16 * 1024 {
            return;
        }
    }
    if head.is_empty() {
        return;
    }
    let request_line = head[0].trim_end().to_owned();
    let mut parts = request_line.splitn(3, ' ');
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let mut content_length: usize = 0;
    let mut authorization = String::new();
    for line in &head[1..] {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
            && let Ok(length) = value.trim().parse::<usize>()
        {
            content_length = length.min(1024 * 1024);
        }
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("authorization")
        {
            authorization = value.trim().to_owned();
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        let mut read = 0;
        while read < content_length {
            match reader.read(&mut body[read..]).await {
                Ok(0) => return,
                Ok(count) => read += count,
                Err(_) => return,
            }
        }
    }
    if authorization != fixture.expected_auth {
        let payload = json!({"_tag": "UnauthorizedError", "message": "Authentication required"});
        write_response(&mut writer, 401, &payload.to_string()).await;
        return;
    }
    let path = target.split('?').next().unwrap_or_default().to_owned();
    let query = target.split_once('?').map(|(_, query)| query.to_owned());
    fixture.log(&method, &path, &body).await;
    if path == "/api/event" {
        serve_sse(fixture, writer).await;
        return;
    }
    let payload = route(&fixture, &method, &path, query.as_deref(), &body).await;
    match payload {
        Some((status, text)) => write_response(&mut writer, status, &text).await,
        None => {
            // A hung handler (the lost-reply script): hold the connection
            // until the client deadline fires.
            std::future::pending::<()>().await;
        }
    }
}

async fn write_response(writer: &mut tokio::net::tcp::OwnedWriteHalf, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = writer.write_all(head.as_bytes()).await;
    let _ = writer.write_all(body.as_bytes()).await;
}

async fn serve_sse(fixture: Arc<Fixture>, mut writer: tokio::net::tcp::OwnedWriteHalf) {
    const HEAD: &str = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\nTransfer-Encoding: chunked\r\n\r\n";
    if writer.write_all(HEAD.as_bytes()).await.is_err() {
        return;
    }
    let mut receiver = fixture.events.subscribe();
    let mut closer = fixture.sse_close.subscribe();
    let connected = json!({"id": "evt_fixture_connected", "type": "server.connected", "data": {}});
    if write_chunk(&mut writer, &format!("data: {connected}\n\n"))
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            biased;
            _ = closer.recv() => {
                // Simulated transport loss: close the stream mid-turn.
                let _ = writer.shutdown().await;
                return;
            }
            next = receiver.recv() => {
                match next {
                    Ok(envelope) => {
                        if write_chunk(&mut writer, &format!("data: {envelope}\n\n"))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        }
    }
}

async fn write_chunk(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    payload: &str,
) -> std::io::Result<()> {
    writer
        .write_all(format!("{:x}\r\n", payload.len()).as_bytes())
        .await?;
    writer.write_all(payload.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await
}

/// Route one fixture request. `None` hangs the connection (lost reply).
async fn route(
    fixture: &Fixture,
    method: &str,
    path: &str,
    query: Option<&str>,
    body: &[u8],
) -> Option<(u16, String)> {
    if method == "GET" && path == "/api/config" {
        let payload = json!([
            {"type": "document", "path": fixture.config_path,
             "info": {"model": {"providerID": "test-provider", "model": "test-model"}, "mcp": {}}},
            {"type": "directory", "path": fixture.config_path.rsplit(['/', '\\']).nth(1).unwrap_or_default()},
        ]);
        return Some((200, payload.to_string()));
    }
    if method == "POST" && path == "/api/session" {
        let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let id = fixture.script.session_id.clone();
        let model = fixture.script.create_model.clone().unwrap_or_else(|| {
            // Echo the requested model in the installed spelling; the
            // request may use either `providerID` or `providerId`.
            let requested = request.get("model").cloned().unwrap_or(Value::Null);
            let provider = requested
                .get("providerID")
                .or_else(|| requested.get("providerId"))
                .cloned()
                .unwrap_or(Value::Null);
            json!({
                "id": requested.get("id").cloned().unwrap_or(Value::Null),
                "providerID": provider,
            })
        });
        let directory = fixture.script.create_dir.clone().unwrap_or_else(|| {
            request
                .get("location")
                .and_then(|location| location.get("directory"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        });
        let mut sessions = fixture.sessions.lock().await;
        let seq = 0;
        sessions.insert(
            id.clone(),
            FixtureSession {
                model: model.clone(),
                directory: directory.clone(),
                seq: seq + 1,
                inbox_id: None,
                terminal: None,
            },
        );
        drop(sessions);
        let created = json!({
            "id": format!("evt_{id}_created"),
            "created": 1000,
            "type": "session.created",
            "location": {"directory": directory},
            "data": {"sessionID": id},
            "durable": {"aggregateID": id, "seq": seq, "version": 1},
        });
        let _ = fixture.events.send(created.to_string());
        let payload = json!({"data": {
            "id": id,
            "model": model,
            "time": {"created": 1000, "updated": 1000},
            "location": {"directory": directory},
        }});
        return Some((200, payload.to_string()));
    }
    if let Some(rest) = path.strip_prefix("/api/session/") {
        let (id, tail) = match rest.split_once('/') {
            Some((id, tail)) => (id, format!("/{tail}")),
            None => (rest, String::new()),
        };
        if fixture.deleted.lock().await.contains(id) {
            return Some((404, not_found(id)));
        }
        if !fixture.sessions.lock().await.contains_key(id) {
            return Some((404, not_found(id)));
        }
        if method == "GET" && tail.is_empty() {
            let sessions = fixture.sessions.lock().await;
            let session = sessions.get(id).expect("fixture session");
            let payload = json!({"data": {
                "id": id,
                "model": session.model,
                "time": {"created": 1000, "updated": 1000},
                "location": {"directory": session.directory},
            }});
            return Some((200, payload.to_string()));
        }
        if method == "DELETE" && tail.is_empty() {
            if fixture.script.delete_status != 204 {
                // The session was already gone: DELETE reports 404 and the
                // session stays absent for the follow-up readback.
                fixture.deleted.lock().await.insert(id.to_owned());
                return Some((fixture.script.delete_status, not_found(id)));
            }
            fixture.deleted.lock().await.insert(id.to_owned());
            let seq = next_seq(fixture, id).await;
            let deleted = json!({
                "id": format!("evt_{id}_deleted"),
                "created": 9000,
                "type": "session.deleted",
                "data": {"sessionID": id},
                "durable": {"aggregateID": id, "seq": seq, "version": 2},
            });
            let _ = fixture.events.send(deleted.to_string());
            return Some((204, String::new()));
        }
        if method == "POST" && tail == "/prompt" {
            if fixture.script.prompt_hang {
                return None;
            }
            if fixture.script.prompt_status != 200 {
                let payload = json!({
                    "_tag": "InvalidRequestError",
                    "message": "scripted prompt refusal",
                    "kind": "Payload",
                });
                return Some((fixture.script.prompt_status, payload.to_string()));
            }
            let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let text = request
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mut counter = fixture.inbox_counter.lock().await;
            *counter += 1;
            let inbox_id = format!("msg_fixture_{:03}", *counter);
            drop(counter);
            {
                let mut sessions = fixture.sessions.lock().await;
                let session = sessions.get_mut(id).expect("fixture session");
                session.inbox_id = Some(inbox_id.clone());
                session.terminal = Some(fixture.script.terminal.clone());
            }
            if fixture.script.drop_sse_on_prompt {
                // Admit the input, then lose the stream before any
                // association fact flows: the send must settle unknown.
                let _ = fixture.sse_close.send(());
                let payload = json!({"data": {
                    "id": inbox_id,
                    "sessionID": id,
                    "time": {"created": 2000},
                    "type": "user",
                    "payload": {"text": text},
                    "delivery": "steer",
                }});
                return Some((200, payload.to_string()));
            }
            let enqueued_seq = next_seq(fixture, id).await;
            let started_seq = next_seq(fixture, id).await;
            let delivered_seq = next_seq(fixture, id).await;
            let terminal_seq = next_seq(fixture, id).await;
            let started_id = format!("evt_{id}_started_{enqueued_seq}");
            for envelope in [
                json!({
                    "id": format!("evt_{id}_enqueued_{enqueued_seq}"),
                    "created": 2000,
                    "type": "session.inbox.enqueued",
                    "data": {"sessionID": id, "inboxID": inbox_id,
                             "item": {"type": "user", "payload": {"text": text}, "delivery": "steer"}},
                    "durable": {"aggregateID": id, "seq": enqueued_seq, "version": 1},
                }),
                json!({
                    "id": started_id,
                    "created": 2001,
                    "type": "session.execution.started",
                    "data": {"sessionID": id},
                    "durable": {"aggregateID": id, "seq": started_seq, "version": 1},
                }),
                json!({
                    "id": format!("evt_{id}_delivered_{delivered_seq}"),
                    "created": 2002,
                    "type": "session.inbox.delivered",
                    "data": {"sessionID": id, "inboxID": inbox_id},
                    "durable": {"aggregateID": id, "seq": delivered_seq, "version": 1},
                }),
                terminal_envelope(fixture, id, terminal_seq),
            ] {
                let _ = fixture.events.send(envelope.to_string());
            }
            let payload = json!({"data": {
                "id": inbox_id,
                "sessionID": id,
                "time": {"created": 2000},
                "type": "user",
                "payload": {"text": text},
                "delivery": "steer",
            }});
            return Some((200, payload.to_string()));
        }
        if method == "GET" && tail == "/message" {
            let _ = query;
            let sessions = fixture.sessions.lock().await;
            let session = sessions.get(id).expect("fixture session");
            let Some(inbox_id) = session.inbox_id.clone() else {
                let payload = json!({"data": [], "cursor": {"previous": null, "next": null}});
                return Some((200, payload.to_string()));
            };
            let terminal = session.terminal.clone().unwrap_or_default();
            let outcome = match terminal.as_str() {
                "session.execution.succeeded" => "succeeded",
                "session.execution.interrupted" => "interrupted",
                _ => "failed",
            };
            let mut data = vec![idle_message(fixture, outcome)];
            if let Some(text) = fixture.script.assistant_text.clone() {
                data.push(json!({
                    "id": "msg_fixture_assistant_001",
                    "time": {"created": 2003, "completed": 2004},
                    "type": "assistant",
                    "agent": "build",
                    "model": {"id": "test-model", "providerID": "test-provider"},
                    "content": [{"type": "text", "text": text}],
                }));
            }
            data.push(json!({
                "id": inbox_id,
                "time": {"created": 2000},
                "text": "fixture input",
                "type": "user",
            }));
            let payload = json!({"data": data, "cursor": {"previous": null, "next": null}});
            return Some((200, payload.to_string()));
        }
    }
    let payload = json!({"_tag": "UnknownError", "message": "fixture has no such route"});
    Some((404, payload.to_string()))
}

fn idle_message(fixture: &Fixture, outcome: &str) -> Value {
    let mut message = json!({
        "id": "msg_fixture_idle_001",
        "time": {"created": 2005},
        "type": "idle",
        "outcome": outcome,
    });
    if let Some(error) = fixture.script.error.clone() {
        message["error"] = error;
    }
    message
}

fn terminal_envelope(fixture: &Fixture, id: &str, seq: u64) -> Value {
    let terminal = fixture.script.terminal.clone();
    let mut data = json!({"sessionID": id});
    if terminal == "session.execution.failed"
        && let Some(error) = fixture.script.error.clone()
    {
        data["error"] = error;
    }
    if terminal == "session.execution.interrupted" {
        data["reason"] = Value::String("user".to_owned());
    }
    json!({
        "id": format!("evt_{id}_terminal_{seq}"),
        "created": 2005,
        "type": terminal,
        "data": data,
        "durable": {"aggregateID": id, "seq": seq, "version": 1},
    })
}

async fn next_seq(fixture: &Fixture, id: &str) -> u64 {
    let mut sessions = fixture.sessions.lock().await;
    let session = sessions.get_mut(id).expect("fixture session");
    let seq = session.seq;
    session.seq += 1;
    seq
}

fn not_found(id: &str) -> String {
    json!({
        "_tag": "SessionNotFoundError",
        "sessionID": id,
        "message": format!("Session not found: {id}"),
    })
    .to_string()
}

impl Fixture {
    async fn log(&self, method: &str, path: &str, body: &[u8]) {
        // The workspace tokio has no `fs` feature; a tiny blocking append
        // is fine for the fixture log.
        use std::io::Write;
        let entry = json!({
            "method": method,
            "path": path,
            "body": String::from_utf8_lossy(body).into_owned(),
        });
        let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        else {
            return;
        };
        let _ = file.write_all(format!("{entry}\n").as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Host-side driver (emulates the daemon: process.put, events.put, source.gap)
// ---------------------------------------------------------------------------

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;

struct Host {
    writes: Option<mpsc::Sender<String>>,
    pending: Pending,
    inbound: mpsc::Receiver<HostRequest>,
    next_id: u64,
    next_seq: i64,
    child: tokio::process::Child,
    stderr: Option<tokio::task::JoinHandle<String>>,
}

struct HostRequest {
    id: Value,
    method: String,
    params: Value,
}

struct Scenario {
    root: PathBuf,
    agent_id: String,
    host: Host,
}

impl Host {
    async fn spawn(root: &Path) -> Self {
        let mut command = tokio::process::Command::new(CLI);
        command
            .arg("--data-dir")
            .arg(root.join("data"))
            .arg("plugin")
            .arg("opencode")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            command.creation_flags(0x0800_0000);
        }
        // The plugin child must never mistake itself for the fixture.
        command.env_remove("ELIOT_FAKE_OPENCODE_SCRIPT");
        let mut child = command.spawn().expect("spawn plugin opencode");
        let stdin = child.stdin.take().expect("plugin stdin");
        let stdout = child.stdout.take().expect("plugin stdout");
        let stderr = child.stderr.take().expect("plugin stderr");
        let stderr_task = tokio::spawn(async move {
            let mut text = String::new();
            let mut stderr = stderr;
            let _ = stderr.read_to_string(&mut text).await;
            text.chars().take(2048).collect()
        });
        let (writes, mut write_rx) = mpsc::channel::<String>(32);
        tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(line) = write_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if stdin.write_all(b"\n").await.is_err() {
                    break;
                }
                if stdin.flush().await.is_err() {
                    break;
                }
            }
        });
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (inbound_tx, inbound) = mpsc::channel::<HostRequest>(32);
        let reader_pending = Arc::clone(&pending);
        tokio::spawn(async move {
            let mut lines = AsyncBufReader::new(stdout).lines();
            loop {
                let line = match lines.next_line().await {
                    Ok(Some(line)) => line,
                    _ => break,
                };
                if line.trim().is_empty() {
                    continue;
                }
                let frame: Value = match serde_json::from_str(&line) {
                    Ok(frame) => frame,
                    Err(_) => continue,
                };
                if let Some(method) = frame.get("method").and_then(Value::as_str) {
                    let _ = inbound_tx
                        .send(HostRequest {
                            id: frame.get("id").cloned().unwrap_or(Value::Null),
                            method: method.to_owned(),
                            params: frame.get("params").cloned().unwrap_or(Value::Null),
                        })
                        .await;
                    continue;
                }
                let id = frame
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let sender = reader_pending.lock().await.remove(&id);
                let Some(sender) = sender else { continue };
                if let Some(result) = frame.get("result") {
                    let _ = sender.send(Ok(result.clone()));
                } else {
                    // Log the full error for failure diagnostics; scenarios
                    // assert on the code only.
                    eprintln!("plugin error response: {frame}");
                    let code = frame
                        .get("error")
                        .and_then(|error| error.get("data"))
                        .and_then(|data| data.get("code"))
                        .and_then(Value::as_str)
                        .unwrap_or("rpc_error")
                        .to_owned();
                    let _ = sender.send(Err(code));
                }
            }
        });
        Self {
            writes: Some(writes),
            pending,
            inbound,
            next_id: 0,
            next_seq: 1,
            child,
            stderr: Some(stderr_task),
        }
    }

    fn writer(&self) -> mpsc::Sender<String> {
        self.writes.clone().expect("host write queue")
    }

    /// Pump one host call, acking every inbound batch while it is in
    /// flight. Native commits may land on either side of the response, so
    /// the pumped `(kind, data)` pairs return with the outcome and the
    /// scenario asserts on both.
    async fn pump_call(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> (Result<Value, String>, Vec<(String, Value)>) {
        let rx = self.begin_call(method, params).await;
        // One deadline for the whole call: inbound traffic and the response
        // share it, so a legitimately slow call (the lost-reply script)
        // never trips a per-recv timeout while waiting.
        let deadline = tokio::time::Instant::now() + timeout;
        let mut rx = std::pin::pin!(tokio::time::timeout_at(deadline, rx));
        let mut committed: Vec<(String, Value)> = Vec::new();
        let outcome = loop {
            tokio::select! {
                biased;
                request = tokio::time::timeout_at(deadline, self.inbound.recv()) => {
                    match request {
                        Ok(Some(request)) => match request.method.as_str() {
                            "events.put" => committed.push(ack_events_put(self, &request).await),
                            "source.gap" => {
                                let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                                let seq = self.next_seq;
                                self.next_seq += 1;
                                self.respond(&request.id, json!({"gap_id": gap_id, "seq": seq})).await;
                                committed.push(("source.gap".to_owned(), request.params));
                            }
                            other => panic!("unexpected host request during {method}: {other}"),
                        },
                        Ok(None) => panic!("plugin request channel closed during {method}"),
                        Err(_) => break (&mut rx).await,
                    }
                }
                result = &mut rx => break result,
            }
        };
        let outcome = outcome
            .expect("host call deadline")
            .expect("host response channel");
        // Drain any commit that raced the response onto the wire after it.
        while let Ok(request) =
            tokio::time::timeout(Duration::from_millis(200), self.inbound.recv()).await
        {
            let Some(request) = request else { break };
            match request.method.as_str() {
                "events.put" => committed.push(ack_events_put(self, &request).await),
                "source.gap" => {
                    let gap_id = request
                        .params
                        .get("gap_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    let seq = self.next_seq;
                    self.next_seq += 1;
                    self.respond(&request.id, json!({"gap_id": gap_id, "seq": seq}))
                        .await;
                    committed.push(("source.gap".to_owned(), request.params));
                }
                other => panic!("unexpected host request racing {method}: {other}"),
            }
        }
        (outcome, committed)
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let (outcome, committed) = self.pump_call(method, params, OP_TIMEOUT).await;
        assert!(
            committed.is_empty(),
            "expected no commits during {method}: {committed:?}"
        );
        outcome
    }

    async fn begin_call(
        &mut self,
        method: &str,
        params: Value,
    ) -> oneshot::Receiver<Result<Value, String>> {
        self.next_id += 1;
        let id = format!("host-call-{}", self.next_id);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);
        let frame = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.writer()
            .send(frame.to_string())
            .await
            .expect("host write queue");
        rx
    }

    async fn respond(&self, id: &Value, result: Value) {
        let frame = json!({"jsonrpc": "2.0", "id": id, "result": result});
        self.writer()
            .send(frame.to_string())
            .await
            .expect("host write queue");
    }

    async fn shutdown_child(&mut self) -> String {
        drop(self.writes.take());
        let status = tokio::time::timeout(OP_TIMEOUT, self.child.wait())
            .await
            .expect("plugin exit deadline")
            .expect("plugin wait");
        assert!(status.success(), "plugin exit status: {status}");
        self.stderr.take().unwrap().await.unwrap_or_default()
    }
}

/// Ack one `events.put` batch, returning its single `(kind, data)`.
async fn ack_events_put(host: &mut Host, request: &HostRequest) -> (String, Value) {
    let events = request
        .params
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(events.len(), 1, "one native record per batch");
    let pair = (
        events[0]
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        events[0].get("data").cloned().unwrap_or(Value::Null),
    );
    let first = host.next_seq;
    host.next_seq += 1;
    host.respond(
        &request.id,
        json!({
            "first_seq": first, "last_seq": first,
            "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
        }),
    )
    .await;
    pair
}

struct PendingStart {
    root: PathBuf,
    agent_id: String,
    host: Host,
    params: Value,
    generation: i64,
}

struct FailedAttempt {
    host: Host,
    params: Value,
}

impl Scenario {
    async fn start(script: &Value, profile: Value) -> Self {
        let agent_id = ulid::Ulid::new().to_string();
        let (pending, rx) = Self::setup_start(script, profile, agent_id, 1).await;
        pending.finish(rx).await
    }

    async fn setup_start(
        script: &Value,
        profile: Value,
        agent_id: String,
        generation: i64,
    ) -> (PendingStart, oneshot::Receiver<Result<Value, String>>) {
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-opencode-plugin-test-{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ));
        std::fs::create_dir_all(root.join("data")).unwrap();
        std::fs::create_dir_all(root.join("instance")).unwrap();
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        let mut host = Host::spawn(&root).await;
        let manifest = host
            .call(
                "initialize",
                json!({
                    "protocol": "eliot-plugin/1",
                    "credential": format!("credential-{}", ulid::Ulid::new()),
                    "instance_dir": root.join("instance").to_string_lossy(),
                    "config": {},
                }),
            )
            .await
            .expect("initialize");
        assert_eq!(
            manifest.get("name").and_then(Value::as_str),
            Some("opencode")
        );
        assert_eq!(
            manifest.get("protocol").and_then(Value::as_str),
            Some("eliot-plugin/1")
        );
        assert_eq!(manifest["verbs"], json!(["launch", "send", "stop"]));
        assert_eq!(
            manifest["sources"]["controlled"],
            json!({"message": "stream", "session": "stream", "turn": "stream"})
        );
        let start_id = ulid::Ulid::new().to_string();
        let self_exe = std::env::current_exe().expect("test executable");
        let mut full_profile = profile;
        full_profile["cmd"] = Value::String(self_exe.to_string_lossy().into_owned());
        let params = json!({
            "agent_id": agent_id,
            "generation": generation,
            "command_id": start_id,
            "profile": full_profile,
            "workspace": root.join("workspace").to_string_lossy(),
            "target_environment": {
                "ELIOT_FAKE_OPENCODE_SCRIPT": script.to_string(),
                "ELIOT_FAKE_OPENCODE_LOG": root.join("requests.jsonl").to_string_lossy(),
                "ELIOT_FAKE_OPENCODE_REPORT": root.join("report.json").to_string_lossy(),
            },
            "note": null,
            "source_plan": {"session": "stream", "turn": "stream", "message": "stream"},
            "source_state": null,
            "resume_from": null,
            "initial": null,
        });
        let rx = host.begin_call("agent.start", params.clone()).await;
        (
            PendingStart {
                root,
                agent_id,
                host,
                params,
                generation,
            },
            rx,
        )
    }

    fn report(&self) -> Value {
        let text =
            std::fs::read_to_string(self.root.join("report.json")).expect("fake server report");
        serde_json::from_str(&text).expect("fake server report JSON")
    }

    fn requests(&self) -> Vec<Value> {
        let text = std::fs::read_to_string(self.root.join("requests.jsonl")).unwrap_or_default();
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("request log JSON"))
            .collect()
    }
}

impl PendingStart {
    async fn finish(mut self, rx: oneshot::Receiver<Result<Value, String>>) -> Scenario {
        let deadline = tokio::time::Instant::now() + OP_TIMEOUT;
        let mut rx = std::pin::pin!(tokio::time::timeout_at(deadline, rx));
        let mut ready_data: Option<Value> = None;
        let ready = loop {
            tokio::select! {
                biased;
                request = tokio::time::timeout_at(deadline, self.host.inbound.recv()) => {
                    let request = match request {
                        Ok(Some(request)) => request,
                        Ok(None) => panic!("plugin request channel closed during start"),
                        Err(_) => break (&mut rx).await,
                    };
                    match request.method.as_str() {
                        "process.put" => {
                            answer_process_put(&mut self.host, &self.agent_id, self.generation, &request).await;
                        }
                        "events.put" => {
                            let (kind, data) = ack_events_put(&mut self.host, &request).await;
                            assert_eq!(kind, "session.ready");
                            assert!(ready_data.is_none(), "session.ready commits once");
                            ready_data = Some(data);
                        }
                        other => panic!("unexpected host request during start: {other}"),
                    }
                }
                result = &mut rx => break result,
            }
        };
        let ready = ready
            .expect("agent.start deadline")
            .expect("agent.start response channel")
            .expect("agent.start ready");
        let start_id = self
            .params
            .get("command_id")
            .and_then(Value::as_str)
            .expect("start command id");
        assert_eq!(ready.get("state").and_then(Value::as_str), Some("ready"));
        assert_eq!(
            ready.get("agent_id").and_then(Value::as_str),
            Some(self.agent_id.as_str())
        );
        assert_eq!(
            ready.get("generation").and_then(Value::as_i64),
            Some(self.generation)
        );
        assert_eq!(
            ready.get("command_id").and_then(Value::as_str),
            Some(start_id)
        );
        let ready_data = ready_data.expect("session.ready batch");
        let native_scope = ready_data
            .get("native_scope")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            native_scope.starts_with("opencode-store:"),
            "native scope derives from the per-Agent store: {native_scope}"
        );
        let native_ref = ready_data
            .get("native_ref")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(!native_ref.is_empty());
        assert_eq!(
            ready.get("native_scope").and_then(Value::as_str),
            Some(native_scope)
        );
        assert_eq!(
            ready.get("native_ref").and_then(Value::as_str),
            Some(native_ref)
        );
        assert_eq!(
            ready["requested"].get("model").and_then(Value::as_str),
            Some(MODEL)
        );
        assert_eq!(
            ready["effective"].get("model").and_then(Value::as_str),
            Some(MODEL)
        );
        assert_eq!(
            ready["effective"]
                .get("model_verified")
                .and_then(Value::as_bool),
            Some(true)
        );
        Scenario {
            root: self.root,
            agent_id: self.agent_id,
            host: self.host,
        }
    }

    async fn finish_expecting_error(
        mut self,
        rx: oneshot::Receiver<Result<Value, String>>,
        expected_code: &str,
    ) -> FailedAttempt {
        let deadline = tokio::time::Instant::now() + OP_TIMEOUT;
        let mut rx = std::pin::pin!(tokio::time::timeout_at(deadline, rx));
        let outcome = loop {
            tokio::select! {
                biased;
                request = tokio::time::timeout_at(deadline, self.host.inbound.recv()) => {
                    let request = match request {
                        Ok(Some(request)) => request,
                        Ok(None) => panic!("plugin request channel closed during start"),
                        Err(_) => break (&mut rx).await,
                    };
                    match request.method.as_str() {
                        "process.put" => {
                            answer_process_put(&mut self.host, &self.agent_id, self.generation, &request).await;
                        }
                        "source.gap" => {
                            let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                            let seq = self.host.next_seq;
                            self.host.next_seq += 1;
                            self.host.respond(&request.id, json!({"gap_id": gap_id, "seq": seq})).await;
                        }
                        "events.put" => panic!("no session.ready may commit on a failed start"),
                        other => panic!("unexpected host request during start: {other}"),
                    }
                }
                result = &mut rx => break result,
            }
        };
        let outcome = outcome
            .expect("agent.start deadline")
            .expect("agent.start response channel");
        assert_eq!(outcome, Err(expected_code.to_owned()));
        FailedAttempt {
            host: self.host,
            params: self.params,
        }
    }
}

impl FailedAttempt {
    async fn retry(&mut self, expected_code: &str) {
        let outcome = self.host.call("agent.start", self.params.clone()).await;
        assert_eq!(outcome, Err(expected_code.to_owned()));
    }
}

async fn answer_process_put(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    request: &HostRequest,
) {
    let pid = request
        .params
        .get("pid")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    assert!(pid > 0);
    assert_eq!(
        request.params.get("agent_id").and_then(Value::as_str),
        Some(agent_id)
    );
    assert_eq!(
        request.params.get("generation").and_then(Value::as_i64),
        Some(generation)
    );
    assert_eq!(
        request.params.get("role").and_then(Value::as_str),
        Some("harness")
    );
    host.respond(
        &request.id,
        json!({
            "identity": {"host_boot_id": "test-boot", "pid": pid, "start_time": 1},
            "owner": format!("agent:{agent_id}#{generation}"),
            "role": "harness",
            "state": "running",
            "exit_code": null,
            "updated_at": 1,
        }),
    )
    .await;
}

fn opencode_profile() -> Value {
    json!({
        "harness": "opencode",
        "model": MODEL,
        "pass_env": ["ELIOT_FAKE_OPENCODE_SCRIPT", "ELIOT_FAKE_OPENCODE_LOG", "ELIOT_FAKE_OPENCODE_REPORT"],
    })
}

fn script(fields: Value) -> Value {
    let mut base = json!({
        "terminal": "session.execution.succeeded",
        "assistant_text": "PONG",
    });
    for (key, value) in fields.as_object().cloned().unwrap_or_default() {
        base[key] = value;
    }
    base
}

/// One send round trip. Returns the send id, the applied turn id, and every
/// committed `(kind, data)` in commit order (commits may land on either
/// side of the response).
async fn send_turn(scenario: &mut Scenario, text: &str) -> (String, String, Vec<(String, Value)>) {
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": text},
                "turn_id": null,
                "request_id": null,
            }),
            OP_TIMEOUT,
        )
        .await;
    let applied = outcome.expect("send applies");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("applied turn id")
        .to_owned();
    assert!(!turn_id.is_empty());
    (send_id, turn_id, committed)
}

async fn stop_agent(scenario: &mut Scenario, force: bool) -> Vec<(String, Value)> {
    let stop_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.stop",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": stop_id,
                "force": force,
            }),
            OP_TIMEOUT,
        )
        .await;
    let stopped = outcome.expect("stop applies");
    assert_eq!(
        stopped.get("state").and_then(Value::as_str),
        Some("applied")
    );
    committed
}

fn kinds(committed: &[(String, Value)]) -> Vec<&str> {
    committed.iter().map(|(kind, _)| kind.as_str()).collect()
}

async fn scenario_launch_send_snapshot_stop() {
    let mut scenario = Scenario::start(&script(json!({})), opencode_profile()).await;
    // The fixture proves the per-Agent isolation: its config path sits
    // under the plugin instance dir and names the profile model.
    let report = scenario.report();
    let config_path = report
        .get("config_path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        config_path.contains("instance"),
        "per-Agent config path: {config_path}"
    );
    assert_eq!(
        report.get("config_model").and_then(Value::as_str),
        Some(MODEL)
    );
    assert_eq!(
        report.get("config_mcp").cloned().unwrap_or(Value::Null),
        json!({})
    );
    assert_eq!(
        report.get("has_password").and_then(Value::as_bool),
        Some(true)
    );

    let (send_id, turn_id, committed) = send_turn(&mut scenario, "reply PONG").await;
    assert_eq!(
        kinds(&committed),
        ["turn.started", "message.completed", "turn.completed"]
    );
    assert_eq!(
        committed[0].1.get("command_id").and_then(Value::as_str),
        Some(send_id.as_str())
    );
    assert_eq!(
        committed[0].1.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert_eq!(
        committed[1].1.get("text").and_then(Value::as_str),
        Some("PONG")
    );
    assert_eq!(
        committed[2].1.get("status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        committed[2].1.get("final_text").and_then(Value::as_str),
        Some("PONG")
    );

    // Snapshot after the terminal turn: idle, exact turn facts, known result.
    let agent_id = scenario.agent_id.clone();
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "turn_id": null,
            }),
        )
        .await
        .expect("snapshot");
    assert_eq!(snapshot.get("state").and_then(Value::as_str), Some("idle"));
    assert_eq!(
        snapshot["last_turn"].get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert_eq!(
        snapshot["last_turn"]
            .get("final_text")
            .and_then(Value::as_str),
        Some("PONG")
    );
    assert_eq!(
        snapshot.get("result_known").and_then(Value::as_bool),
        Some(true)
    );

    let committed = stop_agent(&mut scenario, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);

    // Exactly one prompt reached the server; stop closed the session
    // (DELETE) and proved its absence (GET 404).
    let requests = scenario.requests();
    let prompts = requests
        .iter()
        .filter(|entry| {
            entry.get("method").and_then(Value::as_str) == Some("POST")
                && entry
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|path| path.ends_with("/prompt"))
        })
        .count();
    assert_eq!(prompts, 1);
    let mut saw_delete = false;
    let mut saw_get_after_delete = false;
    for entry in &requests {
        let method = entry
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method == "DELETE" && path.starts_with("/api/session/") {
            saw_delete = true;
        }
        if saw_delete && method == "GET" && path.starts_with("/api/session/") {
            saw_get_after_delete = true;
        }
    }
    assert!(saw_delete, "stop issues the native close");
    assert!(saw_get_after_delete, "stop proves absence after the close");

    let shutdown = scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    assert_eq!(
        shutdown.get("state").and_then(Value::as_str),
        Some("shutdown")
    );
    scenario.host.shutdown_child().await;
}

async fn scenario_failed_turn_reports_native_error() {
    let mut scenario = Scenario::start(
        &script(json!({
            "terminal": "session.execution.failed",
            "assistant_text": null,
            "error": {"type": "provider.no-route", "message": "Model unavailable: x/y"},
        })),
        opencode_profile(),
    )
    .await;
    let (send_id, turn_id, committed) = send_turn(&mut scenario, "doomed").await;
    // No assistant text: no message.completed, and the failed turn carries
    // the exact native error.
    assert_eq!(kinds(&committed), ["turn.started", "turn.completed"]);
    assert_eq!(
        committed[1].1.get("command_id").and_then(Value::as_str),
        Some(send_id.as_str())
    );
    assert_eq!(
        committed[1].1.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert_eq!(
        committed[1].1.get("status").and_then(Value::as_str),
        Some("failed")
    );
    assert_eq!(
        committed[1].1["native_error"]
            .get("message")
            .and_then(Value::as_str),
        Some("Model unavailable: x/y")
    );
    assert!(
        committed[1].1.get("final_text").is_none(),
        "no text was produced: {}",
        committed[1].1
    );
    let committed = stop_agent(&mut scenario, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.call("shutdown", json!({})).await.unwrap();
    scenario.host.shutdown_child().await;
}

async fn scenario_model_mismatch_is_rejected_before_ready() {
    let script = script(json!({
        "create_model": {"id": "other-model", "providerID": "other-provider"},
    }));
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = Scenario::setup_start(&script, opencode_profile(), agent_id, 1).await;
    let mut failed = pending.finish_expecting_error(rx, "profile_error").await;
    failed.retry("profile_error").await;
}

async fn scenario_location_mismatch_is_rejected_before_ready() {
    let other = std::env::temp_dir()
        .join("eliot-opencode-plugin-test-elsewhere")
        .to_string_lossy()
        .into_owned();
    let script = script(json!({ "create_dir": other }));
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = Scenario::setup_start(&script, opencode_profile(), agent_id, 1).await;
    let mut failed = pending.finish_expecting_error(rx, "profile_error").await;
    failed.retry("profile_error").await;
}

async fn scenario_prompt_400_is_rejected_without_rewrite() {
    let mut scenario =
        Scenario::start(&script(json!({ "prompt_status": 400 })), opencode_profile()).await;
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let params = json!({
        "agent_id": agent_id,
        "generation": 1,
        "command_id": send_id,
        "verb": "send",
        "args": {"text": "refused"},
        "turn_id": null,
        "request_id": null,
    });
    for _ in 0..2 {
        let (outcome, committed) = scenario
            .host
            .pump_call("agent.command", params.clone(), OP_TIMEOUT)
            .await;
        let rejected = outcome.expect("rejected prompt answers");
        assert_eq!(
            rejected.get("state").and_then(Value::as_str),
            Some("rejected")
        );
        assert!(committed.is_empty(), "no turn opens: {committed:?}");
    }
    let prompts = scenario
        .requests()
        .iter()
        .filter(|entry| {
            entry
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|path| path.ends_with("/prompt"))
        })
        .count();
    assert_eq!(prompts, 1, "the replay sends no second write");
    let committed = stop_agent(&mut scenario, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.call("shutdown", json!({})).await.unwrap();
    scenario.host.shutdown_child().await;
}

async fn scenario_lost_prompt_reply_is_unknown_without_rewrite() {
    let mut scenario =
        Scenario::start(&script(json!({ "prompt_hang": true })), opencode_profile()).await;
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let params = json!({
        "agent_id": agent_id,
        "generation": 1,
        "command_id": send_id,
        "verb": "send",
        "args": {"text": "hanging"},
        "turn_id": null,
        "request_id": null,
    });
    let (outcome, committed) = scenario
        .host
        .pump_call("agent.command", params.clone(), HANG_TIMEOUT)
        .await;
    assert_eq!(outcome, Err("unknown".to_owned()));
    assert!(committed.is_empty());
    // The replay is unknown without new native I/O.
    let (outcome, _) = scenario
        .host
        .pump_call("agent.command", params, OP_TIMEOUT)
        .await;
    assert_eq!(outcome, Err("unknown".to_owned()));
    let prompts = scenario
        .requests()
        .iter()
        .filter(|entry| {
            entry
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|path| path.ends_with("/prompt"))
        })
        .count();
    assert_eq!(prompts, 1, "exactly one prompt write");
    // Shutdown ends the unstopped generation, so it commits session.ended.
    let (outcome, committed) = scenario
        .host
        .pump_call("shutdown", json!({}), OP_TIMEOUT)
        .await;
    assert_eq!(
        outcome
            .expect("shutdown")
            .get("state")
            .and_then(Value::as_str),
        Some("shutdown")
    );
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.shutdown_child().await;
}

async fn scenario_stop_of_gone_session_applies() {
    let mut scenario =
        Scenario::start(&script(json!({ "delete_status": 404 })), opencode_profile()).await;
    let committed = stop_agent(&mut scenario, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.call("shutdown", json!({})).await.unwrap();
    scenario.host.shutdown_child().await;
}

async fn scenario_invalid_profile_rejected_before_spawn() {
    // (cmd wrapper rejection lives in the daemon config layer, next to the
    // Muse rule it mirrors; the plugin trusts the frozen profile there.)
    let cases: Vec<(Value, Option<&str>)> = vec![
        (json!({"harness": "opencode", "model": MODEL}), None),
        (
            json!({
                "harness": "opencode",
                "model": "no-slash-model",
                "cmd": "C:/Tools/opencode.exe",
                "pass_env": [],
            }),
            None,
        ),
        (
            json!({
                "harness": "opencode",
                "model": MODEL,
                "cmd": "C:/Tools/opencode.exe",
                "pass_env": [],
            }),
            Some("missing-workspace"),
        ),
    ];
    for (profile, workspace_override) in cases {
        let agent_id = ulid::Ulid::new().to_string();
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-opencode-plugin-test-{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ));
        std::fs::create_dir_all(root.join("data")).unwrap();
        std::fs::create_dir_all(root.join("instance")).unwrap();
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        let workspace = match workspace_override {
            Some(name) => root.join(name).to_string_lossy().into_owned(),
            None => root.join("workspace").to_string_lossy().into_owned(),
        };
        let mut host = Host::spawn(&root).await;
        host.call(
            "initialize",
            json!({
                "protocol": "eliot-plugin/1",
                "credential": format!("credential-{}", ulid::Ulid::new()),
                "instance_dir": root.join("instance").to_string_lossy(),
                "config": {},
            }),
        )
        .await
        .expect("initialize");
        let outcome = host
            .call(
                "agent.start",
                json!({
                    "agent_id": agent_id,
                    "generation": 1,
                    "command_id": ulid::Ulid::new().to_string(),
                    "profile": profile,
                    "workspace": workspace,
                    "target_environment": {},
                    "note": null,
                    "source_plan": {"session": "stream", "turn": "stream", "message": "stream"},
                    "source_state": null,
                    "resume_from": null,
                    "initial": null,
                }),
            )
            .await;
        assert_eq!(outcome, Err("profile_error".to_owned()), "{profile}");
        // No server spawned: the plugin never got past validation, and no
        // per-Agent config was generated.
        assert!(
            !root.join("instance").join("config").exists(),
            "no config without a spawn"
        );
        host.call("shutdown", json!({})).await.unwrap();
        host.shutdown_child().await;
    }
}

async fn scenario_sse_loss_during_send_gaps() {
    let mut scenario = Scenario::start(
        &script(json!({ "drop_sse_on_prompt": true })),
        opencode_profile(),
    )
    .await;
    // The prompt is admitted (200) but the stream dies before any
    // association fact flows: the send settles unknown and the loss gaps
    // the source. The gap may land on either side of the response.
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": "lost"},
                "turn_id": null,
                "request_id": null,
            }),
            OP_TIMEOUT,
        )
        .await;
    assert_eq!(outcome, Err("unknown".to_owned()));
    let gap = committed
        .iter()
        .find(|(kind, _)| kind == "source.gap")
        .expect("the stream loss gaps the source");
    assert_eq!(gap.1.get("source").and_then(Value::as_str), Some("stream"));
    let reason = gap
        .1
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        reason.contains("event stream closed"),
        "loss reason names the stream: {reason}"
    );
    // Exactly one prompt write; the admitted input is never repeated.
    let prompts = scenario
        .requests()
        .iter()
        .filter(|entry| {
            entry
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|path| path.ends_with("/prompt"))
        })
        .count();
    assert_eq!(prompts, 1);
    // Shutdown ends the unstopped generation, so it commits session.ended.
    let (outcome, committed) = scenario
        .host
        .pump_call("shutdown", json!({}), OP_TIMEOUT)
        .await;
    assert_eq!(
        outcome
            .expect("shutdown")
            .get("state")
            .and_then(Value::as_str),
        Some("shutdown")
    );
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.shutdown_child().await;
}
