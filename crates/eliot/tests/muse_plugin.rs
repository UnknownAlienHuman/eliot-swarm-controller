//! Muse plugin protocol tests with a scripted fake MSP host.
//!
//! This target runs with `harness = false` (see `[[test]]` in Cargo.toml) so
//! the test binary can re-execute itself as the fake `muse serve` host: the
//! plugin spawns `cmd` with MSP arguments, and a libtest harness would corrupt
//! that stdio stream. When `ELIOT_FAKE_MSP_SCRIPT` is set, `main` runs the
//! fixture loop instead of the scenarios.
//!
//! Fixture payloads follow the installed stable MSP schema (Muse Code 1.4.0);
//! the plugin under test is the real `eliot plugin muse` binary driven over
//! the Eliot plugin protocol, with this file emulating the host side
//! (`process.put`, `events.put`, `source.gap`).

use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::sync::{Mutex, mpsc, oneshot};

const CLI: &str = env!("CARGO_BIN_EXE_eliot");
const MODEL: &str = "muse-spark-1.3-contributor";
const EFFORT: &str = "medium";
const OP_TIMEOUT: Duration = Duration::from_secs(15);

fn main() {
    if let Ok(script) = std::env::var("ELIOT_FAKE_MSP_SCRIPT") {
        fake_msp_main(&script);
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        scenario_launch_send_snapshot_stop().await;
        println!("ok - launch/send/snapshot/stop");
        scenario_two_agent_messages_snapshot_matches_stream().await;
        println!("ok - two agentMessages: snapshot matches the stream fold");
        scenario_failed_turn_reports_restriction().await;
        println!("ok - failed turn reports its restriction");
        scenario_lost_reply_is_unknown_without_rewrite().await;
        println!("ok - lost reply is unknown without a second write");
        scenario_invalid_profile_rejected_before_spawn().await;
        println!("ok - invalid profile rejected before spawn");
    });
    println!("muse_plugin: all scenarios passed");
}

// ---------------------------------------------------------------------------
// Fake MSP host fixture (runs as a child of the plugin under test)
// ---------------------------------------------------------------------------

struct FakeMsp {
    script: String,
    report_path: PathBuf,
    session_id: Option<String>,
    turn_active: bool,
    cursor_seq: u64,
    view_log: Vec<(String, String, Value)>,
    counts: HashMap<String, u64>,
}

fn fake_msp_main(script: &str) {
    let report_path = std::env::var("ELIOT_FAKE_MSP_REPORT")
        .map(PathBuf::from)
        .expect("ELIOT_FAKE_MSP_REPORT");
    let mut fake = FakeMsp {
        script: script.to_owned(),
        report_path,
        session_id: None,
        turn_active: false,
        cursor_seq: 0,
        view_log: Vec::new(),
        counts: HashMap::new(),
    };
    fake.run();
}

impl FakeMsp {
    fn run(&mut self) {
        let stdin = std::io::stdin().lock();
        let lines = BufReader::new(stdin).lines();
        let mut stdout = std::io::stdout().lock();
        for line in lines {
            let line = line.expect("fixture stdin");
            if line.trim().is_empty() {
                continue;
            }
            let frame: Value = serde_json::from_str(&line).expect("fixture request JSON");
            // The report is rewritten after every frame so a killed host still
            // leaves exact counts behind.
            if let Some(method) = frame.get("method").and_then(Value::as_str) {
                *self.counts.entry(method.to_owned()).or_insert(0) += 1;
            }
            self.write_report();
            if frame.get("id").is_none() {
                continue; // Notification (`initialized`); no reply.
            }
            if !self.handle_call(&frame, &mut stdout) {
                return;
            }
            stdout.flush().expect("fixture stdout");
            self.write_report();
        }
        self.write_report();
    }

    fn handle_call(&mut self, frame: &Value, stdout: &mut std::io::StdoutLock) -> bool {
        let id = frame.get("id").cloned().unwrap_or(Value::Null);
        let method = frame
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = frame.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "initialize" => self.reply(stdout, &id, self.initialize_result()),
            "model/list" => self.reply(stdout, &id, self.model_list_result()),
            "session/start" => {
                let session_id = format!("fake-session-{}", self.counts["session/start"]);
                self.session_id = Some(session_id.clone());
                let workspace = params
                    .get("workspaceRoot")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let model = params
                    .get("modelId")
                    .and_then(Value::as_str)
                    .unwrap_or(MODEL)
                    .to_owned();
                let cursor = self.next_cursor();
                let session = self.session_object(&session_id, "idle", None, &model, &workspace);
                self.reply(
                    stdout,
                    &id,
                    json!({
                        "session": session,
                        "viewCursor": cursor,
                    }),
                );
                self.emit(stdout, "session/started", json!({"session": session}));
            }
            "session/setReasoningEffort" => {
                let command = params.get("commandId").cloned().unwrap_or(Value::Null);
                self.reply(
                    stdout,
                    &id,
                    json!({"commandId": command, "status": "accepted"}),
                );
            }
            "turn/start" => {
                if self.script == "drop-turn-start" {
                    // Lost reply: exit without answering. The report already
                    // records this receipt.
                    std::process::exit(0);
                }
                let command = params
                    .get("commandId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let session_id = self.session_id.clone().unwrap_or_default();
                self.turn_active = true;
                self.reply(
                    stdout,
                    &id,
                    json!({
                        "commandId": command,
                        "disposition": "started",
                        "startedNewTurn": true,
                        "status": "accepted",
                        "turnId": command,
                    }),
                );
                let cursor = self.next_cursor();
                self.emit(
                    stdout,
                    "turn/started",
                    json!({
                        "commandId": command,
                        "sessionId": session_id,
                        "sourceRange": Self::source_range(),
                        "turnId": command,
                        "viewCursor": cursor,
                    }),
                );
                let cursor = self.next_cursor();
                self.emit(
                    stdout,
                    "item/started",
                    json!({
                        "item": {
                            "itemId": format!("fake-item-{}-1", command),
                            "kind": "agentMessage",
                            "revision": 1,
                            "status": "inProgress",
                            "turnId": command,
                        },
                        "sessionId": session_id,
                        "viewCursor": cursor,
                    }),
                );
                let cursor = self.next_cursor();
                let final_text = Self::final_text(&self.script);
                self.emit(
                    stdout,
                    "item/completed",
                    json!({
                        "item": {
                            "itemId": format!("fake-item-{}-1", command),
                            "kind": "agentMessage",
                            "revision": 2,
                            "status": "completed",
                            "text": final_text,
                            "turnId": command,
                        },
                        "sessionId": session_id,
                        "sourceRange": Self::source_range(),
                        "viewCursor": cursor,
                    }),
                );
                if self.script == "two-messages" {
                    // A routine multi-message turn: the stream fold keeps the
                    // LAST agentMessage, and the snapshot readback must agree.
                    let cursor = self.next_cursor();
                    self.emit(
                        stdout,
                        "item/completed",
                        json!({
                            "item": {
                                "itemId": format!("fake-item-{}-2", command),
                                "kind": "agentMessage",
                                "revision": 2,
                                "status": "completed",
                                "text": "Second reply from the fake MSP host.",
                                "turnId": command,
                            },
                            "sessionId": session_id,
                            "sourceRange": Self::source_range(),
                            "viewCursor": cursor,
                        }),
                    );
                }
                let cursor = self.next_cursor();
                if self.script == "failed-turn" {
                    self.emit(
                        stdout,
                        "turn/completed",
                        json!({
                            "error": {
                                "kind": "authRequired",
                                "message": "fake auth expired",
                                "retryable": true,
                            },
                            "sessionId": session_id,
                            "sourceRange": Self::source_range(),
                            "terminal": "failed",
                            "turnId": command,
                            "viewCursor": cursor,
                        }),
                    );
                } else {
                    self.emit(
                        stdout,
                        "turn/completed",
                        json!({
                            "sessionId": session_id,
                            "sourceRange": Self::source_range(),
                            "terminal": "completed",
                            "turnId": command,
                            "viewCursor": cursor,
                        }),
                    );
                }
                self.turn_active = false;
            }
            "session/read" => {
                let session_id = self.session_id.clone().unwrap_or_default();
                let (status, active): (&str, Option<&str>) = if self.turn_active {
                    ("running", Some("fake-active-turn"))
                } else {
                    ("idle", None)
                };
                self.reply(
                    stdout,
                    &id,
                    json!({
                        "history": {"items": [], "mode": "inline", "snapshot": null},
                        "pendingRequests": [],
                        "session": self.session_object(&session_id, status, active, MODEL, ""),
                        "viewCursor": self.head_cursor(),
                    }),
                );
            }
            "view/page" => {
                let events = self.page_events(&params);
                self.reply(
                    stdout,
                    &id,
                    json!({"events": events.0, "nextCursor": events.1}),
                );
            }
            _ => self.reply_error(stdout, &id, -32601, "Method not found"),
        }
        true
    }

    fn reply(&self, stdout: &mut std::io::StdoutLock, id: &Value, result: Value) {
        let frame = json!({"jsonrpc": "2.0", "id": id, "result": result});
        writeln!(stdout, "{}", frame).expect("fixture reply");
    }

    fn reply_error(&self, stdout: &mut std::io::StdoutLock, id: &Value, code: i64, message: &str) {
        let frame = json!({
            "jsonrpc": "2.0", "id": id,
            "error": {"code": code, "message": message},
        });
        writeln!(stdout, "{}", frame).expect("fixture error reply");
    }

    fn emit(&mut self, stdout: &mut std::io::StdoutLock, method: &str, params: Value) {
        let cursor = params
            .get("viewCursor")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if !cursor.is_empty() {
            self.view_log
                .push((cursor, method.to_owned(), params.clone()));
        }
        let frame = json!({"jsonrpc": "2.0", "method": method, "params": params});
        writeln!(stdout, "{}", frame).expect("fixture event");
    }

    fn next_cursor(&mut self) -> String {
        self.cursor_seq += 1;
        format!("fake-cursor-{}", self.cursor_seq)
    }

    fn head_cursor(&self) -> String {
        format!("fake-cursor-{}", self.cursor_seq)
    }

    fn page_events(&self, params: &Value) -> (Vec<Value>, Value) {
        let limit = params
            .get("limit")
            .and_then(Value::as_i64)
            .unwrap_or(100)
            .clamp(1, 1000) as usize;
        let backward = params.get("direction").and_then(Value::as_str) == Some("backward");
        let cursor = params.get("cursor").and_then(Value::as_str);
        let mut events: Vec<(String, String, Value)> = self.view_log.clone();
        if backward {
            events.reverse();
            if let Some(cursor) = cursor {
                if let Some(position) = events.iter().position(|(at, _, _)| at == cursor) {
                    events = events[position + 1..].to_vec();
                } else {
                    events.clear();
                }
            }
            let mut page: Vec<(String, String, Value)> = events.into_iter().take(limit).collect();
            let next = page.last().map(|(at, _, _)| Value::String(at.clone()));
            // Pages are always ascending by cursor, in both directions.
            page.reverse();
            (
                page.into_iter()
                    .map(|(_, method, params)| json!({"method": method, "params": params}))
                    .collect(),
                next.unwrap_or(Value::Null),
            )
        } else {
            if let Some(cursor) = cursor {
                if let Some(position) = events.iter().position(|(at, _, _)| at == cursor) {
                    events = events[position + 1..].to_vec();
                } else {
                    events.clear();
                }
            }
            let page: Vec<(String, String, Value)> = events.into_iter().take(limit).collect();
            let next = page.last().map(|(at, _, _)| Value::String(at.clone()));
            (
                page.into_iter()
                    .map(|(_, method, params)| json!({"method": method, "params": params}))
                    .collect(),
                next.unwrap_or(Value::Null),
            )
        }
    }

    fn initialize_result(&self) -> Value {
        json!({
            "experimentalApi": false,
            "grantedCapabilities": [],
            "museHome": "C:\\fake-muse-home",
            "platformFamily": "windows",
            "platformOs": "windows",
            "schema": {
                "fingerprint": "sha256:36466f634c8c78a812462ec941187fd4547b232ee06153e5feb2a1482f0d3d7f",
                "version": 1,
            },
            "serverInfo": {"name": "fake-msp", "version": "1.4.0-R4161.1"},
            "userAgent": "fake-msp/1.4.0",
        })
    }

    fn model_list_result(&self) -> Value {
        json!({
            "models": [{
                "contextLimit": 200000,
                "cost": {"input": 0.0, "output": 0.0},
                "description": "Fake catalogue row shaped like the installed schema.",
                "displayLabel": "Muse Spark Contributor",
                "isActive": true,
                "isDefault": true,
                "modelId": MODEL,
                "outputLimit": 64000,
                "profileId": "test-profile",
                "providerId": "meta",
                "releaseDate": "2026-01-01",
                "variants": ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"],
            }],
            "profileId": "test-profile",
            "providerId": "meta",
            "source": "test",
        })
    }

    fn session_object(
        &self,
        session_id: &str,
        status: &str,
        active_turn: Option<&str>,
        model: &str,
        workspace: &str,
    ) -> Value {
        json!({
            "activeTurnId": active_turn,
            "createdAt": "2026-11-03T00:00:00Z",
            "forkedFrom": null,
            "modelId": model,
            "path": "C:\\fake-muse-home\\sessions\\fake.log",
            "providerId": "meta",
            "sessionId": session_id,
            "status": status,
            "turnCount": 0,
            "updatedAt": "2026-11-03T00:00:00Z",
            "workspaceRoot": if workspace.is_empty() { Value::Null } else { Value::String(workspace.to_owned()) },
        })
    }

    fn source_range() -> Value {
        json!({
            "first": {"streamSeq": 1},
            "last": {"streamSeq": 1},
            "stream": {"kind": "run"},
        })
    }

    fn final_text(script: &str) -> &'static str {
        match script {
            "failed-turn" => "partial reply before the failure.",
            _ => "Hello from the fake MSP host.",
        }
    }

    fn write_report(&self) {
        let report = json!({
            "script": self.script,
            "counts": self.counts,
            "session_starts": self.counts.get("session/start").copied().unwrap_or(0),
            "turn_starts": self.counts.get("turn/start").copied().unwrap_or(0),
        });
        let _ = std::fs::write(&self.report_path, serde_json::to_string(&report).unwrap());
    }
}

// ---------------------------------------------------------------------------
// Host-side emulator: drives `eliot plugin muse` over the plugin protocol
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
            .arg("muse")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            command.creation_flags(0x0800_0000);
        }
        // The plugin child must never mistake itself for the fixture.
        command.env_remove("ELIOT_FAKE_MSP_SCRIPT");
        let mut child = command.spawn().expect("spawn plugin muse");
        let stdin = child.stdin.take().expect("plugin stdin");
        let stdout = child.stdout.take().expect("plugin stdout");
        let stderr = child.stderr.take().expect("plugin stderr");
        let stderr_task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
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

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let rx = self.begin_call(method, params).await;
        tokio::time::timeout(OP_TIMEOUT, rx)
            .await
            .expect("host call deadline")
            .expect("host response channel")
    }

    /// Send a call and return its owned response receiver, so pumps can poll
    /// inbound host requests while the call is in flight.
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

    async fn next_request(&mut self) -> HostRequest {
        tokio::time::timeout(OP_TIMEOUT, self.inbound.recv())
            .await
            .expect("host request deadline")
            .expect("plugin request channel")
    }

    /// Answer one `events.put`, returning its committed events.
    async fn answer_events_put(
        &mut self,
        agent_id: &str,
        generation: i64,
        expected_kind: &str,
    ) -> Vec<Value> {
        let request = self.next_request().await;
        assert_eq!(
            request.method, "events.put",
            "expected {expected_kind}, plugin asked {}",
            request.method
        );
        assert_eq!(
            request.params.get("agent_id").and_then(Value::as_str),
            Some(agent_id)
        );
        assert_eq!(
            request.params.get("generation").and_then(Value::as_i64),
            Some(generation)
        );
        assert_eq!(
            request.params.get("source").and_then(Value::as_str),
            Some("stream")
        );
        let events = request
            .params
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert_eq!(events.len(), 1, "one native record per batch");
        assert_eq!(
            events[0].get("kind").and_then(Value::as_str),
            Some(expected_kind)
        );
        let first = self.next_seq;
        self.next_seq += events.len() as i64;
        self.respond(
            &request.id,
            json!({
                "first_seq": first,
                "last_seq": first + events.len() as i64 - 1,
                "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
            }),
        )
        .await;
        events
    }

    async fn shutdown_child(&mut self) -> String {
        // Like the daemon, close the plugin's stdin after shutdown: the
        // plugin's reader teardown completes on EOF.
        drop(self.writes.take());
        let status = tokio::time::timeout(OP_TIMEOUT, self.child.wait())
            .await
            .expect("plugin exit deadline")
            .expect("plugin wait");
        assert!(status.success(), "plugin exit status: {status}");
        self.stderr.take().unwrap().await.unwrap_or_default()
    }
}

impl Scenario {
    async fn start(script: &str, profile: Value) -> Self {
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-muse-plugin-test-{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ));
        std::fs::create_dir_all(root.join("data")).unwrap();
        std::fs::create_dir_all(root.join("instance")).unwrap();
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        let agent_id = ulid::Ulid::new().to_string();
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
        assert_eq!(manifest.get("name").and_then(Value::as_str), Some("muse"));
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
        let rx = host
            .begin_call(
                "agent.start",
                json!({
                    "agent_id": agent_id,
                    "generation": 1,
                    "command_id": start_id,
                    "profile": full_profile,
                    "workspace": root.join("workspace").to_string_lossy(),
                    "target_environment": {
                        "ELIOT_FAKE_MSP_SCRIPT": script,
                        "ELIOT_FAKE_MSP_REPORT": root.join("report.json").to_string_lossy(),
                    },
                    "note": null,
                    "source_plan": {"session": "stream", "turn": "stream", "message": "stream"},
                    "source_state": null,
                    "resume_from": null,
                    "initial": null,
                }),
            )
            .await;
        let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
        // `agent.start` only resolves after the harness child registers and
        // the session.ready batch commits; answer both while it is in flight.
        let mut ready_data: Option<Value> = None;
        let ready = loop {
            tokio::select! {
                biased;
                request = host.next_request() => {
                    match request.method.as_str() {
                        "process.put" => {
                            let pid = request.params.get("pid").and_then(Value::as_u64).unwrap_or(0);
                            assert!(pid > 0);
                            assert_eq!(request.params.get("role").and_then(Value::as_str), Some("harness"));
                            host.respond(&request.id, json!({
                                "identity": {"host_boot_id": "test-boot", "pid": pid, "start_time": 1},
                                "owner": format!("agent:{agent_id}#1"),
                                "role": "harness",
                                "state": "running",
                                "exit_code": null,
                                "updated_at": 1,
                            })).await;
                        }
                        "events.put" => {
                            let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                            assert_eq!(events.len(), 1, "one native record per batch");
                            assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("session.ready"));
                            ready_data = Some(events[0].get("data").cloned().unwrap_or(Value::Null));
                            let first = host.next_seq;
                            host.next_seq += 1;
                            host.respond(&request.id, json!({
                                "first_seq": first, "last_seq": first,
                                "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                            })).await;
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
        assert_eq!(ready.get("state").and_then(Value::as_str), Some("ready"));
        assert_eq!(
            ready.get("agent_id").and_then(Value::as_str),
            Some(agent_id.as_str())
        );
        assert_eq!(ready.get("generation").and_then(Value::as_i64), Some(1));
        assert_eq!(
            ready.get("command_id").and_then(Value::as_str),
            Some(start_id.as_str())
        );
        let ready_data = ready_data.expect("session.ready batch");
        let native_scope = ready_data
            .get("native_scope")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            native_scope.starts_with("muse-home:"),
            "native scope derives from the host home: {native_scope}"
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
            ready["requested"].get("effort").and_then(Value::as_str),
            Some(EFFORT)
        );
        assert_eq!(
            ready["effective"].get("model").and_then(Value::as_str),
            Some(MODEL)
        );
        assert_eq!(
            ready["effective"].get("effort").and_then(Value::as_str),
            Some(EFFORT)
        );
        assert_eq!(
            ready["effective"]
                .get("effort_verified")
                .and_then(Value::as_bool),
            Some(true)
        );
        Self {
            root,
            agent_id,
            host,
        }
    }

    fn report(&self) -> Value {
        let text = std::fs::read_to_string(self.root.join("report.json")).expect("fake MSP report");
        serde_json::from_str(&text).expect("fake MSP report JSON")
    }
}

fn muse_profile() -> Value {
    json!({
        "harness": "muse",
        "model": MODEL,
        "effort": EFFORT,
        "permissions": "workspace-write",
        "approval_policy": "never",
        "pass_env": ["ELIOT_FAKE_MSP_SCRIPT", "ELIOT_FAKE_MSP_REPORT"],
    })
}

/// One send round trip. The plugin only answers `agent.command` after its
/// `turn.started` batch commits, so inbound batches are acked while the call
/// is in flight; the remaining turn facts are drained afterwards. Returns the
/// send id, the applied turn id and every committed `(kind, data)` in order.
async fn send_turn(
    host: &mut Host,
    agent_id: &str,
    text: &str,
) -> (String, String, Vec<(String, Value)>) {
    let send_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
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
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut committed: Vec<(String, Value)> = Vec::new();
    let applied = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                committed.push((
                    events[0].get("kind").and_then(Value::as_str).unwrap_or_default().to_owned(),
                    events[0].get("data").cloned().unwrap_or(Value::Null),
                ));
                let first = host.next_seq;
                host.next_seq += 1;
                host.respond(&request.id, json!({
                    "first_seq": first, "last_seq": first,
                    "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                })).await;
            }
            result = &mut rx => break result,
        }
    };
    let applied = applied
        .expect("send deadline")
        .expect("send response channel")
        .expect("send applied");
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
    // The ack follows `turn.started`; later facts may already be committed or
    // still in flight. Drain the canonical turn sequence in commit order.
    while committed.last().map(|(kind, _)| kind.as_str()) != Some("turn.completed") {
        let expected = match committed.last().map(|(kind, _)| kind.as_str()) {
            Some("turn.started") => "message.completed",
            Some("message.completed") => "turn.completed",
            other => panic!("turn facts out of order after {other:?}"),
        };
        let events = host.answer_events_put(agent_id, 1, expected).await;
        committed.push((
            expected.to_owned(),
            events[0].get("data").cloned().unwrap_or(Value::Null),
        ));
    }
    for (kind, data) in &committed {
        assert_eq!(
            data.get("turn_id").and_then(Value::as_str),
            Some(turn_id.as_str()),
            "turn binding of {kind}"
        );
        assert_eq!(
            data.get("command_id").and_then(Value::as_str),
            Some(send_id.as_str()),
            "command binding of {kind}"
        );
    }
    (send_id, turn_id, committed)
}

/// Like `send_turn`, but the turn may commit any number of `message.completed`
/// batches (routine once tools are involved) before `turn.completed`.
async fn send_multi_message_turn(
    host: &mut Host,
    agent_id: &str,
    text: &str,
) -> (String, String, Vec<(String, Value)>) {
    let send_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
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
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut committed: Vec<(String, Value)> = Vec::new();
    let applied = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                committed.push((
                    events[0].get("kind").and_then(Value::as_str).unwrap_or_default().to_owned(),
                    events[0].get("data").cloned().unwrap_or(Value::Null),
                ));
                let first = host.next_seq;
                host.next_seq += 1;
                host.respond(&request.id, json!({
                    "first_seq": first, "last_seq": first,
                    "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                })).await;
            }
            result = &mut rx => break result,
        }
    };
    let applied = applied
        .expect("send deadline")
        .expect("send response channel")
        .expect("send applied");
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
    // Drain every message fact in commit order; only `turn.completed` ends it.
    assert_eq!(
        committed.first().map(|(kind, _)| kind.as_str()),
        Some("turn.started")
    );
    while committed.last().map(|(kind, _)| kind.as_str()) != Some("turn.completed") {
        let request = host.next_request().await;
        assert_eq!(request.method, "events.put");
        let events = request
            .params
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert_eq!(events.len(), 1, "one native record per batch");
        let kind = events[0]
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        assert!(
            kind == "message.completed" || kind == "turn.completed",
            "turn facts out of order after {:?}",
            committed.last().map(|(kind, _)| kind.as_str())
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
        committed.push((kind, events[0].get("data").cloned().unwrap_or(Value::Null)));
    }
    for (kind, data) in &committed {
        assert_eq!(
            data.get("turn_id").and_then(Value::as_str),
            Some(turn_id.as_str()),
            "turn binding of {kind}"
        );
        assert_eq!(
            data.get("command_id").and_then(Value::as_str),
            Some(send_id.as_str()),
            "command binding of {kind}"
        );
    }
    (send_id, turn_id, committed)
}

/// One stop round trip with the same in-flight acknowledgement. Returns the
/// committed `(kind, data)` batches; gaps are acknowledged and recorded too.
async fn stop_agent(host: &mut Host, agent_id: &str, force: bool) -> Vec<(String, Value)> {
    let stop_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.stop",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": stop_id,
                "force": force,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut committed: Vec<(String, Value)> = Vec::new();
    let stopped = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                match request.method.as_str() {
                    "events.put" => {
                        let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                        assert_eq!(events.len(), 1, "one native record per batch");
                        committed.push((
                            events[0].get("kind").and_then(Value::as_str).unwrap_or_default().to_owned(),
                            events[0].get("data").cloned().unwrap_or(Value::Null),
                        ));
                        let first = host.next_seq;
                        host.next_seq += 1;
                        host.respond(&request.id, json!({
                            "first_seq": first, "last_seq": first,
                            "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                        })).await;
                    }
                    "source.gap" => {
                        let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                        let seq = host.next_seq;
                        host.next_seq += 1;
                        host.respond(&request.id, json!({"gap_id": gap_id, "seq": seq})).await;
                        committed.push(("source.gap".to_owned(), request.params));
                    }
                    other => panic!("unexpected host request during stop: {other}"),
                }
            }
            result = &mut rx => break result,
        }
    };
    let stopped = stopped
        .expect("stop deadline")
        .expect("stop response channel")
        .expect("stop applied");
    assert_eq!(
        stopped.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert!(
        committed.iter().any(|(kind, _)| kind == "session.ended"),
        "stop commits session.ended: {committed:?}"
    );
    committed
}

async fn scenario_launch_send_snapshot_stop() {
    let mut scenario = Scenario::start("happy", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (send_id, turn_id, committed) =
        send_turn(&mut scenario.host, &agent_id, "Say hello.").await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["turn.started", "message.completed", "turn.completed"]
    );
    assert_eq!(
        committed[1].1.get("text").and_then(Value::as_str),
        Some("Hello from the fake MSP host.")
    );
    assert_eq!(
        committed[2].1.get("status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        committed[2].1.get("final_text").and_then(Value::as_str),
        Some("Hello from the fake MSP host.")
    );

    // Native readback proves the exact terminal turn through session/read and
    // the durable view records.
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "turn_id": Value::Null,
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
            .get("terminal")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snapshot["last_turn"]
            .get("final_text")
            .and_then(Value::as_str),
        Some("Hello from the fake MSP host.")
    );
    assert_eq!(
        snapshot.get("result_known").and_then(Value::as_bool),
        Some(true)
    );

    // A second turn on the same session works after the first completes.
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, "Again.").await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["turn.started", "message.completed", "turn.completed"]
    );

    let committed = stop_agent(&mut scenario.host, &agent_id, false).await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["session.ended"]
    );
    let shutdown = scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    assert_eq!(
        shutdown.get("state").and_then(Value::as_str),
        Some("shutdown")
    );
    let stderr = scenario.host.shutdown_child().await;
    assert!(!stderr.contains("panicked"), "plugin stderr: {stderr}");

    let report = scenario.report();
    assert_eq!(report["session_starts"], json!(1));
    assert_eq!(report["turn_starts"], json!(2));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_two_agent_messages_snapshot_matches_stream() {
    let mut scenario = Scenario::start("two-messages", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (send_id, turn_id, committed) =
        send_multi_message_turn(&mut scenario.host, &agent_id, "Say it twice.").await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        [
            "turn.started",
            "message.completed",
            "message.completed",
            "turn.completed"
        ]
    );
    assert_eq!(
        committed[1].1.get("text").and_then(Value::as_str),
        Some("Hello from the fake MSP host.")
    );
    assert_eq!(
        committed[2].1.get("text").and_then(Value::as_str),
        Some("Second reply from the fake MSP host.")
    );
    // The stream fold keeps the LAST agentMessage ...
    assert_eq!(
        committed[3].1.get("status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        committed[3].1.get("final_text").and_then(Value::as_str),
        Some("Second reply from the fake MSP host.")
    );

    // ... and the snapshot readback over the durable view must agree with it.
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "turn_id": Value::Null,
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
            .get("terminal")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snapshot["last_turn"]
            .get("final_text")
            .and_then(Value::as_str),
        Some("Second reply from the fake MSP host.")
    );
    assert_eq!(
        snapshot.get("result_known").and_then(Value::as_bool),
        Some(true)
    );

    stop_agent(&mut scenario.host, &agent_id, false).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    let stderr = scenario.host.shutdown_child().await;
    assert!(!stderr.contains("panicked"), "plugin stderr: {stderr}");
    assert_eq!(scenario.report()["turn_starts"], json!(1));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_failed_turn_reports_restriction() {
    let mut scenario = Scenario::start("failed-turn", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, "Fail please.").await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["turn.started", "message.completed", "turn.completed"]
    );
    let data = &committed[2].1;
    assert_eq!(data.get("status").and_then(Value::as_str), Some("failed"));
    assert_eq!(
        data["native_error"].get("message").and_then(Value::as_str),
        Some("fake auth expired")
    );
    assert_eq!(
        data["provider_restriction"]
            .get("kind")
            .and_then(Value::as_str),
        Some("auth")
    );
    assert_eq!(
        data.get("final_text").and_then(Value::as_str),
        Some("partial reply before the failure.")
    );
    stop_agent(&mut scenario.host, &agent_id, true).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    assert_eq!(scenario.report()["turn_starts"], json!(1));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_lost_reply_is_unknown_without_rewrite() {
    let mut scenario = Scenario::start("drop-turn-start", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();
    let send_id = ulid::Ulid::new().to_string();
    // The fixture exits without answering turn/start: the outcome is unknown,
    // and the input is never written again.
    let rx = scenario
        .host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": "Into the void."},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let outcome = loop {
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                // The dead host may surface a stream gap first; acknowledge it
                // and keep waiting for the send outcome.
                assert_eq!(request.method, "source.gap");
                let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                let seq = scenario.host.next_seq;
                scenario.host.next_seq += 1;
                scenario.host.respond(&request.id, json!({"gap_id": gap_id, "seq": seq})).await;
            }
            result = &mut rx => break result,
        }
    };
    let outcome = outcome
        .expect("send deadline")
        .expect("send response channel");
    assert_eq!(outcome, Err("unknown".to_owned()));
    // A retained retry of the same command stays unknown without new native I/O.
    let retry = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": "Into the void."},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    assert_eq!(retry, Err("unknown".to_owned()));
    assert_eq!(scenario.report()["turn_starts"], json!(1));

    // The dead host's stream gap may arrive before, during or after the
    // stop; stop_agent acknowledges it and waits for the ended batch.
    stop_agent(&mut scenario.host, &agent_id, true).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    assert_eq!(scenario.report()["turn_starts"], json!(1));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_invalid_profile_rejected_before_spawn() {
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-muse-plugin-test-{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::create_dir_all(root.join("instance")).unwrap();
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let agent_id = ulid::Ulid::new().to_string();
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
    let start_id = ulid::Ulid::new().to_string();
    let outcome = host
        .call(
            "agent.start",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": start_id,
                "profile": {"harness": "muse"},
                "workspace": root.join("workspace").to_string_lossy(),
                "target_environment": {},
                "note": null,
                "source_plan": {"session": "stream", "turn": "stream", "message": "stream"},
                "source_state": null,
                "resume_from": null,
                "initial": null,
            }),
        )
        .await;
    assert_eq!(outcome, Err("profile_error".to_owned()));
    host.call("shutdown", json!({})).await.expect("shutdown");
    host.shutdown_child().await;
    let report = root.join("report.json");
    assert!(
        !report.exists(),
        "no host may spawn before profile validation"
    );
    let _ = std::fs::remove_dir_all(&root);
}
