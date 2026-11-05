//! Antigravity plugin protocol tests with a scripted fake AGY host.
//!
//! This target runs with `harness = false` so the test binary can re-execute
//! itself as the fake `agy` executable: the plugin spawns `cmd` with AGY
//! stream-json arguments, and a libtest harness would corrupt that stdio
//! stream. When `ELIOT_FAKE_AGY_SCRIPT` is set, `main` runs the fixture loop
//! instead of the scenarios.
//!
//! Fixture payloads follow the recorded AGY 1.5.0 shapes
//! (`plugins/antigravity/tests/fixtures/agy-1.5.0-soft-denial.ndjson`):
//! native input is `{event:user,message:{role:user,content:TEXT}}`, output is
//! `event:init/step_update/result`, `result.response` is final, cumulative
//! usage replaces, and there is no native turn id so only one input is
//! outstanding. No approval/interrupt semantics are invented. The fixture also
//! emits captured `system_message` and `checkpoint` bookkeeping steps with their
//! recorded structural fields only; the plugin treats them as known metadata
//! that neither associate input nor produce text or a terminal.

use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::sync::{Mutex, mpsc, oneshot};

const CLI: &str = env!("CARGO_BIN_EXE_swarm");
const MODEL: &str = "gemini-3.8-flash-high";
const OP_TIMEOUT: Duration = Duration::from_secs(15);
const FRESH_CONV: &str = "00000000-0000-4000-8000-000000000001";

fn main() {
    if let Ok(script) = std::env::var("ELIOT_FAKE_AGY_SCRIPT") {
        fake_agy_main(&script);
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        scenario_launch_two_sends().await;
        println!("ok - launch/two sends tied to exact final_text, init first");
        scenario_stop_resume_no_replay().await;
        println!("ok - EOF stop and resume without replaying old prompt");
        scenario_lost_result_unknown_no_rewrite().await;
        println!("ok - lost result is unknown without a second write");
        scenario_checkpoint_frame_no_gap().await;
        println!("ok - captured checkpoint among actual turn frames, valid final result, no gap");
        scenario_unknown_step_causes_gap().await;
        println!("ok - unknown step type still produces explicit gap");
        scenario_command_history_tracking().await;
        println!(
            "ok - command history tracking: monotonic seq, constant memory, conflicts and eviction"
        );
    });
    println!("antigravity_plugin: all scenarios passed");
}

// ---------------------------------------------------------------------------
// Fake AGY fixture (runs as a child of the plugin under test)
// ---------------------------------------------------------------------------

fn argv_value(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.windows(2).find_map(|pair| {
        if pair[0] == name {
            Some(pair[1].clone())
        } else {
            None
        }
    })
}

fn write_fake_report(
    path: &Path,
    script: &str,
    conv: &str,
    model: &str,
    inputs: &[String],
    sys: u64,
) {
    let report = json!({
        "script": script,
        "conversation": conv,
        "model": model,
        "inputs": inputs,
        "input_count": inputs.len(),
        "system_messages": sys,
    });
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, serde_json::to_string(&report).unwrap()).is_err() {
        return;
    }
    let _ = std::fs::rename(&tmp, path);
}

fn fake_agy_main(script: &str) {
    let report_path = std::env::var("ELIOT_FAKE_AGY_REPORT")
        .map(PathBuf::from)
        .expect("ELIOT_FAKE_AGY_REPORT");
    let workspace = std::env::var("ELIOT_FAKE_AGY_WORKSPACE").expect("ELIOT_FAKE_AGY_WORKSPACE");
    let resume_arg = argv_value("--conversation");
    let resumed = resume_arg.is_some();
    let conv = resume_arg.unwrap_or_else(|| FRESH_CONV.to_owned());
    let model = argv_value("--model").unwrap_or_else(|| MODEL.to_owned());
    let mut stdout = std::io::stdout().lock();
    // Init is emitted before any input is read, mirroring the recording.
    let init = json!({"event":"init","conversation_id":conv,
        "init":{"model":model,"cwd":workspace,"permission_mode":"request-review"}});
    writeln!(stdout, "{}", init).expect("fixture init");
    stdout.flush().expect("fixture stdout");
    let mut inputs: Vec<String> = Vec::new();
    let mut step_index: u64 = 0;
    let mut sys_count: u64 = 0;
    // Captured idle bookkeeping while pre-input: structural fields only, no body.
    let idle = json!({"event":"step_update","step_update":
        {"conversation_id":conv,"step_index":step_index,
         "state":"DONE","step_type":"system_message"}});
    step_index += 1;
    sys_count += 1;
    writeln!(stdout, "{}", idle).expect("fixture step");
    stdout.flush().expect("fixture stdout");
    write_fake_report(&report_path, script, &conv, &model, &inputs, sys_count);
    let stdin = std::io::stdin().lock();
    for line in BufReader::new(stdin).lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let frame: Value = serde_json::from_str(&line).expect("fixture input JSON");
        assert_eq!(frame.get("event").and_then(Value::as_str), Some("user"));
        let text = frame["message"]["content"].as_str().expect("user content");
        assert_eq!(frame["message"]["role"].as_str(), Some("user"));
        inputs.push(text.to_owned());
        write_fake_report(&report_path, script, &conv, &model, &inputs, sys_count);
        if script == "lost-result" && inputs.len() == 1 {
            let step = json!({"event":"step_update","step_update":
                {"conversation_id":conv,"step_index":step_index,
                 "state":"DONE","step_type":"user_input"}});
            writeln!(stdout, "{}", step).expect("fixture step");
            stdout.flush().expect("fixture stdout");
            std::process::exit(0);
        }
        let n = inputs.len() as u64;
        for step_type in ["user_input", "agent_response"] {
            let step = json!({"event":"step_update","step_update":
                {"conversation_id":conv,"step_index":step_index,
                 "state":"DONE","step_type":step_type}});
            step_index += 1;
            writeln!(stdout, "{}", step).expect("fixture step");
        }
        if script == "checkpoint" {
            // Captured native checkpoint record among actual turn frames.
            let chk = json!({"event":"step_update","step_update":
                {"conversation_id":conv,"step_index":step_index,
                 "state":"DONE","step_type":"checkpoint"}});
            step_index += 1;
            writeln!(stdout, "{}", chk).expect("fixture step");
        }
        if script == "unknown-step" {
            // Unknown step type frame among turn frames.
            let unk = json!({"event":"step_update","step_update":
                {"conversation_id":conv,"step_index":step_index,
                 "state":"DONE","step_type":"unknown_future_step"}});
            step_index += 1;
            writeln!(stdout, "{}", unk).expect("fixture step");
        }
        if resumed {
            // Captured bookkeeping between input and result on a resumed turn.
            let meta = json!({"event":"step_update","step_update":
                {"conversation_id":conv,"step_index":step_index,
                 "state":"DONE","step_type":"system_message"}});
            step_index += 1;
            sys_count += 1;
            writeln!(stdout, "{}", meta).expect("fixture step");
        }
        // Cumulative usage replaces; the plugin must not sum across turns.
        let result = json!({"event":"result","result":{"conversation_id":conv,
            "status":"SUCCESS","response":format!("reply:{text}"),
            "finished_reason":"completed","num_turns":1,
            "usage":{"input_tokens":100*n,"output_tokens":10*n,
                "thinking_tokens":0,"cache_read_tokens":0,"total_tokens":110*n},
            "denied_actions":[]}});
        writeln!(stdout, "{}", result).expect("fixture result");
        stdout.flush().expect("fixture stdout");
        write_fake_report(&report_path, script, &conv, &model, &inputs, sys_count);
    }
}

// ---------------------------------------------------------------------------
// Host-side emulator: drives `swarm plugin antigravity` over the plugin wire
// ---------------------------------------------------------------------------

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;
type CallRx =
    Pin<Box<dyn Future<Output = Result<Result<Value, String>, oneshot::error::RecvError>> + Send>>;

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
    start_result: Value,
}

struct PendingStart {
    root: PathBuf,
    agent_id: String,
    host: Host,
    generation: i64,
}

impl Host {
    async fn spawn(root: &Path) -> Self {
        let mut command = tokio::process::Command::new(CLI);
        command
            .arg("--data-dir")
            .arg(root.join("data"))
            .arg("plugin")
            .arg("antigravity")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            command.creation_flags(0x0800_0000);
        }
        command.env_remove("ELIOT_FAKE_AGY_SCRIPT");
        let mut child = command.spawn().expect("spawn plugin antigravity");
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
                let result = match frame.get("result") {
                    Some(result) => Ok(result.clone()),
                    None => Err(frame
                        .get("error")
                        .and_then(|error| error.get("data"))
                        .and_then(|data| data.get("code"))
                        .and_then(Value::as_str)
                        .unwrap_or("rpc_error")
                        .to_owned()),
                };
                let _ = sender.send(result);
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

    async fn begin_call(&mut self, method: &str, params: Value) -> CallRx {
        self.next_id += 1;
        let id = format!("host-call-{}", self.next_id);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);
        let frame = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.writer()
            .send(frame.to_string())
            .await
            .expect("host write");
        Box::pin(rx)
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let rx = self.begin_call(method, params).await;
        tokio::time::timeout(OP_TIMEOUT, rx)
            .await
            .expect("host call deadline")
            .expect("host response channel")
    }

    async fn respond(&self, id: &Value, result: Value) {
        let frame = json!({"jsonrpc": "2.0", "id": id, "result": result});
        self.writer()
            .send(frame.to_string())
            .await
            .expect("host write");
    }

    async fn next_request(&mut self) -> HostRequest {
        tokio::time::timeout(OP_TIMEOUT, self.inbound.recv())
            .await
            .expect("host request deadline")
            .expect("plugin request channel")
    }

    async fn answer_events_put(&mut self, expected_kind: &str) -> Vec<Value> {
        let request = self.next_request().await;
        assert_eq!(request.method, "events.put");
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
        self.next_seq += 1;
        self.respond(
            &request.id,
            json!({
                "first_seq": first, "last_seq": first,
                "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
            }),
        )
        .await;
        events
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

async fn answer_source_gap(host: &mut Host, request: &HostRequest) -> String {
    let reason = request
        .params
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let gap_id = request
        .params
        .get("gap_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let seq = host.next_seq;
    host.next_seq += 1;
    host.respond(&request.id, json!({"gap_id": gap_id, "seq": seq}))
        .await;
    reason
}

fn agy_profile() -> Value {
    json!({
        "harness": "antigravity",
        "model": MODEL,
        "pass_env": ["ELIOT_FAKE_AGY_SCRIPT", "ELIOT_FAKE_AGY_REPORT", "ELIOT_FAKE_AGY_WORKSPACE"],
    })
}

fn fake_report(root: &Path) -> Value {
    let text = std::fs::read_to_string(root.join("report.json")).expect("fake AGY report");
    serde_json::from_str(&text).expect("fake AGY report JSON")
}

async fn setup_start(
    script: &str,
    agent_id: String,
    generation: i64,
    resume_from: Value,
    source_state: Value,
) -> (PendingStart, CallRx) {
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-agy-plugin-test-{}",
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
        Some("antigravity")
    );
    assert_eq!(
        manifest["verbs"],
        json!(["launch", "resume", "send", "stop"])
    );
    assert_eq!(
        manifest["sources"]["controlled"],
        json!({"session": "stream", "turn": "stream", "message": "stream", "tool": "stream"})
    );
    let start_id = ulid::Ulid::new().to_string();
    let self_exe = std::env::current_exe().expect("test executable");
    let mut profile = agy_profile();
    profile["cmd"] = Value::String(self_exe.to_string_lossy().into_owned());
    let params = json!({
        "agent_id": agent_id,
        "generation": generation,
        "command_id": start_id,
        "profile": profile,
        "workspace": root.join("workspace").to_string_lossy(),
        "target_environment": {
            "ELIOT_FAKE_AGY_SCRIPT": script,
            "ELIOT_FAKE_AGY_REPORT": root.join("report.json").to_string_lossy(),
            "ELIOT_FAKE_AGY_WORKSPACE": root.join("workspace").to_string_lossy(),
        },
        "note": null,
        "source_plan": {"session": "stream", "turn": "stream", "message": "stream", "tool": "stream"},
        "source_state": source_state,
        "resume_from": resume_from,
        "initial": null,
    });
    let rx = host.begin_call("agent.start", params).await;
    let pending = PendingStart {
        root,
        agent_id,
        host,
        generation,
    };
    (pending, rx)
}

impl PendingStart {
    async fn finish(mut self, rx: CallRx, expected_native_ref: Option<&str>) -> Scenario {
        let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
        let mut ready_data: Option<Value> = None;
        let ready = loop {
            tokio::select! {
                biased;
                request = self.host.next_request() => {
                    match request.method.as_str() {
                        "process.put" => {
                            answer_process_put(&mut self.host, &self.agent_id, self.generation, &request).await;
                        }
                        "events.put" => {
                            let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                            assert_eq!(events.len(), 1);
                            assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("session.ready"));
                            ready_data = Some(events[0].get("data").cloned().unwrap_or(Value::Null));
                            let first = self.host.next_seq;
                            self.host.next_seq += 1;
                            self.host.respond(&request.id, json!({
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
            .expect("start channel")
            .expect("start ready");
        assert_eq!(ready.get("state").and_then(Value::as_str), Some("ready"));
        let ready_data = ready_data.expect("session.ready batch");
        let native_ref = ready_data
            .get("native_ref")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(!native_ref.is_empty());
        if let Some(expected) = expected_native_ref {
            assert_eq!(
                native_ref, expected,
                "resume reopens the exact conversation"
            );
        }
        assert_eq!(
            ready.get("native_ref").and_then(Value::as_str),
            Some(native_ref)
        );
        assert_eq!(
            ready["effective"].get("model").and_then(Value::as_str),
            Some(MODEL)
        );
        Scenario {
            root: self.root,
            agent_id: self.agent_id,
            host: self.host,
            start_result: ready,
        }
    }
}

async fn send_turn_with_id_and_seq(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    send_id: &str,
    seq: i64,
    text: &str,
) -> (String, Vec<(String, Value)>) {
    let rx = host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id, "generation": generation, "command_id": send_id,
                "verb": "send", "args": {"text": text}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq,
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
                assert_eq!(events.len(), 1);
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
        .expect("send channel")
        .expect("send applied");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("turn id")
        .to_owned();
    while committed.last().map(|(kind, _)| kind.as_str()) != Some("turn.completed") {
        let expected = match committed.last().map(|(kind, _)| kind.as_str()) {
            Some("turn.started") => "message.completed",
            Some("message.completed") => "turn.completed",
            other => panic!("turn facts out of order after {other:?}"),
        };
        let events = host.answer_events_put(expected).await;
        committed.push((
            expected.to_owned(),
            events[0].get("data").cloned().unwrap_or(Value::Null),
        ));
    }
    for (kind, data) in &committed {
        assert_eq!(
            data.get("turn_id").and_then(Value::as_str),
            Some(turn_id.as_str()),
            "turn of {kind}"
        );
        assert_eq!(
            data.get("command_id").and_then(Value::as_str),
            Some(send_id),
            "cmd of {kind}"
        );
    }
    (turn_id, committed)
}

async fn send_turn(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    text: &str,
) -> (String, String, Vec<(String, Value)>) {
    let send_id = ulid::Ulid::new().to_string();
    let seq = host.next_seq;
    host.next_seq += 5;
    let (turn_id, committed) =
        send_turn_with_id_and_seq(host, agent_id, generation, &send_id, seq, text).await;
    (send_id, turn_id, committed)
}

async fn snapshot(host: &mut Host, agent_id: &str, generation: i64, command_id: &str) -> Value {
    host.call(
        "agent.snapshot",
        json!({"agent_id": agent_id, "generation": generation, "command_id": command_id}),
    )
    .await
    .expect("snapshot responds")
}

async fn stop_clean(host: &mut Host, agent_id: &str, generation: i64) -> Vec<(String, Value)> {
    let stop_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.stop",
            json!({"agent_id": agent_id, "generation": generation, "command_id": stop_id, "force": false}),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut committed = Vec::new();
    let stopped = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events.len(), 1);
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
    let stopped = stopped
        .expect("stop deadline")
        .expect("stop channel")
        .expect("stop applied");
    assert_eq!(
        stopped.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert!(committed.iter().any(|(kind, _)| kind == "session.ended"));
    committed
}

async fn scenario_launch_two_sends() {
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = setup_start("happy", agent_id, 1, Value::Null, Value::Null).await;
    let mut scenario = pending.finish(rx, None).await;
    // Init proved readiness before any input was dispatched.
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(0));
    let (send1, turn1, first) =
        send_turn(&mut scenario.host, &scenario.agent_id, 1, "first prompt").await;
    assert_eq!(
        first[1],
        (
            "message.completed".to_owned(),
            json!({
        "command_id": send1, "turn_id": turn1, "text": "reply:first prompt"})
        )
    );
    assert_eq!(first[2].0, "turn.completed");
    assert_eq!(
        first[2].1.get("status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        first[2].1.get("final_text").and_then(Value::as_str),
        Some("reply:first prompt")
    );
    let snap1 = snapshot(&mut scenario.host, &scenario.agent_id, 1, &send1).await;
    assert_eq!(
        snap1.get("result_known").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap1["usage"].get("total_tokens").and_then(Value::as_u64),
        Some(110)
    );
    assert_eq!(
        snap1.get("source_unknown").and_then(Value::as_bool),
        Some(false)
    );
    let (send2, turn2, second) =
        send_turn(&mut scenario.host, &scenario.agent_id, 1, "second prompt").await;
    assert_ne!(turn1, turn2, "one local turn per sequential send");
    assert_eq!(
        second[2].1.get("final_text").and_then(Value::as_str),
        Some("reply:second prompt")
    );
    // Cumulative usage replaces the previous counter; it is never summed.
    let snap2 = snapshot(&mut scenario.host, &scenario.agent_id, 1, &send2).await;
    assert_eq!(
        snap2["usage"].get("total_tokens").and_then(Value::as_u64),
        Some(220)
    );
    assert_eq!(
        snap2.get("source_unknown").and_then(Value::as_bool),
        Some(false)
    );
    // Idle metadata neither gapped the source nor fabricated a turn/message:
    // both turns still hold their exact bindings and the fixture sent one.
    assert_eq!(
        fake_report(&scenario.root)["inputs"],
        json!(["first prompt", "second prompt"])
    );
    assert_eq!(fake_report(&scenario.root)["system_messages"], json!(1));
    stop_clean(&mut scenario.host, &scenario.agent_id, 1).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_stop_resume_no_replay() {
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = setup_start("happy", agent_id.clone(), 1, Value::Null, Value::Null).await;
    let mut gen1 = pending.finish(rx, None).await;
    let native_ref = gen1
        .start_result
        .get("native_ref")
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    let native_scope = gen1
        .start_result
        .get("native_scope")
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    let (_, turn1, _) = send_turn(&mut gen1.host, &gen1.agent_id, 1, "old prompt").await;
    stop_clean(&mut gen1.host, &gen1.agent_id, 1).await;
    gen1.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen1.host.shutdown_child().await;
    // Resume generation 2 on the exact retained conversation.
    let resume_from = json!({"native_scope": native_scope, "native_ref": native_ref});
    let source_state = json!({
        "retained_generation": 1,
        "retained_source": {"agent_id": agent_id, "generation": 1,
            "source": "stream", "families": ["session", "turn", "message", "tool"]},
        "current_turn": {"turn_id": turn1, "state": "completed",
            "status": "completed", "started_at": 1, "completed_at": 2},
    });
    let (pending, rx) = setup_start("happy", agent_id, 2, resume_from, source_state).await;
    let mut gen2 = pending.finish(rx, Some(&native_ref)).await;
    // The resumed native process received no replayed prompt before the new send.
    assert_eq!(fake_report(&gen2.root)["input_count"], json!(0));
    assert_eq!(fake_report(&gen2.root)["conversation"], json!(native_ref));
    let (send_new, _, committed) = send_turn(&mut gen2.host, &gen2.agent_id, 2, "new prompt").await;
    assert_eq!(
        committed[2].1.get("final_text").and_then(Value::as_str),
        Some("reply:new prompt")
    );
    assert_eq!(fake_report(&gen2.root)["inputs"], json!(["new prompt"]));
    // Idle plus mid-turn metadata on the resumed turn: no gap, exact bindings.
    assert_eq!(fake_report(&gen2.root)["system_messages"], json!(2));
    let snap_new = snapshot(&mut gen2.host, &gen2.agent_id, 2, &send_new).await;
    assert_eq!(
        snap_new.get("source_unknown").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        snap_new.get("result_known").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap_new["usage"]
            .get("total_tokens")
            .and_then(Value::as_u64),
        Some(110)
    );
    stop_clean(&mut gen2.host, &gen2.agent_id, 2).await;
    gen2.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen2.host.shutdown_child().await;
}

async fn scenario_lost_result_unknown_no_rewrite() {
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = setup_start("lost-result", agent_id, 1, Value::Null, Value::Null).await;
    let mut scenario = pending.finish(rx, None).await;
    let send_id = ulid::Ulid::new().to_string();
    let seq = scenario.host.next_seq;
    scenario.host.next_seq += 5;
    let rx = scenario
        .host
        .begin_call(
            "agent.command",
            json!({"agent_id": scenario.agent_id, "generation": 1,
                "command_id": send_id, "verb": "send",
                "args": {"text": "doomed prompt"}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq}),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let applied = loop {
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("turn.started"));
                let first = scenario.host.next_seq;
                scenario.host.next_seq += 1;
                scenario.host.respond(&request.id, json!({
                    "first_seq": first, "last_seq": first,
                    "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                })).await;
            }
            result = &mut rx => break result,
        }
    };
    let applied = applied
        .expect("send deadline")
        .expect("send channel")
        .expect("send applied");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    // The native process exited without a result: the stream gaps, the turn
    // stays unresolved, and the snapshot retains unknown.
    let gap = scenario.host.next_request().await;
    assert_eq!(gap.method, "source.gap");
    answer_source_gap(&mut scenario.host, &gap).await;
    let snap = snapshot(&mut scenario.host, &scenario.agent_id, 1, &send_id).await;
    assert_eq!(
        snap.get("source_unknown").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.get("result_known").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(snap.get("state").and_then(Value::as_str), Some("unknown"));
    // The latched send is not rewritten: the native report still shows one input.
    let retry = scenario
        .host
        .call(
            "agent.command",
            json!({"agent_id": scenario.agent_id, "generation": 1,
                "command_id": send_id, "verb": "send",
                "args": {"text": "doomed prompt"}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq}),
        )
        .await
        .expect("retry responds");
    assert_eq!(retry.get("state").and_then(Value::as_str), Some("applied"));
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(1));
    // The idle metadata was present and the lost-result behavior is unchanged.
    assert_eq!(fake_report(&scenario.root)["system_messages"], json!(1));
    stop_clean(&mut scenario.host, &scenario.agent_id, 1).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_checkpoint_frame_no_gap() {
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = setup_start("checkpoint", agent_id, 1, Value::Null, Value::Null).await;
    let mut scenario = pending.finish(rx, None).await;
    let (send_id, turn_id, committed) =
        send_turn(&mut scenario.host, &scenario.agent_id, 1, "hello prompt").await;
    assert_eq!(
        committed[1],
        (
            "message.completed".to_owned(),
            json!({
                "command_id": send_id, "turn_id": turn_id, "text": "reply:hello prompt"
            })
        )
    );
    assert_eq!(committed[2].0, "turn.completed");
    assert_eq!(
        committed[2].1.get("status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        committed[2].1.get("final_text").and_then(Value::as_str),
        Some("reply:hello prompt")
    );
    let snap = snapshot(&mut scenario.host, &scenario.agent_id, 1, &send_id).await;
    assert_eq!(
        snap.get("result_known").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.get("source_unknown").and_then(Value::as_bool),
        Some(false)
    );
    stop_clean(&mut scenario.host, &scenario.agent_id, 1).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_unknown_step_causes_gap() {
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = setup_start("unknown-step", agent_id, 1, Value::Null, Value::Null).await;
    let mut scenario = pending.finish(rx, None).await;
    let send_id = ulid::Ulid::new().to_string();
    let seq = scenario.host.next_seq;
    scenario.host.next_seq += 5;
    let rx = scenario
        .host
        .begin_call(
            "agent.command",
            json!({"agent_id": scenario.agent_id, "generation": 1,
                "command_id": send_id, "verb": "send",
                "args": {"text": "prompt with unknown step"}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq}),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let applied = loop {
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("turn.started"));
                let first = scenario.host.next_seq;
                scenario.host.next_seq += 1;
                scenario.host.respond(&request.id, json!({
                    "first_seq": first, "last_seq": first,
                    "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                })).await;
            }
            result = &mut rx => break result,
        }
    };
    let applied = applied
        .expect("send deadline")
        .expect("send channel")
        .expect("send applied");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    // The native process emitted an unknown step type: explicit source.gap,
    // not cleared retroactively by any subsequent frame.
    let gap = scenario.host.next_request().await;
    assert_eq!(gap.method, "source.gap");
    let reason = gap.params.get("reason").and_then(Value::as_str).unwrap();
    assert!(reason.contains("Antigravity step type is unconnected"));
    assert!(reason.contains("unknown_future_step"));
    answer_source_gap(&mut scenario.host, &gap).await;
    let snap = snapshot(&mut scenario.host, &scenario.agent_id, 1, &send_id).await;
    assert_eq!(
        snap.get("source_unknown").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.get("result_known").and_then(Value::as_bool),
        Some(false)
    );
    stop_clean(&mut scenario.host, &scenario.agent_id, 1).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn assert_invalid_seq_before_io(scenario: &mut Scenario) {
    let missing_seq = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "send", "args": {"text": "missing seq"}, "turn_id": null, "request_id": null,
            }),
        )
        .await;
    assert_eq!(missing_seq.unwrap_err(), "invalid");
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(0));

    let zero_seq = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "send", "args": {"text": "zero seq"}, "turn_id": null, "request_id": null,
                "dispatch_seq": 0,
            }),
        )
        .await;
    assert_eq!(zero_seq.unwrap_err(), "invalid");

    let neg_seq = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "send", "args": {"text": "neg seq"}, "turn_id": null, "request_id": null,
                "dispatch_seq": -5,
            }),
        )
        .await;
    assert_eq!(neg_seq.unwrap_err(), "invalid");
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(0));
}

async fn assert_conflicts_before_io(scenario: &mut Scenario, cmd_id: &str, seq: i64) {
    let conf_id = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1, "command_id": "diff-id",
                "verb": "send", "args": {"text": "prompt 1"}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq,
            }),
        )
        .await;
    assert_eq!(conf_id.unwrap_err(), "source_integrity");

    let conf_seq = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1, "command_id": cmd_id,
                "verb": "send", "args": {"text": "prompt 1"}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq + 1,
            }),
        )
        .await;
    assert_eq!(conf_seq.unwrap_err(), "source_integrity");

    let conf_text = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1, "command_id": cmd_id,
                "verb": "send", "args": {"text": "diff text"}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq,
            }),
        )
        .await;
    assert_eq!(conf_text.unwrap_err(), "source_integrity");
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(1));
}

async fn scenario_command_history_tracking() {
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = setup_start("happy", agent_id.clone(), 1, Value::Null, Value::Null).await;
    let mut scenario = pending.finish(rx, None).await;

    assert_invalid_seq_before_io(&mut scenario).await;

    let cmd1_id = "cmd-track-1";
    let cmd1_seq = 10;
    scenario.host.next_seq = 15;
    let (turn1, _) = send_turn_with_id_and_seq(
        &mut scenario.host,
        &scenario.agent_id,
        1,
        cmd1_id,
        cmd1_seq,
        "prompt 1",
    )
    .await;
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(1));

    let retry = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1, "command_id": cmd1_id,
                "verb": "send", "args": {"text": "prompt 1"}, "turn_id": null, "request_id": null,
                "dispatch_seq": cmd1_seq,
            }),
        )
        .await
        .expect("exact retry responds");
    assert_eq!(retry.get("state").and_then(Value::as_str), Some("applied"));
    assert_eq!(
        retry.get("turn_id").and_then(Value::as_str),
        Some(turn1.as_str())
    );
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(1));

    assert_conflicts_before_io(&mut scenario, cmd1_id, cmd1_seq).await;

    let cmd2_id = "cmd-track-2";
    let cmd2_seq = 50_000;
    scenario.host.next_seq = 50_005;
    let (_turn2, _) = send_turn_with_id_and_seq(
        &mut scenario.host,
        &scenario.agent_id,
        1,
        cmd2_id,
        cmd2_seq,
        "prompt 2",
    )
    .await;
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(2));

    let evicted = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1, "command_id": cmd1_id,
                "verb": "send", "args": {"text": "prompt 1"}, "turn_id": null, "request_id": null,
                "dispatch_seq": cmd1_seq,
            }),
        )
        .await;
    assert_eq!(evicted.unwrap_err(), "unknown");
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(2));

    let snap_evicted = snapshot(&mut scenario.host, &scenario.agent_id, 1, cmd1_id).await;
    assert_eq!(
        snap_evicted.get("state").and_then(Value::as_str),
        Some("unknown")
    );
    assert_eq!(
        snap_evicted.get("result_known").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        snap_evicted.get("source_unknown").and_then(Value::as_bool),
        Some(true)
    );

    stop_clean(&mut scenario.host, &scenario.agent_id, 1).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}
