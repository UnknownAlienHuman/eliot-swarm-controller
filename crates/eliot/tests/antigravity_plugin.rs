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

#[allow(dead_code)]
mod support;

use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
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

// Unix may observe an exited descendant without its numeric exit code.
fn harness_exit_code_accepted(value: &Value) -> bool {
    value == 3 || (cfg!(unix) && value.is_null())
}

fn main() {
    if std::env::var_os("ELIOT_FAKE_AGY_HOLD_STDOUT_RELEASE").is_some() {
        hold_stdout_main();
        return;
    }
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
        scenario_exit_with_held_stdout_is_unknown().await;
        println!("ok - exited leader drains held stdout to a bounded unknown gap");
        scenario_checkpoint_frame_no_gap().await;
        println!("ok - captured checkpoint among actual turn frames, valid final result, no gap");
        scenario_unknown_step_causes_gap().await;
        println!("ok - unknown step type still produces explicit gap");
        scenario_command_history_tracking().await;
        println!(
            "ok - command history tracking: monotonic seq, constant memory, conflicts and eviction"
        );
    });
    drop(runtime);
    scenario_exit_with_held_stdout_public_projection();
    println!("ok - CLI show/result expose stale source and unknown turn after leader exit");
    println!("antigravity_plugin: all scenarios passed");
}

fn hold_stdout_main() {
    let marker = |name: &str| {
        PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("missing {name}")))
    };
    let ready = marker("ELIOT_FAKE_AGY_HOLDER_READY");
    let release = marker("ELIOT_FAKE_AGY_HOLDER_RELEASE");
    let exited = marker("ELIOT_FAKE_AGY_HOLDER_EXITED");
    std::fs::write(ready, "ready").expect("holder ready marker");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !release.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "holder release deadline"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    std::fs::write(exited, "exited").expect("holder exit marker");
}

fn wait_for_marker(path: &Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "marker deadline: {path:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

struct ReleaseOnDrop(PathBuf);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, "release");
    }
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
        if script == "exit3-held-stdout" && inputs.len() == 1 {
            let step = json!({"event":"step_update","step_update":
                {"conversation_id":conv,"step_index":step_index,
                 "state":"DONE","step_type":"user_input"}});
            writeln!(stdout, "{}", step).expect("fixture step");
            stdout.flush().expect("fixture stdout");

            let allow_exit = PathBuf::from(
                std::env::var("ELIOT_FAKE_AGY_EXIT_AFTER_TURN")
                    .expect("ELIOT_FAKE_AGY_EXIT_AFTER_TURN"),
            );
            wait_for_marker(&allow_exit);
            let executable = std::env::current_exe().expect("test executable");
            let holder = std::process::Command::new(executable)
                .env(
                    "ELIOT_FAKE_AGY_HOLD_STDOUT_RELEASE",
                    std::env::var_os("ELIOT_FAKE_AGY_HOLDER_RELEASE")
                        .expect("ELIOT_FAKE_AGY_HOLDER_RELEASE"),
                )
                .env(
                    "ELIOT_FAKE_AGY_HOLDER_READY",
                    std::env::var_os("ELIOT_FAKE_AGY_HOLDER_READY")
                        .expect("ELIOT_FAKE_AGY_HOLDER_READY"),
                )
                .env(
                    "ELIOT_FAKE_AGY_HOLDER_EXITED",
                    std::env::var_os("ELIOT_FAKE_AGY_HOLDER_EXITED")
                        .expect("ELIOT_FAKE_AGY_HOLDER_EXITED"),
                )
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn stdout holder");
            std::fs::write(
                std::env::var("ELIOT_FAKE_AGY_HOLDER_PID").expect("holder pid path"),
                holder.id().to_string(),
            )
            .expect("holder pid marker");
            wait_for_marker(&PathBuf::from(
                std::env::var("ELIOT_FAKE_AGY_HOLDER_READY").expect("holder ready path"),
            ));
            drop(holder);
            std::process::exit(3);
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
    recent_requests: VecDeque<String>,
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

impl HostRequest {
    fn diagnostic(&self) -> String {
        let method = match self.method.as_str() {
            "events.put" => "events.put",
            "source.gap" => "source.gap",
            "source.health" => "source.health",
            "source.state" => "source.state",
            "source.handoff" => "source.handoff",
            "process.put" => "process.put",
            _ => "other",
        };
        let id_class = match &self.id {
            Value::String(_) => "string",
            Value::Number(_) => "number",
            Value::Null => "null",
            _ => "other",
        };
        let step = self
            .params
            .get("events")
            .and_then(Value::as_array)
            .and_then(|events| events.first())
            .and_then(|event| event.get("kind"))
            .and_then(Value::as_str)
            .map(|kind| match kind {
                "turn.started" => "turn_started",
                "turn.completed" => "turn_completed",
                "message.completed" => "message_completed",
                "session.ended" => "session_ended",
                _ => "other",
            })
            .or_else(|| {
                if method != "source.gap" {
                    return None;
                }
                Some(match self.params.get("reason").and_then(Value::as_str) {
                    Some("Antigravity output was incomplete") => "gap_incomplete_output",
                    Some("Antigravity stream closed outside a planned stop") => "gap_unplanned_eof",
                    Some("Antigravity native queue overflowed") => "gap_queue_overflow",
                    Some(reason) if reason.starts_with("Antigravity step type is unconnected") => {
                        "gap_unconnected_step"
                    }
                    Some(reason)
                        if reason.starts_with("Antigravity output has no outstanding input") =>
                    {
                        "gap_unassociated_step"
                    }
                    Some(_) => "gap_other",
                    None => "gap_reason_missing",
                })
            })
            .unwrap_or("none");
        format!("{method}/{id_class}/step={step}")
    }
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
            recent_requests: VecDeque::with_capacity(4),
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
        self.next_request_for("unspecified plugin request").await
    }

    async fn next_request_for(&mut self, expected: &'static str) -> HostRequest {
        let request = match tokio::time::timeout(OP_TIMEOUT, self.inbound.recv()).await {
            Ok(Some(request)) => request,
            Err(_) => panic!(
                "host request deadline; expected={expected}; recent={}",
                self.recent_requests
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ),
            Ok(None) => panic!(
                "plugin request channel closed; expected={expected}; recent={}",
                self.recent_requests
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ),
        };
        if self.recent_requests.len() == 4 {
            self.recent_requests.pop_front();
        }
        self.recent_requests.push_back(request.diagnostic());
        request
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
    let gap_ack = eliot_core::SourceGapAck { gap_id, seq };
    host.respond(
        &request.id,
        serde_json::to_value(gap_ack).expect("source.gap ACK DTO"),
    )
    .await;
    let expected = eliot_core::SourceHealthUpdate::new(
        request.params["agent_id"].as_str().expect("gap agent id"),
        request.params["generation"]
            .as_i64()
            .expect("gap generation"),
        "stream",
        "stale",
    );
    expected.validate().expect("expected source health DTO");
    let health = host
        .next_request_for("source.health after source.gap")
        .await;
    assert_eq!(
        health.method,
        "source.health",
        "unexpected request: {}",
        health.diagnostic()
    );
    let mut health_params = health.params.clone();
    let has_credential = health_params
        .as_object_mut()
        .and_then(|params| params.remove("credential"))
        .is_some_and(|credential| credential.is_string());
    assert!(has_credential, "source.health credential missing");
    let actual: eliot_core::SourceHealthUpdate =
        serde_json::from_value(health_params).expect("source.health DTO");
    actual.validate().expect("valid source.health DTO");
    assert_eq!(actual, expected, "source.health scope or state mismatch");
    host.respond(
        &health.id,
        serde_json::to_value(expected).expect("source.health ACK DTO"),
    )
    .await;
    reason
}

fn agy_profile() -> Value {
    json!({
        "harness": "antigravity",
        "model": MODEL,
        "pass_env": [
            "ELIOT_FAKE_AGY_SCRIPT",
            "ELIOT_FAKE_AGY_REPORT",
            "ELIOT_FAKE_AGY_WORKSPACE",
            "ELIOT_FAKE_AGY_EXIT_AFTER_TURN",
            "ELIOT_FAKE_AGY_HOLDER_PID",
            "ELIOT_FAKE_AGY_HOLDER_READY",
            "ELIOT_FAKE_AGY_HOLDER_RELEASE",
            "ELIOT_FAKE_AGY_HOLDER_EXITED"
        ],
    })
}

fn fake_report(root: &Path) -> Value {
    fake_report_path(&root.join("report.json"))
}

fn fake_report_path(path: &Path) -> Value {
    let text = std::fs::read_to_string(path).expect("fake AGY report");
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
    let mut target_environment = json!({
        "ELIOT_FAKE_AGY_SCRIPT": script,
        "ELIOT_FAKE_AGY_REPORT": root.join("report.json").to_string_lossy(),
        "ELIOT_FAKE_AGY_WORKSPACE": root.join("workspace").to_string_lossy(),
    });
    if script == "exit3-held-stdout" {
        target_environment["ELIOT_FAKE_AGY_EXIT_AFTER_TURN"] =
            json!(root.join("allow-leader-exit").to_string_lossy().to_string());
        target_environment["ELIOT_FAKE_AGY_HOLDER_PID"] =
            json!(root.join("holder.pid").to_string_lossy().to_string());
        target_environment["ELIOT_FAKE_AGY_HOLDER_READY"] =
            json!(root.join("holder-ready").to_string_lossy().to_string());
        target_environment["ELIOT_FAKE_AGY_HOLDER_RELEASE"] =
            json!(root.join("holder-release").to_string_lossy().to_string());
        target_environment["ELIOT_FAKE_AGY_HOLDER_EXITED"] =
            json!(root.join("holder-exited").to_string_lossy().to_string());
    }
    let params = json!({
        "agent_id": agent_id,
        "generation": generation,
        "command_id": start_id,
        "profile": profile,
        "workspace": root.join("workspace").to_string_lossy(),
        "target_environment": target_environment,
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
            request = host.next_request_for("events.put during agent.stop") => {
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
    let agent_id = "a101".to_owned();
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
    // Association first: the send reply is buffered by the BackgroundPeer
    // oneshot, so consume the committed turn.started, then any valid EOF
    // gap/health, then await the already-created reply. A gap before
    // association still fails the first-request check below.
    let started = scenario
        .host
        .next_request_for("events.put after send (native step user_input)")
        .await;
    assert_eq!(
        started.method,
        "events.put",
        "unexpected request while associating send: {}",
        started.diagnostic()
    );
    let events = started
        .params
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        events[0].get("kind").and_then(Value::as_str),
        Some("turn.started"),
        "unexpected event: {}",
        started.diagnostic()
    );
    assert_eq!(events.len(), 1);
    assert_eq!(
        started.params.get("agent_id").and_then(Value::as_str),
        Some(scenario.agent_id.as_str())
    );
    assert_eq!(
        started.params.get("generation").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        started.params.get("source").and_then(Value::as_str),
        Some("stream")
    );
    assert_eq!(
        events[0]
            .get("data")
            .and_then(|data| data.get("command_id"))
            .and_then(Value::as_str),
        Some(send_id.as_str())
    );
    let turn_id = events[0]
        .get("data")
        .and_then(|data| data.get("turn_id"))
        .and_then(Value::as_str)
        .expect("turn.started turn_id")
        .to_owned();
    assert!(!turn_id.is_empty());
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(1));
    let first = scenario.host.next_seq;
    scenario.host.next_seq += 1;
    scenario
        .host
        .respond(
            &started.id,
            json!({
                "first_seq": first, "last_seq": first,
                "cursor": started.params.get("cursor").cloned().unwrap_or(Value::Null),
            }),
        )
        .await;
    let gap = scenario
        .host
        .next_request_for("source.gap after process exit (native step user_input)")
        .await;
    assert_eq!(gap.method, "source.gap");
    answer_source_gap(&mut scenario.host, &gap).await;
    let applied = tokio::time::timeout(OP_TIMEOUT, rx)
        .await
        .expect("send deadline")
        .expect("send channel")
        .expect("send applied");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(
        applied.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    let snap = snapshot(&mut scenario.host, &scenario.agent_id, 1, &send_id).await;
    assert_eq!(
        snap.get("source_unknown").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.get("source_health_stale").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.get("source_health_unknown").and_then(Value::as_bool),
        Some(false)
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

async fn scenario_exit_with_held_stdout_is_unknown() {
    let agent_id = "a102".to_owned();
    let (pending, rx) =
        setup_start("exit3-held-stdout", agent_id, 1, Value::Null, Value::Null).await;
    let mut scenario = pending.finish(rx, None).await;
    let _release_holder = ReleaseOnDrop(scenario.root.join("holder-release"));
    let send_id = ulid::Ulid::new().to_string();
    let seq = scenario.host.next_seq;
    scenario.host.next_seq += 5;
    let rx = scenario
        .host
        .begin_call(
            "agent.command",
            json!({"agent_id": scenario.agent_id, "generation": 1,
                "command_id": send_id, "verb": "send",
                "args": {"text": "held stdout prompt"}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq}),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(Duration::from_secs(5), rx));
    let mut turn_id = None;
    let applied = loop {
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array)
                    .cloned().unwrap_or_default();
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("turn.started"));
                turn_id = events[0].get("data").and_then(|data| data.get("turn_id"))
                    .and_then(Value::as_str).map(ToOwned::to_owned);
                assert!(turn_id.as_ref().is_some_and(|turn| !turn.is_empty()));
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
        .expect("send association deadline")
        .expect("send channel")
        .expect("turn.started association");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    let turn_id = turn_id.expect("committed turn id");

    // The leader is still alive and waiting for this marker; no gap is valid yet.
    let early =
        tokio::time::timeout(Duration::from_millis(100), scenario.host.inbound.recv()).await;
    assert!(
        early.is_err(),
        "source loss was reported before process exit"
    );
    std::fs::write(scenario.root.join("allow-leader-exit"), "exit").expect("release leader");

    let exit_deadline = std::time::Instant::now() + Duration::from_millis(500);
    let exiting = loop {
        let snapshot = snapshot(&mut scenario.host, &scenario.agent_id, 1, &send_id).await;
        if snapshot.get("process_exited").and_then(Value::as_bool) == Some(true) {
            break snapshot;
        }
        assert!(
            std::time::Instant::now() < exit_deadline,
            "owned process exit observation deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(
        exiting.get("state").and_then(Value::as_str),
        Some("unknown")
    );
    assert_eq!(
        exiting.get("source_unknown").and_then(Value::as_bool),
        Some(false),
        "stdout still has its bounded drain window"
    );

    let during_drain = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1,
                "command_id": ulid::Ulid::new().to_string(), "verb": "send",
                "args": {"text": "must not dispatch during drain"},
                "turn_id": null, "request_id": null, "dispatch_seq": seq + 1,
            }),
        )
        .await
        .expect_err("process exit fences a new input before source loss is committed");
    assert_eq!(during_drain, "unknown");
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(1));
    let no_gap_yet =
        tokio::time::timeout(Duration::from_millis(50), scenario.host.inbound.recv()).await;
    assert!(
        no_gap_yet.is_err(),
        "source gap preceded the drain deadline"
    );

    let gap = tokio::time::timeout(Duration::from_secs(5), scenario.host.inbound.recv())
        .await
        .expect("bounded process-exit drain")
        .expect("source gap request");
    assert_eq!(gap.method, "source.gap");
    answer_source_gap(&mut scenario.host, &gap).await;

    let snap = snapshot(&mut scenario.host, &scenario.agent_id, 1, &send_id).await;
    assert_eq!(snap.get("state").and_then(Value::as_str), Some("unknown"));
    assert_eq!(
        snap.get("source_unknown").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.get("source_health_stale").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        snap.get("source_health_unknown").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        snap.get("result_known").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(snap.get("last_turn"), Some(&Value::Null));
    assert_eq!(
        snap.get("active_turn").and_then(Value::as_str),
        Some(turn_id.as_str())
    );

    // Replaying the retained command returns its known association without a second write.
    let retry = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1, "command_id": send_id,
                "verb": "send", "args": {"text": "held stdout prompt"},
                "turn_id": null, "request_id": null, "dispatch_seq": seq,
            }),
        )
        .await
        .expect("retained command response");
    assert_eq!(retry.get("state").and_then(Value::as_str), Some("applied"));
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(1));

    let rejected = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": scenario.agent_id, "generation": 1,
                "command_id": ulid::Ulid::new().to_string(), "verb": "send",
                "args": {"text": "must not dispatch"}, "turn_id": null, "request_id": null,
                "dispatch_seq": seq + 1,
            }),
        )
        .await
        .expect_err("lost source blocks a new input");
    assert_eq!(rejected, "unknown");
    assert_eq!(fake_report(&scenario.root)["input_count"], json!(1));

    let holder_pid =
        std::fs::read_to_string(scenario.root.join("holder.pid")).expect("exact helper pid marker");
    assert!(holder_pid.trim().parse::<u32>().is_ok());
    std::fs::write(scenario.root.join("holder-release"), "release").expect("release holder");
    tokio::time::timeout(Duration::from_secs(3), async {
        while !scenario.root.join("holder-exited").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owned test descendant cleanup");

    stop_clean(&mut scenario.host, &scenario.agent_id, 1).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

fn scenario_exit_with_held_stdout_public_projection() {
    let mut fixture = support::Fixture::new("antigravity public exit drain");
    let report = fixture.root.join("agy-report.json");
    let allow_exit = fixture.root.join("allow-leader-exit");
    let holder_pid = fixture.root.join("holder.pid");
    let holder_ready = fixture.root.join("holder-ready");
    let holder_release = fixture.root.join("holder-release");
    let holder_exited = fixture.root.join("holder-exited");
    let _release_holder = ReleaseOnDrop(holder_release.clone());

    let (agent, generation, generation_argument) = setup_held_stdout_public_projection(
        &mut fixture,
        &report,
        &allow_exit,
        &holder_pid,
        &holder_ready,
        &holder_release,
        &holder_exited,
    );

    let send_id = ulid::Ulid::new().to_string();
    let prompt = "held stdout public prompt";
    let _submitted = fixture.json(&[
        "--id",
        &send_id,
        "send",
        &agent,
        "--generation",
        &generation_argument,
        prompt,
    ]);
    let send = fixture.wait_command(&send_id, "applied");
    let turn_id = send["turn_id"].as_str().expect("committed turn binding");
    let before_exit = fixture.events();
    assert!(before_exit.iter().any(|event| {
        event["kind"] == "turn.started"
            && event["agent"] == agent
            && event["generation"] == generation
            && event["data"]["command_id"] == send_id
            && event["data"]["turn_id"] == turn_id
    }));
    assert!(!before_exit.iter().any(|event| {
        event["kind"] == "source.gap"
            && event["agent"] == agent
            && event["generation"] == generation
    }));
    assert_eq!(fake_report_path(&report)["input_count"], json!(1));
    std::fs::write(&allow_exit, "exit").expect("release leader");

    let deadline = std::time::Instant::now() + OP_TIMEOUT;
    let events = wait_for_public_source_loss(
        &mut fixture,
        &agent,
        generation,
        &generation_argument,
        turn_id,
        &holder_pid,
        deadline,
    );

    assert_public_source_loss_projection(
        &mut fixture,
        &agent,
        generation,
        &generation_argument,
        turn_id,
        &events,
    );

    let process_deadline = std::time::Instant::now() + OP_TIMEOUT;
    wait_for_harness_exit_readback(
        &mut fixture,
        &agent,
        generation,
        &generation_argument,
        process_deadline,
    );

    assert_public_send_replay(
        &mut fixture,
        &agent,
        &generation_argument,
        &send_id,
        prompt,
        &report,
    );
    release_holder_and_stop(
        &mut fixture,
        &agent,
        &generation_argument,
        &holder_pid,
        &holder_release,
        &holder_exited,
    );
}

fn setup_held_stdout_public_projection(
    fixture: &mut support::Fixture,
    report: &Path,
    allow_exit: &Path,
    holder_pid: &Path,
    holder_ready: &Path,
    holder_release: &Path,
    holder_exited: &Path,
) -> (String, i64, String) {
    let workspace_path = fixture.workspace.clone();
    fixture.env("ELIOT_FAKE_AGY_SCRIPT", "exit3-held-stdout");
    fixture.env("ELIOT_FAKE_AGY_REPORT", report.as_os_str());
    fixture.env("ELIOT_FAKE_AGY_WORKSPACE", workspace_path.as_os_str());
    fixture.env("ELIOT_FAKE_AGY_EXIT_AFTER_TURN", allow_exit.as_os_str());
    fixture.env("ELIOT_FAKE_AGY_HOLDER_PID", holder_pid.as_os_str());
    fixture.env("ELIOT_FAKE_AGY_HOLDER_READY", holder_ready.as_os_str());
    fixture.env("ELIOT_FAKE_AGY_HOLDER_RELEASE", holder_release.as_os_str());
    fixture.env("ELIOT_FAKE_AGY_HOLDER_EXITED", holder_exited.as_os_str());
    fixture.start();

    let executable = std::env::current_exe()
        .expect("test executable")
        .to_string_lossy()
        .into_owned();
    let config = format!(
        "[profiles.agy-exit]\nharness = \"antigravity\"\nmodel = \"{MODEL}\"\ncmd = {executable:?}\npass_env = [\"ELIOT_FAKE_AGY_SCRIPT\", \"ELIOT_FAKE_AGY_REPORT\", \"ELIOT_FAKE_AGY_WORKSPACE\", \"ELIOT_FAKE_AGY_EXIT_AFTER_TURN\", \"ELIOT_FAKE_AGY_HOLDER_PID\", \"ELIOT_FAKE_AGY_HOLDER_READY\", \"ELIOT_FAKE_AGY_HOLDER_RELEASE\", \"ELIOT_FAKE_AGY_HOLDER_EXITED\"]\n"
    );
    std::fs::write(fixture.data.join("config.toml"), config)
        .expect("write Antigravity profile into the initialized private fixture");

    let workspace = fixture.workspace.to_string_lossy().into_owned();
    let launched = fixture.json(&["launch", "--profile", "agy-exit", "--workspace", &workspace]);
    assert_eq!(launched["state"], "applied");
    let agent = launched["target"]
        .as_str()
        .expect("launched Agent")
        .to_owned();
    let generation = launched["generation"]
        .as_i64()
        .expect("launched generation");
    let generation_argument = generation.to_string();
    (agent, generation, generation_argument)
}

fn wait_for_public_source_loss(
    fixture: &mut support::Fixture,
    agent: &str,
    generation: i64,
    generation_argument: &str,
    turn_id: &str,
    holder_pid: &Path,
    deadline: std::time::Instant,
) -> Vec<Value> {
    loop {
        let events = fixture.events();
        let has_gap = events.iter().any(|event| {
            event["kind"] == "source.gap"
                && event["agent"] == agent
                && event["generation"] == generation
                && event["data"]["source"] == "stream"
        });
        let has_stale_health = events.iter().any(|event| {
            event["kind"] == "source.health"
                && event["agent"] == agent
                && event["generation"] == generation
                && event["data"]["source"] == "stream"
                && event["data"]["health"] == "stale"
        });
        if has_gap && has_stale_health {
            return events;
        }
        if std::time::Instant::now() >= deadline {
            panic_public_source_loss_deadline(
                fixture,
                agent,
                generation,
                generation_argument,
                turn_id,
                holder_pid,
                &events,
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn panic_public_source_loss_deadline(
    fixture: &mut support::Fixture,
    agent: &str,
    generation: i64,
    generation_argument: &str,
    turn_id: &str,
    holder_pid: &Path,
    events: &[Value],
) -> ! {
    let show = fixture.json(&["show", agent]);
    let source_state = show
        .get("source_states")
        .and_then(Value::as_array)
        .and_then(|sources| {
            sources
                .iter()
                .find(|source| source.get("source").and_then(Value::as_str) == Some("stream"))
        });
    let lifecycle = source_state
        .and_then(|source| source.get("state"))
        .and_then(Value::as_str)
        .filter(|state| matches!(*state, "open" | "closed"))
        .unwrap_or("unknown");
    let health = source_state
        .and_then(|source| source.get("health"))
        .and_then(Value::as_str)
        .filter(|health| matches!(*health, "ok" | "slow" | "stale" | "error"))
        .unwrap_or("unknown");
    let gap = events.iter().rev().find(|event| {
        event["kind"] == "source.gap"
            && event["agent"] == agent
            && event["generation"] == generation
            && event["data"]["source"] == "stream"
    });
    let stale_health = events.iter().rev().find(|event| {
        event["kind"] == "source.health"
            && event["agent"] == agent
            && event["generation"] == generation
            && event["data"]["source"] == "stream"
            && event["data"]["health"] == "stale"
    });
    let leader_exit = events.iter().rev().find(|event| {
        event["kind"] == "process.exited"
            && event["agent"] == agent
            && event["generation"] == generation
            && event["data"]["role"] == "harness"
            && harness_exit_code_accepted(&event["data"]["exit_code"])
    });
    let event_time = |event: Option<&Value>| {
        event.map_or(
            Value::Null,
            |event| json!({"seq": event["seq"], "ts": event["ts"]}),
        )
    };

    let processes = fixture.json(&[
        "processes",
        agent,
        "--generation",
        generation_argument,
        "--limit",
        "50",
    ]);
    let process_rows = processes.get("processes").and_then(Value::as_array);
    let holder_process_id = std::fs::read_to_string(holder_pid)
        .ok()
        .and_then(|pid| pid.trim().parse::<u32>().ok());
    let holder_alive = holder_process_id.and_then(|pid| {
        process_rows
            .and_then(|rows| {
                rows.iter().find(|process| {
                    process.get("pid").and_then(Value::as_u64) == Some(u64::from(pid))
                })
            })
            .map(|process| process["state"] == "running")
    });
    let plugin_alive = process_rows
        .and_then(|rows| rows.iter().find(|process| process["role"] == "plugin"))
        .map(|process| process["state"] == "running");
    let turn_result = fixture.json(&[
        "result",
        agent,
        "--generation",
        generation_argument,
        "--limit",
        "10",
    ]);
    let turn_state = turn_result
        .get("turns")
        .and_then(Value::as_array)
        .and_then(|turns| {
            turns
                .iter()
                .find(|turn| turn.get("turn_id").and_then(Value::as_str) == Some(turn_id))
        })
        .and_then(|turn| turn.get("state"))
        .and_then(Value::as_str)
        .filter(|state| {
            matches!(
                *state,
                "started" | "running" | "completed" | "failed" | "unknown"
            )
        })
        .unwrap_or("other");
    let last_event_seq = events
        .iter()
        .filter_map(|event| event.get("seq").and_then(Value::as_i64))
        .max();
    let snapshot = json!({
        "harness_leader_exit_seen": leader_exit.is_some(),
        "holder_alive": holder_alive,
        "source_gap_seen": gap.is_some(),
        "stale_health_seen": stale_health.is_some(),
        "plugin_alive": plugin_alive,
        "stream_lifecycle": lifecycle,
        "stream_health": health,
        "gap_present": source_state.is_some_and(|source| !source["gap_id"].is_null()),
        "last_store_seq": source_state.and_then(|source| source.get("last_store_seq")),
        "last_event_seq": last_event_seq,
        "last_turn_state": turn_state,
        "events": {
            "harness_exit": event_time(leader_exit),
            "stream_gap": event_time(gap),
            "stream_stale_health": event_time(stale_health),
        },
    });
    panic!("public source-loss projection deadline: {snapshot}");
}

fn assert_public_source_loss_projection(
    fixture: &mut support::Fixture,
    agent: &str,
    generation: i64,
    generation_argument: &str,
    turn_id: &str,
    events: &[Value],
) {
    let view = fixture.json(&["show", agent]);
    // Agent state remains its last committed busy state; source health exposes the loss.
    assert_eq!(view["state"], "busy");
    assert_eq!(view["observation"]["liveness"]["state"], "busy");
    assert_eq!(view["observation"]["liveness"]["source_healthy"], false);
    let result = fixture.json(&[
        "result",
        agent,
        "--generation",
        generation_argument,
        "--limit",
        "10",
    ]);
    assert!(
        !result["turns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|turn| { turn["turn_id"] == turn_id && turn["state"] == "completed" })
    );
    assert!(!events.iter().any(|event| {
        event["agent"] == agent
            && event["generation"] == generation
            && event["kind"] == "turn.completed"
            && event["data"]["turn_id"] == turn_id
    }));
    assert!(!events.iter().any(|event| {
        event["agent"] == agent
            && event["generation"] == generation
            && matches!(
                event["kind"].as_str(),
                Some("session.ended" | "session.lost")
            )
    }));
}

fn wait_for_harness_exit_readback(
    fixture: &mut support::Fixture,
    agent: &str,
    generation: i64,
    generation_argument: &str,
    process_deadline: std::time::Instant,
) {
    loop {
        let processes = fixture.json(&[
            "processes",
            agent,
            "--generation",
            generation_argument,
            "--limit",
            "50",
        ]);
        let harness_exit = processes["processes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|process| {
                process["role"] == "harness"
                    && process["state"] == "exited"
                    // Unix monitoring may leave the harness exit code unknown.
                    && harness_exit_code_accepted(&process["exit_code"])
            });
        if harness_exit.is_some() {
            break;
        }
        if std::time::Instant::now() >= process_deadline {
            let rows = processes["processes"].as_array().unwrap();
            let harness: Vec<serde_json::Value> = rows
                .iter()
                .filter(|process| process["role"] == "harness")
                .map(|process| {
                    let exit = if process["exit_code"] == 3 {
                        "3"
                    } else if process["exit_code"].is_null() {
                        "null"
                    } else {
                        "other"
                    };
                    json!({
                        "state": process["state"],
                        "exit_code_class": exit,
                        "owner_generation_match": process["owner"]
                            .as_str()
                            .map(|owner| owner.ends_with(&format!("#{generation}"))),
                    })
                })
                .collect();
            panic!(
                "public harness exit readback deadline: total_rows={} harness_count={} harness={:?} processes_truncated={:?}",
                rows.len(),
                harness.len(),
                harness,
                processes.get("processes_truncated")
            );
        }
        assert!(
            std::time::Instant::now() < process_deadline,
            "public harness exit readback deadline"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn assert_public_send_replay(
    fixture: &mut support::Fixture,
    agent: &str,
    generation_argument: &str,
    send_id: &str,
    prompt: &str,
    report: &Path,
) {
    // Replaying the same public command id returns the retained applied ACK without a new input.
    let replay = fixture.json(&[
        "--id",
        send_id,
        "send",
        agent,
        "--generation",
        generation_argument,
        prompt,
    ]);
    assert_eq!(replay["state"], "applied");
    assert_eq!(fake_report_path(report)["input_count"], json!(1));
}

fn release_holder_and_stop(
    fixture: &mut support::Fixture,
    agent: &str,
    generation_argument: &str,
    holder_pid: &Path,
    holder_release: &Path,
    holder_exited: &Path,
) {
    assert!(
        std::fs::read_to_string(holder_pid)
            .expect("exact holder pid marker")
            .trim()
            .parse::<u32>()
            .is_ok()
    );
    std::fs::write(holder_release, "release").expect("release held stdout child");
    wait_for_marker(holder_exited);

    let stopped = fixture.json(&["stop", agent, "--generation", generation_argument]);
    fixture.wait_command(stopped["command_id"].as_str().unwrap(), "applied");
    fixture.stop(false);
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
    let agent_id = "a103".to_owned();
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
