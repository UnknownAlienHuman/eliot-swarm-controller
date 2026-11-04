//! Claude plugin protocol tests with a scripted fake stream-json CLI.
//!
//! This target runs with `harness = false` (see `[[test]]` in Cargo.toml) so
//! the test binary can re-execute itself as the fake `claude -p` CLI: the
//! plugin spawns `cmd` with stream-json arguments, and a libtest harness
//! would corrupt that stdio stream. When `ELIOT_FAKE_CLAUDE_SCRIPT` is set,
//! `main` runs the fixture loop instead of the scenarios.
//!
//! Fixture payloads follow the controller-verified CLI behavior (note 47,
//! Claude Code 2.1.278); the plugin under test is the real
//! `swarm plugin claude` binary driven over the Eliot plugin protocol, with
//! this file emulating the host side (`process.put`, `events.put`,
//! `source.gap`).
//!
//! Hang safety (note 61): every scenario runs under a hard per-scenario
//! timeout that fails the run instead of living for hours. On timeout the
//! plugin child dies by its `kill_on_drop` handle and the fake CLI's
//! stdin-EOF exit is confirmed through the `exit` marker it writes into its
//! report; on success the plugin's own reaped exit wait plus the shutdown
//! assertions prove both children died.

use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::sync::{Mutex, mpsc, oneshot};

const CLI: &str = env!("CARGO_BIN_EXE_swarm");
const MODEL: &str = "claude-opus-4-6";
const OP_TIMEOUT: Duration = Duration::from_secs(15);
/// Hard backstop per scenario: a hang fails the run instead of living for
/// hours (note 61). Generous: a healthy scenario takes seconds.
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(120);
/// Grace for an owned child to exit once its stdin closes or its owner dies.
const CHILD_GRACE: Duration = Duration::from_secs(15);
const EXIT_POLL: Duration = Duration::from_millis(50);

fn main() {
    if let Ok(script) = std::env::var("ELIOT_FAKE_CLAUDE_SCRIPT") {
        fake_claude_main(&script);
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        run_scenario(
            "launch/send/snapshot/stop",
            OwnedChildren::default(),
            scenario_launch_send_snapshot_stop,
        )
        .await;
        run_scenario(
            "two assistant messages: snapshot matches the stream fold",
            OwnedChildren::default(),
            scenario_two_assistant_messages_snapshot_matches_stream,
        )
        .await;
        run_scenario(
            "failed turn reports its terminal reason",
            OwnedChildren::default(),
            scenario_failed_turn_reports_terminal_reason,
        )
        .await;
        run_scenario(
            "terminal reason is redacted",
            OwnedChildren::default(),
            scenario_terminal_reason_is_redacted,
        )
        .await;
        run_scenario(
            "cancelled input is rejected without a second write",
            OwnedChildren::default(),
            scenario_cancelled_input_is_rejected_without_rewrite,
        )
        .await;
        run_scenario(
            "cancelled active turn reaches a terminal state",
            OwnedChildren::default(),
            scenario_cancelled_active_turn_reaches_terminal,
        )
        .await;
        run_scenario(
            "dropped input is unknown without a second write",
            OwnedChildren::default(),
            scenario_dropped_input_is_unknown_without_rewrite,
        )
        .await;
        run_scenario(
            "interrupt applied once, then stale_turn",
            OwnedChildren::default(),
            scenario_interrupt_applied_then_stale,
        )
        .await;
        run_scenario(
            "interrupt reports a non-empty still_queued",
            OwnedChildren::default(),
            scenario_interrupt_reports_nonempty_still_queued,
        )
        .await;
        run_scenario(
            "unknown model is rejected before session.ready",
            OwnedChildren::default(),
            scenario_model_not_in_catalogue_rejected_before_ready,
        )
        .await;
        run_scenario(
            "session mismatch is unknown with an explicit gap",
            OwnedChildren::default(),
            scenario_session_mismatch_is_unknown_with_gap,
        )
        .await;
        run_scenario(
            "non-idle initialize is rejected before session.ready",
            OwnedChildren::default(),
            scenario_not_idle_rejected_before_ready,
        )
        .await;
        run_scenario(
            "invalid profile rejected before spawn",
            OwnedChildren::default(),
            scenario_invalid_profile_rejected_before_spawn,
        )
        .await;
    });
    println!("claude_plugin: all scenarios passed");
}

/// Run one scenario under the hard per-scenario timeout. On success the
/// scenario's own assertions already prove both children died: every scenario
/// ends with `shutdown_child` (which waits for the plugin's successful exit),
/// and before that the plugin's stop/shutdown reaps the fake CLI by handle on
/// every branch, returning `unknown` (which the scenario rejects) if the
/// fake's exit cannot be confirmed. On timeout the scenario future is dropped
/// — which drops its [`Host`], killing the owned plugin child by its
/// `kill_on_drop` handle — the fake CLI's stdin-EOF exit is confirmed through
/// its report marker, and then the run panics with the scenario name.
async fn run_scenario<F, Fut>(name: &'static str, owned: OwnedChildren, scenario: F)
where
    F: FnOnce(OwnedChildren) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    match tokio::time::timeout(SCENARIO_TIMEOUT, scenario(owned.clone())).await {
        Ok(()) => {
            println!("ok - {name}");
        }
        Err(_) => {
            let cli = if owned.cli_exited().await {
                "exit confirmed"
            } else {
                "exit UNCONFIRMED"
            };
            panic!(
                "scenario {name} timed out after {SCENARIO_TIMEOUT:?}; \
                 the owned plugin child was killed, fake CLI {cli} ({})",
                owned.describe().await
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Fake Claude CLI fixture (runs as a child of the plugin under test)
// ---------------------------------------------------------------------------

struct FakeClaude {
    script: String,
    report_path: PathBuf,
    argv_session: String,
    argv_model: String,
    argv: Vec<String>,
    controls: HashMap<String, u64>,
    user_inputs: u64,
    /// Set only on an exit path, so the scenario can verify this exact CLI
    /// died with it instead of leaking (note 61).
    exit: Option<String>,
}

fn fake_claude_main(script: &str) {
    let report_path = std::env::var("ELIOT_FAKE_CLAUDE_REPORT")
        .map(PathBuf::from)
        .expect("ELIOT_FAKE_CLAUDE_REPORT");
    let argv: Vec<String> = std::env::args().collect();
    let flag = |name: &str| {
        argv.iter()
            .position(|arg| arg == name)
            .and_then(|pos| argv.get(pos + 1))
            .cloned()
            .unwrap_or_default()
    };
    let mut fake = FakeClaude {
        script: script.to_owned(),
        report_path,
        argv_session: flag("--session-id"),
        argv_model: flag("--model"),
        argv,
        controls: HashMap::new(),
        user_inputs: 0,
        exit: None,
    };
    fake.write_report();
    let _panic_marker = PanicMarker {
        report_path: fake.report_path.clone(),
    };
    fake.run();
}

/// Marks the fake CLI's exit even when it panics (e.g. a broken-pipe write
/// racing the owner's death on a scenario timeout), so a missing marker
/// always means a live CLI rather than an unwitnessed panic. Only adds the
/// marker to the last report; counts stay as the fixture left them.
struct PanicMarker {
    report_path: PathBuf,
}

impl Drop for PanicMarker {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        let Ok(text) = std::fs::read_to_string(&self.report_path) else {
            return;
        };
        let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
            return;
        };
        value["exit"] = Value::String("panicked".to_owned());
        let Ok(text) = serde_json::to_string(&value) else {
            return;
        };
        let tmp = self.report_path.with_extension("tmp");
        let _ = std::fs::write(&tmp, &text).and_then(|()| std::fs::rename(&tmp, &self.report_path));
    }
}

impl FakeClaude {
    fn run(&mut self) {
        let stdin = std::io::stdin().lock();
        let lines = BufReader::new(stdin).lines();
        let mut stdout = std::io::stdout().lock();
        for line in lines {
            let line = line.expect("fixture stdin");
            if line.trim().is_empty() {
                continue;
            }
            let frame: Value = serde_json::from_str(&line).expect("fixture stdin JSON");
            if !self.handle_frame(&frame, &mut stdout) {
                return;
            }
            stdout.flush().expect("fixture stdout");
            self.write_report();
        }
        // Stdin EOF: the owner closed input (or died). Mark the exit so the
        // scenario observes this exact CLI is gone.
        self.exit = Some("stdin-eof".to_owned());
        self.write_report();
    }

    /// False ends the fixture (clean exit or scripted drop).
    fn handle_frame(&mut self, frame: &Value, stdout: &mut std::io::StdoutLock) -> bool {
        match frame.get("type").and_then(Value::as_str) {
            Some("control_request") => {
                let request_id = frame
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let subtype = frame
                    .get("request")
                    .and_then(|request| request.get("subtype"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                *self.controls.entry(subtype.clone()).or_insert(0) += 1;
                self.write_report();
                match subtype.as_str() {
                    "initialize" => {
                        self.emit(
                            stdout,
                            json!({
                                "type": "control_response",
                                "response": {
                                    "subtype": "success",
                                    "request_id": request_id,
                                    "response": self.initialize_response(),
                                },
                            }),
                        );
                        // SessionStart hook lines carry the session id before
                        // any input, exactly like the installed CLI.
                        let session_id = if self.script == "session-mismatch" {
                            "00000000-0000-0000-0000-000000000000".to_owned()
                        } else {
                            self.argv_session.clone()
                        };
                        self.emit(
                            stdout,
                            json!({
                                "type": "system",
                                "subtype": "session_start",
                                "session_id": session_id,
                            }),
                        );
                    }
                    "interrupt" => {
                        let still_queued = if self.script == "interrupt-queued" {
                            json!(["queued-turn-1"])
                        } else {
                            json!([])
                        };
                        self.emit(
                            stdout,
                            json!({
                                "type": "control_response",
                                "response": {
                                    "subtype": "success",
                                    "request_id": request_id,
                                    "response": {"still_queued": still_queued},
                                },
                            }),
                        );
                        if self.script == "interrupt-race" || self.script == "interrupt-queued" {
                            // The CLI flushes partial text as it interrupts;
                            // the adapter must keep it before the terminal.
                            self.emit_assistant(stdout, "fake partial text. ");
                            self.emit_turn_result(stdout, "interrupted");
                        }
                    }
                    _ => {
                        self.emit(
                            stdout,
                            json!({
                                "type": "control_response",
                                "response": {
                                    "subtype": "success",
                                    "request_id": request_id,
                                    "response": {},
                                },
                            }),
                        );
                    }
                }
                true
            }
            Some("user") => {
                self.user_inputs += 1;
                self.write_report();
                if self.script == "drop-input" {
                    self.exit = Some("scripted-drop".to_owned());
                    self.write_report();
                    std::process::exit(0);
                }
                let uuid = frame
                    .get("uuid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                if self.script == "cancelled-turn" {
                    self.emit(
                        stdout,
                        json!({
                            "type": "command_lifecycle",
                            "command_uuid": uuid,
                            "state": "cancelled",
                        }),
                    );
                    return true;
                }
                self.emit(
                    stdout,
                    json!({
                        "type": "system",
                        "subtype": "init",
                        "session_id": self.argv_session,
                    }),
                );
                self.emit(
                    stdout,
                    json!({
                        "type": "command_lifecycle",
                        "command_uuid": uuid,
                        "state": "started",
                    }),
                );
                if self.script == "interrupt-race" || self.script == "interrupt-queued" {
                    // Park the turn: no assistant text and no terminal
                    // record until the interrupt arrives.
                    return true;
                }
                if self.script == "cancel-active" {
                    // The queued turn is cancelled after association; no
                    // result record follows, the cancellation is terminal.
                    self.emit(
                        stdout,
                        json!({
                            "type": "command_lifecycle",
                            "command_uuid": uuid,
                            "state": "cancelled",
                        }),
                    );
                    return true;
                }
                let texts: Vec<String> = match self.script.as_str() {
                    "two-messages" => {
                        vec!["fake part one. ".to_owned(), "fake part two.".to_owned()]
                    }
                    _ => vec!["fake reply to the turn.".to_owned()],
                };
                for text in texts {
                    self.emit_assistant(stdout, &text);
                }
                let terminal_reason = if self.script == "failed-turn" {
                    "failed".to_owned()
                } else if self.script == "secret-reason" {
                    // The report path is a pass_env secret: the committed
                    // status must carry the redaction marker, never the path.
                    format!("completed {}", self.report_path.to_string_lossy())
                } else {
                    "completed".to_owned()
                };
                self.emit_turn_result(stdout, &terminal_reason);
                true
            }
            _ => true,
        }
    }

    fn initialize_response(&self) -> Value {
        let models = if self.script == "model-mismatch" {
            json!([{"value": "other-model", "resolvedModel": "other-model"}])
        } else {
            json!([{"value": MODEL, "resolvedModel": MODEL}])
        };
        let session_state = if self.script == "not-idle" {
            "busy"
        } else {
            "idle"
        };
        json!({
            "models": models,
            "current_permission_mode": "default",
            "pid": std::process::id(),
            "session_state": session_state,
            "account": {"uuid": "fake-account"},
        })
    }

    fn emit_assistant(&self, stdout: &mut std::io::StdoutLock, text: &str) {
        self.emit(
            stdout,
            json!({
                "type": "assistant",
                "session_id": self.argv_session,
                "message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": text}],
                },
            }),
        );
    }

    fn emit_turn_result(&self, stdout: &mut std::io::StdoutLock, terminal_reason: &str) {
        self.emit(
            stdout,
            json!({
                "type": "result",
                "subtype": "success",
                "session_id": self.argv_session,
                "terminal_reason": terminal_reason,
                "total_cost_usd": 0.01,
                "usage": {"input_tokens": 10, "output_tokens": 20},
                "modelUsage": {MODEL: {"input_tokens": 10, "output_tokens": 20}},
                "permission_denials": [],
            }),
        );
    }

    fn emit(&self, stdout: &mut std::io::StdoutLock, record: Value) {
        writeln!(stdout, "{}", record).expect("fixture stdout");
    }

    /// The report is rewritten after every frame so a killed CLI still
    /// leaves exact counts behind. The write is atomic (temp + rename) so a
    /// concurrently read report is never truncated.
    fn write_report(&self) {
        let report = json!({
            "argv": self.argv,
            "session_id": self.argv_session,
            "model": self.argv_model,
            "controls": self.controls,
            "user_inputs": self.user_inputs,
            "exit": self.exit,
        });
        let text = serde_json::to_string(&report).expect("report JSON");
        let tmp = self.report_path.with_extension("tmp");
        std::fs::write(&tmp, &text).expect("report tmp");
        std::fs::rename(&tmp, &self.report_path).expect("report rename");
    }
}

// ---------------------------------------------------------------------------
// Host side: drive the real plugin binary over the Eliot plugin protocol
// ---------------------------------------------------------------------------

/// Owned native children recorded while one scenario runs, so the scenario
/// timeout path can verify the exact owned processes — never by name.
///
/// The plugin child is owned by [`Host`] and dies by its `kill_on_drop`
/// handle whenever the scenario future is dropped. The fake CLI is owned by
/// the plugin and exits on stdin EOF; its exit is observed through the
/// `exit` marker it writes into its report (atomic temp + rename, so a
/// concurrently read report is never truncated).
#[derive(Clone, Default)]
struct OwnedChildren {
    inner: std::sync::Arc<Mutex<OwnedChildrenInner>>,
}

#[derive(Default)]
struct OwnedChildrenInner {
    report: Option<PathBuf>,
    cli_pid: Option<u64>,
}

impl OwnedChildren {
    async fn set_report(&self, report: PathBuf) {
        self.inner.lock().await.report = Some(report);
    }

    async fn set_cli_pid(&self, pid: u64) {
        self.inner.lock().await.cli_pid = Some(pid);
    }

    /// True once the fake CLI's exit marker is observed, when the scenario
    /// never spawned one, or when its report never appeared (the CLI never
    /// started). Bounded by [`CHILD_GRACE`]: never hangs the run.
    async fn cli_exited(&self) -> bool {
        let report = self.inner.lock().await.report.clone();
        let Some(report) = report else {
            return true;
        };
        let deadline = tokio::time::Instant::now() + CHILD_GRACE;
        loop {
            if let Ok(text) = std::fs::read_to_string(&report)
                && let Ok(value) = serde_json::from_str::<Value>(&text)
                && value.get("exit").and_then(Value::as_str).is_some()
            {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                // A live CLI writes its report within milliseconds of exec;
                // a report that never appeared means it never started.
                return !report.exists();
            }
            tokio::time::sleep(EXIT_POLL).await;
        }
    }

    async fn describe(&self) -> String {
        let inner = self.inner.lock().await;
        format!(
            "report={}, cli_pid={}",
            inner.report.as_ref().map_or("none".to_owned(), |path| path
                .to_string_lossy()
                .into_owned()),
            inner
                .cli_pid
                .map_or("none".to_owned(), |pid| pid.to_string())
        )
    }
}

struct HostRequest {
    id: Value,
    method: String,
    params: Value,
}

type PendingCalls = std::sync::Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;

struct Host {
    child: tokio::process::Child,
    writes: Option<mpsc::UnboundedSender<String>>,
    inbound: mpsc::UnboundedReceiver<HostRequest>,
    pending: PendingCalls,
    next_id: u64,
    next_seq: i64,
    stderr: Option<tokio::task::JoinHandle<String>>,
}

impl Host {
    async fn spawn(root: &std::path::Path) -> Self {
        let mut child = tokio::process::Command::new(CLI)
            .arg("--data-dir")
            .arg(root.join("data"))
            .arg("plugin")
            .arg("claude")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // A dropped scenario (timeout, panic) kills the owned plugin by
            // this handle; the fake CLI it owns then exits on stdin EOF.
            .kill_on_drop(true)
            .spawn()
            .expect("spawn claude plugin");
        let stdin = child.stdin.take().expect("plugin stdin");
        let stdout = child.stdout.take().expect("plugin stdout");
        let stderr = child.stderr.take().expect("plugin stderr");
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<String>();
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
        let pending: PendingCalls = std::sync::Arc::new(Mutex::new(HashMap::new()));
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let reader_pending = std::sync::Arc::clone(&pending);
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
                let frame: Value = serde_json::from_str(&line).expect("plugin frame JSON");
                if frame.get("method").is_some() {
                    let request = HostRequest {
                        id: frame.get("id").cloned().unwrap_or(Value::Null),
                        method: frame
                            .get("method")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        params: frame.get("params").cloned().unwrap_or(Value::Null),
                    };
                    if inbound_tx.send(request).is_err() {
                        break;
                    }
                } else {
                    let id = frame
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    let result = match frame.get("result") {
                        Some(result) => Ok(result.clone()),
                        None => Err(frame
                            .get("error")
                            .map(|error| error.to_string())
                            .unwrap_or_else(|| "missing result".to_owned())),
                    };
                    if let Some(sender) = reader_pending.lock().await.remove(&id) {
                        let _ = sender.send(result);
                    }
                }
            }
        });
        let stderr_handle = tokio::spawn(async move {
            let mut text = String::new();
            let mut reader = AsyncBufReader::new(stderr);
            let _ = reader.read_to_string(&mut text).await;
            text
        });
        Self {
            child,
            writes: Some(write_tx),
            inbound: inbound_rx,
            pending,
            next_id: 0,
            next_seq: 1,
            stderr: Some(stderr_handle),
        }
    }

    fn writer(&self) -> &mpsc::UnboundedSender<String> {
        self.writes.as_ref().expect("host write queue")
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let rx = self.begin_call(method, params).await;
        tokio::time::timeout(OP_TIMEOUT, rx)
            .await
            .expect("host call deadline")
            .expect("host call channel")
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
            .expect("host write queue");
        rx
    }

    async fn respond(&self, id: &Value, result: Value) {
        let frame = json!({"jsonrpc": "2.0", "id": id, "result": result});
        self.writer()
            .send(frame.to_string())
            .expect("host write queue");
    }

    async fn next_request(&mut self) -> HostRequest {
        tokio::time::timeout(OP_TIMEOUT, self.inbound.recv())
            .await
            .expect("host request deadline")
            .expect("plugin request channel")
    }

    /// Answer one `events.put` of any kind, returning `(kind, data)`.
    async fn answer_any_events_put(&mut self, agent_id: &str) -> (String, Value) {
        let request = self.next_request().await;
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
        let data = events[0].get("data").cloned().unwrap_or(Value::Null);
        assert_eq!(
            request.params.get("agent_id").and_then(Value::as_str),
            Some(agent_id)
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
        (kind, data)
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

struct Scenario {
    root: PathBuf,
    agent_id: String,
    host: Host,
}

impl Scenario {
    async fn start(script: &str, profile: Value, owned: &OwnedChildren) -> Self {
        let (scenario, _) = Self::start_raw(script, profile, true, owned).await;
        scenario
    }

    /// `expect_ready` pumps `process.put` plus (on success) the `session.ready`
    /// batch while `agent.start` is in flight. Returns the scenario and the
    /// start result on success, or the scenario parts with the error text.
    async fn start_raw(
        script: &str,
        profile: Value,
        expect_ready: bool,
        owned: &OwnedChildren,
    ) -> (Self, Result<(Value, Value), String>) {
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-claude-plugin-test-{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ));
        owned.set_report(root.join("report.json")).await;
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
        assert_eq!(manifest.get("name").and_then(Value::as_str), Some("claude"));
        assert_eq!(
            manifest.get("protocol").and_then(Value::as_str),
            Some("eliot-plugin/1")
        );
        assert_eq!(
            manifest["verbs"],
            json!(["launch", "send", "stop", "interrupt"])
        );
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
                        "ELIOT_FAKE_CLAUDE_SCRIPT": script,
                        "ELIOT_FAKE_CLAUDE_REPORT": root.join("report.json").to_string_lossy(),
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
        // `agent.start` only resolves after the CLI registers and (on
        // success) the session.ready batch commits; answer all host
        // requests while it is in flight.
        let mut ready_data: Option<Value> = None;
        let mut gapped = false;
        let outcome = loop {
            tokio::select! {
                biased;
                request = host.next_request() => {
                    match request.method.as_str() {
                        "process.put" => {
                            let pid = request.params.get("pid").and_then(Value::as_u64).unwrap_or(0);
                            assert!(pid > 0);
                            owned.set_cli_pid(pid).await;
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
                        "source.gap" => {
                            gapped = true;
                            let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                            host.respond(&request.id, json!({"gap_id": gap_id, "seq": host.next_seq})).await;
                            host.next_seq += 1;
                        }
                        other => panic!("unexpected host request during start: {other}"),
                    }
                }
                result = &mut rx => break result,
            }
        };
        let resolved = outcome
            .expect("agent.start deadline")
            .expect("agent.start response channel");
        let scenario = Self {
            root,
            agent_id,
            host,
        };
        match resolved {
            Ok(ready) if expect_ready => {
                assert_eq!(ready.get("state").and_then(Value::as_str), Some("ready"));
                assert_eq!(
                    ready.get("agent_id").and_then(Value::as_str),
                    Some(scenario.agent_id.as_str())
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
                    native_scope.starts_with("claude-config:"),
                    "native scope names the CLI config identity: {native_scope}"
                );
                let native_ref = ready_data
                    .get("native_ref")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                assert!(is_uuid(native_ref), "minted session id: {native_ref}");
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
                    ready["requested"]
                        .get("permission_mode")
                        .and_then(Value::as_str),
                    Some("workspace-write")
                );
                assert_eq!(
                    ready["effective"].get("model").and_then(Value::as_str),
                    Some(MODEL)
                );
                assert_eq!(
                    ready["effective"]
                        .get("permission_mode")
                        .and_then(Value::as_str),
                    Some("default")
                );
                assert_eq!(
                    ready["effective"]
                        .get("session_state")
                        .and_then(Value::as_str),
                    Some("idle")
                );
                assert!(
                    ready["effective"]
                        .get("pid")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        > 0,
                    "initialize pid readback"
                );
                assert!(!gapped, "no gap on the happy path");
                (scenario, Ok((ready, ready_data)))
            }
            Ok(ready) => (scenario, Ok((ready, ready_data.unwrap_or(Value::Null)))),
            Err(error) => (scenario, Err(error)),
        }
    }

    fn report(&self) -> Value {
        let text = std::fs::read_to_string(self.root.join("report.json")).expect("fake CLI report");
        serde_json::from_str(&text).expect("fake CLI report JSON")
    }

    /// Assert the fake CLI exited the expected way. Deterministic after
    /// stop: the plugin reaps the fake by handle before answering, and the
    /// marker write happens before the fake's exit.
    fn assert_cli_exit(&self, expected: &str) {
        assert_eq!(
            self.report().get("exit").and_then(Value::as_str),
            Some(expected),
            "fake CLI exit marker"
        );
    }
}

fn is_uuid(value: &str) -> bool {
    value.len() == 36
        && value
            .chars()
            .enumerate()
            .all(|(i, c)| matches!(i, 8 | 13 | 18 | 23) == (c == '-') || c.is_ascii_hexdigit())
}

fn claude_profile() -> Value {
    json!({
        "harness": "claude",
        "model": MODEL,
        "permissions": "workspace-write",
        "approval_policy": "never",
        "pass_env": ["ELIOT_FAKE_CLAUDE_SCRIPT", "ELIOT_FAKE_CLAUDE_REPORT"],
    })
}

/// One send round trip. The plugin only answers `agent.command` after the
/// exact `command_lifecycle` association commits `turn.started`, so inbound
/// batches are acked while the call is in flight; the remaining turn facts
/// are drained afterwards on either side of the response. Returns the send
/// id, the applied turn uuid and every committed `(kind, data)` in order.
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
        .expect("applied turn uuid")
        .to_owned();
    assert!(is_uuid(&turn_id), "minted turn uuid: {turn_id}");
    // The ack follows `turn.started`; later facts may already be committed
    // or still in flight. Drain the canonical turn sequence in commit order.
    assert_eq!(
        committed.first().map(|(kind, _)| kind.as_str()),
        Some("turn.started"),
        "association commits before the ack"
    );
    while committed.last().map(|(kind, _)| kind.as_str()) != Some("turn.completed") {
        match committed.last().map(|(kind, _)| kind.as_str()) {
            Some("turn.started") | Some("message.completed") => {}
            other => panic!("turn facts out of order after {other:?}"),
        }
        // Any number of `message.completed` batches may precede the terminal.
        let (kind, data) = host.answer_any_events_put(agent_id).await;
        assert!(
            kind == "message.completed" || kind == "turn.completed",
            "turn facts out of order: {kind}"
        );
        committed.push((kind, data));
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

async fn stop_agent(host: &mut Host, agent_id: &str) {
    let stop_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.stop",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": stop_id,
                "force": false,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut ended = false;
    let stopped = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("session.ended"));
                ended = true;
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
        .expect("stop response channel")
        .expect("stop applied");
    assert_eq!(
        stopped.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert!(ended, "stop commits session.ended");
}

/// Park one turn: the association commits, the terminal stays pending.
/// Returns the applied turn uuid. Only the parking scripts
/// (`interrupt-race`, `interrupt-queued`) withhold the terminal.
async fn park_turn(host: &mut Host, agent_id: &str, text: &str) -> String {
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
    let applied = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("turn.started"));
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
    applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("parked turn uuid")
        .to_owned()
}

/// Drain the interrupted turn's facts: the partial text is kept and the
/// terminal is `interrupted`.
async fn drain_interrupted_turn(host: &mut Host, agent_id: &str, turn_id: &str) {
    let mut saw_message = false;
    let mut saw_terminal = false;
    while !saw_terminal {
        let (kind, data) = host.answer_any_events_put(agent_id).await;
        match kind.as_str() {
            "message.completed" => {
                saw_message = true;
                assert_eq!(data.get("turn_id").and_then(Value::as_str), Some(turn_id));
            }
            "turn.completed" => {
                saw_terminal = true;
                assert_eq!(data.get("turn_id").and_then(Value::as_str), Some(turn_id));
                assert_eq!(
                    data.get("status").and_then(Value::as_str),
                    Some("interrupted")
                );
            }
            other => panic!("unexpected turn fact: {other}"),
        }
    }
    assert!(saw_message, "interrupted turn keeps its text");
}

async fn scenario_launch_send_snapshot_stop(owned: OwnedChildren) {
    let mut scenario = Scenario::start("ok", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (send_id, turn_id, committed) = send_turn(&mut scenario.host, &agent_id, "hello").await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        vec!["turn.started", "message.completed", "turn.completed"]
    );
    let terminal = committed
        .iter()
        .find(|(kind, _)| kind == "turn.completed")
        .map(|(_, data)| data)
        .expect("terminal data");
    assert_eq!(
        terminal.get("status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        terminal.get("final_text").and_then(Value::as_str),
        Some("fake reply to the turn.")
    );
    // Snapshot of the exact turn carries the committed terminal facts.
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "turn_id": turn_id,
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
        snapshot["last_turn"].get("status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        snapshot["last_turn"]
            .get("final_text")
            .and_then(Value::as_str),
        Some("fake reply to the turn.")
    );
    assert_eq!(
        snapshot.get("result_known").and_then(Value::as_bool),
        Some(true)
    );
    // Steer, inject and approve stay unsupported in PR1.
    for verb in ["steer", "inject", "approve"] {
        let error = scenario
            .host
            .call(
                "agent.command",
                json!({
                    "agent_id": agent_id,
                    "generation": 1,
                    "command_id": ulid::Ulid::new().to_string(),
                    "verb": verb,
                    "args": {"text": "x"},
                    "turn_id": turn_id,
                    "request_id": null,
                }),
            )
            .await
            .expect_err("verb must be unsupported");
        assert!(error.contains("unsupported"), "{verb}: {error}");
    }
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("stdin-eof");
    // The CLI ran with the exact stream-json surface and the minted ids.
    let report = scenario.report();
    let argv = report
        .get("argv")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let argv: Vec<&str> = argv.iter().filter_map(Value::as_str).collect();
    for flag in [
        "-p",
        "--input-format",
        "--output-format",
        "--verbose",
        "--session-id",
        "--model",
    ] {
        assert!(argv.contains(&flag), "argv carries {flag}: {argv:?}");
    }
    assert_eq!(report.get("model").and_then(Value::as_str), Some(MODEL));
    let native_ref = report
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(is_uuid(native_ref), "argv session id: {native_ref}");
    assert_eq!(report.get("user_inputs").and_then(Value::as_u64), Some(1));
    assert_eq!(
        report["controls"].get("initialize").and_then(Value::as_u64),
        Some(1)
    );
    assert!(report["controls"].get("interrupt").is_none());
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

async fn scenario_two_assistant_messages_snapshot_matches_stream(owned: OwnedChildren) {
    let mut scenario = Scenario::start("two-messages", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (send_id, _, committed) = send_turn(&mut scenario.host, &agent_id, "hello").await;
    let messages: Vec<&str> = committed
        .iter()
        .filter(|(kind, _)| kind == "message.completed")
        .map(|(_, data)| data.get("text").and_then(Value::as_str).unwrap_or_default())
        .collect();
    assert_eq!(messages, vec!["fake part one. ", "fake part two."]);
    let terminal = committed
        .iter()
        .find(|(kind, _)| kind == "turn.completed")
        .map(|(_, data)| data)
        .expect("terminal data");
    // The stream fold concatenates the turn's assistant text.
    assert_eq!(
        terminal.get("final_text").and_then(Value::as_str),
        Some("fake part one. fake part two.")
    );
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
            }),
        )
        .await
        .expect("snapshot");
    assert_eq!(
        snapshot["last_turn"]
            .get("final_text")
            .and_then(Value::as_str),
        Some("fake part one. fake part two.")
    );
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_failed_turn_reports_terminal_reason(owned: OwnedChildren) {
    let mut scenario = Scenario::start("failed-turn", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, "hello").await;
    let terminal = committed
        .iter()
        .find(|(kind, _)| kind == "turn.completed")
        .map(|(_, data)| data)
        .expect("terminal data");
    assert_eq!(
        terminal.get("status").and_then(Value::as_str),
        Some("failed")
    );
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_terminal_reason_is_redacted(owned: OwnedChildren) {
    let mut scenario = Scenario::start("secret-reason", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, "hello").await;
    let terminal = committed
        .iter()
        .find(|(kind, _)| kind == "turn.completed")
        .map(|(_, data)| data)
        .expect("terminal data");
    assert_eq!(
        terminal.get("status").and_then(Value::as_str),
        Some("completed [REDACTED]")
    );
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_cancelled_input_is_rejected_without_rewrite(owned: OwnedChildren) {
    let mut scenario = Scenario::start("cancelled-turn", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let send_id = ulid::Ulid::new().to_string();
    let send_params = json!({
        "agent_id": agent_id,
        "generation": 1,
        "command_id": send_id,
        "verb": "send",
        "args": {"text": "hello"},
        "turn_id": null,
        "request_id": null,
    });
    let rx = scenario
        .host
        .begin_call("agent.command", send_params.clone())
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    // A cancelled input commits nothing: the first inbound frame must be
    // the rejection, never a host request.
    let rejected = tokio::select! {
        biased;
        request = scenario.host.next_request() => {
            panic!("cancelled input commits nothing, plugin asked {}", request.method);
        }
        result = &mut rx => result,
    };
    let rejected = rejected
        .expect("send deadline")
        .expect("send response channel")
        .expect("send rejected");
    assert_eq!(
        rejected.get("state").and_then(Value::as_str),
        Some("rejected")
    );
    // An exact retry replays the retained rejection without a second write.
    let replay = scenario
        .host
        .call("agent.command", send_params)
        .await
        .expect("rejected replay");
    assert_eq!(
        replay.get("state").and_then(Value::as_str),
        Some("rejected")
    );
    assert_eq!(
        scenario.report().get("user_inputs").and_then(Value::as_u64),
        Some(1)
    );
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_cancelled_active_turn_reaches_terminal(owned: OwnedChildren) {
    let mut scenario = Scenario::start("cancel-active", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (send_id, _, committed) = send_turn(&mut scenario.host, &agent_id, "hello").await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        vec!["turn.started", "turn.completed"]
    );
    let terminal = committed
        .iter()
        .find(|(kind, _)| kind == "turn.completed")
        .map(|(_, data)| data)
        .expect("terminal data");
    assert_eq!(
        terminal.get("status").and_then(Value::as_str),
        Some("cancelled")
    );
    // The turn is terminal, not stuck active: the session is idle and a
    // following send is not busy.
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
            }),
        )
        .await
        .expect("snapshot");
    assert_eq!(snapshot.get("state").and_then(Value::as_str), Some("idle"));
    assert_eq!(
        snapshot["last_turn"].get("status").and_then(Value::as_str),
        Some("cancelled")
    );
    let (_, _, recommitted) = send_turn(&mut scenario.host, &agent_id, "again").await;
    assert_eq!(
        recommitted
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        vec!["turn.started", "turn.completed"]
    );
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_dropped_input_is_unknown_without_rewrite(owned: OwnedChildren) {
    let mut scenario = Scenario::start("drop-input", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let send_id = ulid::Ulid::new().to_string();
    let send_params = json!({
        "agent_id": agent_id,
        "generation": 1,
        "command_id": send_id,
        "verb": "send",
        "args": {"text": "hello"},
        "turn_id": null,
        "request_id": null,
    });
    let rx = scenario
        .host
        .begin_call("agent.command", send_params.clone())
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut gapped = false;
    let outcome = loop {
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                assert_eq!(request.method, "source.gap");
                gapped = true;
                let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                scenario.host.respond(&request.id, json!({"gap_id": gap_id, "seq": scenario.host.next_seq})).await;
                scenario.host.next_seq += 1;
            }
            result = &mut rx => break result,
        }
    };
    let error = outcome
        .expect("send deadline")
        .expect("send response channel")
        .expect_err("dropped input is unknown");
    assert!(error.contains("unknown"), "{error}");
    assert!(gapped, "stream loss is an explicit gap");
    // An exact retry stays unknown without a second write.
    let retry = scenario.host.begin_call("agent.command", send_params).await;
    let retry = tokio::time::timeout(OP_TIMEOUT, retry)
        .await
        .expect("retry deadline")
        .expect("retry channel")
        .expect_err("retry stays unknown");
    assert!(retry.contains("unknown"), "{retry}");
    assert_eq!(
        scenario.report().get("user_inputs").and_then(Value::as_u64),
        Some(1)
    );
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("scripted-drop");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_interrupt_applied_then_stale(owned: OwnedChildren) {
    let mut scenario = Scenario::start("interrupt-race", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let turn_id = park_turn(&mut scenario.host, &agent_id, "park me").await;
    // The bound active turn interrupts exactly once.
    let interrupted = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "interrupt",
                "args": {},
                "turn_id": turn_id,
                "request_id": null,
            }),
        )
        .await
        .expect("interrupt applied");
    assert_eq!(
        interrupted.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(
        interrupted.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert_eq!(interrupted.get("still_queued"), Some(&json!([])));
    // The terminal follows on either side of the interrupt response.
    drain_interrupted_turn(&mut scenario.host, &agent_id, &turn_id).await;
    // After the terminal, the same turn refuses without a second write.
    let stale = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "interrupt",
                "args": {},
                "turn_id": turn_id,
                "request_id": null,
            }),
        )
        .await
        .expect_err("interrupt after terminal is stale");
    assert!(stale.contains("stale_turn"), "{stale}");
    assert_eq!(
        scenario.report()["controls"]
            .get("interrupt")
            .and_then(Value::as_u64),
        Some(1)
    );
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_interrupt_reports_nonempty_still_queued(owned: OwnedChildren) {
    let mut scenario = Scenario::start("interrupt-queued", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let turn_id = park_turn(&mut scenario.host, &agent_id, "park me").await;
    let interrupted = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "interrupt",
                "args": {},
                "turn_id": turn_id,
                "request_id": null,
            }),
        )
        .await
        .expect("interrupt applied");
    assert_eq!(
        interrupted.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(
        interrupted.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert_eq!(
        interrupted.get("still_queued"),
        Some(&json!(["queued-turn-1"]))
    );
    drain_interrupted_turn(&mut scenario.host, &agent_id, &turn_id).await;
    stop_agent(&mut scenario.host, &agent_id).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_model_not_in_catalogue_rejected_before_ready(owned: OwnedChildren) {
    let (mut scenario, outcome) =
        Scenario::start_raw("model-mismatch", claude_profile(), false, &owned).await;
    let error = outcome.expect_err("unknown model must fail start");
    assert!(error.contains("profile_error"), "{error}");
    assert_eq!(
        scenario.report()["controls"]
            .get("initialize")
            .and_then(Value::as_u64),
        Some(1)
    );
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_session_mismatch_is_unknown_with_gap(owned: OwnedChildren) {
    let (mut scenario, outcome) =
        Scenario::start_raw("session-mismatch", claude_profile(), false, &owned).await;
    let error = outcome.expect_err("session mismatch must fail start");
    assert!(error.contains("unknown"), "{error}");
    // Shutdown still settles the owned session: `session.ended` commits
    // while the shutdown call is in flight.
    let rx = scenario.host.begin_call("shutdown", json!({})).await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut ended = false;
    let shutdown = loop {
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("session.ended"));
                ended = true;
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
    let shutdown = shutdown
        .expect("shutdown deadline")
        .expect("shutdown channel")
        .expect("shutdown");
    assert_eq!(
        shutdown.get("state").and_then(Value::as_str),
        Some("shutdown")
    );
    assert!(ended, "shutdown commits session.ended");
    scenario.host.shutdown_child().await;
}

async fn scenario_not_idle_rejected_before_ready(owned: OwnedChildren) {
    let (mut scenario, outcome) =
        Scenario::start_raw("not-idle", claude_profile(), false, &owned).await;
    let error = outcome.expect_err("non-idle initialize must fail start");
    assert!(error.contains("profile_error"), "{error}");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_invalid_profile_rejected_before_spawn(_owned: OwnedChildren) {
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-claude-plugin-test-{}",
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
    // Missing model, then missing explicit cmd, then a profile for another
    // harness: all fail before any spawn.
    for profile in [
        json!({"harness": "claude"}),
        json!({"harness": "claude", "model": MODEL}),
        json!({"harness": "muse", "model": MODEL}),
    ] {
        let outcome = host
            .call(
                "agent.start",
                json!({
                    "agent_id": agent_id,
                    "generation": 1,
                    "command_id": ulid::Ulid::new().to_string(),
                    "profile": profile,
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
        let error = outcome.expect_err("invalid profile must fail start");
        assert!(error.contains("profile_error"), "{error}");
    }
    host.call("shutdown", json!({})).await.expect("shutdown");
    host.shutdown_child().await;
    assert!(
        !root.join("report.json").exists(),
        "no native process spawns for an invalid profile"
    );
    let _ = std::fs::remove_dir_all(&root);
}
