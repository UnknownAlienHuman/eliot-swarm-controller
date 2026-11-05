//! Deterministic recorded-native-wire tests for Codex native-child observation.
//!
//! Re-executes the test executable as a fake Codex app-server on stdio
//! when `ELIOT_FAKE_CODEX_SCRIPT` is set in the environment.

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[allow(dead_code)]
#[path = "support/mod.rs"]
mod support;
use support::{Fixture, cli};

fn main() {
    if let Ok(script) = std::env::var("ELIOT_FAKE_CODEX_SCRIPT") {
        fake_codex_main(&script);
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        scenario_native_child_observation().await;
    });
    println!("codex_plugin: all scenarios passed");
}

struct FakeCodex {
    script: String,
    report_path: PathBuf,
    codex_home: String,
    rejected_unknown_request: bool,
}

impl FakeCodex {
    fn new(script: &str) -> Self {
        let report_path = std::env::var("ELIOT_FAKE_CODEX_REPORT")
            .map(PathBuf::from)
            .expect("ELIOT_FAKE_CODEX_REPORT");
        let codex_home = std::env::var("CODEX_HOME").expect("CODEX_HOME fixture selector");
        Self {
            script: script.to_owned(),
            report_path,
            codex_home,
            rejected_unknown_request: false,
        }
    }

    fn run(&mut self) {
        let stdin = std::io::stdin().lock();
        let lines = BufReader::new(stdin).lines();
        let mut stdout = std::io::stdout().lock();
        for line in lines {
            let line = line.expect("fake codex stdin line");
            if line.trim().is_empty() {
                continue;
            }
            let frame: Value = serde_json::from_str(&line).expect("valid JSON-RPC frame");
            if frame.get("id") == Some(&json!(888)) {
                if frame
                    .get("error")
                    .and_then(|e| e.get("code"))
                    .and_then(Value::as_i64)
                    == Some(-32601)
                {
                    self.rejected_unknown_request = true;
                    self.write_report();
                }
                continue;
            }
            if frame.get("id").is_none() {
                continue; // Notification (`initialized`)
            }
            if frame.get("method").is_none() {
                continue;
            }
            if !self.handle_call(&frame, &mut stdout) {
                break;
            }
            stdout.flush().expect("flush fake codex stdout");
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

        if self.handle_init_and_config(method, &id, stdout) {
            return true;
        }
        if self.handle_thread_and_turn(method, &id, &params, stdout) {
            return true;
        }
        self.reply_error(stdout, &id, -32601, "unsupported");
        true
    }

    fn handle_init_and_config(
        &self,
        method: &str,
        id: &Value,
        stdout: &mut std::io::StdoutLock,
    ) -> bool {
        match method {
            "initialize" => {
                self.reply(
                    stdout,
                    id,
                    json!({
                        "codexHome": self.codex_home,
                        "platformFamily": "windows",
                        "platformOs": "windows",
                        "userAgent": "codex-test",
                    }),
                );
            }
            "config/read" => {
                self.reply(
                    stdout,
                    id,
                    json!({
                        "config": {
                            "sqlite_home": self.codex_home,
                            "model_provider": "openai",
                            "openai_base_url": "https://chatgpt.com/backend-api/codex",
                        },
                        "origins": {
                            "sqlite_home": {
                                "name": {"type": "sessionFlags"},
                                "version": "1.0",
                            },
                        },
                        "layers": [{
                            "config": {"sqlite_home": self.codex_home},
                            "name": {"type": "sessionFlags"},
                            "version": "1.0",
                            "disabledReason": null,
                        }],
                    }),
                );
            }
            "configRequirements/read" => {
                self.reply(stdout, id, json!({"requirements": null}));
            }
            "account/read" => {
                self.reply(stdout, id, json!({"account": {"type": "chatgpt"}}));
            }
            "model/list" => {
                self.reply(
                    stdout,
                    id,
                    json!({
                        "data": [{
                            "id": "test-model",
                            "model": "test-model",
                            "displayName": "Test Model",
                            "description": "Test Model",
                            "hidden": false,
                            "isDefault": true,
                            "defaultReasoningEffort": "medium",
                            "supportedReasoningEfforts": [{
                                "description": "Medium effort",
                                "reasoningEffort": "medium",
                            }],
                        }]
                    }),
                );
            }
            _ => return false,
        }
        true
    }

    fn handle_thread_and_turn(
        &mut self,
        method: &str,
        id: &Value,
        params: &Value,
        stdout: &mut std::io::StdoutLock,
    ) -> bool {
        match method {
            "thread/start" => {
                self.emit(
                    stdout,
                    "thread/started",
                    json!({"thread": {"id": "th-parent", "source": "appServer"}}),
                );
                let cwd = params
                    .get("cwd")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.reply(
                    stdout,
                    id,
                    json!({
                        "thread": {"id": "th-parent"},
                        "model": "test-model",
                        "modelProvider": "openai",
                        "cwd": cwd,
                        "sandbox": "workspace-write",
                        "approvalPolicy": "never",
                        "reasoningEffort": "medium",
                    }),
                );
            }
            "turn/start" => {
                self.reply(
                    stdout,
                    id,
                    json!({"turn": {"id": "turn-p1", "items": [], "status": "inProgress"}}),
                );
                stdout.flush().expect("flush turn/start reply");
                self.run_scenario_wire(stdout);
            }
            _ => return false,
        }
        true
    }

    fn run_scenario_wire(&mut self, stdout: &mut std::io::StdoutLock) {
        self.emit_parent_and_early_child(stdout);
        self.emit_confirm_and_nested_child(stdout);
        self.emit_above_fanout_and_conflict(stdout);
        self.emit_child_request_and_completions(stdout);
        self.write_report();
    }

    fn emit_parent_and_early_child(&self, stdout: &mut std::io::StdoutLock) {
        self.emit(
            stdout,
            "turn/started",
            json!({
                "threadId": "th-parent",
                "turn": {"id": "turn-p1", "items": [], "status": "inProgress"},
            }),
        );
        self.emit(
            stdout,
            "item/started",
            json!({
                "threadId": "th-parent",
                "turnId": "turn-p1",
                "startedAtMs": 1_000,
                "item": {
                    "type": "collabAgentToolCall",
                    "id": "spawn-c1",
                    "tool": "spawnAgent",
                    "status": "inProgress",
                    "senderThreadId": "th-parent",
                    "receiverThreadIds": [],
                    "agentsStates": {},
                },
            }),
        );
        self.emit(
            stdout,
            "turn/started",
            json!({
                "threadId": "th-child-1",
                "turn": {"id": "turn-c1", "items": [], "status": "inProgress"},
            }),
        );
    }

    fn emit_confirm_and_nested_child(&self, stdout: &mut std::io::StdoutLock) {
        self.emit(
            stdout,
            "item/completed",
            json!({
                "threadId": "th-parent",
                "turnId": "turn-p1",
                "completedAtMs": 1_000,
                "item": {
                    "type": "collabAgentToolCall",
                    "id": "spawn-c1",
                    "tool": "spawnAgent",
                    "status": "completed",
                    "senderThreadId": "th-parent",
                    "receiverThreadIds": ["th-child-1"],
                    "agentsStates": {},
                },
            }),
        );
        let c1_thread_started = json!({
            "thread": {
                "id": "th-child-1",
                "source": {
                    "subAgent": {
                        "thread_spawn": {
                            "parent_thread_id": "th-parent",
                            "depth": 1,
                        }
                    }
                },
            }
        });
        self.emit(stdout, "thread/started", c1_thread_started.clone());
        self.emit(stdout, "thread/started", c1_thread_started);

        self.emit(
            stdout,
            "item/started",
            json!({
                "threadId": "th-child-1",
                "turnId": "turn-c1",
                "startedAtMs": 1_000,
                "item": {
                    "type": "collabAgentToolCall",
                    "id": "spawn-nested",
                    "tool": "spawnAgent",
                    "status": "inProgress",
                    "senderThreadId": "th-child-1",
                    "receiverThreadIds": [],
                    "agentsStates": {},
                },
            }),
        );
        self.emit(
            stdout,
            "thread/started",
            json!({
                "thread": {
                    "id": "th-child-nested",
                    "source": {
                        "subAgent": {
                            "thread_spawn": {
                                "parent_thread_id": "th-child-1",
                                "depth": 2,
                            }
                        }
                    },
                }
            }),
        );
        self.emit(
            stdout,
            "item/completed",
            json!({
                "threadId": "th-child-1",
                "turnId": "turn-c1",
                "completedAtMs": 1_000,
                "item": {
                    "type": "collabAgentToolCall",
                    "id": "spawn-nested",
                    "tool": "spawnAgent",
                    "status": "completed",
                    "senderThreadId": "th-child-1",
                    "receiverThreadIds": ["th-child-nested"],
                    "agentsStates": {},
                },
            }),
        );
        self.emit(
            stdout,
            "turn/started",
            json!({
                "threadId": "th-child-nested",
                "turn": {"id": "turn-nested-1", "items": [], "status": "inProgress"},
            }),
        );
    }

    fn emit_above_fanout_and_conflict(&self, stdout: &mut std::io::StdoutLock) {
        self.emit(
            stdout,
            "item/started",
            json!({
                "threadId": "th-parent",
                "turnId": "turn-p1",
                "startedAtMs": 1_000,
                "item": {
                    "type": "collabAgentToolCall",
                    "id": "spawn-c2",
                    "tool": "spawnAgent",
                    "status": "inProgress",
                    "senderThreadId": "th-parent",
                    "receiverThreadIds": [],
                    "agentsStates": {},
                },
            }),
        );
        self.emit(
            stdout,
            "item/completed",
            json!({
                "threadId": "th-parent",
                "turnId": "turn-p1",
                "completedAtMs": 1_000,
                "item": {
                    "type": "collabAgentToolCall",
                    "id": "spawn-c2",
                    "tool": "spawnAgent",
                    "status": "completed",
                    "senderThreadId": "th-parent",
                    "receiverThreadIds": ["th-child-2"],
                    "agentsStates": {},
                },
            }),
        );
        self.emit(
            stdout,
            "thread/started",
            json!({
                "thread": {
                    "id": "th-child-2",
                    "source": {
                        "subAgent": {
                            "thread_spawn": {
                                "parent_thread_id": "th-parent",
                                "depth": 1,
                            }
                        }
                    },
                }
            }),
        );
        self.emit(
            stdout,
            "thread/started",
            json!({
                "thread": {
                    "id": "th-conflict",
                    "source": {
                        "subAgent": {
                            "thread_spawn": {
                                "parent_thread_id": "th-unknown-parent",
                                "depth": 1,
                            }
                        }
                    },
                }
            }),
        );
    }

    fn emit_child_request_and_completions(&self, stdout: &mut std::io::StdoutLock) {
        self.emit_request(
            stdout,
            &json!(888),
            "item/commandExecution/requestApproval",
            json!({
                "threadId": "th-child-1",
                "turnId": "turn-c1",
                "itemId": "cmd-c1",
                "startedAtMs": 1_000,
            }),
        );
        stdout.flush().expect("flush child request 888");

        self.emit(
            stdout,
            "item/completed",
            json!({
                "threadId": "th-parent",
                "turnId": "turn-p1",
                "completedAtMs": 1_000,
                "item": {
                    "type": "agentMessage",
                    "id": "msg-p1",
                    "text": "Parent completed first.",
                },
            }),
        );
        self.emit(
            stdout,
            "turn/completed",
            json!({
                "threadId": "th-parent",
                "turn": {"id": "turn-p1", "items": [], "status": "completed"},
            }),
        );
        stdout.flush().expect("flush parent completion");

        std::thread::sleep(Duration::from_millis(150));
        self.emit(
            stdout,
            "item/completed",
            json!({
                "threadId": "th-child-1",
                "turnId": "turn-c1",
                "completedAtMs": 1_000,
                "item": {
                    "type": "agentMessage",
                    "id": "msg-c1",
                    "text": "Child completed after parent.",
                },
            }),
        );
        self.emit(
            stdout,
            "turn/completed",
            json!({
                "threadId": "th-child-1",
                "turn": {"id": "turn-c1", "items": [], "status": "completed"},
            }),
        );
        self.emit(
            stdout,
            "item/completed",
            json!({
                "threadId": "th-child-nested",
                "turnId": "turn-nested-1",
                "completedAtMs": 1_000,
                "item": {
                    "type": "agentMessage",
                    "id": "msg-nested-1",
                    "text": "Nested completed.",
                },
            }),
        );
        self.emit(
            stdout,
            "turn/completed",
            json!({
                "threadId": "th-child-nested",
                "turn": {"id": "turn-nested-1", "items": [], "status": "completed"},
            }),
        );
        stdout.flush().expect("flush child completions");
    }

    fn reply(&self, stdout: &mut std::io::StdoutLock, id: &Value, result: Value) {
        let frame = json!({"jsonrpc": "2.0", "id": id, "result": result});
        writeln!(stdout, "{}", frame).expect("fixture reply");
    }

    fn reply_error(&self, stdout: &mut std::io::StdoutLock, id: &Value, code: i64, message: &str) {
        let frame = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message},
        });
        writeln!(stdout, "{}", frame).expect("fixture error reply");
    }

    fn emit(&self, stdout: &mut std::io::StdoutLock, method: &str, params: Value) {
        let frame = json!({"jsonrpc": "2.0", "method": method, "params": params});
        writeln!(stdout, "{}", frame).expect("fixture notification");
    }

    fn emit_request(
        &self,
        stdout: &mut std::io::StdoutLock,
        id: &Value,
        method: &str,
        params: Value,
    ) {
        let frame = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        writeln!(stdout, "{}", frame).expect("fixture request");
    }

    fn write_report(&self) {
        let report = json!({
            "script": self.script,
            "rejected_unknown_request": self.rejected_unknown_request,
        });
        let _ = std::fs::write(&self.report_path, serde_json::to_string(&report).unwrap());
    }
}

fn fake_codex_main(script: &str) {
    let mut fake = FakeCodex::new(script);
    fake.run();
}

async fn scenario_native_child_observation() {
    let mut fixture = Fixture::new("codex-children");
    let report_path = fixture.root.join("report.json");
    let codex_home = fixture.root.join("codex_home");
    std::fs::create_dir_all(&codex_home).expect("create codex_home");

    fixture
        .env("ELIOT_FAKE_CODEX_SCRIPT", "scenario-children")
        .env("ELIOT_FAKE_CODEX_REPORT", &report_path)
        .env("CODEX_HOME", &codex_home);

    let self_exe = std::env::current_exe().expect("current test exe");
    let config_toml = format!(
        r#"[profiles.codex_test]
harness = "codex"
cmd = {:?}
model = "test-model"
effort = "medium"
pass_env = [
    "ELIOT_FAKE_CODEX_SCRIPT",
    "ELIOT_FAKE_CODEX_REPORT",
    "CODEX_HOME",
]
"#,
        self_exe.to_string_lossy()
    );
    std::fs::create_dir_all(&fixture.data).expect("create data dir");
    std::fs::write(fixture.data.join("config.toml"), config_toml).expect("write config.toml");

    fixture.start();

    let launch_reply = cli(
        &fixture.data,
        &[
            "launch",
            "--profile",
            "codex_test",
            "--workspace",
            fixture.workspace.to_str().unwrap(),
            "--wait",
        ],
    );
    let launch_json = launch_reply.json();
    let root_agent_id = launch_json["target"]
        .as_str()
        .or_else(|| launch_json["id"].as_str())
        .expect("launched agent id")
        .to_owned();

    let _send_reply = cli(
        &fixture.data,
        &["send", &root_agent_id, "run children", "--wait"],
    );

    let (child_1_id, nested_id, child_2_id) = verify_top_agents(&fixture.data, &root_agent_id);

    verify_show_agents(
        &fixture.data,
        &root_agent_id,
        &child_1_id,
        &nested_id,
        &child_2_id,
    );

    verify_events_and_report(&fixture, &report_path, &root_agent_id, &child_1_id);

    fixture.stop(false);
}

fn verify_top_agents(data: &Path, root_id: &str) -> (String, String, String) {
    let top_reply = cli(data, &["top"]);
    let top_json = top_reply.json();
    let agents = top_json["agents"].as_array().expect("agents array");

    let root = agents
        .iter()
        .find(|a| a["id"] == root_id)
        .expect("root agent in top");
    assert_eq!(root["depth"], 0);

    let c1 = agents
        .iter()
        .find(|a| a["native_ref"] == "th-child-1")
        .expect("child-1 in top");
    assert_eq!(c1["depth"], 1);
    assert_eq!(c1["parent"], root_id);
    assert_eq!(c1["root"], root_id);
    assert_eq!(c1["origin"], "native");
    assert_eq!(c1["observe_only"], true);
    let c1_id = c1["id"].as_str().unwrap().to_owned();

    let nested = agents
        .iter()
        .find(|a| a["native_ref"] == "th-child-nested")
        .expect("nested in top");
    assert_eq!(nested["depth"], 2);
    assert_eq!(nested["parent"], c1_id);
    assert_eq!(nested["root"], root_id);
    assert_eq!(nested["origin"], "native");
    assert_eq!(nested["observe_only"], true);
    let nested_id = nested["id"].as_str().unwrap().to_owned();

    let c2 = agents
        .iter()
        .find(|a| a["native_ref"] == "th-child-2")
        .expect("child-2 in top");
    assert_eq!(c2["depth"], 1);
    assert_eq!(c2["parent"], root_id);
    assert_eq!(c2["root"], root_id);
    let c2_id = c2["id"].as_str().unwrap().to_owned();

    (c1_id, nested_id, c2_id)
}

fn verify_show_agents(data: &Path, root_id: &str, c1_id: &str, nested_id: &str, c2_id: &str) {
    let show_1 = cli(data, &["show", c1_id]).json();
    let a1 = show_1.get("agent").unwrap_or(&show_1);
    assert_eq!(a1["id"], c1_id);
    assert_eq!(a1["depth"], 1);
    assert_eq!(a1["parent"], root_id);
    assert_eq!(a1["root"], root_id);
    assert_eq!(a1["native_ref"], "th-child-1");
    assert_eq!(a1["origin"], "native");
    assert_eq!(a1["observe_only"], true);

    let show_nested = cli(data, &["show", nested_id]).json();
    let an = show_nested.get("agent").unwrap_or(&show_nested);
    assert_eq!(an["id"], nested_id);
    assert_eq!(an["depth"], 2);
    assert_eq!(an["parent"], c1_id);
    assert_eq!(an["root"], root_id);
    assert_eq!(an["native_ref"], "th-child-nested");

    let show_2 = cli(data, &["show", c2_id]).json();
    let a2 = show_2.get("agent").unwrap_or(&show_2);
    assert_eq!(a2["id"], c2_id);
    assert_eq!(a2["depth"], 1);
    assert_eq!(a2["parent"], root_id);
}

fn verify_events_and_report(fixture: &Fixture, report_path: &Path, root_id: &str, c1_id: &str) {
    let events = fixture.events();
    let parent_turn_completed_idx = events
        .iter()
        .position(|e| e["kind"] == "turn.completed" && e["agent"] == root_id)
        .expect("parent turn.completed event");

    let child_1_turn_completed_idx = events
        .iter()
        .position(|e| e["kind"] == "turn.completed" && e["agent"] == c1_id)
        .expect("child-1 turn.completed event");
    assert!(
        child_1_turn_completed_idx > parent_turn_completed_idx,
        "child turn completed after parent"
    );

    let child_msg = events
        .iter()
        .find(|e| e["kind"] == "message.completed" && e["agent"] == c1_id)
        .expect("child-1 message.completed event");
    assert_eq!(child_msg["data"]["text"], "Child completed after parent.");

    let report_content = std::fs::read_to_string(report_path).expect("read report");
    let report: Value = serde_json::from_str(&report_content).expect("parse report");
    assert_eq!(
        report["rejected_unknown_request"], true,
        "unknown child request must be rejected with -32601"
    );
}
