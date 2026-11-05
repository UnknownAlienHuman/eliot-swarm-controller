//! Connected cross-harness mail/CAS regression: AGY + Claude fakes.
//!
//! This target runs with `harness = false` so the test binary can re-execute
//! itself as both fake native CLIs: the daemon spawns `cmd` for each vendor
//! profile, and a libtest harness would corrupt those stdio streams. When
//! `ELIOT_FAKE_AGY_SCRIPT` is set, `main` runs the minimal AGY fixture;
//! when `ELIOT_FAKE_CLAUDE_SCRIPT` is set, it runs the minimal Claude fixture
//! (including `auth status`); otherwise it runs the owner coordination.
//!
//! Required path: owner CLI -> Router/Store -> two different real vendor
//! plugins (`antigravity`, `claude`) -> their native fake speaks the smallest
//! existing happy conversation, invokes the real `swarm` CLI under its
//! automatically inherited `ELIOT_AGENT_*` principal, and reports only
//! semantic outputs in the native terminal text. The owner only launches,
//! sends narrow deterministic instructions carrying the peer Agent id, and
//! reads final assertions. Agent tokens never leave process env: the fakes
//! never read or print them, and the owner never performs an Agent action.
//!
//! Native shapes follow the existing fixtures: AGY 1.5.0
//! (`plugins/antigravity/tests/fixtures/agy-1.5.0-soft-denial.ndjson`,
//! `init` first, then `user_input`/`agent_response` steps and a `result`
//! whose `response` is final) and the controller-verified Claude stream-json
//! (`control_request/initialize` -> `session_start`, then per `user` input an
//! `init` + `started` + one `assistant` text + a `completed` result).

mod support;

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};

const CLI: &str = env!("CARGO_BIN_EXE_swarm");
const AGY_MODEL: &str = "gemini-3.8-flash-high";
const CLAUDE_MODEL: &str = "claude-opus-4-6";
const FRESH_CONV: &str = "00000000-0000-4000-8000-000000000001";

fn main() {
    if let Ok(script) = std::env::var("ELIOT_FAKE_AGY_SCRIPT") {
        fake_agy_main(&script);
        return;
    }
    if let Ok(script) = std::env::var("ELIOT_FAKE_CLAUDE_SCRIPT") {
        fake_claude_main(&script);
        return;
    }
    owner_main();
    println!("cross_harness_mail_cas: all scenarios passed");
}

// ---------------------------------------------------------------------------
// Owner coordination (launches, narrow instructions, final reads only)
//
// CLI shapes below match the working owner tests and DTOs:
// - `launch --wait` (no prompt) prints a Command: `target`, `generation`.
// - `send --wait` prints a Turn: `turn_id`, `state`, `final_text`.
// - `result --turn-id` prints AgentResultResponse: `turns[0].final_text`
//   is the retained readback (see `cross_harness_second_result_and_no_loop`).
// - `show` prints a flat AgentGetResponse: `project` at top level.
// - `inbox` prints MailInboxResponse/MailDelivery; `msg send` prints
//   MailSendResponse/MailDeliverySummary; `msg ack` prints MailAckResponse.
// - `var set/get` print VarSetResponse/VarGetResponse.
// ---------------------------------------------------------------------------

fn owner_main() {
    let mut fixture = support::Fixture::new("cross-harness mail cas");
    fixture.start();
    fixture.stop(false);
    let cmd = std::env::current_exe()
        .expect("test executable")
        .to_string_lossy()
        .into_owned();
    let data_str = fixture.data.to_string_lossy().into_owned();
    let config = format!(
        "[profiles.agy-fake]\nharness = \"antigravity\"\nmodel = \"{AGY_MODEL}\"\ncmd = {}\npass_env = [\"ELIOT_FAKE_AGY_SCRIPT\", \"ELIOT_FAKE_CROSS_DATA\"]\n\n[profiles.claude-fake]\nharness = \"claude\"\nmodel = \"{CLAUDE_MODEL}\"\ncmd = {}\npass_env = [\"ELIOT_FAKE_CLAUDE_SCRIPT\", \"ELIOT_FAKE_CROSS_DATA\"]\n",
        serde_json::to_string(&cmd).expect("cmd toml"),
        serde_json::to_string(&cmd).expect("cmd toml"),
    );
    std::fs::write(fixture.data.join("config.toml"), config).expect("write profiles");
    fixture
        .env("ELIOT_FAKE_AGY_SCRIPT", "cross-mail-cas")
        .env("ELIOT_FAKE_CLAUDE_SCRIPT", "cross-mail-cas")
        .env("ELIOT_FAKE_CROSS_DATA", &data_str);
    fixture.start();

    let (a, a_gen, b, b_gen, scope) = launch_vendor_pair(&fixture);
    mail_roundtrip(&fixture, &a, a_gen, &b, b_gen);
    var_cas_roundtrip(&fixture, &a, a_gen, &b, b_gen, &scope);

    for agent in [&a, &b] {
        let stop: Value = fixture.json(&["stop", agent]);
        fixture.wait_command(
            stop["command_id"].as_str().expect("stop command"),
            "applied",
        );
    }
    fixture.stop(false);
}

/// Launch one AGY and one Claude Agent on the same canonical workspace, so
/// both share the same project label. Returns both ids, generations, and the
/// owner var scope `project:<label>` read from the flat `show` output.
fn launch_vendor_pair(fixture: &support::Fixture) -> (String, i64, String, i64, String) {
    let workspace = fixture.workspace.to_string_lossy().into_owned();
    let a_launch: Value = fixture.json(&[
        "launch",
        "--profile",
        "agy-fake",
        "--workspace",
        &workspace,
        "--wait",
    ]);
    let a = a_launch["target"].as_str().expect("agy agent").to_owned();
    let a_gen = a_launch["generation"].as_i64().expect("agy generation");
    let b_launch: Value = fixture.json(&[
        "launch",
        "--profile",
        "claude-fake",
        "--workspace",
        &workspace,
        "--wait",
    ]);
    let b = b_launch["target"]
        .as_str()
        .expect("claude agent")
        .to_owned();
    let b_gen = b_launch["generation"].as_i64().expect("claude generation");
    let a_show: Value = fixture.json(&["show", &a]);
    assert_eq!(a_show["id"], a);
    assert_eq!(a_show["generation"], a_gen);
    let project = a_show["project"]
        .as_str()
        .expect("project label")
        .to_owned();
    let b_show: Value = fixture.json(&["show", &b]);
    assert_eq!(b_show["project"], project, "shared project label");
    (a, a_gen, b, b_gen, format!("project:{project}"))
}

/// Send one instruction and return the retained terminal text: `send --wait`
/// prints the Turn, then `result --turn-id` prints the retained
/// AgentResultResponse whose `turns[0]` must match.
fn send_and_read_retained(
    fixture: &support::Fixture,
    agent: &str,
    generation: i64,
    text: &str,
) -> String {
    let gen_arg = generation.to_string();
    let sent: Value = match support::cli(
        &fixture.data,
        &[
            "send",
            agent,
            "--generation",
            &gen_arg,
            "--wait",
            "--timeout-seconds",
            "10",
            "--",
            text,
        ],
    ) {
        reply if reply.status.success() => {
            serde_json::from_str(&reply.stdout).expect("CLI JSON response")
        }
        reply => panic!(
            "CLI send exit {:?}; stderr omitted because a redaction regression may contain credentials",
            reply.status.code()
        ),
    };
    assert_eq!(sent["agent"], agent);
    assert_eq!(sent["generation"], generation);
    assert_eq!(sent["state"], "completed");
    assert_eq!(sent["status"], "completed");
    let turn_id = sent["turn_id"].as_str().expect("send turn id").to_owned();
    let send_text = sent["final_text"].as_str().expect("send text").to_owned();
    let result: Value = fixture.json(&[
        "result",
        agent,
        "--generation",
        &gen_arg,
        "--turn-id",
        &turn_id,
        "--limit",
        "1",
    ]);
    assert_eq!(result["agent"], agent);
    assert_eq!(result["generation"], generation);
    let turns = result["turns"].as_array().expect("retained turns");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0]["turn_id"], turn_id);
    assert_eq!(turns[0]["state"], "completed");
    let retained = turns[0]["final_text"]
        .as_str()
        .expect("retained text")
        .to_owned();
    assert_eq!(retained, send_text, "retained readback matches send");
    retained
}

fn mail_roundtrip(fixture: &support::Fixture, a: &str, a_gen: i64, b: &str, b_gen: i64) {
    let mail_text = "hello-cross-harness";
    let mail_thread = "cross";
    let send_instr = format!("XMAIL SEND peer={b} thread={mail_thread} text={mail_text}");
    let a_text1 = send_and_read_retained(fixture, a, a_gen, &send_instr);
    let delivery = parse_kv(&a_text1, "delivery").expect("delivery id");
    assert!(
        a_text1.contains(&format!("recipient={b}")),
        "agy reports the addressed recipient"
    );
    let inbox_b: Value = fixture.json(&["inbox", b]);
    let rows = inbox_b["deliveries"].as_array().expect("inbox rows");
    assert_eq!(rows.len(), 1, "one addressed delivery");
    assert_eq!(rows[0]["delivery_id"], delivery);
    assert_eq!(
        rows[0]["sender"], a,
        "actor binding: sender is the AGY Agent"
    );
    assert_eq!(rows[0]["sender_generation"], a_gen);
    assert_eq!(rows[0]["recipient"], b);
    assert_eq!(rows[0]["recipient_generation"], b_gen);
    assert_eq!(rows[0]["text"], mail_text);
    assert_eq!(rows[0]["thread"], mail_thread);
    assert!(rows[0]["read_at"].is_null(), "delivery starts unread");

    let ack_instr = format!(
        "XMAIL READACK expect_delivery={delivery} expect_sender={a} expect_text={mail_text}"
    );
    let b_text1 = send_and_read_retained(fixture, b, b_gen, &ack_instr);
    assert!(
        b_text1.contains(&format!("delivery={delivery}")),
        "claude reports the exact delivery"
    );
    assert!(
        b_text1.contains(&format!("sender={a}")),
        "claude reports the bound sender"
    );
    let inbox_b2: Value = fixture.json(&["inbox", b]);
    assert!(
        inbox_b2["deliveries"].as_array().expect("rows").is_empty(),
        "exact ack empties the recipient inbox"
    );
}

fn var_cas_roundtrip(
    fixture: &support::Fixture,
    a: &str,
    a_gen: i64,
    b: &str,
    b_gen: i64,
    scope: &str,
) {
    let var_key = "cross-cas";
    let var_val = "v1";
    let create_instr = format!("XVAR CREATE key={var_key} value={var_val}");
    let a_text2 = send_and_read_retained(fixture, a, a_gen, &create_instr);
    assert!(a_text2.contains("version=1"), "CAS create reports v1");
    let owner_var1: Value = fixture.json(&["var", "get", "--scope", scope, var_key]);
    assert_eq!(owner_var1["scope"], scope);
    assert_eq!(owner_var1["key"], var_key);
    assert_eq!(owner_var1["version"], 1);
    assert_eq!(owner_var1["value"], var_val);
    assert_eq!(owner_var1["set_by"], format!("agent:{a}:{a_gen}"));

    let stale_instr = format!("XVAR STALE_READ key={var_key} expect=0 value=stale");
    let b_text2 = send_and_read_retained(fixture, b, b_gen, &stale_instr);
    assert!(
        b_text2.contains("code=version_conflict"),
        "stale version rejects"
    );
    assert!(b_text2.contains("version=1"), "shared read sees v1");
    assert!(
        b_text2.contains(&format!("value={var_val}")),
        "shared read sees the winner value"
    );
    let owner_var2: Value = fixture.json(&["var", "get", "--scope", scope, var_key]);
    assert_eq!(owner_var2["version"], 1, "stale write changed nothing");
    assert_eq!(owner_var2["value"], var_val);
}

fn parse_kv(text: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    text.split_ascii_whitespace()
        .find_map(|token| token.strip_prefix(&prefix).map(str::to_owned))
}

// ---------------------------------------------------------------------------
// Native fake tool: real `swarm` CLI under the inherited Agent principal
// ---------------------------------------------------------------------------

/// Classify a mutating-CLI failure by its actual stderr text. The CLI
/// prints `id=<ULID>` before the error (mutation_id), so the first line is
/// not the error. Returns the allowlisted static token only when a line
/// starts with `error: version_conflict` and the exit code is 3; any other
/// error returns `other` and fails the caller assertion. Never returns raw
/// stderr or credentials; never invents an error from the instruction.
fn classify_cli_error(stderr: &[u8], exit_code: Option<i32>) -> &'static str {
    let text = String::from_utf8_lossy(stderr);
    let has_conflict = text
        .lines()
        .any(|line| line.starts_with("error: version_conflict"));
    if exit_code == Some(3) && has_conflict {
        "version_conflict"
    } else {
        "other"
    }
}

/// Run the real CLI as the fake's own Agent. The process environment already
/// carries `ELIOT_AGENT_ID/GENERATION/TOKEN` from the plugin host; this helper
/// inherits it untouched and never reads or prints it. It reports only
/// semantic JSON fields on success and only the first stderr line on failure.
fn agent_cli_json(data_dir: &str, args: &[&str]) -> Result<Value, String> {
    // Exactly one real CLI invocation: stdin null, stdout/stderr piped,
    // single Command::output() which spawns and waits the same child.
    let output = std::process::Command::new(CLI)
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--json")
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("agent CLI output");
    if output.status.success() {
        serde_json::from_slice(&output.stdout).map_err(|_| "agent CLI JSON".to_owned())
    } else {
        Err(classify_cli_error(&output.stderr, output.status.code()).to_owned())
    }
}

fn run_instruction(instr: &str) -> String {
    // Queued mail wakes the recipient with a generic notice turn
    // ("N unread messages; run swarm inbox"). That turn carries no body and
    // must not fail the fake; the owner waits for its own instruction turns
    // by turn id, so the wake turn never mixes with the assertions below.
    if instr.contains("unread messages; run swarm inbox") {
        return "XMAIL_WAKE_NOTICED".to_owned();
    }
    let data_dir = std::env::var("ELIOT_FAKE_CROSS_DATA").expect("cross data dir");
    if let Some(rest) = instr.strip_prefix("XMAIL SEND ") {
        let peer = parse_kv(rest, "peer").expect("peer Agent");
        let thread = parse_kv(rest, "thread").expect("thread");
        let text = parse_kv(rest, "text").expect("text");
        let resp = agent_cli_json(
            &data_dir,
            &["msg", "send", "--to", &peer, "--thread", &thread, &text],
        )
        .expect("agent mail send");
        let deliveries = resp["deliveries"].as_array().expect("fanout");
        assert_eq!(deliveries.len(), 1, "one addressed recipient");
        let delivery = deliveries[0]["delivery_id"].as_str().expect("delivery");
        let recipient = deliveries[0]["recipient"].as_str().expect("recipient");
        format!("XMAIL_SENT delivery={delivery} recipient={recipient}")
    } else if let Some(rest) = instr.strip_prefix("XMAIL READACK ") {
        let expect_delivery = parse_kv(rest, "expect_delivery").expect("expected delivery");
        let expect_sender = parse_kv(rest, "expect_sender").expect("expected sender");
        let expect_text = parse_kv(rest, "expect_text").expect("expected text");
        let inbox = agent_cli_json(&data_dir, &["inbox"]).expect("agent inbox");
        let rows = inbox["deliveries"].as_array().expect("inbox rows");
        let row = rows
            .iter()
            .find(|row| row["delivery_id"] == expect_delivery)
            .expect("exact delivery in own mailbox");
        assert_eq!(row["sender"], expect_sender);
        assert_eq!(row["text"], expect_text);
        let delivery = row["delivery_id"].as_str().expect("delivery").to_owned();
        let sender = row["sender"].as_str().expect("sender").to_owned();
        let text = row["text"].as_str().expect("text").to_owned();
        let acked =
            agent_cli_json(&data_dir, &["msg", "ack", "--delivery", &delivery]).expect("agent ack");
        let ack_list = acked["acked"].as_array().expect("acked ids");
        assert_eq!(ack_list.len(), 1);
        assert_eq!(ack_list[0], delivery);
        format!("XMAIL_ACKED delivery={delivery} sender={sender} text={text}")
    } else if let Some(rest) = instr.strip_prefix("XVAR CREATE ") {
        let key = parse_kv(rest, "key").expect("key");
        let value = parse_kv(rest, "value").expect("value");
        let resp = agent_cli_json(
            &data_dir,
            &["var", "set", "--expect-version", "0", &key, &value],
        )
        .expect("agent var create");
        let version = resp["version"].as_i64().expect("version");
        format!("XVAR_SET version={version} value={value}")
    } else if let Some(rest) = instr.strip_prefix("XVAR STALE_READ ") {
        let key = parse_kv(rest, "key").expect("key");
        let expect = parse_kv(rest, "expect").expect("expect version");
        let value = parse_kv(rest, "value").expect("value");
        let err = agent_cli_json(
            &data_dir,
            &["var", "set", "--expect-version", &expect, &key, &value],
        )
        .expect_err("stale CAS must reject");
        assert!(
            err.contains("version_conflict"),
            "stale rejection code, got: {err}"
        );
        let got = agent_cli_json(&data_dir, &["var", "get", &key]).expect("agent var read");
        let version = got["version"].as_i64().expect("version");
        let seen = got["value"].as_str().expect("value").to_owned();
        format!("XVAR_STALE_ERROR code=version_conflict XVAR_GOT version={version} value={seen}")
    } else {
        panic!("unknown cross-harness instruction");
    }
}

// ---------------------------------------------------------------------------
// Minimal fake AGY host (fresh launch + sends; EOF stop)
// ---------------------------------------------------------------------------

fn fake_agy_main(script: &str) {
    assert_eq!(script, "cross-mail-cas", "unexpected AGY script");
    let args: Vec<String> = std::env::args().collect();
    let argv_value = |name: &str| {
        args.windows(2).find_map(|pair| {
            if pair[0] == name {
                Some(pair[1].clone())
            } else {
                None
            }
        })
    };
    let conv = argv_value("--conversation").unwrap_or_else(|| FRESH_CONV.to_owned());
    let model = argv_value("--model").unwrap_or_else(|| AGY_MODEL.to_owned());
    assert_eq!(model, AGY_MODEL, "AGY model binding");
    // The plugin spawns the native CLI with cwd=workspace; echo it back so
    // init proves the requested workspace without another channel.
    let cwd = std::env::current_dir()
        .expect("fake cwd")
        .to_string_lossy()
        .into_owned();
    let mut stdout = std::io::stdout().lock();
    let init = json!({"event":"init","conversation_id":conv,
        "init":{"model":model,"cwd":cwd,"permission_mode":"request-review"}});
    writeln!(stdout, "{init}").expect("fake init");
    stdout.flush().expect("fake flush");
    let mut step_index: u64 = 0;
    let idle = json!({"event":"step_update","step_update":
        {"conversation_id":conv,"step_index":step_index,
         "state":"DONE","step_type":"system_message"}});
    step_index += 1;
    writeln!(stdout, "{idle}").expect("fake idle");
    stdout.flush().expect("fake flush");
    let stdin = std::io::stdin().lock();
    let mut n: u64 = 0;
    for line in BufReader::new(stdin).lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let frame: Value = serde_json::from_str(&line).expect("fake input JSON");
        assert_eq!(frame.get("event").and_then(Value::as_str), Some("user"));
        let text = frame["message"]["content"]
            .as_str()
            .expect("user content")
            .to_owned();
        let reply = run_instruction(&text);
        n += 1;
        for step_type in ["user_input", "agent_response"] {
            let step = json!({"event":"step_update","step_update":
                {"conversation_id":conv,"step_index":step_index,
                 "state":"DONE","step_type":step_type}});
            step_index += 1;
            writeln!(stdout, "{step}").expect("fake step");
        }
        let result = json!({"event":"result","result":{"conversation_id":conv,
            "status":"SUCCESS","response":reply,
            "finished_reason":"completed","num_turns":1,
            "usage":{"input_tokens":100*n,"output_tokens":10*n,
                "thinking_tokens":0,"cache_read_tokens":0,"total_tokens":110*n},
            "denied_actions":[]}});
        writeln!(stdout, "{result}").expect("fake result");
        stdout.flush().expect("fake flush");
    }
}

// ---------------------------------------------------------------------------
// Minimal fake Claude CLI (auth check + initialize + sends; EOF stop)
// ---------------------------------------------------------------------------

fn fake_claude_main(script: &str) {
    assert_eq!(script, "cross-mail-cas", "unexpected Claude script");
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("auth") {
        if argv.get(2).map(String::as_str) == Some("status") {
            println!("{}", json!({"loggedIn": true, "authMethod": "claude.ai"}));
            std::process::exit(0);
        }
        std::process::exit(1);
    }
    let flag = |name: &str| {
        argv.iter()
            .position(|arg| arg == name)
            .and_then(|pos| argv.get(pos + 1))
            .cloned()
            .unwrap_or_default()
    };
    let argv_session = flag("--session-id");
    let argv_resume = flag("--resume");
    let argv_model = flag("--model");
    let argv_perm = flag("--permission-mode");
    assert_eq!(argv_model, CLAUDE_MODEL, "Claude model binding");
    let session_id = if !argv_resume.is_empty() {
        argv_resume
    } else {
        argv_session
    };
    assert!(!session_id.is_empty(), "Claude session id");
    run_claude_frames(session_id, argv_perm);
}

fn run_claude_frames(session_id: String, argv_perm: String) {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();
    for line in BufReader::new(stdin).lines() {
        let line = line.expect("fake stdin");
        if line.trim().is_empty() {
            continue;
        }
        let frame: Value = serde_json::from_str(&line).expect("fake frame JSON");
        match frame.get("type").and_then(Value::as_str) {
            Some("control_request") => {
                let request_id = frame
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let subtype = frame
                    .get("request")
                    .and_then(|request| request.get("subtype"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if subtype == "initialize" {
                    let mode = if argv_perm.is_empty() {
                        "default".to_owned()
                    } else {
                        argv_perm.clone()
                    };
                    let response = json!({
                        "type": "control_response",
                        "response": {
                            "subtype": "success",
                            "request_id": request_id,
                            "response": {
                                "models": [{"value": CLAUDE_MODEL, "resolvedModel": CLAUDE_MODEL}],
                                "current_permission_mode": mode,
                                "pid": std::process::id(),
                                "session_state": "idle",
                                "account": {"uuid": "fake-account"},
                            },
                        },
                    });
                    writeln!(stdout, "{response}").expect("fake control");
                    let start = json!({
                        "type": "system",
                        "subtype": "session_start",
                        "session_id": session_id,
                    });
                    writeln!(stdout, "{start}").expect("fake start");
                } else {
                    let response = json!({
                        "type": "control_response",
                        "response": {
                            "subtype": "success",
                            "request_id": request_id,
                            "response": {},
                        },
                    });
                    writeln!(stdout, "{response}").expect("fake control");
                }
                stdout.flush().expect("fake flush");
            }
            Some("user") => {
                let uuid = frame
                    .get("uuid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let text = frame
                    .get("message")
                    .and_then(|message| message.get("content"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let started_init = json!({
                    "type": "system",
                    "subtype": "init",
                    "session_id": session_id,
                });
                writeln!(stdout, "{started_init}").expect("fake init");
                let started = json!({
                    "type": "command_lifecycle",
                    "command_uuid": uuid,
                    "state": "started",
                });
                writeln!(stdout, "{started}").expect("fake started");
                stdout.flush().expect("fake flush");
                let reply = run_instruction(&text);
                let assistant = json!({
                    "type": "assistant",
                    "session_id": session_id,
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": reply}],
                    },
                });
                writeln!(stdout, "{assistant}").expect("fake assistant");
                let result = json!({
                    "type": "result",
                    "subtype": "success",
                    "session_id": session_id,
                    "terminal_reason": "completed",
                    "total_cost_usd": 0.01,
                    "usage": {"input_tokens": 10, "output_tokens": 20},
                    "modelUsage": {CLAUDE_MODEL: {"input_tokens": 10, "output_tokens": 20}},
                    "permission_denials": [],
                });
                writeln!(stdout, "{result}").expect("fake result");
                stdout.flush().expect("fake flush");
            }
            _ => {}
        }
    }
}
