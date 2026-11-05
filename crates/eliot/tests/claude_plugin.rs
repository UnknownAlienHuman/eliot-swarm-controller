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
            "rejected oversized send does not wedge the session",
            OwnedChildren::default(),
            scenario_rejected_send_does_not_wedge_session,
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
            "late trailing record still rejects with known exit and drain",
            OwnedChildren::default(),
            scenario_late_trailing_record_rejects_with_known_drain,
        )
        .await;
        run_scenario(
            "poisoned drain keeps unknown with the controller retained",
            OwnedChildren::default(),
            scenario_poisoned_drain_keeps_unknown_controller,
        )
        .await;
        run_scenario(
            "a logged-out CLI logs in through its own login, then launches",
            OwnedChildren::default(),
            scenario_logged_out_cli_logs_in_then_launches,
        )
        .await;
        run_scenario(
            "an abandoned CLI login rejects the start before any session",
            OwnedChildren::default(),
            scenario_abandoned_login_rejects_start,
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
        run_scenario(
            "resume realigns drifted settings and sends without a resend",
            OwnedChildren::default(),
            scenario_resume_realigns_drifted_settings_and_sends,
        )
        .await;
        run_scenario(
            "resume without drift skips the permission setter",
            OwnedChildren::default(),
            scenario_resume_without_drift_skips_permission_setter,
        )
        .await;
        run_scenario(
            "foreign resume scope rejected before spawn",
            OwnedChildren::default(),
            scenario_resume_scope_mismatch_rejected_before_spawn,
        )
        .await;
        run_scenario(
            "manual permission mode quirk reported as requested vs effective",
            OwnedChildren::default(),
            scenario_permission_mode_quirk_reported,
        )
        .await;
        run_scenario(
            "approve allows with the echoed input",
            OwnedChildren::default(),
            scenario_approve_allows_with_echoed_input,
        )
        .await;
        run_scenario(
            "approve redacts tool input secrets in committed details",
            OwnedChildren::default(),
            scenario_approve_redacts_tool_input_secrets,
        )
        .await;
        run_scenario(
            "deny denies with a message",
            OwnedChildren::default(),
            scenario_deny_denies_with_message,
        )
        .await;
        run_scenario(
            "reply guards reject without a rewrite",
            OwnedChildren::default(),
            scenario_reply_guards_reject_without_rewrite,
        )
        .await;
        run_scenario(
            "unreplied request closes with the turn",
            OwnedChildren::default(),
            scenario_unreplied_request_closes_with_the_turn,
        )
        .await;
        run_scenario(
            "lost reply is unknown without a rewrite",
            OwnedChildren::default(),
            scenario_lost_reply_is_unknown_without_rewrite,
        )
        .await;
        run_scenario(
            "refused initialize is unknown without a rewrite",
            OwnedChildren::default(),
            scenario_initialize_error_is_unknown_without_rewrite,
        )
        .await;
        run_scenario(
            "setter echo mismatch is unknown",
            OwnedChildren::default(),
            scenario_setter_echo_mismatch_is_unknown,
        )
        .await;
        run_scenario(
            "missing retained session is unknown",
            OwnedChildren::default(),
            scenario_retained_session_missing_is_unknown,
        )
        .await;
        run_scenario(
            "stale-generation resume is rejected before spawn",
            OwnedChildren::default(),
            scenario_stale_generation_resume_rejected_before_spawn,
        )
        .await;
        run_scenario(
            "late old-generation frames gap without applying",
            OwnedChildren::default(),
            scenario_late_old_generation_frames_gap_without_applying,
        )
        .await;
        run_scenario(
            "stray permission ask gets an explicit deny",
            OwnedChildren::default(),
            scenario_stray_permission_ask_gets_explicit_deny,
        )
        .await;
        run_scenario(
            "duplicate permission id denied and original fenced",
            OwnedChildren::default(),
            scenario_duplicate_permission_id_denied_and_original_fenced,
        )
        .await;
        run_scenario(
            "pending overflow denied without opening",
            OwnedChildren::default(),
            scenario_pending_overflow_denied_without_opening,
        )
        .await;
        run_scenario(
            "repeated dropped id never rewritten nor retried",
            OwnedChildren::default(),
            scenario_repeated_dropped_id_never_rewritten_nor_retried,
        )
        .await;
        run_scenario(
            "permission identity exhaustion fences and closes the native input",
            OwnedChildren::default(),
            scenario_permission_identity_exhaustion_fails_closed,
        )
        .await;
        run_scenario(
            "unknown owner reply never denies a conflicting callback again",
            OwnedChildren::default(),
            scenario_unknown_owner_reply_conflict_no_second_io,
        )
        .await;
        run_scenario(
            "turn result never clears a live callback fence",
            OwnedChildren::default(),
            scenario_callback_fence_survives_turn_result,
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
    argv_resume: String,
    argv_model: String,
    argv_permission_mode: String,
    argv: Vec<String>,
    controls: HashMap<String, u64>,
    user_inputs: u64,
    /// Set only on an exit path, so the scenario can verify this exact CLI
    /// died with it instead of leaking (note 61).
    exit: Option<String>,
    permission_replies: Vec<Value>,
}

fn fake_claude_main(script: &str) {
    let report_path = std::env::var("ELIOT_FAKE_CLAUDE_REPORT")
        .map(PathBuf::from)
        .expect("ELIOT_FAKE_CLAUDE_REPORT");
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("auth") {
        fake_auth(script, &report_path, argv.get(2).map(String::as_str));
        return;
    }
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
        argv_resume: flag("--resume"),
        argv_model: flag("--model"),
        argv_permission_mode: flag("--permission-mode"),
        argv,
        controls: HashMap::new(),
        user_inputs: 0,
        exit: None,
        permission_replies: Vec::new(),
    };
    fake.write_report();
    let _panic_marker = PanicMarker {
        report_path: fake.report_path.clone(),
    };
    fake.run();
}

/// `claude auth status|login` for the plugin's pre-launch login check. A
/// `logged-out-*` script starts logged out; `auth login` logs in only for
/// `logged-out-login-completes` (writing `login.marker` beside the report).
fn fake_auth(script: &str, report_path: &std::path::Path, command: Option<&str>) {
    let marker = report_path.with_file_name("login.marker");
    match command {
        Some("status") => {
            let logged_in = !script.starts_with("logged-out") || marker.exists();
            println!(
                "{}",
                json!({"loggedIn": logged_in, "authMethod": "claude.ai"})
            );
            std::process::exit(if logged_in { 0 } else { 1 });
        }
        Some("login") if script == "logged-out-login-completes" => {
            std::fs::write(&marker, b"ok").expect("write login marker");
        }
        _ => std::process::exit(1),
    }
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
        // Stdin EOF: the owner closed input (graceful terminate) or died.
        // Late scripts emit their trailing record here, during drain: clean
        // late proves profile_error with known exit/drain, poisoned proves
        // true drain uncertainty stays unknown with the controller retained.
        if self.script == "model-mismatch-late" {
            self.emit(
                &mut stdout,
                json!({
                    "type": "system",
                    "subtype": "session_start",
                    "session_id": self.session_id(),
                }),
            );
            stdout.flush().expect("fixture stdout");
        } else if self.script == "model-mismatch-poisoned" {
            writeln!(stdout, "{{invalid-json").expect("fixture stdout");
            stdout.flush().expect("fixture stdout");
        }
        self.exit = Some("stdin-eof".to_owned());
        self.write_report();
    }

    /// False ends the fixture (clean exit or scripted drop).
    fn handle_frame(&mut self, frame: &Value, stdout: &mut std::io::StdoutLock) -> bool {
        match frame.get("type").and_then(Value::as_str) {
            Some("control_request") => self.handle_control_request(frame, stdout),
            Some("control_response") => self.handle_control_response(frame, stdout),
            Some("user") => self.handle_user_input(frame, stdout),
            _ => {}
        }
        true
    }

    fn handle_control_request(&mut self, frame: &Value, stdout: &mut std::io::StdoutLock) {
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
            "initialize" => self.handle_initialize(stdout, &request_id),
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
            "set_model" => {
                // Success with no readback payload, like the CLI.
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
            "set_permission_mode" => {
                // Success echoing the applied mode, like the CLI — except the
                // drift script answers with the CLI's own mode instead of the
                // requested one, like the installed manual→default quirk.
                let mode = if self.script == "setter-drift" {
                    Value::String("default".to_owned())
                } else {
                    frame
                        .get("request")
                        .and_then(|request| request.get("mode"))
                        .cloned()
                        .unwrap_or(Value::Null)
                };
                self.emit(
                    stdout,
                    json!({
                        "type": "control_response",
                        "response": {
                            "subtype": "success",
                            "request_id": request_id,
                            "response": {"mode": mode},
                        },
                    }),
                );
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
    }

    fn handle_initialize(&mut self, stdout: &mut std::io::StdoutLock, request_id: &str) {
        if self.script == "initialize-error" {
            // The CLI refuses the call outright: the reply leaves the
            // outcome uncertain, never rejected.
            self.emit(
                stdout,
                json!({
                    "type": "control_response",
                    "response": {
                        "subtype": "error",
                        "request_id": request_id,
                        "response": {"message": "fake initialize refused"},
                    },
                }),
            );
            return;
        }
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
        if matches!(
            self.script.as_str(),
            "model-mismatch-late" | "model-mismatch-poisoned"
        ) {
            // Late trailing record arrives only after stdin EOF during
            // graceful terminate+drain, so the close-before-drain race is
            // deterministic without sleeps: old code drops the queue before
            // EOF and overflows, fixed code buffers through drain.
            return;
        }
        // SessionStart hook lines carry the session id before
        // any input, exactly like the installed CLI. A resumed
        // CLI re-announces the same retained id; the missing
        // script announces a fresh one instead.
        let session_id = if self.script == "session-mismatch" || self.script == "resume-missing" {
            "00000000-0000-0000-0000-000000000000".to_owned()
        } else {
            self.session_id()
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

    fn handle_control_response(&mut self, frame: &Value, stdout: &mut std::io::StdoutLock) {
        // A permission decision for one asked `can_use_tool`.
        let body = frame.get("response").cloned().unwrap_or(Value::Null);
        self.permission_replies.push(json!({
            "request_id": body.get("request_id").cloned().unwrap_or(Value::Null),
            "subtype": body.get("subtype").cloned().unwrap_or(Value::Null),
            "result": body.get("response").cloned().unwrap_or(Value::Null),
        }));
        self.write_report();
        if self.handle_permission_fault_response(&body, stdout) {
            return;
        }
        if self.script == "overflow-repeat-asks"
            && body
                .get("response")
                .and_then(|result| result.get("behavior"))
                .and_then(Value::as_str)
                == Some("allow")
        {
            // A slot freed; the dropped seventeenth repeats anyway. Still
            // fenced: never a second deny nor an open. The turn stays parked.
            self.emit_permission_request_with(
                stdout,
                "fake-perm-17",
                "Bash",
                "fake-tooluse-17",
                json!({"command": "ls /tmp"}),
            );
            stdout.flush().expect("fixture stdout");
            self.write_report();
            return;
        }
        if matches!(self.script.as_str(), "approve" | "approve-secret" | "deny") {
            // The decided turn runs to its terminal once the reply
            // lands, like the CLI continuing past the prompt.
            let allowed = body
                .get("response")
                .and_then(|result| result.get("behavior"))
                .and_then(Value::as_str)
                == Some("allow");
            let text = if allowed {
                "fake reply after decision. "
            } else {
                "fake reply after denial. "
            };
            self.emit_assistant(stdout, text);
            self.emit_turn_result(stdout, "completed");
        }
    }

    fn handle_user_input(&mut self, frame: &Value, stdout: &mut std::io::StdoutLock) {
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
            return;
        }
        if self.script == "stale-frame" {
            // A late generation-1 frame: this generation never submitted
            // that turn, so it gaps instead of applying.
            self.emit(
                stdout,
                json!({
                    "type": "command_lifecycle",
                    "command_uuid": "00000000-0000-0000-0000-000000000001",
                    "state": "started",
                }),
            );
        }
        self.emit(
            stdout,
            json!({
                "type": "system",
                "subtype": "init",
                "session_id": self.session_id(),
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
            return;
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
            return;
        }
        if self.handle_parked_permission_scripts(stdout) {
            return;
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
        if self.script == "stray-ask" {
            // The turn is over: this ask has no exact active turn, so it
            // gets an explicit deny plus a gap, never an open.
            self.emit_permission_request(stdout);
        }
    }

    /// Parked permission scripts: the turn stays open on its asks. True when
    /// this script was handled and the caller must return without a terminal.
    fn handle_parked_permission_scripts(&mut self, stdout: &mut std::io::StdoutLock) -> bool {
        if self.script == "owner-unknown-conflict" {
            self.emit_permission_range(stdout, 16);
            return true;
        }
        if self.script == "permission-fence-exhausted" {
            self.emit_permission_range(stdout, 34);
            return true;
        }
        if self.script == "deny-result-repeat" {
            self.emit_permission_range(stdout, 17);
            return true;
        }
        if matches!(
            self.script.as_str(),
            "approve" | "approve-secret" | "deny" | "drop-reply"
        ) {
            // Park the turn on its permission ask: the terminal only
            // follows the decision (or never, for the dropped reply).
            self.emit_permission_request(stdout);
            if self.script == "drop-reply" {
                stdout.flush().expect("fixture stdout");
                self.exit = Some("scripted-drop".to_owned());
                self.write_report();
                std::process::exit(0);
            }
            return true;
        }
        if self.script == "request-close" {
            // The turn ends with its ask unanswered: opened, then the
            // terminal with no decision in between.
            self.emit_permission_request(stdout);
            self.emit_assistant(stdout, "fake reply with an open ask. ");
            self.emit_turn_result(stdout, "completed");
            return true;
        }
        if self.script == "duplicate-ask" {
            // One open ask, then the same native id with another ask: an
            // explicit deny plus a gap, and the retained original is fenced
            // uncertain (native holds one callback per id; which ask the deny
            // consumed is UNKNOWN, never claimed). The turn stays parked.
            self.emit_permission_request(stdout);
            self.emit_permission_request_with(
                stdout,
                "fake-perm-1",
                "Write",
                "fake-tooluse-2",
                json!({"path": "/tmp/other"}),
            );
            return true;
        }
        if self.script == "overflow-asks" {
            // Sixteen opens fill the retained bound; the seventeenth gets an
            // explicit deny plus a gap, never an open. The turn stays parked.
            for i in 1..=17 {
                self.emit_permission_request_with(
                    stdout,
                    &format!("fake-perm-{i}"),
                    "Bash",
                    &format!("fake-tooluse-{i}"),
                    json!({"command": "ls /tmp"}),
                );
            }
            return true;
        }
        if self.script == "overflow-repeat-asks" {
            // Like overflow-asks, plus an immediate repeat of the dropped
            // seventeenth: the repeat never causes a second deny nor an open.
            // A further repeat follows the first approval (see control
            // responses), still fenced even though a slot freed.
            for i in 1..=17 {
                self.emit_permission_request_with(
                    stdout,
                    &format!("fake-perm-{i}"),
                    "Bash",
                    &format!("fake-tooluse-{i}"),
                    json!({"command": "ls /tmp"}),
                );
            }
            self.emit_permission_request_with(
                stdout,
                "fake-perm-17",
                "Bash",
                "fake-tooluse-17",
                json!({"command": "ls /tmp"}),
            );
            return true;
        }
        false
    }

    /// The owned session: `--resume` continues the retained id, otherwise
    /// the minted `--session-id`.
    fn session_id(&self) -> String {
        if !self.argv_resume.is_empty() {
            self.argv_resume.clone()
        } else {
            self.argv_session.clone()
        }
    }

    /// One documented-shape `can_use_tool` ask on the active turn.
    fn emit_permission_range(&self, stdout: &mut std::io::StdoutLock, end: usize) {
        for index in 1..=end {
            self.emit_permission_request_with(
                stdout,
                &format!("fake-perm-{index}"),
                "Bash",
                &format!("fake-tooluse-{index}"),
                json!({"command":"ls /tmp"}),
            );
        }
    }

    fn handle_permission_fault_response(
        &mut self,
        body: &Value,
        stdout: &mut std::io::StdoutLock,
    ) -> bool {
        let request_id = body
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match self.script.as_str() {
            "permission-fence-exhausted" => true,
            "owner-unknown-conflict" => {
                if request_id == "fake-perm-1" && self.permission_replies.len() == 1 {
                    self.emit_permission_request_with(
                        stdout,
                        "fake-perm-1",
                        "Bash",
                        "replacement-tool",
                        json!({"command":"replacement"}),
                    );
                    self.emit_permission_request_with(
                        stdout,
                        "fake-perm-17",
                        "Bash",
                        "barrier-tool",
                        json!({"command":"barrier"}),
                    );
                    stdout.flush().expect("permission fault frames");
                } else if request_id == "fake-perm-17" {
                    self.finish_permission_barrier();
                }
                true
            }
            "deny-result-repeat" => {
                if request_id == "fake-perm-17" && self.permission_replies.len() == 1 {
                    self.emit_turn_result(stdout, "completed");
                    self.emit_permission_request_with(
                        stdout,
                        "fake-perm-17",
                        "Bash",
                        "replacement-tool",
                        json!({"command":"replacement"}),
                    );
                    self.emit_permission_request_with(
                        stdout,
                        "fake-perm-18",
                        "Bash",
                        "barrier-tool",
                        json!({"command":"barrier"}),
                    );
                    stdout.flush().expect("post-result fault frames");
                } else if request_id == "fake-perm-18" {
                    self.finish_permission_barrier();
                }
                true
            }
            _ => false,
        }
    }

    fn finish_permission_barrier(&mut self) -> ! {
        // The last ordinary deny proves every earlier callback on this pipe
        // was processed while native input was live; no timing inference.
        self.exit = Some("permission-barrier".to_owned());
        self.write_report();
        std::process::exit(0)
    }

    fn emit_permission_request(&self, stdout: &mut std::io::StdoutLock) {
        let input = if self.script == "approve-secret" {
            json!({"command": "deploy token-abc-123"})
        } else {
            json!({"command": "ls /tmp"})
        };
        self.emit_permission_request_with(stdout, "fake-perm-1", "Bash", "fake-tooluse-1", input);
    }

    fn emit_permission_request_with(
        &self,
        stdout: &mut std::io::StdoutLock,
        request_id: &str,
        tool_name: &str,
        tool_use_id: &str,
        input: Value,
    ) {
        self.emit(
            stdout,
            json!({
                "type": "control_request",
                "request_id": request_id,
                "request": {
                    "subtype": "can_use_tool",
                    "tool_name": tool_name,
                    "tool_use_id": tool_use_id,
                    "input": input,
                },
            }),
        );
    }

    fn initialize_response(&self) -> Value {
        let models = if matches!(
            self.script.as_str(),
            "model-mismatch" | "model-mismatch-late" | "model-mismatch-poisoned"
        ) {
            json!([{"value": "other-model", "resolvedModel": "other-model"}])
        } else {
            json!([{"value": MODEL, "resolvedModel": MODEL}])
        };
        let session_state = if self.script == "not-idle" {
            "busy"
        } else {
            "idle"
        };
        // The CLI echoes its launch permission mode; the drift and quirk
        // scripts pin a readback that differs from the requested one.
        let current_permission_mode = match self.script.as_str() {
            "resume-realign" | "quirk-manual" => "default".to_owned(),
            // Drifted from the requested manual, so the permission setter
            // runs — and then echoes the CLI's own mode instead.
            "setter-drift" => "plan".to_owned(),
            _ if !self.argv_permission_mode.is_empty() => self.argv_permission_mode.clone(),
            _ => "default".to_owned(),
        };
        json!({
            "models": models,
            "current_permission_mode": current_permission_mode,
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
                "session_id": self.session_id(),
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
                "session_id": self.session_id(),
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
            "resume": self.argv_resume,
            "model": self.argv_model,
            "permission_mode": self.argv_permission_mode,
            "controls": self.controls,
            "user_inputs": self.user_inputs,
            "exit": self.exit,
            "permission_replies": self.permission_replies,
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
        self.begin_call_with_id(&id, method, params).await
    }

    /// Send a call under an explicit JSON-RPC id. The oversized-send
    /// scenario pins a 1-char id to shrink the frame it measures and fits;
    /// every other scenario uses [`Host::begin_call`].
    async fn begin_call_with_id(
        &mut self,
        id: &str,
        method: &str,
        params: Value,
    ) -> oneshot::Receiver<Result<Value, String>> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.to_owned(), tx);
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

    /// Answer every host request while `agent.start` is in flight: the CLI
    /// registration plus (on success) the `session.ready` batch. Returns the
    /// start outcome, the ready batch data, and whether a gap was recorded.
    async fn pump_start_outcome(
        &mut self,
        owned: &OwnedChildren,
        agent_id: &str,
        generation: i64,
        rx: oneshot::Receiver<Result<Value, String>>,
    ) -> (Result<Value, String>, Option<Value>, bool) {
        let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
        // `agent.start` only resolves after the CLI registers and (on
        // success) the session.ready batch commits; answer all host
        // requests while it is in flight.
        let mut ready_data: Option<Value> = None;
        let mut gapped = false;
        let outcome = loop {
            tokio::select! {
                biased;
                request = self.next_request() => {
                    match request.method.as_str() {
                        "process.put" => {
                            let pid = request.params.get("pid").and_then(Value::as_u64).unwrap_or(0);
                            assert!(pid > 0);
                            owned.set_cli_pid(pid).await;
                            assert_eq!(request.params.get("role").and_then(Value::as_str), Some("harness"));
                            self.respond(&request.id, json!({
                                "identity": {"host_boot_id": "test-boot", "pid": pid, "start_time": 1},
                                "owner": format!("agent:{agent_id}#{generation}"),
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
                            let first = self.next_seq;
                            self.next_seq += 1;
                            self.respond(&request.id, json!({
                                "first_seq": first, "last_seq": first,
                                "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                            })).await;
                        }
                        "source.gap" => {
                            gapped = true;
                            let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                            self.respond(&request.id, json!({"gap_id": gap_id, "seq": self.next_seq})).await;
                            self.next_seq += 1;
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
        (resolved, ready_data, gapped)
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
        // Note 61: never leave the child behind. If it does not exit, kill
        // it explicitly (the deadline assert below still fails the run).
        let status = match tokio::time::timeout(OP_TIMEOUT, self.child.wait()).await {
            Ok(status) => status.expect("plugin wait"),
            Err(_) => {
                let _ = self.child.start_kill();
                tokio::time::timeout(OP_TIMEOUT, self.child.wait())
                    .await
                    .expect("plugin kill deadline")
                    .expect("plugin wait")
            }
        };
        assert!(status.success(), "plugin exit status: {status}");
        self.stderr.take().unwrap().await.unwrap_or_default()
    }
}

struct Scenario {
    root: PathBuf,
    agent_id: String,
    host: Host,
}

struct StartConfig {
    script: String,
    profile: Value,
    agent_id: Option<String>,
    generation: i64,
    command_id: Option<String>,
    resume_from: Value,
    source_state: Value,
    extra_env: Vec<(String, String)>,
    expect_ready: bool,
    requested_mode: Option<String>,
    effective_mode: String,
}

impl StartConfig {
    fn fresh(script: &str, profile: Value) -> Self {
        Self {
            script: script.to_owned(),
            profile,
            agent_id: None,
            generation: 1,
            command_id: None,
            resume_from: Value::Null,
            source_state: Value::Null,
            extra_env: Vec::new(),
            expect_ready: true,
            requested_mode: None,
            effective_mode: "default".to_owned(),
        }
    }
}

struct ReadyExpectations {
    generation: i64,
    start_id: String,
    requested_mode: Option<String>,
    effective_mode: String,
}

fn assert_claude_manifest(manifest: &Value) {
    assert_eq!(manifest.get("name").and_then(Value::as_str), Some("claude"));
    assert_eq!(
        manifest.get("protocol").and_then(Value::as_str),
        Some("eliot-plugin/1")
    );
    assert_eq!(
        manifest["verbs"],
        json!([
            "launch",
            "resume",
            "send",
            "interrupt",
            "approve",
            "deny",
            "stop"
        ])
    );
    assert_eq!(
        manifest["sources"]["controlled"],
        json!({"message": "stream", "request": "stream", "session": "stream", "turn": "stream"})
    );
}

impl Scenario {
    async fn start(script: &str, profile: Value, owned: &OwnedChildren) -> Self {
        let (scenario, _) = Self::start_full(StartConfig::fresh(script, profile), owned).await;
        scenario
    }

    /// `expect_ready` pumps `process.put` plus (on success) the `session.ready`
    /// batch while `agent.start` is in flight. Returns the scenario and the
    /// start result on success, or the scenario parts with the error text.
    async fn start_full(
        config: StartConfig,
        owned: &OwnedChildren,
    ) -> (Self, Result<(Value, Value), String>) {
        let StartConfig {
            script,
            profile,
            agent_id,
            generation,
            command_id,
            resume_from,
            source_state,
            extra_env,
            expect_ready,
            requested_mode,
            effective_mode,
        } = config;
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-claude-plugin-test-{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ));
        owned.set_report(root.join("report.json")).await;
        std::fs::create_dir_all(root.join("data")).unwrap();
        std::fs::create_dir_all(root.join("instance")).unwrap();
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        // A resumed generation keeps its Agent id; a fresh one mints it.
        let agent_id = agent_id.unwrap_or_else(|| ulid::Ulid::new().to_string());
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
        assert_claude_manifest(&manifest);
        let start_id = command_id.unwrap_or_else(|| ulid::Ulid::new().to_string());
        let self_exe = std::env::current_exe().expect("test executable");
        let mut full_profile = profile;
        full_profile["cmd"] = Value::String(self_exe.to_string_lossy().into_owned());
        let mut target_environment = json!({
            "ELIOT_FAKE_CLAUDE_SCRIPT": script,
            "ELIOT_FAKE_CLAUDE_REPORT": root.join("report.json").to_string_lossy(),
        });
        for (name, value) in &extra_env {
            target_environment[name] = Value::String(value.clone());
        }
        let rx = host
            .begin_call(
                "agent.start",
                json!({
                    "agent_id": agent_id,
                    "generation": generation,
                    "command_id": start_id,
                    "profile": full_profile,
                    "workspace": root.join("workspace").to_string_lossy(),
                    "target_environment": target_environment,
                    "note": null,
                    "source_plan": {"session": "stream", "turn": "stream", "message": "stream", "request": "stream"},
                    "source_state": source_state,
                    "resume_from": resume_from,
                    "initial": null,
                }),
            )
            .await;
        let (resolved, ready_data, gapped) = host
            .pump_start_outcome(owned, &agent_id, generation, rx)
            .await;
        let scenario = Self {
            root,
            agent_id,
            host,
        };
        let expectations = ReadyExpectations {
            generation,
            start_id,
            requested_mode,
            effective_mode,
        };
        scenario.check_start(resolved, ready_data, gapped, expect_ready, expectations)
    }

    /// Assert the pumped start outcome against the scenario's expectations.
    /// On an expected ready the committed `session.ready` batch and the
    /// requested/effective settings must all match; anything else passes the
    /// outcome through for the scenario to assert.
    fn check_start(
        self,
        resolved: Result<Value, String>,
        ready_data: Option<Value>,
        gapped: bool,
        expect_ready: bool,
        expectations: ReadyExpectations,
    ) -> (Self, Result<(Value, Value), String>) {
        let ReadyExpectations {
            generation,
            start_id,
            requested_mode,
            effective_mode,
        } = expectations;
        match resolved {
            Ok(ready) if expect_ready => {
                assert_eq!(ready.get("state").and_then(Value::as_str), Some("ready"));
                assert_eq!(
                    ready.get("agent_id").and_then(Value::as_str),
                    Some(self.agent_id.as_str())
                );
                assert_eq!(
                    ready.get("generation").and_then(Value::as_i64),
                    Some(generation)
                );
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
                    requested_mode.as_deref()
                );
                // The CLI never reads the active model back: effective stays
                // null and the requested id travels under `requested`.
                assert_eq!(ready["effective"].get("model"), Some(&Value::Null));
                assert_eq!(
                    ready["effective"]
                        .get("permission_mode")
                        .and_then(Value::as_str),
                    Some(effective_mode.as_str())
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
                (self, Ok((ready, ready_data)))
            }
            Ok(ready) => (self, Ok((ready, ready_data.unwrap_or(Value::Null)))),
            Err(error) => (self, Err(error)),
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

fn claude_profile_with_mode(mode: &str) -> Value {
    let mut profile = claude_profile();
    profile["permission_mode"] = Value::String(mode.to_owned());
    profile
}

/// Valid retained readback for resuming `agent_id` into generation 2.
fn resume_source_state(agent_id: &str) -> Value {
    json!({
        "retained_generation": 1,
        "retained_source": {
            "agent_id": agent_id,
            "generation": 1,
            "source": "stream",
            "families": ["session", "turn", "message"],
        },
        "current_turn": null,
    })
}

/// Run a full generation 1 (start, one turn, stop, shutdown) and return the
/// retained `(agent id, native scope, native ref)` for a resume scenario.
async fn retained_subject(owned: &OwnedChildren) -> (String, String, String) {
    let (mut gen1, outcome) =
        Scenario::start_full(StartConfig::fresh("ok", claude_profile()), owned).await;
    let (ready, _) = outcome.expect("gen1 ready");
    let agent_id = gen1.agent_id.clone();
    let native_scope = ready["native_scope"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let native_ref = ready["native_ref"].as_str().unwrap_or_default().to_owned();
    assert!(!native_scope.is_empty() && is_uuid(&native_ref));
    send_turn(&mut gen1.host, &agent_id, 1, "hello").await;
    stop_agent(&mut gen1.host, &agent_id, 1).await;
    gen1.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen1.host.shutdown_child().await;
    (agent_id, native_scope, native_ref)
}

/// Begin a send and pump until the applied response AND the exact
/// `request.opened` batch both land. The native commit may arrive on either
/// side of the response (note 48). Returns `(send id, turn id, opened data)`.
async fn send_until_permission_asked(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    text: &str,
) -> (String, String, Value) {
    let send_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": text},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut applied: Option<Value> = None;
    let mut kinds: Vec<String> = Vec::new();
    let mut opened: Option<Value> = None;
    loop {
        if applied.is_some() && opened.is_some() {
            break;
        }
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                let kind = events[0].get("kind").and_then(Value::as_str).unwrap_or_default().to_owned();
                if kind == "request.opened" {
                    opened = Some(events[0].get("data").cloned().unwrap_or(Value::Null));
                }
                kinds.push(kind);
                let first = host.next_seq;
                host.next_seq += 1;
                host.respond(&request.id, json!({
                    "first_seq": first, "last_seq": first,
                    "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                })).await;
            }
            // Guarded: polling the spent response a second time panics.
            result = &mut rx, if applied.is_none() => {
                applied = Some(result
                    .expect("send deadline")
                    .expect("send response channel")
                    .expect("send applied"));
            }
        }
    }
    let applied = applied.expect("send applied");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("applied turn uuid")
        .to_owned();
    // The association commits before anything the turn asks for.
    assert_eq!(
        kinds.first().map(String::as_str),
        Some("turn.started"),
        "association commits before the ask: {kinds:?}"
    );
    assert_eq!(kinds.last().map(String::as_str), Some("request.opened"));
    let opened = opened.expect("request.opened batch");
    assert_eq!(
        opened.get("command_id").and_then(Value::as_str),
        Some(send_id.as_str())
    );
    assert_eq!(
        opened.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    (send_id, turn_id, opened)
}

/// Begin a send and pump until the applied response, the turn terminal, and
/// one gap all land. Returns the applied value, the host-observed event
/// sequence (`"gap"` marks the gap), and the gap reasons.
async fn send_until_gapped_turn_ends(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    text: &str,
) -> (Value, Vec<String>, Vec<String>) {
    let send_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": text},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut applied: Option<Value> = None;
    let mut sequence: Vec<String> = Vec::new();
    let mut gaps: Vec<String> = Vec::new();
    loop {
        let terminal = sequence.iter().any(|event| event == "turn.completed");
        if applied.is_some() && terminal && !gaps.is_empty() {
            break;
        }
        tokio::select! {
            biased;
            request = host.next_request() => {
                match request.method.as_str() {
                    "events.put" => {
                        let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                        assert_eq!(events.len(), 1, "one native record per batch");
                        sequence.push(events[0].get("kind").and_then(Value::as_str).unwrap_or_default().to_owned());
                        let first = host.next_seq;
                        host.next_seq += 1;
                        host.respond(&request.id, json!({
                            "first_seq": first, "last_seq": first,
                            "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                        })).await;
                    }
                    "source.gap" => {
                        gaps.push(request.params.get("reason").and_then(Value::as_str).unwrap_or_default().to_owned());
                        sequence.push("gap".to_owned());
                        let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                        host.respond(&request.id, json!({"gap_id": gap_id, "seq": host.next_seq})).await;
                        host.next_seq += 1;
                    }
                    other => panic!("unexpected host request: {other}"),
                }
            }
            // Guarded: polling the spent response a second time panics.
            result = &mut rx, if applied.is_none() => {
                applied = Some(result
                    .expect("send deadline")
                    .expect("send response channel")
                    .expect("send applied"));
            }
        }
    }
    (applied.expect("send applied"), sequence, gaps)
}

/// Send one approve/deny and pump until the reply value lands, collecting
/// every committed batch on either side of it (note 48). The resolution
/// always commits before its own ack, so an applied reply collects
/// `request.resolved` first, possibly followed by the decided turn's own
/// facts racing the response; rejected and unknown collect nothing.
/// Returns the reply value, the collected batches in order, and the reply id.
async fn reply_for_permission(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    verb: &str,
    args: Value,
    request_id: &str,
    turn_id: &str,
) -> (Value, Vec<(String, Value)>, String) {
    let reply_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": reply_id,
                "verb": verb,
                "args": args,
                "turn_id": turn_id,
                "request_id": request_id,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut collected: Vec<(String, Value)> = Vec::new();
    let reply = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                collected.push((
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
            result = &mut rx => {
                break result
                    .expect("reply deadline")
                    .expect("reply response channel")
                    .expect("reply value");
            }
        }
    };
    (reply, collected, reply_id)
}

/// Drain the rest of one decided turn: `collected` holds batches the reply
/// pump already gathered, and reading continues until `turn.completed`.
/// Returns every batch in commit order.
async fn drain_decided_turn(
    host: &mut Host,
    agent_id: &str,
    collected: Vec<(String, Value)>,
) -> Vec<(String, Value)> {
    let mut committed = collected;
    while committed.last().map(|(kind, _)| kind.as_str()) != Some("turn.completed") {
        let (kind, data) = host.answer_any_events_put(agent_id).await;
        assert!(
            matches!(
                kind.as_str(),
                "request.resolved" | "message.completed" | "turn.completed"
            ),
            "decided turn facts out of order: {kind}"
        );
        committed.push((kind, data));
    }
    committed
}

/// One send round trip. The plugin only answers `agent.command` after the
/// exact `command_lifecycle` association commits `turn.started`, so inbound
/// batches are acked while the call is in flight; the remaining turn facts
/// are drained afterwards on either side of the response. Returns the send
/// id, the applied turn uuid and every committed `(kind, data)` in order.
async fn send_turn(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    text: &str,
) -> (String, String, Vec<(String, Value)>) {
    let send_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
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

async fn stop_agent(host: &mut Host, agent_id: &str, generation: i64) {
    let stop_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.stop",
            json!({
                "agent_id": agent_id,
                "generation": generation,
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
                // Retained asks close before the terminal session fact.
                let kind = events[0].get("kind").and_then(Value::as_str).unwrap_or_default();
                assert!(
                    kind == "session.ended" || kind == "request.closed",
                    "stop commits only closes and the session end: {kind}"
                );
                ended = ended || kind == "session.ended";
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

/// Shut down a plugin whose owned session never reached readiness:
/// `session.ended` still commits while the shutdown call is in flight.
async fn shutdown_with_session_end(host: &mut Host) {
    let rx = host.begin_call("shutdown", json!({})).await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut ended = false;
    let shutdown = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
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
    let shutdown = shutdown
        .expect("shutdown deadline")
        .expect("shutdown channel")
        .expect("shutdown");
    assert_eq!(
        shutdown.get("state").and_then(Value::as_str),
        Some("shutdown")
    );
    assert!(ended, "shutdown commits session.ended");
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
    let (send_id, turn_id, committed) = send_turn(&mut scenario.host, &agent_id, 1, "hello").await;
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
    // Steer, inject and answer stay unsupported: no exact native mechanism.
    for verb in ["steer", "inject", "answer"] {
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
    stop_agent(&mut scenario.host, &agent_id, 1).await;
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
    let (send_id, _, committed) = send_turn(&mut scenario.host, &agent_id, 1, "hello").await;
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
    stop_agent(&mut scenario.host, &agent_id, 1).await;
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
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, 1, "hello").await;
    let terminal = committed
        .iter()
        .find(|(kind, _)| kind == "turn.completed")
        .map(|(_, data)| data)
        .expect("terminal data");
    assert_eq!(
        terminal.get("status").and_then(Value::as_str),
        Some("failed")
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
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
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, 1, "hello").await;
    let terminal = committed
        .iter()
        .find(|(kind, _)| kind == "turn.completed")
        .map(|(_, data)| data)
        .expect("terminal data");
    assert_eq!(
        terminal.get("status").and_then(Value::as_str),
        Some("completed [REDACTED]")
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
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
    stop_agent(&mut scenario.host, &agent_id, 1).await;
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
    let (send_id, _, committed) = send_turn(&mut scenario.host, &agent_id, 1, "hello").await;
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
    let (_, _, recommitted) = send_turn(&mut scenario.host, &agent_id, 1, "again").await;
    assert_eq!(
        recommitted
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        vec!["turn.started", "turn.completed"]
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
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
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("scripted-drop");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

/// A send that fails the native encode after the pending-turn registration
/// (64c) answers `rejected` and unwedges: the next send on the same session
/// is accepted, not `busy`. Without the `cancel_turn` on the no-write path
/// the follow-up send fails `busy` and this scenario fails.
async fn scenario_rejected_send_does_not_wedge_session(owned: OwnedChildren) {
    // 1-char ids shrink the `agent.command` envelope: the oversized prompt
    // must fit the 1 MiB plugin frame while its native user-input encoding
    // exceeds the 1 MiB native frame, so the host envelope must be the
    // smaller of the two. `turn_id`/`request_id` are omitted (`#[serde(default)]`).
    let mut start = StartConfig::fresh("ok", claude_profile());
    start.agent_id = Some("a".to_owned());
    let (mut scenario, _) = Scenario::start_full(start, &owned).await;
    let agent_id = scenario.agent_id.clone();
    const MAX_FRAME_BYTES: usize = 1024 * 1024;
    const MAX_TEXT_BYTES: usize = 512 * 1024;
    // Both frames carry the same prompt; each `"` escapes to 2 bytes, so a
    // quotes-only prompt of N chars adds exactly 2N bytes to each frame past
    // its measured empty-text length. Lengths, not key order, matter.
    let host_empty = json!({
        "jsonrpc": "2.0",
        "id": "b",
        "method": "agent.command",
        "params": {
            "agent_id": agent_id,
            "generation": 1,
            "command_id": "big",
            "verb": "send",
            "args": {"text": ""},
        },
    })
    .to_string()
    .len();
    // Mirror of the plugin's native user-input record
    // (`plugins/claude/src/native/types.rs` `OutboundUserInput`, no
    // `skip_serializing_if` on `parent_tool_use_id`); uuids are always 36
    // chars (`host.rs` `new_uuid`).
    let pad36 = "x".repeat(36);
    let native_empty = json!({
        "type": "user",
        "uuid": pad36,
        "session_id": "x".repeat(36),
        "parent_tool_use_id": Value::Null,
        "message": {"role": "user", "content": ""},
    })
    .to_string()
    .len();
    // Largest N whose host frame (plus newline) still fits, smallest N whose
    // native encoding (plus newline) exceeds the bound; use the middle.
    let host_max = (MAX_FRAME_BYTES - 1 - host_empty) / 2;
    let native_min = (MAX_FRAME_BYTES - 1 - native_empty) / 2 + 1;
    assert!(
        native_min <= host_max,
        "no quote count fits the host frame while overflowing the native frame \
         (host_empty={host_empty}, native_empty={native_empty}); envelopes drifted"
    );
    let quotes = (native_min + host_max) / 2;
    assert!(
        quotes <= MAX_TEXT_BYTES,
        "oversized prompt must pass the host 512 KiB pre-check to reach the native encode"
    );
    let big = "\"".repeat(quotes);
    let send_params = json!({
        "agent_id": agent_id,
        "generation": 1,
        "command_id": "big",
        "verb": "send",
        "args": {"text": big},
    });
    let rx = scenario
        .host
        .begin_call_with_id("b", "agent.command", send_params)
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    // The failed encode commits nothing: the first inbound frame must be
    // the rejection, never a host request.
    let rejected = tokio::select! {
        biased;
        request = scenario.host.next_request() => {
            panic!("rejected input commits nothing, plugin asked {}", request.method);
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
    assert_eq!(
        scenario.report().get("user_inputs").and_then(Value::as_u64),
        Some(0),
        "the rejected send wrote nothing to the CLI"
    );
    // The session is not wedged behind the withdrawn pending turn: a normal
    // send on the same session applies (without the 64c fix this is `busy`).
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, 1, "hello").await;
    assert!(
        committed.iter().any(|(kind, _)| kind == "turn.completed"),
        "follow-up turn completes"
    );
    assert_eq!(
        scenario.report().get("user_inputs").and_then(Value::as_u64),
        Some(1)
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
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
    stop_agent(&mut scenario.host, &agent_id, 1).await;
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
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_logged_out_cli_logs_in_then_launches(owned: OwnedChildren) {
    let config = StartConfig::fresh("logged-out-login-completes", claude_profile());
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
    outcome.expect("the CLI's own login completes, then the session starts");
    assert!(
        scenario.root.join("login.marker").exists(),
        "the plugin ran the CLI's login before launching"
    );
    assert_eq!(
        scenario.report()["controls"]
            .get("initialize")
            .and_then(Value::as_u64),
        Some(1)
    );
    shutdown_with_session_end(&mut scenario.host).await;
    scenario.host.shutdown_child().await;
}

async fn scenario_abandoned_login_rejects_start(owned: OwnedChildren) {
    let mut config = StartConfig::fresh("logged-out-login-abandons", claude_profile());
    config.expect_ready = false;
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("an unfinished login must fail start");
    assert!(error.contains("native_not_dispatched"), "{error}");
    assert!(
        !scenario.root.join("report.json").exists(),
        "no session CLI was started"
    );
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_model_not_in_catalogue_rejected_before_ready(owned: OwnedChildren) {
    let mut config = StartConfig::fresh("model-mismatch", claude_profile());
    config.expect_ready = false;
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
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

async fn scenario_late_trailing_record_rejects_with_known_drain(owned: OwnedChildren) {
    // The trailing session_start arrives only after stdin EOF during
    // graceful terminate+drain: closing the event consumer first would drop
    // the queue and overflow even with a confirmed child exit (PR284 race).
    // Fixed order keeps the suppressed consumer alive through drain, so the
    // start still fails profile_error with known exit/drain and no I/O.
    let start_command_id = ulid::Ulid::new().to_string();
    let mut config = StartConfig::fresh("model-mismatch-late", claude_profile());
    config.expect_ready = false;
    config.command_id = Some(start_command_id.clone());
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("late trailing record must still fail profile");
    assert!(error.contains("profile_error"), "{error}");
    assert!(!error.contains("unknown"), "{error}");
    assert_eq!(
        scenario.report()["controls"]
            .get("initialize")
            .and_then(Value::as_u64),
        Some(1),
        "no second initialize write"
    );
    assert_eq!(
        scenario.report().get("user_inputs").and_then(Value::as_u64),
        Some(0),
        "no session input was sent"
    );
    scenario.assert_cli_exit("stdin-eof");
    retry_same_start_expects(
        &mut scenario,
        &start_command_id,
        "model-mismatch-late",
        "profile_error",
    )
    .await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_poisoned_drain_keeps_unknown_controller(owned: OwnedChildren) {
    // True drain uncertainty (invalid trailing JSON during graceful drain)
    // stays unknown with the controller retained, never recoded to the
    // underlying profile mismatch. Exit is known, drain is not.
    let start_command_id = ulid::Ulid::new().to_string();
    let mut config = StartConfig::fresh("model-mismatch-poisoned", claude_profile());
    config.expect_ready = false;
    config.command_id = Some(start_command_id.clone());
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("poisoned drain must fail start");
    assert!(error.contains("unknown"), "{error}");
    assert!(!error.contains("profile_error"), "{error}");
    assert!(
        error.contains("unconfirmed at"),
        "bounded cleanup stage and cause retained: {error}"
    );
    assert_eq!(
        scenario.report()["controls"]
            .get("initialize")
            .and_then(Value::as_u64),
        Some(1),
        "no second initialize write"
    );
    assert_eq!(
        scenario.report().get("user_inputs").and_then(Value::as_u64),
        Some(0),
        "no session input was sent"
    );
    scenario.assert_cli_exit("stdin-eof");
    retry_same_start_expects(
        &mut scenario,
        &start_command_id,
        "model-mismatch-poisoned",
        "unknown",
    )
    .await;
    // Genuine drain uncertainty: shutdown must stay unknown, never success.
    // Contract: terminate_session requires begin/exit/drain/ended all Ok;
    // here drain is malformed and the event consumer is already closed, so
    // shutdown reports the exact unknown while the plugin still exits 0
    // after responding. Failed-start (no controller) would succeed here, so
    // this unknown also proves the controller is retained.
    let shutdown_error = scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect_err("poisoned shutdown must stay unknown");
    assert!(shutdown_error.contains("unknown"), "{shutdown_error}");
    assert!(
        shutdown_error.contains("Claude process/output drain or session.ended outcome is unknown"),
        "{shutdown_error}"
    );
    assert_eq!(
        scenario.report()["controls"]
            .get("initialize")
            .and_then(Value::as_u64),
        Some(1),
        "shutdown performs no new native I/O"
    );
    assert_eq!(
        scenario.report().get("user_inputs").and_then(Value::as_u64),
        Some(0),
        "shutdown sends no input"
    );
    scenario.assert_cli_exit("stdin-eof");
    scenario.host.shutdown_child().await;
}

/// Retry the exact same start ids: the retained outcome replays with no new
/// native I/O (controls and inputs unchanged, no new CLI spawn).
async fn retry_same_start_expects(
    scenario: &mut Scenario,
    start_command_id: &str,
    script: &str,
    expected_code: &str,
) {
    let agent_id = scenario.agent_id.clone();
    let workspace = scenario
        .root
        .join("workspace")
        .to_string_lossy()
        .into_owned();
    let report_path = scenario
        .root
        .join("report.json")
        .to_string_lossy()
        .into_owned();
    let self_exe = std::env::current_exe().expect("test executable");
    let mut full_profile = claude_profile();
    full_profile["cmd"] = Value::String(self_exe.to_string_lossy().into_owned());
    let outcome = scenario
        .host
        .call(
            "agent.start",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": start_command_id,
                "profile": full_profile,
                "workspace": workspace,
                "target_environment": {
                    "ELIOT_FAKE_CLAUDE_SCRIPT": script,
                    "ELIOT_FAKE_CLAUDE_REPORT": report_path,
                },
                "note": null,
                "source_plan": {"session": "stream", "turn": "stream", "message": "stream", "request": "stream"},
                "source_state": null,
                "resume_from": null,
                "initial": null,
            }),
        )
        .await;
    let error = outcome.expect_err("same-start retry must replay its failure");
    assert!(error.contains(expected_code), "{error}");
    assert_eq!(
        scenario.report()["controls"]
            .get("initialize")
            .and_then(Value::as_u64),
        Some(1),
        "retry performs no new native I/O"
    );
    assert_eq!(
        scenario.report().get("user_inputs").and_then(Value::as_u64),
        Some(0),
        "retry sends no input"
    );
}

async fn scenario_session_mismatch_is_unknown_with_gap(owned: OwnedChildren) {
    let mut config = StartConfig::fresh("session-mismatch", claude_profile());
    config.expect_ready = false;
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("session mismatch must fail start");
    assert!(error.contains("unknown"), "{error}");
    shutdown_with_session_end(&mut scenario.host).await;
    scenario.host.shutdown_child().await;
}

async fn scenario_not_idle_rejected_before_ready(owned: OwnedChildren) {
    let mut config = StartConfig::fresh("not-idle", claude_profile());
    config.expect_ready = false;
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
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
                    "source_plan": {"session": "stream", "turn": "stream", "message": "stream", "request": "stream"},
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

async fn scenario_resume_realigns_drifted_settings_and_sends(owned: OwnedChildren) {
    let (agent_id, native_scope, native_ref) = retained_subject(&owned).await;
    // Generation 2 resumes the exact retained session id.
    let config = StartConfig {
        script: "resume-realign".to_owned(),
        profile: claude_profile_with_mode("plan"),
        agent_id: Some(agent_id.clone()),
        generation: 2,
        command_id: None,
        resume_from: json!({"native_scope": native_scope, "native_ref": native_ref}),
        source_state: resume_source_state(&agent_id),
        extra_env: Vec::new(),
        expect_ready: true,
        requested_mode: Some("plan".to_owned()),
        effective_mode: "plan".to_owned(),
    };
    let (mut gen2, outcome) = Scenario::start_full(config, &owned).await;
    let (ready, _) = outcome.expect("gen2 ready");
    assert_eq!(
        ready.get("native_ref").and_then(Value::as_str),
        Some(native_ref.as_str())
    );
    assert_eq!(
        ready.get("native_scope").and_then(Value::as_str),
        Some(native_scope.as_str())
    );
    // The old prompt is never resent: no input before the new send.
    assert_eq!(
        gen2.report().get("user_inputs").and_then(Value::as_u64),
        Some(0)
    );
    // The drifted permission mode is realigned via setters; the model always
    // is. The CLI runs under `--resume` with no minted `--session-id`.
    let report = gen2.report();
    assert_eq!(
        report["controls"].get("initialize").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        report["controls"].get("set_model").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        report["controls"]
            .get("set_permission_mode")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        report.get("resume").and_then(Value::as_str),
        Some(native_ref.as_str())
    );
    let argv = report
        .get("argv")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let argv: Vec<&str> = argv.iter().filter_map(Value::as_str).collect();
    for flag in [
        "--resume",
        "--permission-prompt-tool",
        "stdio",
        "--permission-mode",
        "plan",
        "--model",
    ] {
        assert!(argv.contains(&flag), "resume argv carries {flag}: {argv:?}");
    }
    assert!(
        !argv.contains(&"--session-id"),
        "resume never mints a session id: {argv:?}"
    );
    send_turn(&mut gen2.host, &agent_id, 2, "continue").await;
    assert_eq!(
        gen2.report().get("user_inputs").and_then(Value::as_u64),
        Some(1)
    );
    stop_agent(&mut gen2.host, &agent_id, 2).await;
    gen2.assert_cli_exit("stdin-eof");
    gen2.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen2.host.shutdown_child().await;
}

async fn scenario_resume_without_drift_skips_permission_setter(owned: OwnedChildren) {
    let (agent_id, native_scope, native_ref) = retained_subject(&owned).await;
    let config = StartConfig {
        script: "resume-ok".to_owned(),
        profile: claude_profile_with_mode("plan"),
        agent_id: Some(agent_id.clone()),
        generation: 2,
        command_id: None,
        resume_from: json!({"native_scope": native_scope, "native_ref": native_ref}),
        source_state: resume_source_state(&agent_id),
        extra_env: Vec::new(),
        expect_ready: true,
        requested_mode: Some("plan".to_owned()),
        effective_mode: "plan".to_owned(),
    };
    let (mut gen2, outcome) = Scenario::start_full(config, &owned).await;
    outcome.expect("gen2 ready");
    // The mode already matches: only the model setter runs.
    let report = gen2.report();
    assert_eq!(
        report["controls"].get("set_model").and_then(Value::as_u64),
        Some(1)
    );
    assert!(
        report["controls"].get("set_permission_mode").is_none(),
        "no permission setter without drift: {:?}",
        report["controls"]
    );
    stop_agent(&mut gen2.host, &agent_id, 2).await;
    gen2.assert_cli_exit("stdin-eof");
    gen2.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen2.host.shutdown_child().await;
}

async fn scenario_resume_scope_mismatch_rejected_before_spawn(owned: OwnedChildren) {
    let (agent_id, _, native_ref) = retained_subject(&owned).await;
    let config = StartConfig {
        script: "resume-ok".to_owned(),
        profile: claude_profile_with_mode("plan"),
        agent_id: Some(agent_id.clone()),
        generation: 2,
        command_id: None,
        resume_from: json!({
            "native_scope": "claude-config:/elsewhere",
            "native_ref": native_ref,
        }),
        source_state: resume_source_state(&agent_id),
        extra_env: Vec::new(),
        expect_ready: false,
        requested_mode: Some("plan".to_owned()),
        effective_mode: "plan".to_owned(),
    };
    let (mut gen2, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("foreign scope must fail resume");
    assert!(
        error.contains("outside the authenticated native scope"),
        "{error}"
    );
    gen2.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen2.host.shutdown_child().await;
    assert!(
        !gen2.root.join("report.json").exists(),
        "no native process spawns for a foreign resume subject"
    );
}

async fn scenario_permission_mode_quirk_reported(owned: OwnedChildren) {
    // The installed CLI reads `manual` back as `default`: requested vs
    // effective is reported, never forced, and no setter runs on a fresh
    // start.
    let config = StartConfig {
        script: "quirk-manual".to_owned(),
        profile: claude_profile_with_mode("manual"),
        agent_id: None,
        generation: 1,
        command_id: None,
        resume_from: Value::Null,
        source_state: Value::Null,
        extra_env: Vec::new(),
        expect_ready: true,
        requested_mode: Some("manual".to_owned()),
        effective_mode: "default".to_owned(),
    };
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
    outcome.expect("ready reports the quirk");
    let report = scenario.report();
    assert_eq!(
        report.get("permission_mode").and_then(Value::as_str),
        Some("manual")
    );
    assert!(
        report["controls"].get("set_model").is_none()
            && report["controls"].get("set_permission_mode").is_none(),
        "fresh start applies settings via argv only: {:?}",
        report["controls"]
    );
    let agent_id = scenario.agent_id.clone();
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_approve_allows_with_echoed_input(owned: OwnedChildren) {
    let mut scenario = Scenario::start("approve", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (_, turn_id, opened) =
        send_until_permission_asked(&mut scenario.host, &agent_id, 1, "list files").await;
    assert_eq!(
        opened.get("request_id").and_then(Value::as_str),
        Some("fake-perm-1")
    );
    assert_eq!(
        opened.get("kind").and_then(Value::as_str),
        Some("permission")
    );
    assert_eq!(
        opened["details"].get("tool_name").and_then(Value::as_str),
        Some("Bash")
    );
    assert_eq!(
        opened["details"]["input"]
            .get("command")
            .and_then(Value::as_str),
        Some("ls /tmp")
    );
    let (reply, collected, reply_id) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        json!({}),
        "fake-perm-1",
        &turn_id,
    )
    .await;
    assert_eq!(reply.get("state").and_then(Value::as_str), Some("applied"));
    assert_eq!(
        reply.get("request_id").and_then(Value::as_str),
        Some("fake-perm-1")
    );
    // The resolution binds the exact reply and carries no open details.
    assert_eq!(
        collected.first().map(|(kind, _)| kind.as_str()),
        Some("request.resolved")
    );
    let resolved = &collected
        .first()
        .map(|(_, data)| data)
        .expect("resolved data");
    assert_eq!(
        resolved.get("command_id").and_then(Value::as_str),
        Some(reply_id.as_str())
    );
    assert_eq!(
        resolved.get("request_id").and_then(Value::as_str),
        Some("fake-perm-1")
    );
    assert_eq!(
        resolved.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert!(resolved.get("kind").is_none() && resolved.get("details").is_none());
    let committed = drain_decided_turn(&mut scenario.host, &agent_id, collected).await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        vec!["request.resolved", "message.completed", "turn.completed"]
    );
    assert_eq!(
        committed[2].1.get("final_text").and_then(Value::as_str),
        Some("fake reply after decision. ")
    );
    // The native decision echoes the asked input back as `updatedInput`.
    let replies = scenario
        .report()
        .get("permission_replies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        replies.as_slice(),
        &[json!({
            "request_id": "fake-perm-1",
            "subtype": "success",
            "result": {"behavior": "allow", "updatedInput": {"command": "ls /tmp"}},
        })]
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_approve_redacts_tool_input_secrets(owned: OwnedChildren) {
    let config = StartConfig {
        script: "approve-secret".to_owned(),
        profile: claude_profile(),
        agent_id: None,
        generation: 1,
        command_id: None,
        resume_from: Value::Null,
        source_state: Value::Null,
        extra_env: vec![("FAKE_CLAUDE_TOKEN".to_owned(), "token-abc-123".to_owned())],
        expect_ready: true,
        requested_mode: None,
        effective_mode: "default".to_owned(),
    };
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
    outcome.expect("ready");
    let agent_id = scenario.agent_id.clone();
    let (_, turn_id, opened) =
        send_until_permission_asked(&mut scenario.host, &agent_id, 1, "deploy").await;
    // The committed details redact the registered secret ...
    let committed_command = opened["details"]["input"]
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        committed_command.contains("[REDACTED]") && !committed_command.contains("token-abc-123"),
        "committed input redacts the secret: {committed_command}"
    );
    let (reply, collected, _) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        json!({}),
        "fake-perm-1",
        &turn_id,
    )
    .await;
    assert_eq!(reply.get("state").and_then(Value::as_str), Some("applied"));
    drain_decided_turn(&mut scenario.host, &agent_id, collected).await;
    // ... while the native decision still echoes the original input.
    let replies = scenario
        .report()
        .get("permission_replies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(replies.len(), 1);
    assert_eq!(
        replies[0]["result"]["updatedInput"]
            .get("command")
            .and_then(Value::as_str),
        Some("deploy token-abc-123")
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_deny_denies_with_message(owned: OwnedChildren) {
    let mut scenario = Scenario::start("deny", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (_, turn_id, _) =
        send_until_permission_asked(&mut scenario.host, &agent_id, 1, "list files").await;
    let (reply, collected, reply_id) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "deny",
        json!({}),
        "fake-perm-1",
        &turn_id,
    )
    .await;
    assert_eq!(reply.get("state").and_then(Value::as_str), Some("applied"));
    assert_eq!(
        collected.first().map(|(kind, _)| kind.as_str()),
        Some("request.resolved")
    );
    assert_eq!(
        collected
            .first()
            .map(|(_, data)| data)
            .and_then(|data| data.get("command_id"))
            .and_then(Value::as_str),
        Some(reply_id.as_str())
    );
    let committed = drain_decided_turn(&mut scenario.host, &agent_id, collected).await;
    assert_eq!(
        committed.last().map(|(kind, _)| kind.as_str()),
        Some("turn.completed")
    );
    let replies = scenario
        .report()
        .get("permission_replies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        replies.as_slice(),
        &[json!({
            "request_id": "fake-perm-1",
            "subtype": "success",
            "result": {"behavior": "deny", "message": "Eliot denied this tool call."},
        })]
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_reply_guards_reject_without_rewrite(owned: OwnedChildren) {
    let mut scenario = Scenario::start("approve", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (_, turn_id, _) =
        send_until_permission_asked(&mut scenario.host, &agent_id, 1, "list files").await;
    // An unknown request, non-empty args, and a foreign turn all refuse
    // without a native write and without touching the open ask.
    for (verb, args, request_id, turn) in [
        (
            "approve",
            json!({}),
            "no-such-request".to_owned(),
            turn_id.clone(),
        ),
        (
            "approve",
            json!({"choice": "x"}),
            "fake-perm-1".to_owned(),
            turn_id.clone(),
        ),
        (
            "approve",
            json!({}),
            "fake-perm-1".to_owned(),
            ulid::Ulid::new().to_string(),
        ),
    ] {
        let (reply, collected, _) = reply_for_permission(
            &mut scenario.host,
            &agent_id,
            1,
            verb,
            args,
            &request_id,
            &turn,
        )
        .await;
        assert_eq!(
            reply.get("state").and_then(Value::as_str),
            Some("rejected"),
            "{verb} guard must refuse"
        );
        assert!(collected.is_empty(), "refused reply commits nothing");
    }
    // The exact reply still applies afterwards.
    let (reply, collected, _) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        json!({}),
        "fake-perm-1",
        &turn_id,
    )
    .await;
    assert_eq!(reply.get("state").and_then(Value::as_str), Some("applied"));
    // A second decision finds nothing pending and refuses without a rewrite.
    let (again, collected_again, _) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "deny",
        json!({}),
        "fake-perm-1",
        &turn_id,
    )
    .await;
    assert_eq!(again.get("state").and_then(Value::as_str), Some("rejected"));
    // The second pump may already hold the decided turn's facts; fold them
    // into the drain so nothing is lost.
    let mut collected_all = collected;
    collected_all.extend(collected_again);
    let committed = drain_decided_turn(&mut scenario.host, &agent_id, collected_all).await;
    assert_eq!(
        committed.last().map(|(kind, _)| kind.as_str()),
        Some("turn.completed")
    );
    let replies = scenario
        .report()
        .get("permission_replies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(replies.len(), 1, "exactly one native decision");
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_unreplied_request_closes_with_the_turn(owned: OwnedChildren) {
    let mut scenario = Scenario::start("request-close", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let send_id = ulid::Ulid::new().to_string();
    let rx = scenario
        .host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": "hello"},
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
            request = scenario.host.next_request() => {
                assert_eq!(request.method, "events.put");
                let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                committed.push((
                    events[0].get("kind").and_then(Value::as_str).unwrap_or_default().to_owned(),
                    events[0].get("data").cloned().unwrap_or(Value::Null),
                ));
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
        .expect("send response channel")
        .expect("send applied");
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("applied turn uuid")
        .to_owned();
    while committed.last().map(|(kind, _)| kind.as_str()) != Some("turn.completed") {
        let (kind, data) = scenario.host.answer_any_events_put(&agent_id).await;
        committed.push((kind, data));
    }
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            "turn.started",
            "request.opened",
            "message.completed",
            "request.closed",
            "turn.completed"
        ]
    );
    let closed = committed
        .iter()
        .find(|(kind, _)| kind == "request.closed")
        .map(|(_, data)| data)
        .expect("closed data");
    assert_eq!(
        closed.get("request_id").and_then(Value::as_str),
        Some("fake-perm-1")
    );
    assert_eq!(
        closed.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert!(
        closed.get("command_id").is_none(),
        "close carries no command binding"
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_lost_reply_is_unknown_without_rewrite(owned: OwnedChildren) {
    let mut scenario = Scenario::start("drop-reply", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    // The turn associates and asks, then the CLI dies: the ask commits, then
    // the stream loss gaps. The send response may land on either side.
    let send_id = ulid::Ulid::new().to_string();
    let rx = scenario
        .host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": "hello"},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut applied: Option<Value> = None;
    let mut kinds: Vec<String> = Vec::new();
    let mut gapped = false;
    loop {
        if applied.is_some() && kinds.iter().any(|kind| kind == "request.opened") && gapped {
            break;
        }
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                match request.method.as_str() {
                    "events.put" => {
                        let events = request.params.get("events").and_then(Value::as_array).cloned().unwrap_or_default();
                        assert_eq!(events.len(), 1, "one native record per batch");
                        kinds.push(events[0].get("kind").and_then(Value::as_str).unwrap_or_default().to_owned());
                        let first = scenario.host.next_seq;
                        scenario.host.next_seq += 1;
                        scenario.host.respond(&request.id, json!({
                            "first_seq": first, "last_seq": first,
                            "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                        })).await;
                    }
                    "source.gap" => {
                        gapped = true;
                        let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                        scenario.host.respond(&request.id, json!({"gap_id": gap_id, "seq": scenario.host.next_seq})).await;
                        scenario.host.next_seq += 1;
                    }
                    other => panic!("unexpected host request: {other}"),
                }
            }
            // Guarded: polling the spent response a second time panics.
            result = &mut rx, if applied.is_none() => {
                applied = Some(result
                    .expect("send deadline")
                    .expect("send response channel")
                    .expect("send applied"));
            }
        }
    }
    let applied = applied.expect("send applied");
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("applied turn uuid")
        .to_owned();
    assert_eq!(kinds.as_slice(), ["turn.started", "request.opened"]);
    // The reply write dies with the stream: unknown, twice, with no decision
    // ever delivered.
    for verb in ["approve", "deny"] {
        let (reply, collected, _) = reply_for_permission(
            &mut scenario.host,
            &agent_id,
            1,
            verb,
            json!({}),
            "fake-perm-1",
            &turn_id,
        )
        .await;
        assert_eq!(
            reply.get("state").and_then(Value::as_str),
            Some("unknown"),
            "{verb} after stream loss stays unknown"
        );
        assert!(collected.is_empty(), "unknown reply commits nothing");
    }
    let report = scenario.report();
    assert_eq!(report.get("user_inputs").and_then(Value::as_u64), Some(1));
    assert_eq!(
        report
            .get("permission_replies")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    // Stop still settles: the unanswered ask closes before `session.ended`.
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("scripted-drop");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_initialize_error_is_unknown_without_rewrite(owned: OwnedChildren) {
    // A refused initialize leaves the CLI state uncertain: the start fails
    // `unknown` (never recoded to `profile_error`) with no second write.
    let mut config = StartConfig::fresh("initialize-error", claude_profile());
    config.expect_ready = false;
    let (mut scenario, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("refused initialize must fail start");
    assert!(error.contains("unknown"), "{error}");
    assert!(!error.contains("profile_error"), "{error}");
    assert_eq!(
        scenario.report()["controls"]
            .get("initialize")
            .and_then(Value::as_u64),
        Some(1),
        "no second initialize write"
    );
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_setter_echo_mismatch_is_unknown(owned: OwnedChildren) {
    // Resume with a manual profile: the drifted mode runs the permission
    // setter, the CLI echoes its own mode instead, and the start fails
    // `unknown` — an unexpected echo is not a profile error.
    let (agent_id, native_scope, native_ref) = retained_subject(&owned).await;
    let config = StartConfig {
        script: "setter-drift".to_owned(),
        profile: claude_profile_with_mode("manual"),
        agent_id: Some(agent_id.clone()),
        generation: 2,
        command_id: None,
        resume_from: json!({"native_scope": native_scope, "native_ref": native_ref}),
        source_state: resume_source_state(&agent_id),
        extra_env: Vec::new(),
        expect_ready: false,
        requested_mode: Some("manual".to_owned()),
        // Unchecked: the start fails before readiness.
        effective_mode: "default".to_owned(),
    };
    let (mut gen2, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("echo mismatch must fail resume");
    assert!(error.contains("unknown"), "{error}");
    assert!(!error.contains("profile_error"), "{error}");
    let report = gen2.report();
    assert_eq!(
        report["controls"].get("set_model").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        report["controls"]
            .get("set_permission_mode")
            .and_then(Value::as_u64),
        Some(1),
        "no second setter write"
    );
    assert_eq!(
        report.get("resume").and_then(Value::as_str),
        Some(native_ref.as_str())
    );
    gen2.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen2.host.shutdown_child().await;
}

async fn scenario_retained_session_missing_is_unknown(owned: OwnedChildren) {
    // The resumed CLI announces a fresh session instead of the retained one:
    // no `session.ready` commits and the start fails `unknown` with a gap.
    let (agent_id, native_scope, native_ref) = retained_subject(&owned).await;
    let config = StartConfig {
        script: "resume-missing".to_owned(),
        profile: claude_profile(),
        agent_id: Some(agent_id.clone()),
        generation: 2,
        command_id: None,
        resume_from: json!({"native_scope": native_scope, "native_ref": native_ref}),
        source_state: resume_source_state(&agent_id),
        extra_env: Vec::new(),
        expect_ready: false,
        requested_mode: None,
        effective_mode: "default".to_owned(),
    };
    let (mut gen2, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("missing retained session must fail resume");
    assert!(error.contains("unknown"), "{error}");
    assert_eq!(
        gen2.report().get("resume").and_then(Value::as_str),
        Some(native_ref.as_str())
    );
    shutdown_with_session_end(&mut gen2.host).await;
    gen2.host.shutdown_child().await;
}

async fn scenario_stale_generation_resume_rejected_before_spawn(owned: OwnedChildren) {
    // Resuming generation 3 from a generation-1 readback is stale: the
    // resume fails before any native process spawns.
    let (agent_id, native_scope, native_ref) = retained_subject(&owned).await;
    let config = StartConfig {
        script: "resume-ok".to_owned(),
        profile: claude_profile(),
        agent_id: Some(agent_id.clone()),
        generation: 3,
        command_id: None,
        resume_from: json!({"native_scope": native_scope, "native_ref": native_ref}),
        source_state: resume_source_state(&agent_id),
        extra_env: Vec::new(),
        expect_ready: false,
        requested_mode: None,
        effective_mode: "default".to_owned(),
    };
    let (mut gen3, outcome) = Scenario::start_full(config, &owned).await;
    let error = outcome.expect_err("stale resume must fail");
    assert!(error.contains("stale_generation"), "{error}");
    gen3.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen3.host.shutdown_child().await;
    assert!(
        !gen3.root.join("report.json").exists(),
        "no native process spawns for a stale resume subject"
    );
}

async fn scenario_late_old_generation_frames_gap_without_applying(owned: OwnedChildren) {
    // A lifecycle frame for a turn this generation never submitted (the
    // stand-in for a late generation-1 frame) gaps instead of applying, and
    // the submitted turn still runs to its terminal.
    let mut scenario = Scenario::start("stale-frame", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (applied, sequence, gaps) =
        send_until_gapped_turn_ends(&mut scenario.host, &agent_id, 1, "hello").await;
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(
        sequence.as_slice(),
        ["gap", "turn.started", "message.completed", "turn.completed"]
    );
    assert_eq!(gaps.len(), 1);
    assert!(
        gaps[0].contains("did not match the exact submitted turn"),
        "stale frame gaps: {:?}",
        gaps
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_stray_permission_ask_gets_explicit_deny(owned: OwnedChildren) {
    // An ask that arrives with no exact active turn (here: after the turn
    // ended) gets an explicit deny plus a gap — never an open request.
    let mut scenario = Scenario::start("stray-ask", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (applied, sequence, gaps) =
        send_until_gapped_turn_ends(&mut scenario.host, &agent_id, 1, "hello").await;
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("applied turn uuid")
        .to_owned();
    assert_eq!(
        sequence.as_slice(),
        ["turn.started", "message.completed", "turn.completed", "gap"]
    );
    assert_eq!(gaps.len(), 1);
    assert!(
        gaps[0].contains("arrived outside the exact active turn"),
        "stray ask gaps: {:?}",
        gaps
    );
    // A reply to the never-opened ask is rejected without a rewrite.
    let (reply, collected, _) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "deny",
        json!({}),
        "fake-perm-1",
        &turn_id,
    )
    .await;
    assert_eq!(reply.get("state").and_then(Value::as_str), Some("rejected"));
    assert_eq!(
        reply.get("reason").and_then(Value::as_str),
        Some("Claude request is no longer pending")
    );
    assert!(collected.is_empty(), "rejected reply commits nothing");
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    // The deny was piped before input closed, so the exited CLI's report
    // carries it deterministically: exactly one explicit deny.
    let replies = scenario
        .report()
        .get("permission_replies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        replies.as_slice(),
        &[json!({
            "request_id": "fake-perm-1",
            "subtype": "success",
            "result": {"behavior": "deny", "message": "Eliot denied this tool call."},
        })]
    );
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_duplicate_permission_id_denied_and_original_fenced(owned: OwnedChildren) {
    // Same native id with another ask: one explicit deny plus a gap. Native
    // holds a single callback per id, so which ask the deny consumed is
    // UNKNOWN (never claimed) and the retained original is fenced uncertain:
    // late approves stay unknown with no second write and no resolution.
    let mut scenario = Scenario::start("duplicate-ask", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (_, turn_id, opened) =
        send_until_permission_asked(&mut scenario.host, &agent_id, 1, "list files").await;
    assert_eq!(
        opened.get("request_id").and_then(Value::as_str),
        Some("fake-perm-1")
    );
    assert_eq!(
        opened["details"].get("tool_name").and_then(Value::as_str),
        Some("Bash")
    );
    let gap = scenario.host.next_request().await;
    assert_eq!(gap.method, "source.gap");
    let reason = gap
        .params
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        reason.contains("reused its native request id"),
        "duplicate gaps: {reason}"
    );
    let gap_id = gap
        .params
        .get("gap_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    scenario
        .host
        .respond(
            &gap.id,
            json!({"gap_id": gap_id, "seq": scenario.host.next_seq}),
        )
        .await;
    scenario.host.next_seq += 1;
    for verb in ["approve", "deny"] {
        let (reply, collected, _) = reply_for_permission(
            &mut scenario.host,
            &agent_id,
            1,
            verb,
            json!({}),
            "fake-perm-1",
            &turn_id,
        )
        .await;
        assert_eq!(
            reply.get("state").and_then(Value::as_str),
            Some("unknown"),
            "{verb} after a conflicting deny stays unknown"
        );
        assert!(
            reply
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("unresolved"),
            "unknown keeps its reason: {reply:?}"
        );
        assert!(collected.is_empty(), "fenced reply commits nothing");
    }
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    let report = scenario.report();
    let replies = report
        .get("permission_replies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        replies.as_slice(),
        &[json!({
            "request_id": "fake-perm-1",
            "subtype": "success",
            "result": {"behavior": "deny", "message": "Eliot denied this tool call."},
        })]
    );
    assert_eq!(report.get("user_inputs").and_then(Value::as_u64), Some(1));
    assert_eq!(
        report
            .get("controls")
            .and_then(|c| c.get("initialize"))
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

async fn scenario_pending_overflow_denied_without_opening(owned: OwnedChildren) {
    // Sixteen asks open; the seventeenth gets an explicit deny plus a gap and
    // never opens. The retained sixteen still decide; the overflow id rejects.
    let mut scenario = Scenario::start("overflow-asks", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (_, turn_id, first) =
        send_until_permission_asked(&mut scenario.host, &agent_id, 1, "list files").await;
    let mut opened: Vec<String> = vec![
        first
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    ];
    let mut gap_reason: Option<String> = None;
    while opened.len() < 16 || gap_reason.is_none() {
        let request = scenario.host.next_request().await;
        match request.method.as_str() {
            "events.put" => {
                let events = request
                    .params
                    .get("events")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                assert_eq!(
                    events[0].get("kind").and_then(Value::as_str),
                    Some("request.opened")
                );
                let data = events[0].get("data").cloned().unwrap_or(Value::Null);
                opened.push(
                    data.get("request_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                );
                let first_seq = scenario.host.next_seq;
                scenario.host.next_seq += 1;
                scenario
                    .host
                    .respond(
                        &request.id,
                        json!({
                            "first_seq": first_seq, "last_seq": first_seq,
                            "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                        }),
                    )
                    .await;
            }
            "source.gap" => {
                gap_reason = Some(
                    request
                        .params
                        .get("reason")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                );
                let gap_id = request
                    .params
                    .get("gap_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                scenario
                    .host
                    .respond(
                        &request.id,
                        json!({"gap_id": gap_id, "seq": scenario.host.next_seq}),
                    )
                    .await;
                scenario.host.next_seq += 1;
            }
            other => panic!("unexpected host request: {other}"),
        }
    }
    let expected: Vec<String> = (1..=16).map(|i| format!("fake-perm-{i}")).collect();
    assert_eq!(opened, expected);
    assert!(
        gap_reason
            .unwrap_or_default()
            .contains("pending native request bound was exceeded")
    );
    let (rejected, collected, _) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "deny",
        json!({}),
        "fake-perm-17",
        &turn_id,
    )
    .await;
    assert_eq!(
        rejected.get("state").and_then(Value::as_str),
        Some("rejected")
    );
    assert_eq!(
        rejected.get("reason").and_then(Value::as_str),
        Some("Claude request is no longer pending")
    );
    assert!(collected.is_empty(), "overflow id was never opened");
    let (reply, collected, reply_id) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        json!({}),
        "fake-perm-1",
        &turn_id,
    )
    .await;
    assert_eq!(reply.get("state").and_then(Value::as_str), Some("applied"));
    assert_eq!(
        collected.first().map(|(kind, _)| kind.as_str()),
        Some("request.resolved")
    );
    assert_eq!(
        collected[0].1.get("command_id").and_then(Value::as_str),
        Some(reply_id.as_str())
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    let report = scenario.report();
    let replies = report
        .get("permission_replies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        replies.as_slice(),
        &[
            json!({
                "request_id": "fake-perm-17",
                "subtype": "success",
                "result": {"behavior": "deny", "message": "Eliot denied this tool call."},
            }),
            json!({
                "request_id": "fake-perm-1",
                "subtype": "success",
                "result": {"behavior": "allow", "updatedInput": {"command": "ls /tmp"}},
            }),
        ]
    );
    assert_eq!(report.get("user_inputs").and_then(Value::as_u64), Some(1));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn permission_fault_turn(scenario: &mut Scenario, expect_gap: bool) -> (String, String) {
    let command_id = ulid::Ulid::new().to_string();
    let rx = scenario
        .host
        .begin_call(
            "agent.command",
            json!({
                "agent_id":scenario.agent_id, "generation":1, "command_id":command_id,
                "verb":"send", "args":{"text":"list files"}, "turn_id":null, "request_id":null,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut applied: Option<Value> = None;
    let mut opened = Vec::new();
    let mut gapped = false;
    while applied.is_none() || opened.len() < 16 || (expect_gap && !gapped) {
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                match request.method.as_str() {
                    "events.put" => {
                        let event = &request.params["events"][0];
                        let kind = event["kind"].as_str().unwrap_or_default();
                        assert!(matches!(kind, "turn.started" | "request.opened"),
                            "no fabricated terminal or stopped fact: {kind}");
                        if kind == "request.opened" {
                            opened.push(event["data"]["request_id"].as_str().unwrap().to_owned());
                        }
                        let seq = scenario.host.next_seq;
                        scenario.host.next_seq += 1;
                        scenario.host.respond(&request.id, json!({"first_seq":seq,"last_seq":seq,
                            "cursor":request.params.get("cursor").cloned().unwrap_or(Value::Null)})).await;
                    }
                    "source.gap" => {
                        gapped = true;
                        scenario.host.respond(&request.id, json!({"gap_id":request.params["gap_id"],
                            "seq":scenario.host.next_seq})).await;
                        scenario.host.next_seq += 1;
                    }
                    other => panic!("unexpected permission fault request: {other}"),
                }
            }
            result = &mut rx, if applied.is_none() => {
                applied = Some(result.expect("send deadline").expect("send channel").expect("send admission"));
            }
        }
    }
    assert_eq!(
        opened,
        (1..=16)
            .map(|i| format!("fake-perm-{i}"))
            .collect::<Vec<_>>()
    );
    let applied = applied.unwrap();
    assert_eq!(applied["state"], "applied");
    (command_id, applied["turn_id"].as_str().unwrap().to_owned())
}

async fn scenario_permission_identity_exhaustion_fails_closed(owned: OwnedChildren) {
    let mut scenario =
        Scenario::start("permission-fence-exhausted", claude_profile(), &owned).await;
    let agent = scenario.agent_id.clone();
    let (command, turn) = permission_fault_turn(&mut scenario, true).await;
    assert!(
        owned.cli_exited().await,
        "exhaustion must close owned native input before an explicit stop"
    );
    scenario.assert_cli_exit("stdin-eof");
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id":agent,"generation":1,"command_id":command,"turn_id":turn,
            }),
        )
        .await
        .expect("unknown native session snapshot");
    assert_eq!(snapshot["state"], "unknown");
    assert_eq!(snapshot["source_unknown"], true);
    assert_eq!(snapshot["last_turn"], Value::Null);
    let (reply, events, _) = reply_for_permission(
        &mut scenario.host,
        &agent,
        1,
        "approve",
        json!({}),
        "fake-perm-1",
        &turn,
    )
    .await;
    assert_eq!(reply["state"], "unknown");
    assert!(
        events.is_empty(),
        "a fenced session never admits a later owner decision"
    );
    let report = scenario.report();
    let replies = report["permission_replies"].as_array().unwrap();
    assert_eq!(replies.len(), 16);
    assert_eq!(
        replies
            .iter()
            .map(|reply| reply["request_id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        (17..=32)
            .map(|i| format!("fake-perm-{i}"))
            .collect::<Vec<_>>()
    );
    assert!(
        replies
            .iter()
            .all(|reply| reply["result"]["behavior"] == "deny")
    );
    assert_eq!(report["user_inputs"], 1);
    stop_agent(&mut scenario.host, &agent, 1).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn owner_reply_with_unknown_commit(scenario: &mut Scenario, turn: &str) -> Value {
    let rx = scenario.host.begin_call("agent.command", json!({
        "agent_id":scenario.agent_id,"generation":1,"command_id":ulid::Ulid::new().to_string(),
        "verb":"approve","args":{},"request_id":"fake-perm-1","turn_id":turn,
    })).await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut attempted = 0;
    let result = loop {
        tokio::select! {
            biased;
            request = scenario.host.next_request() => {
                assert_eq!(request.method, "events.put");
                assert_eq!(request.params["events"][0]["kind"], "request.resolved");
                attempted += 1;
                // Existing host-ACK fault: malformed acknowledgement leaves
                // the normal owner's delivered decision unknown, CLI still live.
                scenario.host.respond(&request.id, json!({"first_seq":0,"last_seq":0,
                    "cursor":request.params.get("cursor").cloned().unwrap_or(Value::Null)})).await;
            }
            result = &mut rx => break result,
        }
    };
    assert_eq!(attempted, 1);
    result
        .expect("reply deadline")
        .expect("reply channel")
        .expect("unknown reply value")
}

async fn scenario_unknown_owner_reply_conflict_no_second_io(owned: OwnedChildren) {
    let mut scenario = Scenario::start("owner-unknown-conflict", claude_profile(), &owned).await;
    let agent = scenario.agent_id.clone();
    let (command, turn) = permission_fault_turn(&mut scenario, false).await;
    let reply = owner_reply_with_unknown_commit(&mut scenario, &turn).await;
    assert_eq!(reply["state"], "unknown");
    // Overflow callback 17 follows the conflicting callback on the same pipe.
    // Its deny/exit is a deterministic processing barrier, not a timed absence.
    assert!(
        owned.cli_exited().await,
        "callback processing barrier must finish"
    );
    scenario.assert_cli_exit("permission-barrier");
    let report = scenario.report();
    let replies = report["permission_replies"].as_array().unwrap();
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0]["request_id"], "fake-perm-1");
    assert_eq!(replies[0]["result"]["behavior"], "allow");
    assert_eq!(replies[1]["request_id"], "fake-perm-17");
    assert_eq!(replies[1]["result"]["behavior"], "deny");
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id":agent,"generation":1,"command_id":command,"turn_id":turn,
            }),
        )
        .await
        .expect("uncertain snapshot");
    assert_eq!(snapshot["state"], "unknown");
    assert_eq!(snapshot["last_turn"], Value::Null);
    assert_eq!(
        scenario.host.call("shutdown", json!({})).await,
        Err("unknown".to_owned()),
        "an uncertain batch must not fabricate session.ended"
    );
    scenario.host.shutdown_child().await;
}

async fn scenario_callback_fence_survives_turn_result(owned: OwnedChildren) {
    let mut scenario = Scenario::start("deny-result-repeat", claude_profile(), &owned).await;
    let agent = scenario.agent_id.clone();
    let (_, sequence, gaps) =
        send_until_gapped_turn_ends(&mut scenario.host, &agent, 1, "list files").await;
    assert_eq!(gaps.len(), 1);
    assert!(sequence.iter().any(|kind| kind == "turn.completed"));
    assert!(!sequence.iter().any(|kind| kind == "session.ended"));
    assert!(
        owned.cli_exited().await,
        "post-result callback processing barrier must finish"
    );
    scenario.assert_cli_exit("permission-barrier");
    let report = scenario.report();
    let replies = report["permission_replies"].as_array().unwrap();
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0]["request_id"], "fake-perm-17");
    assert_eq!(replies[1]["request_id"], "fake-perm-18");
    assert!(
        replies
            .iter()
            .all(|reply| reply["result"]["behavior"] == "deny")
    );
    assert_eq!(report["user_inputs"], 1);
    stop_agent(&mut scenario.host, &agent, 1).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_repeated_dropped_id_never_rewritten_nor_retried(owned: OwnedChildren) {
    // The dropped seventeenth repeats immediately (still full) and again
    // after a slot frees: never a second deny, never an admission. Unknown
    // is never rewritten known.
    let mut scenario = Scenario::start("overflow-repeat-asks", claude_profile(), &owned).await;
    let agent_id = scenario.agent_id.clone();
    let (_, turn_id, first) =
        send_until_permission_asked(&mut scenario.host, &agent_id, 1, "list files").await;
    let mut opened: Vec<String> = vec![
        first
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    ];
    let mut gap_reason: Option<String> = None;
    while opened.len() < 16 || gap_reason.is_none() {
        let request = scenario.host.next_request().await;
        match request.method.as_str() {
            "events.put" => {
                let events = request
                    .params
                    .get("events")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                assert_eq!(
                    events[0].get("kind").and_then(Value::as_str),
                    Some("request.opened")
                );
                let data = events[0].get("data").cloned().unwrap_or(Value::Null);
                opened.push(
                    data.get("request_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                );
                let first_seq = scenario.host.next_seq;
                scenario.host.next_seq += 1;
                scenario
                    .host
                    .respond(
                        &request.id,
                        json!({
                            "first_seq": first_seq, "last_seq": first_seq,
                            "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                        }),
                    )
                    .await;
            }
            "source.gap" => {
                gap_reason = Some(
                    request
                        .params
                        .get("reason")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                );
                let gap_id = request
                    .params
                    .get("gap_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                scenario
                    .host
                    .respond(
                        &request.id,
                        json!({"gap_id": gap_id, "seq": scenario.host.next_seq}),
                    )
                    .await;
                scenario.host.next_seq += 1;
            }
            other => panic!("unexpected host request: {other}"),
        }
    }
    let expected: Vec<String> = (1..=16).map(|i| format!("fake-perm-{i}")).collect();
    assert_eq!(opened, expected);
    assert!(
        gap_reason
            .unwrap_or_default()
            .contains("pending native request bound was exceeded")
    );
    let (rejected, collected, _) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "deny",
        json!({}),
        "fake-perm-17",
        &turn_id,
    )
    .await;
    assert_eq!(
        rejected.get("state").and_then(Value::as_str),
        Some("rejected")
    );
    assert!(collected.is_empty(), "dropped id was never opened");
    let (reply, collected, _) = reply_for_permission(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        json!({}),
        "fake-perm-1",
        &turn_id,
    )
    .await;
    assert_eq!(reply.get("state").and_then(Value::as_str), Some("applied"));
    assert_eq!(
        collected.first().map(|(kind, _)| kind.as_str()),
        Some("request.resolved")
    );
    stop_agent(&mut scenario.host, &agent_id, 1).await;
    scenario.assert_cli_exit("stdin-eof");
    let report = scenario.report();
    let replies = report
        .get("permission_replies")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let denies_17 = replies
        .iter()
        .filter(|r| r.get("request_id").and_then(Value::as_str) == Some("fake-perm-17"))
        .count();
    assert_eq!(
        denies_17, 1,
        "exactly one deny for the dropped id: {replies:?}"
    );
    assert_eq!(
        replies.as_slice(),
        &[
            json!({
                "request_id": "fake-perm-17",
                "subtype": "success",
                "result": {"behavior": "deny", "message": "Eliot denied this tool call."},
            }),
            json!({
                "request_id": "fake-perm-1",
                "subtype": "success",
                "result": {"behavior": "allow", "updatedInput": {"command": "ls /tmp"}},
            }),
        ]
    );
    assert_eq!(report.get("user_inputs").and_then(Value::as_u64), Some(1));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}
