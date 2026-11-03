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
        scenario_turn_start_internal_error_is_unknown_without_rewrite().await;
        println!("ok - turn/start -32603 is unknown without a second write");
        scenario_turn_start_steered_latches_unknown().await;
        println!("ok - steered turn/start latches unknown without a second write");
        scenario_effort_mismatch_is_rejected_before_ready().await;
        println!("ok - effort mismatch is rejected before session.ready");
        scenario_model_mismatch_is_rejected_before_ready().await;
        println!("ok - model mismatch is rejected before session.ready");
        scenario_workspace_mismatch_is_rejected_before_ready().await;
        println!("ok - workspace mismatch is rejected before session.ready");
        scenario_invalid_profile_rejected_before_spawn().await;
        println!("ok - invalid profile rejected before spawn");
        scenario_resume_reopens_the_retained_session_in_a_new_generation().await;
        println!("ok - resume reopens the retained session in a new generation");
        scenario_resume_of_gone_session_is_an_explicit_error().await;
        println!("ok - resume of a gone session is an explicit error");
        scenario_lost_resume_reply_is_unknown_with_one_write().await;
        println!("ok - lost resume reply is unknown with exactly one write");
        scenario_resume_with_pending_requests_adopts_and_proceeds().await;
        println!("ok - resume with pending requests adopts and proceeds");
        scenario_approve_resolves_the_exact_offered_choice().await;
        println!("ok - approve resolves the exact offered choice");
        scenario_answer_and_deny_settle_prompts().await;
        println!("ok - answer and deny settle prompts");
        scenario_decide_internal_error_is_unknown_without_rewrite().await;
        println!("ok - decide internal error is unknown without rewrite");
        scenario_stale_choice_after_update_is_rejected().await;
        println!("ok - stale choice after update is rejected");
        scenario_stale_requirement_is_rejected_then_valid_reply_applies().await;
        println!("ok - stale requirement is rejected, then a valid reply applies");
        scenario_multistage_approval_decides_each_stage().await;
        println!("ok - multistage approval decides each stage");
        scenario_lost_decide_reply_is_unknown_without_rewrite().await;
        println!("ok - lost decide reply is unknown without a second write");
        scenario_stale_pull_does_not_reopen_stream_resolved_request().await;
        println!("ok - stale pull does not reopen a stream-resolved request");
        scenario_request_details_redact_secrets().await;
        println!("ok - request details redact secrets");
        scenario_resume_with_undecodable_pull_proceeds().await;
        println!("ok - resume with an undecodable pull proceeds");
        scenario_unknown_native_request_is_rejected_with_a_gap().await;
        println!("ok - unknown native request is rejected with a gap");
        scenario_resume_rejects_stale_generation_1_output().await;
        println!("ok - resume rejects stale generation-1 output");
        scenario_resume_refuses_served_history_and_keeps_the_retained_id().await;
        println!("ok - resume refuses served history and keeps the retained id");
    });
    println!("muse_plugin: all scenarios passed");
}

// ---------------------------------------------------------------------------
// Fake MSP host fixture (runs as a child of the plugin under test)
// ---------------------------------------------------------------------------

/// The retained native session and its retained completed turn for the
/// resume scripts. The scenario passes the same pair in `resume_from` and
/// the retained source state.
const RETAINED_SESSION: &str = "fake-retained-session-1";
const RETAINED_TURN: &str = "fake-retained-turn-1";

struct FakeMsp {
    script: String,
    report_path: PathBuf,
    session_id: Option<String>,
    turn_active: bool,
    turn_count: i64,
    cursor_seq: u64,
    view_log: Vec<(String, String, Value)>,
    counts: HashMap<String, u64>,
    next_server_id: u64,
    receipts: u64,
    approval_stage: i64,
    prompts_opened: u64,
    prompts_settled: u64,
    last_turn: Option<String>,
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
        turn_count: 0,
        cursor_seq: 0,
        view_log: Vec::new(),
        counts: HashMap::new(),
        next_server_id: 0,
        receipts: 0,
        approval_stage: 1,
        prompts_opened: 0,
        prompts_settled: 0,
        last_turn: None,
    };
    if matches!(
        fake.script.as_str(),
        "resume-happy"
            | "resume-pending"
            | "drop-session-resume"
            | "resume-stale-replay"
            | "resume-inline-history"
            | "resume-stale-pull"
            | "resume-undecodable-pull"
    ) {
        fake.seed_retained_session();
    }
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
            if frame.get("method").is_none() {
                // Client response to a server-initiated request (a
                // presentation receipt or a typed rejection): record it, no
                // reply.
                if frame.get("result").is_some() || frame.get("error").is_some() {
                    self.receipts += 1;
                    self.write_report();
                }
                continue;
            }
            if !self.handle_call(&frame, &mut stdout) {
                return;
            }
            stdout.flush().expect("fixture stdout");
            self.write_report();
        }
        self.write_report();
    }

    /// A stored session with one completed turn, as a previous Eliot
    /// generation left it. Only `view/page` can see this history; nothing is
    /// pushed until the new generation acts.
    fn seed_retained_session(&mut self) {
        self.session_id = Some(RETAINED_SESSION.to_owned());
        self.turn_count = 1;
        let started_cursor = self.next_cursor();
        self.view_log.push((
            started_cursor.clone(),
            "turn/started".to_owned(),
            json!({
                "commandId": RETAINED_TURN,
                "sessionId": RETAINED_SESSION,
                "sourceRange": Self::source_range(),
                "turnId": RETAINED_TURN,
                "viewCursor": started_cursor,
            }),
        ));
        let item_cursor = self.next_cursor();
        self.view_log.push((
            item_cursor.clone(),
            "item/completed".to_owned(),
            json!({
                "item": {
                    "itemId": "fake-item-retained-1",
                    "kind": "agentMessage",
                    "revision": 2,
                    "status": "completed",
                    "text": "Retained reply from the previous generation.",
                    "turnId": RETAINED_TURN,
                },
                "sessionId": RETAINED_SESSION,
                "sourceRange": Self::source_range(),
                "viewCursor": item_cursor,
            }),
        ));
        let completed_cursor = self.next_cursor();
        self.view_log.push((
            completed_cursor.clone(),
            "turn/completed".to_owned(),
            json!({
                "sessionId": RETAINED_SESSION,
                "sourceRange": Self::source_range(),
                "terminal": "completed",
                "turnId": RETAINED_TURN,
                "viewCursor": completed_cursor,
            }),
        ));
    }

    fn handle_call(&mut self, frame: &Value, stdout: &mut std::io::StdoutLock) -> bool {
        let id = frame.get("id").cloned().unwrap_or(Value::Null);
        let method = frame
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = frame.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "initialize" => {
                // A1 answers userInput/request through Eliot's answer/deny
                // verbs, so the client must declare the capable posture.
                assert_eq!(
                    params
                        .get("capabilities")
                        .and_then(|capabilities| capabilities.get("userInputDialogs")),
                    Some(&Value::Bool(true)),
                    "client declares user-input dialogs"
                );
                self.reply(stdout, &id, self.initialize_result())
            }
            "model/list" => self.reply(stdout, &id, self.model_list_result()),
            "session/start" => {
                let session_id = format!("fake-session-{}", self.counts["session/start"]);
                self.session_id = Some(session_id.clone());
                let workspace = params
                    .get("workspaceRoot")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let workspace = if self.script == "cwd-mismatch" {
                    "C:\\other-workspace".to_owned()
                } else {
                    workspace
                };
                let model = params
                    .get("modelId")
                    .and_then(Value::as_str)
                    .unwrap_or(MODEL)
                    .to_owned();
                let model = if self.script == "model-mismatch" {
                    "other-model".to_owned()
                } else {
                    model
                };
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
                // The durable reconfigure record lands with the ack; serve its
                // view event so the start path can read the standing default
                // back through `view/page`.
                let effort = params
                    .get("reasoningEffort")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let effort = if self.script == "effort-mismatch" {
                    "low".to_owned()
                } else {
                    effort
                };
                let session_id = self.session_id.clone().unwrap_or_default();
                let cursor = self.next_cursor();
                self.emit(
                    stdout,
                    "session/reasoningEffortChanged",
                    json!({
                        "reasoningEffort": effort,
                        "sessionId": session_id,
                        "source": "user",
                        "sourceRange": Self::source_range(),
                        "viewCursor": cursor,
                    }),
                );
            }
            "session/resume" => {
                if self.script == "drop-session-resume" {
                    // Lost reply: exit without answering. The report already
                    // records this receipt.
                    std::process::exit(0);
                }
                let requested = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if self.session_id.as_deref() != Some(requested) || requested.is_empty() {
                    self.reply_error(stdout, &id, -32020, "Session not found");
                    return true;
                }
                assert!(
                    params
                        .get("commandId")
                        .and_then(Value::as_str)
                        .is_some_and(|command| !command.trim().is_empty()),
                    "resume carries its idempotency handle"
                );
                assert_eq!(
                    params.get("excludeItems"),
                    Some(&Value::Bool(true)),
                    "resume pages history itself instead of republishing it"
                );
                assert!(
                    params.get("input").is_none(),
                    "resume never carries old input"
                );
                let workspace = std::env::var("ELIOT_FAKE_MSP_WORKSPACE").unwrap_or_default();
                let session = self.session_object(requested, "idle", None, MODEL, &workspace);
                let pending = if self.script == "resume-pending" {
                    json!([
                        {
                            "kind": "approval",
                            "approvalId": "fake-approval-1",
                            "viewCursor": self.head_cursor(),
                        },
                        {
                            "kind": "userInput",
                            "userInputId": "fake-prompt-1",
                            "viewCursor": self.head_cursor(),
                        },
                    ])
                } else if matches!(
                    self.script.as_str(),
                    "resume-stale-pull" | "resume-undecodable-pull"
                ) {
                    json!([
                        {
                            "kind": "approval",
                            "approvalId": "fake-approval-1",
                            "viewCursor": self.head_cursor(),
                        },
                    ])
                } else {
                    json!([])
                };
                // `resume-inline-history` serves retained history despite
                // `excludeItems: true`: the plugin must refuse the resume
                // instead of republishing it.
                let history = if self.script == "resume-inline-history" {
                    json!({
                        "items": [{
                            "itemId": "fake-item-retained-1",
                            "kind": "agentMessage",
                            "revision": 2,
                            "status": "completed",
                            "text": "Retained reply from the previous generation.",
                            "turnId": RETAINED_TURN,
                        }],
                        "mode": "inline",
                        "snapshot": null,
                    })
                } else {
                    json!({"items": null, "mode": "none", "noneReason": "excluded", "snapshot": null})
                };
                self.reply(
                    stdout,
                    &id,
                    json!({
                        "history": history,
                        "pendingRequests": pending,
                        "session": session,
                        "viewCursor": self.head_cursor(),
                    }),
                );
                self.emit(stdout, "session/started", json!({"session": session}));
                if self.script == "resume-pending" {
                    // SS5.6: the full payloads arrive as re-issued
                    // server-to-client requests right after the resume
                    // response. Both name the retained (dead) turn, so the
                    // new generation receipts and drops them as orphans.
                    let approval = self.approval_request_params(
                        RETAINED_SESSION,
                        RETAINED_TURN,
                        "fake-approval-1",
                        1,
                    );
                    self.emit_request(stdout, "approval/request", approval);
                    let prompt = self.user_input_request_params(
                        RETAINED_SESSION,
                        RETAINED_TURN,
                        "fake-prompt-1",
                    );
                    self.emit_request(stdout, "userInput/request", prompt);
                }
            }
            "turn/start" => {
                if self.script == "drop-turn-start" {
                    // Lost reply: exit without answering. The report already
                    // records this receipt.
                    std::process::exit(0);
                }
                if self.script == "turn-start-internal-error" {
                    self.reply_error(stdout, &id, -32603, "Internal error");
                    return true;
                }
                if self.script == "turn-start-steered" {
                    let command = params
                        .get("commandId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    self.reply(
                        stdout,
                        &id,
                        json!({
                            "commandId": command,
                            "disposition": "steered",
                            "startedNewTurn": false,
                            "status": "accepted",
                            "turnId": "fake-foreign-turn",
                        }),
                    );
                    return true;
                }
                if self.script == "resume-stale-replay" {
                    // Old-host output redelivered on the new generation's
                    // stream: the retained generation-1 turn completion.
                    // Stream-only — it is not in the durable view — and the
                    // plugin must gap it, never apply it to generation 2.
                    let stale = json!({
                        "jsonrpc": "2.0",
                        "method": "turn/completed",
                        "params": {
                            "sessionId": RETAINED_SESSION,
                            "sourceRange": Self::source_range(),
                            "terminal": "completed",
                            "turnId": RETAINED_TURN,
                            "viewCursor": "fake-stale-cursor-1",
                        },
                    });
                    writeln!(stdout, "{stale}").expect("fixture stale event");
                }
                let command = params
                    .get("commandId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let session_id = self.session_id.clone().unwrap_or_default();
                self.turn_active = true;
                self.turn_count += 1;
                self.last_turn = Some(command.clone());
                if self.script == "resume-stale-pull" {
                    // The pending approval resolves on the stream after the
                    // resume snapshot captured it: the re-issued open binds
                    // the live turn, then the resolution wins. Both buffer
                    // until the associate roundtrip, so the stale snapshot
                    // entry (retained-turn-bound) applies strictly after the
                    // stream resolution and must not reopen it.
                    let open =
                        self.approval_request_params(&session_id, &command, "fake-approval-1", 1);
                    self.emit_request(stdout, "approval/request", open);
                    let cursor = self.next_cursor();
                    self.emit(
                        stdout,
                        "approval/resolved",
                        json!({
                            "approvalId": "fake-approval-1",
                            "decision": "approved",
                            "resolvedBy": {"kind": "user"},
                            "sessionId": session_id,
                            "sourceRange": Self::source_range(),
                            "turnId": command,
                            "viewCursor": cursor,
                        }),
                    );
                }
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
                if self.script == "request-unknown-method" {
                    // A request family this slice cannot present: the plugin
                    // rejects it with a typed error and gaps the source while
                    // the turn itself still completes.
                    self.emit_request(stdout, "zzz/unknown", json!({"turnId": command}));
                }
                if matches!(
                    self.script.as_str(),
                    "request-approval"
                        | "request-prompt"
                        | "request-stale-choice"
                        | "request-multistage"
                        | "request-stale-decide"
                        | "request-redact"
                        | "decide-internal-error"
                        | "decide-lost-reply"
                ) {
                    // Parked turn: the request opens now and the completion
                    // lands only after the scenario answers it.
                    self.approval_stage = 1;
                    match self.script.as_str() {
                        "request-prompt" => {
                            let first = self.user_input_request_params(
                                &session_id,
                                &command,
                                "fake-prompt-1",
                            );
                            self.emit_request(stdout, "userInput/request", first);
                            let second = self.user_input_request_params(
                                &session_id,
                                &command,
                                "fake-prompt-2",
                            );
                            self.emit_request(stdout, "userInput/request", second);
                            self.prompts_opened = 2;
                        }
                        "request-stale-choice" => {
                            // The open carries the stage-2 offer and the
                            // update repeats it: the stage-1 choice id the
                            // scenario names is unknown to the current offer
                            // whether the update has applied yet or not, so
                            // the first reply is rejected locally without a
                            // write under both interleavings. (An update
                            // that *changes* the choices cannot
                            // synchronize with the reply — it commits
                            // nothing observable — so the offer-advance path
                            // is covered by the multistage and stale-decide
                            // scenarios instead.)
                            let open = self.approval_request_params(
                                &session_id,
                                &command,
                                "fake-approval-1",
                                2,
                            );
                            self.emit_request(stdout, "approval/request", open);
                            self.approval_stage = 2;
                            let updated =
                                self.approval_updated_params(&session_id, "fake-approval-1", 2);
                            self.emit(stdout, "approval/updated", updated);
                        }
                        "request-redact" => {
                            // The script name travels as a
                            // target-environment value, so it is a
                            // configured secret: embed it in the free-text
                            // offer fields the committed request.opened must
                            // redact. Choice ids stay exact (the scenario
                            // still replies by id).
                            let secret = self.script.clone();
                            let mut open = self.approval_request_params(
                                &session_id,
                                &command,
                                "fake-approval-1",
                                1,
                            );
                            open["subject"]["command"] = json!(format!("deploy --token {secret}"));
                            if let Some(choices) = open
                                .get_mut("availableChoices")
                                .and_then(Value::as_array_mut)
                            {
                                for choice in choices {
                                    if let Some(label) = choice.get_mut("label") {
                                        *label = json!(format!(
                                            "{} {secret}",
                                            label.as_str().unwrap_or_default()
                                        ));
                                    }
                                }
                            }
                            self.emit_request(stdout, "approval/request", open);
                            let mut prompt = self.user_input_request_params(
                                &session_id,
                                &command,
                                "fake-prompt-1",
                            );
                            if let Some(questions) =
                                prompt.get_mut("questions").and_then(Value::as_array_mut)
                            {
                                for question in questions {
                                    if let Some(text) = question.get_mut("question") {
                                        *text = json!(format!(
                                            "{} [{secret}]",
                                            text.as_str().unwrap_or_default()
                                        ));
                                    }
                                    if let Some(options) =
                                        question.get_mut("options").and_then(Value::as_array_mut)
                                    {
                                        for option in options {
                                            if let Some(label) = option.get_mut("label") {
                                                *label = json!(format!(
                                                    "{} {secret}",
                                                    label.as_str().unwrap_or_default()
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                            self.emit_request(stdout, "userInput/request", prompt);
                            self.prompts_opened = 1;
                        }
                        _ => {
                            let open = self.approval_request_params(
                                &session_id,
                                &command,
                                "fake-approval-1",
                                1,
                            );
                            self.emit_request(stdout, "approval/request", open);
                        }
                    }
                    return true;
                }
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
                if self.script == "resume-gone" {
                    self.reply_error(stdout, &id, -32020, "Session not found");
                    return true;
                }
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
            "approval/decide" => {
                let approval_id = params
                    .get("approvalId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let choice_id = params
                    .get("choiceId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let command_id = params
                    .get("commandId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                assert!(!approval_id.is_empty(), "decide names its approval");
                assert!(!choice_id.is_empty(), "decide names its choice");
                assert!(!command_id.is_empty(), "decide mints its command id");
                if self.script == "request-stale-decide" && self.approval_stage == 1 {
                    // The multi-stage race, lost: the stage advanced between
                    // the client's read and intake, so the stage-1 decision
                    // is rejected as stale (never written) and the update
                    // carries the current requirement for the retry.
                    self.approval_stage = 2;
                    self.reply_error(stdout, &id, -32053, "approval requirement is stale");
                    let session_id = self.session_id.clone().unwrap_or_default();
                    let updated = self.approval_updated_params(&session_id, &approval_id, 2);
                    self.emit(stdout, "approval/updated", updated);
                    return true;
                }
                // The multi-stage race guard: the decision must carry the
                // approval's CURRENT requirement, never a stale snapshot.
                assert_eq!(
                    params
                        .get("requirementId")
                        .and_then(|requirement| requirement.get("sourceIndex"))
                        .and_then(Value::as_i64),
                    Some(self.approval_stage),
                    "decide carries the current requirement",
                );
                if self.script == "decide-internal-error" {
                    // An internal failure AFTER intake: the reply proves
                    // nothing about the write, so the plugin must report
                    // unknown and never resubmit.
                    self.reply_error(stdout, &id, -32603, "internal error");
                    return true;
                }
                if self.script == "decide-lost-reply" {
                    // Lost reply: exit without answering. The report already
                    // records this receipt.
                    std::process::exit(0);
                }
                let staged = self.script == "request-multistage" && self.approval_stage == 1;
                self.reply(
                    stdout,
                    &id,
                    json!({
                        "approvalId": approval_id,
                        "commandId": command_id,
                        "status": "accepted",
                        "terminal": !staged,
                    }),
                );
                let session_id = self.session_id.clone().unwrap_or_default();
                if staged {
                    self.approval_stage = 2;
                    let updated = self.approval_updated_params(&session_id, &approval_id, 2);
                    self.emit(stdout, "approval/updated", updated);
                    return true;
                }
                let cursor = self.next_cursor();
                self.emit(
                    stdout,
                    "approval/resolved",
                    json!({
                        "approvalId": approval_id,
                        "decision": if choice_id.starts_with("deny") {
                            "denied"
                        } else {
                            "approved"
                        },
                        "resolvedBy": {"kind": "user"},
                        "sessionId": session_id,
                        "sourceRange": Self::source_range(),
                        "turnId": self.last_turn_id(),
                        "viewCursor": cursor,
                    }),
                );
                // A turn parked on both an approval and prompts completes
                // only once every prompt has settled.
                if self.prompts_opened == 0 {
                    let turn_id = self.last_turn_id();
                    self.complete_open_turn(stdout, &turn_id);
                }
            }
            "userInput/answer" => {
                let user_input_id = params
                    .get("userInputId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let command_id = params
                    .get("commandId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                assert!(!user_input_id.is_empty(), "answer names its prompt");
                assert!(!command_id.is_empty(), "answer mints its command id");
                assert!(
                    params.get("answers").and_then(Value::as_array).is_some(),
                    "answer carries answers",
                );
                self.reply(
                    stdout,
                    &id,
                    json!({
                        "commandId": command_id,
                        "status": "accepted",
                        "userInputId": user_input_id,
                    }),
                );
                self.settle_prompt(stdout, &user_input_id, "answered");
            }
            "userInput/cancel" => {
                let user_input_id = params
                    .get("userInputId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                assert!(!user_input_id.is_empty(), "cancel names its prompt");
                let command_id = params
                    .get("commandId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                self.reply(
                    stdout,
                    &id,
                    json!({
                        "commandId": command_id,
                        "status": "accepted",
                        "userInputId": user_input_id,
                    }),
                );
                self.settle_prompt(stdout, &user_input_id, "cancelled");
            }
            "approval/listPending" => {
                assert_eq!(
                    params.get("sessionId").and_then(Value::as_str),
                    self.session_id.as_deref(),
                    "pull is scoped to the owned session",
                );
                let pulled = if self.script == "resume-pending" {
                    let approval = self.approval_request_params(
                        RETAINED_SESSION,
                        RETAINED_TURN,
                        "fake-approval-1",
                        1,
                    );
                    let prompt = self.user_input_request_params(
                        RETAINED_SESSION,
                        RETAINED_TURN,
                        "fake-prompt-1",
                    );
                    json!({"approvals": [approval], "userInputs": [prompt]})
                } else if self.script == "resume-stale-pull" {
                    let approval = self.approval_request_params(
                        RETAINED_SESSION,
                        RETAINED_TURN,
                        "fake-approval-1",
                        1,
                    );
                    json!({"approvals": [approval], "userInputs": []})
                } else if self.script == "resume-undecodable-pull" {
                    // Well-formed enough to decode the pull, but bound to a
                    // foreign session: describe fails and the entry is
                    // skipped (and gapped) instead of adopted.
                    let mut approval = self.approval_request_params(
                        RETAINED_SESSION,
                        RETAINED_TURN,
                        "fake-approval-1",
                        1,
                    );
                    approval["sessionId"] = json!("fake-foreign-session");
                    json!({"approvals": [approval], "userInputs": []})
                } else {
                    json!({"approvals": [], "userInputs": []})
                };
                self.reply(stdout, &id, pulled);
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

    /// A server-initiated must-answer request. Unlike notifications these
    /// carry an id the client must answer, and they are not view events.
    fn emit_request(&mut self, stdout: &mut std::io::StdoutLock, method: &str, params: Value) {
        self.next_server_id += 1;
        let frame = json!({
            "jsonrpc": "2.0",
            "id": format!("fake-srv-{}", self.next_server_id),
            "method": method,
            "params": params,
        });
        writeln!(stdout, "{}", frame).expect("fixture server request");
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
            "turnCount": self.turn_count,
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

    /// Schema-shaped `approval/request` params for one fake approval at the
    /// given multi-stage source index.
    fn approval_request_params(
        &mut self,
        session_id: &str,
        turn_id: &str,
        approval_id: &str,
        stage: i64,
    ) -> Value {
        let choices = if stage <= 1 {
            json!([
                {"choiceId": "allow", "decision": "approved", "label": "Allow",
                 "scope": "once", "acceptsFeedback": false},
                {"choiceId": "deny", "decision": "denied", "label": "Deny",
                 "scope": "once", "acceptsFeedback": true},
            ])
        } else {
            json!([
                {"choiceId": "allow-session", "decision": "approvedForSession",
                 "label": "Allow for session", "scope": "session",
                 "acceptsFeedback": false},
                {"choiceId": "deny", "decision": "denied", "label": "Deny",
                 "scope": "once", "acceptsFeedback": true},
            ])
        };
        let cursor = self.next_cursor();
        json!({
            "approvalId": approval_id,
            "availableChoices": choices,
            "currentRequirementId": {"approvalId": approval_id, "sourceIndex": stage},
            "itemId": format!("fake-item-{approval_id}"),
            "judgeEscalated": false,
            "protectedWrite": false,
            "rawArgs": "{\"command\":\"ls\"}",
            "sessionId": session_id,
            "sourceRange": Self::source_range(),
            "subject": {"kind": "shell", "command": "ls"},
            "taskId": format!("fake-task-{approval_id}"),
            "toolCallId": format!("fake-call-{approval_id}"),
            "toolName": "shell",
            "turnId": turn_id,
            "viewCursor": cursor,
        })
    }

    /// Schema-shaped `userInput/request` params. Prompt 1 mixes a
    /// single-choice and a multiple-choice question; prompt 2 is free text.
    fn user_input_request_params(
        &mut self,
        session_id: &str,
        turn_id: &str,
        user_input_id: &str,
    ) -> Value {
        let questions = if user_input_id == "fake-prompt-1" {
            json!([
                {"header": "Deploy", "id": "q1",
                 "options": [{"label": "staging"}, {"label": "production"}],
                 "question": "Where?", "selection": {"mode": "single"}},
                {"header": "Flags", "id": "q2",
                 "options": [{"label": "dry-run"}, {"label": "force"}],
                 "question": "Which flags?",
                 "selection": {"mode": "multiple", "minSelections": 1, "maxSelections": 2}},
            ])
        } else {
            json!([
                {"header": "Note", "id": "q9", "options": [],
                 "question": "Say something.", "selection": {"mode": "single"}},
            ])
        };
        let cursor = self.next_cursor();
        json!({
            "itemId": format!("fake-item-{user_input_id}"),
            "questions": questions,
            "sessionId": session_id,
            "sourceRange": Self::source_range(),
            "toolCallId": format!("fake-call-{user_input_id}"),
            "toolName": "ask",
            "turnId": turn_id,
            "userInputId": user_input_id,
            "viewCursor": cursor,
        })
    }

    /// Schema-shaped `approval/updated` params for the given stage.
    fn approval_updated_params(
        &mut self,
        session_id: &str,
        approval_id: &str,
        stage: i64,
    ) -> Value {
        let open = self.approval_request_params(session_id, "unused-turn", approval_id, stage);
        json!({
            "approvalId": approval_id,
            "availableChoices": open["availableChoices"],
            "change": {"kind": "stageSatisfied"},
            "currentRequirementId": open["currentRequirementId"],
            "sessionId": session_id,
            "sourceRange": Self::source_range(),
            "subject": open["subject"],
            "viewCursor": open["viewCursor"],
        })
    }

    fn last_turn_id(&self) -> String {
        self.last_turn.clone().unwrap_or_default()
    }

    /// Settle one prompt; the parked turn completes once every opened prompt
    /// has settled.
    fn settle_prompt(
        &mut self,
        stdout: &mut std::io::StdoutLock,
        user_input_id: &str,
        outcome: &str,
    ) {
        let session_id = self.session_id.clone().unwrap_or_default();
        let cursor = self.next_cursor();
        self.emit(
            stdout,
            "userInput/settled",
            json!({
                "outcome": outcome,
                "sessionId": session_id,
                "sourceRange": Self::source_range(),
                "userInputId": user_input_id,
                "viewCursor": cursor,
            }),
        );
        self.prompts_settled += 1;
        if self.prompts_opened > 0 && self.prompts_settled >= self.prompts_opened {
            let turn_id = self.last_turn_id();
            self.complete_open_turn(stdout, &turn_id);
        }
    }

    /// Complete the fake turn the way `turn/start` does: one agentMessage,
    /// then the terminal record.
    fn complete_open_turn(&mut self, stdout: &mut std::io::StdoutLock, turn_id: &str) {
        let session_id = self.session_id.clone().unwrap_or_default();
        let cursor = self.next_cursor();
        self.emit(
            stdout,
            "item/completed",
            json!({
                "item": {
                    "itemId": format!("fake-item-{turn_id}-1"),
                    "kind": "agentMessage",
                    "revision": 2,
                    "status": "completed",
                    "text": "Hello from the fake MSP host.",
                    "turnId": turn_id,
                },
                "sessionId": session_id,
                "sourceRange": Self::source_range(),
                "viewCursor": cursor,
            }),
        );
        let cursor = self.next_cursor();
        self.emit(
            stdout,
            "turn/completed",
            json!({
                "sessionId": session_id,
                "sourceRange": Self::source_range(),
                "terminal": "completed",
                "turnId": turn_id,
                "viewCursor": cursor,
            }),
        );
        self.turn_active = false;
    }

    fn final_text(script: &str) -> &'static str {
        match script {
            "failed-turn" => "partial reply before the failure.",
            _ => "Hello from the fake MSP host.",
        }
    }

    fn write_report(&self) {
        let method_count = |method: &str| self.counts.get(method).copied().unwrap_or(0);
        let report = json!({
            "script": self.script,
            "counts": self.counts,
            "session_starts": method_count("session/start"),
            "session_resumes": method_count("session/resume"),
            "turn_starts": method_count("turn/start"),
            "decides": method_count("approval/decide"),
            "answers": method_count("userInput/answer"),
            "cancels": method_count("userInput/cancel"),
            "pending_pulls": method_count("approval/listPending"),
            "receipts": self.receipts,
        });
        // Atomic publish: the harness may SIGKILL this fixture at any point
        // (failed-start discard), and a truncated report would fail the
        // scenario with a parse error. A rename lands whole or not at all.
        let tmp = self.report_path.with_extension("json.tmp");
        if std::fs::write(&tmp, serde_json::to_string(&report).unwrap()).is_err() {
            return;
        }
        let _ = std::fs::rename(&tmp, &self.report_path);
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
    start_result: Value,
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

struct PendingStart {
    root: PathBuf,
    agent_id: String,
    host: Host,
    params: Value,
    generation: i64,
}

struct FailedAttempt {
    root: PathBuf,
    agent_id: String,
    host: Host,
    params: Value,
}

impl Scenario {
    async fn start(script: &str, profile: Value) -> Self {
        let agent_id = ulid::Ulid::new().to_string();
        let (pending, rx) =
            Self::setup_start(script, profile, agent_id, 1, Value::Null, Value::Null).await;
        pending.finish(rx, None).await
    }

    async fn start_resume(
        script: &str,
        profile: Value,
        agent_id: String,
        resume_from: Value,
        source_state: Value,
        retained_ref: &str,
    ) -> Self {
        let (pending, rx) =
            Self::setup_start(script, profile, agent_id, 2, resume_from, source_state).await;
        pending.finish(rx, Some(retained_ref)).await
    }

    /// Spawn the plugin and dispatch `agent.start`, returning the in-flight
    /// call for the scenario to drive. Resume runs at generation 2 with the
    /// retained subject, like a new generation after a stopped one.
    async fn setup_start(
        script: &str,
        profile: Value,
        agent_id: String,
        generation: i64,
        resume_from: Value,
        source_state: Value,
    ) -> (PendingStart, oneshot::Receiver<Result<Value, String>>) {
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-muse-plugin-test-{}",
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
        assert_eq!(manifest.get("name").and_then(Value::as_str), Some("muse"));
        assert_eq!(
            manifest.get("protocol").and_then(Value::as_str),
            Some("eliot-plugin/1")
        );
        assert_eq!(
            manifest["verbs"],
            json!([
                "launch", "resume", "send", "approve", "deny", "answer", "stop"
            ])
        );
        assert_eq!(
            manifest["sources"]["controlled"],
            json!({"message": "stream", "request": "stream", "session": "stream", "turn": "stream"})
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
                "ELIOT_FAKE_MSP_SCRIPT": script,
                "ELIOT_FAKE_MSP_REPORT": root.join("report.json").to_string_lossy(),
                "ELIOT_FAKE_MSP_WORKSPACE": root.join("workspace").to_string_lossy(),
            },
            "note": null,
            "source_plan": {"session": "stream", "turn": "stream", "message": "stream", "request": "stream"},
            "source_state": source_state,
            "resume_from": resume_from,
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
        let text = std::fs::read_to_string(self.root.join("report.json")).expect("fake MSP report");
        serde_json::from_str(&text).expect("fake MSP report JSON")
    }
}

impl PendingStart {
    /// Drive the in-flight start to readiness. `expected_native_ref` pins
    /// the resumed session; launch leaves it absent.
    async fn finish(
        mut self,
        rx: oneshot::Receiver<Result<Value, String>>,
        expected_native_ref: Option<&str>,
    ) -> Scenario {
        let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
        // `agent.start` only resolves after the harness child registers and
        // the session.ready batch commits; answer both while it is in flight.
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
                            assert_eq!(events.len(), 1, "one native record per batch");
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
            native_scope.starts_with("muse-home:"),
            "native scope derives from the host home: {native_scope}"
        );
        let native_ref = ready_data
            .get("native_ref")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(!native_ref.is_empty());
        if let Some(expected) = expected_native_ref {
            assert_eq!(
                native_ref, expected,
                "resume reopens the exact retained session"
            );
        }
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
        Scenario {
            root: self.root,
            agent_id: self.agent_id,
            host: self.host,
            start_result: ready,
        }
    }

    /// Drive the in-flight resume start to readiness with one expected source
    /// gap: the undecodable snapshot entry gaps the source after
    /// `session.ready` without failing the resume. Returns the scenario and
    /// the recorded gap reason.
    async fn finish_with_snapshot_gap(
        mut self,
        rx: oneshot::Receiver<Result<Value, String>>,
        expected_native_ref: Option<&str>,
    ) -> (Scenario, String) {
        let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
        // `agent.start` only resolves after the harness child registers, the
        // session.ready batch commits, and the post-readiness snapshot adopt
        // records its gap; answer all three while it is in flight.
        let mut ready_data: Option<Value> = None;
        let mut gap_reason: Option<String> = None;
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
                            assert_eq!(events.len(), 1, "one native record per batch");
                            assert_eq!(events[0].get("kind").and_then(Value::as_str), Some("session.ready"));
                            assert!(ready_data.is_none(), "session.ready commits once");
                            ready_data = Some(events[0].get("data").cloned().unwrap_or(Value::Null));
                            let first = self.host.next_seq;
                            self.host.next_seq += 1;
                            self.host.respond(&request.id, json!({
                                "first_seq": first, "last_seq": first,
                                "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                            })).await;
                        }
                        "source.gap" => {
                            assert!(ready_data.is_some(), "the snapshot gap lands after readiness");
                            assert!(gap_reason.is_none(), "one snapshot gap");
                            gap_reason = Some(
                                request
                                    .params
                                    .get("reason")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_owned(),
                            );
                            let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                            let seq = self.host.next_seq;
                            self.host.next_seq += 1;
                            self.host.respond(&request.id, json!({"gap_id": gap_id, "seq": seq})).await;
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
        let native_ref = ready_data
            .get("native_ref")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(!native_ref.is_empty());
        if let Some(expected) = expected_native_ref {
            assert_eq!(
                native_ref, expected,
                "resume reopens the exact retained session"
            );
        }
        let gap_reason = gap_reason.expect("the snapshot gap was recorded");
        (
            Scenario {
                root: self.root,
                agent_id: self.agent_id,
                host: self.host,
                start_result: ready,
            },
            gap_reason,
        )
    }

    /// Drive the in-flight start to an explicit error. The harness child
    /// still registers (it spawns before preflight/dispatch), a dead host's
    /// stream gap is acknowledged, but no `session.ready` may commit: no
    /// Agent session is created.
    async fn finish_expecting_error(
        mut self,
        rx: oneshot::Receiver<Result<Value, String>>,
        expected_code: &str,
    ) -> FailedAttempt {
        let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
        let outcome = loop {
            tokio::select! {
                biased;
                request = self.host.next_request() => {
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
            root: self.root,
            agent_id: self.agent_id,
            host: self.host,
            params: self.params,
        }
    }
}

impl FailedAttempt {
    fn report(&self) -> Value {
        let text = std::fs::read_to_string(self.root.join("report.json")).expect("fake MSP report");
        serde_json::from_str(&text).expect("fake MSP report JSON")
    }

    /// Reissue the exact retained start: a failed start replays its stored
    /// error and an unknown start stays unknown, both without new native I/O.
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

fn muse_profile() -> Value {
    json!({
        "harness": "muse",
        "model": MODEL,
        "effort": EFFORT,
        "permissions": "workspace-write",
        "approval_policy": "never",
        "pass_env": ["ELIOT_FAKE_MSP_SCRIPT", "ELIOT_FAKE_MSP_REPORT", "ELIOT_FAKE_MSP_WORKSPACE"],
    })
}

/// The native scope the plugin derives from the fake host home. This mirrors
/// `muse_scope` in the plugin (FNV-1a over the Windows-lowercased home); the
/// resume subject must carry it exactly.
fn fake_native_scope() -> String {
    let home = "C:\\fake-muse-home";
    let canonical = if cfg!(windows) {
        home.to_lowercase()
    } else {
        home.to_owned()
    };
    let mut hash = 0xcbf29ce484222325u64;
    for byte in canonical.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("muse-home:{hash:016x}")
}

/// The retained generation-1 readback a resume of the seeded fake session
/// carries: the exact subject plus the retained completed turn.
fn retained_resume_subject(agent_id: &str) -> (Value, Value) {
    let resume_from = json!({
        "native_scope": fake_native_scope(),
        "native_ref": RETAINED_SESSION,
    });
    let source_state = json!({
        "retained_generation": 1,
        "retained_source": {
            "agent_id": agent_id,
            "generation": 1,
            "source": "stream",
            "families": ["session", "turn", "message"],
        },
        "current_turn": {
            "turn_id": RETAINED_TURN,
            "state": "completed",
            "status": "completed",
            "started_at": 1,
            "completed_at": 2,
        },
    });
    (resume_from, source_state)
}

/// One send round trip. The plugin only answers `agent.command` after its
/// `turn.started` batch commits, so inbound batches are acked while the call
/// is in flight; the remaining turn facts are drained afterwards. Returns the
/// send id, the applied turn id and every committed `(kind, data)` in order.
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
        let events = host.answer_events_put(agent_id, generation, expected).await;
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

/// Like `send_turn`, but tolerates exactly one `source.gap` interleaved with
/// the turn batches in arrival order: stale generation-1 output is gapped,
/// never applied. Returns the send id, the applied turn id, every committed
/// `(kind, data)` in order, and the recorded gap reason.
async fn send_turn_with_stale_replay(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    text: &str,
) -> (String, String, Vec<(String, Value)>, String) {
    async fn answer_batch(
        host: &mut Host,
        request: &HostRequest,
        committed: &mut Vec<(String, Value)>,
        gap_reason: &mut Option<String>,
        agent_id: &str,
        generation: i64,
    ) {
        match request.method.as_str() {
            "events.put" => {
                let events = request
                    .params
                    .get("events")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                assert_eq!(events.len(), 1, "one native record per batch");
                committed.push((
                    events[0]
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    events[0].get("data").cloned().unwrap_or(Value::Null),
                ));
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
            }
            "source.gap" => {
                assert!(gap_reason.is_none(), "stale output records exactly one gap");
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
                *gap_reason = Some(
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
                let seq = host.next_seq;
                host.next_seq += 1;
                host.respond(&request.id, json!({"gap_id": gap_id, "seq": seq}))
                    .await;
            }
            other => panic!("unexpected host request during stale-replay send: {other}"),
        }
    }

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
    let mut gap_reason: Option<String> = None;
    let applied = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                answer_batch(host, &request, &mut committed, &mut gap_reason, agent_id, generation).await;
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
    while committed.last().map(|(kind, _)| kind.as_str()) != Some("turn.completed")
        || gap_reason.is_none()
    {
        let request = host.next_request().await;
        answer_batch(
            host,
            &request,
            &mut committed,
            &mut gap_reason,
            agent_id,
            generation,
        )
        .await;
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
    (
        send_id,
        turn_id,
        committed,
        gap_reason.expect("stale output gap"),
    )
}

/// Like `send_turn`, but the turn may commit any number of `message.completed`
/// batches (routine once tools are involved) before `turn.completed`.
async fn send_multi_message_turn(
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
async fn stop_agent(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    force: bool,
) -> Vec<(String, Value)> {
    let stop_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.stop",
            json!({
                "agent_id": agent_id,
                "generation": generation,
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

/// A send whose turn parks on native requests: drain until the applied
/// response plus the script's `expected_opens` request opens, and return the
/// send id, the turn id, and every batch committed. The opens may commit on
/// either side of the applied response (the request frame races the
/// associate roundtrip), so the tail drains by count, not by phase.
async fn send_parked(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    text: &str,
    expected_opens: usize,
) -> (String, String, Vec<(String, Value)>) {
    let command_id = ulid::Ulid::new().to_string();
    let rx = host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": command_id,
                "verb": "send",
                "args": {"text": text},
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut committed: Vec<(String, Value)> = Vec::new();
    let send = loop {
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
    let send = send
        .expect("send deadline")
        .expect("send response channel")
        .expect("send applied");
    assert_eq!(send.get("state").and_then(Value::as_str), Some("applied"));
    assert!(committed.iter().any(|(kind, _)| kind == "turn.started"));
    let turn_id = send
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("turn id")
        .to_owned();
    while committed
        .iter()
        .filter(|(kind, _)| kind == "request.opened")
        .count()
        < expected_opens
    {
        let events = host
            .answer_events_put(agent_id, generation, "request.opened")
            .await;
        committed.push((
            "request.opened".to_owned(),
            events[0].get("data").cloned().unwrap_or(Value::Null),
        ));
    }
    (command_id, turn_id, committed)
}

/// Drive one approve/deny/answer reply and return its command id, result,
/// and the batches committed while it was in flight.
async fn reply(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    verb: &str,
    request_id: &str,
    turn_id: &str,
    args: Value,
) -> (String, Value, Vec<(String, Value)>) {
    reply_with_id(
        host,
        agent_id,
        generation,
        &ulid::Ulid::new().to_string(),
        ReplyTarget {
            verb,
            request_id,
            turn_id,
            args,
        },
    )
    .await
}

struct ReplyTarget<'a> {
    verb: &'a str,
    request_id: &'a str,
    turn_id: &'a str,
    args: Value,
}

async fn reply_with_id(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    command_id: &str,
    target: ReplyTarget<'_>,
) -> (String, Value, Vec<(String, Value)>) {
    let rx = host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": command_id,
                "verb": target.verb,
                "args": target.args,
                "turn_id": target.turn_id,
                "request_id": target.request_id,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let mut committed: Vec<(String, Value)> = Vec::new();
    let result = loop {
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
    let result = result
        .expect("reply deadline")
        .expect("reply response channel")
        .expect("reply responds");
    (command_id.to_owned(), result, committed)
}

/// Drive one reply with fresh command ids until it applies. A reply sent
/// before the processor has applied the stream update it depends on is
/// rejected locally (consumed reply, or choice unknown to the old offer)
/// without touching the native lane; the loop is state-based, never a fixed
/// sleep. Bounded: exhaustion fails the scenario instead of hanging it.
async fn reply_until_applied(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    verb: &str,
    request_id: &str,
    turn_id: &str,
    args: Value,
) -> (String, Value) {
    for _ in 0..500 {
        let (command_id, result, in_flight) = reply(
            host,
            agent_id,
            generation,
            verb,
            request_id,
            turn_id,
            args.clone(),
        )
        .await;
        match result.get("state").and_then(Value::as_str) {
            Some("applied") => {
                // The resolution lands after the reply: the processor is
                // busy inside this control call when the decide ack arrives,
                // so no stream frame can commit while it is in flight.
                assert!(in_flight.is_empty(), "resolution lands after the reply");
                return (command_id, result);
            }
            // Any other non-rejected outcome (unknown, an RPC error shape)
            // is a real failure, not a not-yet-applied update.
            Some("rejected") => {
                assert!(
                    in_flight.is_empty(),
                    "a locally rejected reply commits nothing"
                );
                continue;
            }
            other => panic!("reply ended {other:?} instead of applying: {result}"),
        }
    }
    panic!("reply never applied after 500 attempts");
}

async fn scenario_launch_send_snapshot_stop() {
    let mut scenario = Scenario::start("happy", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (send_id, turn_id, committed) =
        send_turn(&mut scenario.host, &agent_id, 1, "Say hello.").await;
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
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, 1, "Again.").await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["turn.started", "message.completed", "turn.completed"]
    );

    let committed = stop_agent(&mut scenario.host, &agent_id, 1, false).await;
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
        send_multi_message_turn(&mut scenario.host, &agent_id, 1, "Say it twice.").await;
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

    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
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
    let (_, _, committed) = send_turn(&mut scenario.host, &agent_id, 1, "Fail please.").await;
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
    stop_agent(&mut scenario.host, &agent_id, 1, true).await;
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
    stop_agent(&mut scenario.host, &agent_id, 1, true).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    assert_eq!(scenario.report()["turn_starts"], json!(1));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_turn_start_internal_error_is_unknown_without_rewrite() {
    send_unknown_without_rewrite("turn-start-internal-error").await;
}

async fn scenario_turn_start_steered_latches_unknown() {
    send_unknown_without_rewrite("turn-start-steered").await;
}

/// Drive one send against a script whose `turn/start` never applies the
/// input: the send is unknown, a retained retry stays unknown without new
/// native I/O, and a fresh send proves the session latched unknown.
async fn send_unknown_without_rewrite(script: &str) {
    let mut scenario = Scenario::start(script, muse_profile()).await;
    let agent_id = scenario.agent_id.clone();
    let send_id = ulid::Ulid::new().to_string();
    let outcome = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": "Into the disputed turn."},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    assert_eq!(outcome, Err("unknown".to_owned()));
    // A retained retry of the same command stays unknown without new I/O.
    let retry = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": "Into the disputed turn."},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    assert_eq!(retry, Err("unknown".to_owned()));
    // The session latched unknown: even a fresh command cannot proceed.
    let fresh = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "send",
                "args": {"text": "After the latch."},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    assert_eq!(fresh, Err("unknown".to_owned()));
    assert_eq!(scenario.report()["turn_starts"], json!(1));
    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    assert_eq!(scenario.report()["turn_starts"], json!(1));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_effort_mismatch_is_rejected_before_ready() {
    start_mismatch_before_ready("effort-mismatch").await;
}

async fn scenario_model_mismatch_is_rejected_before_ready() {
    start_mismatch_before_ready("model-mismatch").await;
}

async fn scenario_workspace_mismatch_is_rejected_before_ready() {
    start_mismatch_before_ready("cwd-mismatch").await;
}

/// Drive `agent.start` against a script whose effective settings disagree
/// with the frozen profile: the start fails `profile_error` with no
/// `session.ready` batch, and a retained retry repeats the stored error
/// without new native I/O.
async fn start_mismatch_before_ready(script: &str) {
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
    let self_exe = std::env::current_exe().expect("test executable");
    let mut full_profile = muse_profile();
    full_profile["cmd"] = Value::String(self_exe.to_string_lossy().into_owned());
    let start_params = json!({
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
        "source_plan": {"session": "stream", "turn": "stream", "message": "stream", "request": "stream"},
        "source_state": null,
        "resume_from": null,
        "initial": null,
    });
    let rx = host.begin_call("agent.start", start_params.clone()).await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let outcome = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                match request.method.as_str() {
                    "process.put" => {
                        let pid = request.params.get("pid").and_then(Value::as_u64).unwrap_or(0);
                        assert!(pid > 0);
                        host.respond(&request.id, json!({
                            "identity": {"host_boot_id": "test-boot", "pid": pid, "start_time": 1},
                            "owner": format!("agent:{agent_id}#1"),
                            "role": "harness",
                            "state": "running",
                            "exit_code": null,
                            "updated_at": 1,
                        })).await;
                    }
                    other => panic!("no session.ready or gap may publish before the settings check: {other}"),
                }
            }
            result = &mut rx => break result,
        }
    };
    let outcome = outcome
        .expect("agent.start deadline")
        .expect("agent.start response channel");
    assert_eq!(outcome, Err("profile_error".to_owned()));
    // A retained retry of the same start repeats the stored error without
    // new native I/O.
    let retry = host.call("agent.start", start_params).await;
    assert_eq!(retry, Err("profile_error".to_owned()));
    host.call("shutdown", json!({})).await.expect("shutdown");
    host.shutdown_child().await;
    let report: Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("report.json")).expect("fake MSP report"),
    )
    .expect("fake MSP report JSON");
    assert_eq!(report["session_starts"], json!(1));
    let _ = std::fs::remove_dir_all(&root);
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
    // Missing model, then missing explicit cmd: both fail before any spawn.
    for profile in [
        json!({"harness": "muse"}),
        json!({
            "harness": "muse",
            "model": MODEL,
            "effort": EFFORT,
            "permissions": "workspace-write",
            "approval_policy": "never",
            "pass_env": [],
        }),
    ] {
        let start_id = ulid::Ulid::new().to_string();
        let outcome = host
            .call(
                "agent.start",
                json!({
                    "agent_id": agent_id,
                    "generation": 1,
                    "command_id": start_id,
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
        assert_eq!(outcome, Err("profile_error".to_owned()));
    }
    host.call("shutdown", json!({})).await.expect("shutdown");
    host.shutdown_child().await;
    let report = root.join("report.json");
    assert!(
        !report.exists(),
        "no host may spawn before profile validation"
    );
    let _ = std::fs::remove_dir_all(&root);
}

async fn scenario_resume_reopens_the_retained_session_in_a_new_generation() {
    let agent_id = ulid::Ulid::new().to_string();
    let (resume_from, source_state) = retained_resume_subject(&agent_id);
    let mut scenario = Scenario::start_resume(
        "resume-happy",
        muse_profile(),
        agent_id,
        resume_from,
        source_state,
        RETAINED_SESSION,
    )
    .await;
    let agent_id = scenario.agent_id.clone();

    // Resume replays nothing: no fresh session, no turn until new input.
    let report = scenario.report();
    assert_eq!(report["session_starts"], json!(0));
    assert_eq!(report["session_resumes"], json!(1));
    assert_eq!(report["turn_starts"], json!(0));

    // Explicit new input runs as a new turn in the new generation; the
    // retained turn id never reappears as a live turn.
    let (send_id, turn_id, committed) =
        send_turn(&mut scenario.host, &agent_id, 2, "Continue the work.").await;
    assert_ne!(turn_id, RETAINED_TURN);
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

    // Native readback over the resumed session proves the new terminal turn.
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 2,
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
        snapshot.get("result_known").and_then(Value::as_bool),
        Some(true)
    );

    let committed = stop_agent(&mut scenario.host, &agent_id, 2, false).await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["session.ended"]
    );
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    let stderr = scenario.host.shutdown_child().await;
    assert!(!stderr.contains("panicked"), "plugin stderr: {stderr}");

    let report = scenario.report();
    assert_eq!(report["session_starts"], json!(0));
    assert_eq!(report["session_resumes"], json!(1));
    assert_eq!(report["turn_starts"], json!(1));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_resume_of_gone_session_is_an_explicit_error() {
    let agent_id = ulid::Ulid::new().to_string();
    let (resume_from, source_state) = retained_resume_subject(&agent_id);
    let (pending, rx) = Scenario::setup_start(
        "resume-gone",
        muse_profile(),
        agent_id,
        2,
        resume_from,
        source_state,
    )
    .await;
    let mut attempt = pending.finish_expecting_error(rx, "native_readback").await;

    // Preflight refused the gone session, so the resume was never
    // dispatched and no Agent session was created.
    let report = attempt.report();
    assert_eq!(report["counts"]["session/read"], json!(1));
    assert_eq!(report["session_starts"], json!(0));
    assert_eq!(report["session_resumes"], json!(0));
    assert_eq!(report["turn_starts"], json!(0));

    // The failure is retained like a failed start: the same command replays
    // it without new native I/O.
    attempt.retry("native_readback").await;
    assert_eq!(attempt.report()["counts"]["session/read"], json!(1));

    attempt
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    attempt.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&attempt.root);
}

async fn scenario_lost_resume_reply_is_unknown_with_one_write() {
    let agent_id = ulid::Ulid::new().to_string();
    let (resume_from, source_state) = retained_resume_subject(&agent_id);
    let (pending, rx) = Scenario::setup_start(
        "drop-session-resume",
        muse_profile(),
        agent_id,
        2,
        resume_from,
        source_state,
    )
    .await;
    let mut attempt = pending.finish_expecting_error(rx, "unknown").await;
    assert_eq!(attempt.report()["session_resumes"], json!(1));

    // The unknown resume is never dispatched again.
    attempt.retry("unknown").await;
    assert_eq!(attempt.report()["session_resumes"], json!(1));
    assert_eq!(attempt.report()["turn_starts"], json!(0));

    // Tear down through stop like a lost send: the dead host's gap and the
    // ended batch are acknowledged, then the plugin exits cleanly.
    let agent_id = attempt.agent_id.clone();
    stop_agent(&mut attempt.host, &agent_id, 2, true).await;
    attempt
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    attempt.host.shutdown_child().await;
    assert_eq!(attempt.report()["session_resumes"], json!(1));
    let _ = std::fs::remove_dir_all(&attempt.root);
}

async fn scenario_resume_rejects_stale_generation_1_output() {
    let agent_id = ulid::Ulid::new().to_string();
    let (resume_from, source_state) = retained_resume_subject(&agent_id);
    let mut scenario = Scenario::start_resume(
        "resume-stale-replay",
        muse_profile(),
        agent_id,
        resume_from,
        source_state,
        RETAINED_SESSION,
    )
    .await;
    let agent_id = scenario.agent_id.clone();

    // The old host's generation-1 turn completion is redelivered on the new
    // generation's stream during the first send: exactly one gap records the
    // rejection, and the new turn still commits its exact facts.
    let (send_id, turn_id, committed, gap_reason) =
        send_turn_with_stale_replay(&mut scenario.host, &agent_id, 2, "Continue the work.").await;
    assert_eq!(
        gap_reason,
        "Muse terminal notification did not match the exact active turn"
    );
    assert_ne!(turn_id, RETAINED_TURN);
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["turn.started", "message.completed", "turn.completed"]
    );
    for (kind, data) in &committed {
        assert_ne!(
            data.get("turn_id").and_then(Value::as_str),
            Some(RETAINED_TURN),
            "stale turn is never applied to generation 2: {kind}"
        );
        assert!(
            !data
                .to_string()
                .contains("Retained reply from the previous generation"),
            "stale text is never applied to generation 2: {kind}"
        );
    }
    assert_eq!(
        committed[1].1.get("text").and_then(Value::as_str),
        Some("Hello from the fake MSP host.")
    );
    assert_eq!(
        committed[2].1.get("status").and_then(Value::as_str),
        Some("completed")
    );

    // Readback over the resumed session still proves the exact new terminal
    // turn; the retained turn never surfaces as a generation-2 fact.
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 2,
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
        snapshot.get("result_known").and_then(Value::as_bool),
        Some(true)
    );

    // The gap latches the session fail-closed: no further input is accepted
    // on the disputed stream.
    let fresh = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 2,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "send",
                "args": {"text": "After the stale replay."},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    assert_eq!(fresh, Err("unknown".to_owned()));

    stop_agent(&mut scenario.host, &agent_id, 2, false).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    let stderr = scenario.host.shutdown_child().await;
    assert!(!stderr.contains("panicked"), "plugin stderr: {stderr}");

    let report = scenario.report();
    assert_eq!(report["session_starts"], json!(0));
    assert_eq!(report["session_resumes"], json!(1));
    assert_eq!(report["turn_starts"], json!(1));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_resume_refuses_served_history_and_keeps_the_retained_id() {
    let agent_id = ulid::Ulid::new().to_string();
    let (resume_from, source_state) = retained_resume_subject(&agent_id);
    let (pending, rx) = Scenario::setup_start(
        "resume-inline-history",
        muse_profile(),
        agent_id,
        2,
        resume_from,
        source_state,
    )
    .await;
    let mut attempt = pending.finish_expecting_error(rx, "unknown").await;

    // `excludeItems: true` was requested but the host served `history.mode:
    // "inline"`: the resume fails without adopting anything served, exactly
    // one write happened, and no turn started.
    let report = attempt.report();
    assert_eq!(report["session_resumes"], json!(1));
    assert_eq!(report["turn_starts"], json!(0));

    // The unknown resume is never dispatched again.
    attempt.retry("unknown").await;
    assert_eq!(attempt.report()["session_resumes"], json!(1));

    // The retained session id is kept: readback still addresses the exact
    // retained session, never a served foreign identity.
    let start_id = attempt
        .params
        .get("command_id")
        .and_then(Value::as_str)
        .expect("start command id")
        .to_owned();
    let agent_id = attempt.agent_id.clone();
    let snapshot = attempt
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 2,
                "command_id": start_id,
                "turn_id": Value::Null,
            }),
        )
        .await
        .expect("snapshot");
    assert_eq!(snapshot.get("state").and_then(Value::as_str), Some("idle"));
    assert_eq!(
        snapshot.get("native_ref").and_then(Value::as_str),
        Some(RETAINED_SESSION)
    );

    stop_agent(&mut attempt.host, &agent_id, 2, false).await;
    attempt
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    attempt.host.shutdown_child().await;
    assert_eq!(attempt.report()["session_resumes"], json!(1));
    let _ = std::fs::remove_dir_all(&attempt.root);
}

/// A1: the resume late-joiner set no longer refuses. Both pointers name
/// the retained (dead) turn, so the pull stashes them, the re-issued opens
/// receipt-and-drop as orphans, and the new generation's turn runs cleanly
/// with no gap and no stray request batches.
async fn scenario_resume_with_pending_requests_adopts_and_proceeds() {
    let agent_id = ulid::Ulid::new().to_string();
    let (resume_from, source_state) = retained_resume_subject(&agent_id);
    let (pending, rx) = Scenario::setup_start(
        "resume-pending",
        muse_profile(),
        agent_id.clone(),
        2,
        resume_from,
        source_state,
    )
    .await;
    let mut scenario = pending.finish(rx, Some(RETAINED_SESSION)).await;
    assert_eq!(scenario.start_result["snapshot_pull"], json!("ok"));
    assert_eq!(
        scenario.report()["pending_pulls"],
        json!(1),
        "one guarded snapshot pull per resume"
    );

    let (_send_id, _turn_id, committed) =
        send_turn(&mut scenario.host, &agent_id, 2, "Say hello.").await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["turn.started", "message.completed", "turn.completed"]
    );

    // Both re-issued orphans were decided nowhere and never opened: no
    // gap, no request batches.
    let report = scenario.report();
    assert_eq!(report["decides"], json!(0));
    assert_eq!(report["answers"], json!(0));
    assert_eq!(report["turn_starts"], json!(1));

    stop_agent(&mut scenario.host, &agent_id, 2, false).await;
    // Receipts are fire-and-forget: assert them only once the stop roundtrip
    // has flushed the native lane.
    assert_eq!(scenario.report()["receipts"], json!(2));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_approve_resolves_the_exact_offered_choice() {
    let mut scenario = Scenario::start("request-approval", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (send_id, turn_id, committed) =
        send_parked(&mut scenario.host, &agent_id, 1, "List files.", 1).await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["turn.started", "request.opened"]
    );
    let opened = committed
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| data)
        .expect("request.opened");
    assert_eq!(opened["command_id"], json!(send_id));
    assert_eq!(opened["turn_id"], json!(turn_id));
    assert_eq!(opened["kind"], json!("approval"));
    let request_id = opened["request_id"]
        .as_str()
        .expect("request id")
        .to_owned();
    assert!(request_id.starts_with("muse-request:"));
    assert_eq!(opened["details"]["approval_id"], json!("fake-approval-1"));
    assert_eq!(
        opened["details"]["current_requirement"]["sourceIndex"],
        json!(1)
    );
    let choices = opened["details"]["available_choices"]
        .as_array()
        .expect("offered choices");
    assert_eq!(choices.len(), 2);
    assert!(opened["details"].get("subagent_origin").is_none());

    let (reply_id, result, in_flight) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        &request_id,
        &turn_id,
        json!({}),
    )
    .await;
    assert_eq!(result["state"], json!("applied"));
    assert_eq!(result["command_id"], json!(reply_id));
    assert_eq!(result["request_id"], json!(request_id));
    assert_eq!(result["turn_id"], json!(turn_id));
    assert!(in_flight.is_empty(), "resolution lands after the reply");

    let resolved = scenario
        .host
        .answer_events_put(&agent_id, 1, "request.resolved")
        .await;
    assert_eq!(resolved[0]["data"]["command_id"], json!(reply_id));
    assert_eq!(resolved[0]["data"]["request_id"], json!(request_id));
    assert_eq!(resolved[0]["data"]["turn_id"], json!(turn_id));
    scenario
        .host
        .answer_events_put(&agent_id, 1, "message.completed")
        .await;
    scenario
        .host
        .answer_events_put(&agent_id, 1, "turn.completed")
        .await;

    assert_eq!(scenario.report()["decides"], json!(1));

    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    // The receipt is fire-and-forget: assert it only once the stop
    // roundtrip has flushed the native lane.
    assert_eq!(scenario.report()["receipts"], json!(1));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_answer_and_deny_settle_prompts() {
    let mut scenario = Scenario::start("request-prompt", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (_send_id, turn_id, committed) =
        send_parked(&mut scenario.host, &agent_id, 1, "Ask me.", 2).await;
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["turn.started", "request.opened", "request.opened"]
    );
    let prompt_ids = committed
        .iter()
        .filter(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| {
            assert_eq!(data["kind"], json!("user_input"));
            assert_eq!(data["turn_id"], json!(turn_id));
            data["request_id"].as_str().expect("request id").to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(prompt_ids.len(), 2);

    let (answer_id, result, _) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "answer",
        &prompt_ids[0],
        &turn_id,
        json!({"answers": {
            "q1": {"selected_label": "staging"},
            "q2": {"selected_labels": ["dry-run", "force"], "note": "careful"},
        }}),
    )
    .await;
    assert_eq!(result["state"], json!("applied"));
    let resolved = scenario
        .host
        .answer_events_put(&agent_id, 1, "request.resolved")
        .await;
    assert_eq!(resolved[0]["data"]["command_id"], json!(answer_id));

    let (deny_id, result, _) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "deny",
        &prompt_ids[1],
        &turn_id,
        json!({"reason": "not now"}),
    )
    .await;
    assert_eq!(result["state"], json!("applied"));
    let resolved = scenario
        .host
        .answer_events_put(&agent_id, 1, "request.resolved")
        .await;
    assert_eq!(resolved[0]["data"]["command_id"], json!(deny_id));
    scenario
        .host
        .answer_events_put(&agent_id, 1, "message.completed")
        .await;
    scenario
        .host
        .answer_events_put(&agent_id, 1, "turn.completed")
        .await;

    let report = scenario.report();
    assert_eq!(report["answers"], json!(1));
    assert_eq!(report["cancels"], json!(1));

    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    assert_eq!(scenario.report()["receipts"], json!(2));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

/// Note-31a error rule on the decide lane: only schema-documented
/// pre-admission/validation errors prove rejection; a `-32603` internal
/// error after intake is unknown and the decision is never resubmitted.
async fn scenario_decide_internal_error_is_unknown_without_rewrite() {
    let mut scenario = Scenario::start("decide-internal-error", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (_send_id, turn_id, committed) =
        send_parked(&mut scenario.host, &agent_id, 1, "List files.", 1).await;
    let opened = committed
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| data)
        .expect("request.opened");
    let request_id = opened["request_id"].as_str().expect("request id");

    let (reply_id, result, _) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        request_id,
        &turn_id,
        json!({}),
    )
    .await;
    assert_eq!(result["state"], json!("unknown"));
    assert_eq!(scenario.report()["decides"], json!(1));

    // The exact-command retry replays the retained unknown outcome without
    // touching the native lane a second time.
    let (_, retry, _) = reply_with_id(
        &mut scenario.host,
        &agent_id,
        1,
        &reply_id,
        ReplyTarget {
            verb: "approve",
            request_id,
            turn_id: &turn_id,
            args: json!({}),
        },
    )
    .await;
    assert_eq!(retry["state"], json!("unknown"));
    assert_eq!(scenario.report()["decides"], json!(1));

    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

/// A reply naming a choice id the current offer never carried is rejected
/// locally without a write, while a reply against the current offer applies
/// with the current requirement. The update repeats the opened offer, so the
/// first reply behaves identically whether the update has applied yet or not.
async fn scenario_stale_choice_after_update_is_rejected() {
    let mut scenario = Scenario::start("request-stale-choice", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (_send_id, turn_id, committed) =
        send_parked(&mut scenario.host, &agent_id, 1, "List files.", 1).await;
    let opened = committed
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| data)
        .expect("request.opened");
    let request_id = opened["request_id"].as_str().expect("request id");
    // The Store keeps the opened snapshot.
    assert_eq!(
        opened["details"]["current_requirement"]["sourceIndex"],
        json!(2)
    );

    let (_, stale, _) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        request_id,
        &turn_id,
        json!({"choice_id": "allow"}),
    )
    .await;
    assert_eq!(stale["state"], json!("rejected"));
    assert_eq!(scenario.report()["decides"], json!(0));

    let (reply_id, result, _) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        request_id,
        &turn_id,
        json!({}),
    )
    .await;
    assert_eq!(result["state"], json!("applied"));
    let resolved = scenario
        .host
        .answer_events_put(&agent_id, 1, "request.resolved")
        .await;
    assert_eq!(resolved[0]["data"]["command_id"], json!(reply_id));
    scenario
        .host
        .answer_events_put(&agent_id, 1, "message.completed")
        .await;
    scenario
        .host
        .answer_events_put(&agent_id, 1, "turn.completed")
        .await;

    // Exactly the fresh decision wrote, carrying the stage-2 requirement
    // (asserted by the fake at decide time).
    let report = scenario.report();
    assert_eq!(report["decides"], json!(1));

    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

/// Note-39a error rule on the decide lane: a `-32053` stale requirement is a
/// schema-documented validation failure (never written), so the reply is
/// rejected and the request stays open; the update carries the current
/// requirement and the following valid reply applies.
async fn scenario_stale_requirement_is_rejected_then_valid_reply_applies() {
    let mut scenario = Scenario::start("request-stale-decide", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (_send_id, turn_id, committed) =
        send_parked(&mut scenario.host, &agent_id, 1, "List files.", 1).await;
    let opened = committed
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| data)
        .expect("request.opened");
    let request_id = opened["request_id"].as_str().expect("request id");

    // The stage-1 decision loses the multi-stage race: rejected, never
    // written, and the request stays available for a corrected reply.
    let (_, first, in_flight) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        request_id,
        &turn_id,
        json!({}),
    )
    .await;
    assert_eq!(first["state"], json!("rejected"));
    assert!(in_flight.is_empty());
    assert_eq!(scenario.report()["decides"], json!(1));

    // The update carries the current (stage-2) requirement; the following
    // valid reply against it applies and binds the resolution.
    let (reply_id, second) = reply_until_applied(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        request_id,
        &turn_id,
        json!({"choice_id": "allow-session"}),
    )
    .await;
    assert_eq!(second["state"], json!("applied"));
    let resolved = scenario
        .host
        .answer_events_put(&agent_id, 1, "request.resolved")
        .await;
    assert_eq!(resolved[0]["data"]["command_id"], json!(reply_id));
    assert_eq!(resolved[0]["data"]["request_id"], json!(request_id));
    scenario
        .host
        .answer_events_put(&agent_id, 1, "message.completed")
        .await;
    scenario
        .host
        .answer_events_put(&agent_id, 1, "turn.completed")
        .await;

    // Exactly two decides wrote: the stale one and the valid retry.
    // Premature retries decide nothing (the stage-2 choice is unknown to the
    // old offer) and never touch the native lane.
    assert_eq!(scenario.report()["decides"], json!(2));

    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    // The receipt is fire-and-forget: assert it only once the stop
    // roundtrip has flushed the native lane.
    assert_eq!(scenario.report()["receipts"], json!(1));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

/// Transport loss during a decide: the reply is unknown (the write may or
/// may not have landed) and neither an exact retry nor a fresh reply writes
/// a second decision.
async fn scenario_lost_decide_reply_is_unknown_without_rewrite() {
    let mut scenario = Scenario::start("decide-lost-reply", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (_send_id, turn_id, committed) =
        send_parked(&mut scenario.host, &agent_id, 1, "List files.", 1).await;
    let request_id = committed
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| data["request_id"].as_str().expect("request id").to_owned())
        .expect("request.opened");

    let reply_id = ulid::Ulid::new().to_string();
    let result = reply_tolerating_gaps(
        &mut scenario.host,
        &agent_id,
        1,
        &reply_id,
        ReplyTarget {
            verb: "approve",
            request_id: &request_id,
            turn_id: &turn_id,
            args: json!({}),
        },
    )
    .await;
    assert_eq!(result["state"], json!("unknown"));
    assert_eq!(scenario.report()["decides"], json!(1));

    // The exact-command retry replays the retained unknown outcome without
    // touching the native lane a second time.
    let retry = reply_tolerating_gaps(
        &mut scenario.host,
        &agent_id,
        1,
        &reply_id,
        ReplyTarget {
            verb: "approve",
            request_id: &request_id,
            turn_id: &turn_id,
            args: json!({}),
        },
    )
    .await;
    assert_eq!(retry["state"], json!("unknown"));
    assert_eq!(scenario.report()["decides"], json!(1));

    // A fresh reply to the same request is unknown too: the uncertain write
    // is never repeated under another id.
    let fresh = reply_tolerating_gaps(
        &mut scenario.host,
        &agent_id,
        1,
        &ulid::Ulid::new().to_string(),
        ReplyTarget {
            verb: "approve",
            request_id: &request_id,
            turn_id: &turn_id,
            args: json!({}),
        },
    )
    .await;
    assert_eq!(fresh["state"], json!("unknown"));
    assert_eq!(scenario.report()["decides"], json!(1));

    // The dead host's stream gap may arrive before, during or after the
    // stop; stop_agent acknowledges it and waits for the ended batch.
    stop_agent(&mut scenario.host, &agent_id, 1, true).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    assert_eq!(scenario.report()["decides"], json!(1));
    let _ = std::fs::remove_dir_all(&scenario.root);
}

/// Like `reply_with_id`, but tolerates the dead host's stream gap arriving
/// around the reply: acknowledge it and keep waiting for the reply outcome.
async fn reply_tolerating_gaps(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    command_id: &str,
    target: ReplyTarget<'_>,
) -> Value {
    let rx = host
        .begin_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": command_id,
                "verb": target.verb,
                "args": target.args,
                "turn_id": target.turn_id,
                "request_id": target.request_id,
            }),
        )
        .await;
    let mut rx = std::pin::pin!(tokio::time::timeout(OP_TIMEOUT, rx));
    let result = loop {
        tokio::select! {
            biased;
            request = host.next_request() => {
                assert_eq!(request.method, "source.gap");
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
            }
            result = &mut rx => break result,
        }
    };
    result
        .expect("reply deadline")
        .expect("reply response channel")
        .expect("reply responds")
}

/// The observation-counter guard: the resume snapshot is captured before the
/// pull, the pending approval resolves on the stream after the capture, and
/// the stale snapshot entry must not reopen it when it applies.
async fn scenario_stale_pull_does_not_reopen_stream_resolved_request() {
    let agent_id = ulid::Ulid::new().to_string();
    let (resume_from, source_state) = retained_resume_subject(&agent_id);
    let mut scenario = Scenario::start_resume(
        "resume-stale-pull",
        muse_profile(),
        agent_id,
        resume_from,
        source_state,
        RETAINED_SESSION,
    )
    .await;
    let agent_id = scenario.agent_id.clone();
    assert_eq!(scenario.start_result["snapshot_pull"], json!("ok"));
    assert_eq!(scenario.report()["pending_pulls"], json!(1));

    // The stream opens the re-issued approval on the live turn and resolves
    // it, all inside the associate roundtrip; the stale snapshot entry
    // applies strictly after the resolution. The turn completes right
    // behind, so the tail batches may already be in flight when the send
    // applies: drain whatever the parked send has not already collected.
    let (_send_id, turn_id, mut committed) =
        send_parked(&mut scenario.host, &agent_id, 2, "Continue the work.", 1).await;
    for missing in [
        "turn.started",
        "request.opened",
        "request.closed",
        "message.completed",
        "turn.completed",
    ]
    .into_iter()
    .skip(committed.len())
    {
        let more = scenario.host.answer_events_put(&agent_id, 2, missing).await;
        committed.extend(
            more.into_iter()
                .map(|event| (missing.to_owned(), event["data"].clone())),
        );
    }
    assert_eq!(
        committed
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        [
            "turn.started",
            "request.opened",
            "request.closed",
            "message.completed",
            "turn.completed"
        ]
    );
    let opened = &committed[1].1;
    let request_id = opened["request_id"].as_str().expect("request id");
    assert!(
        request_id.contains(&turn_id),
        "the stream opened the live-bound request: {request_id}"
    );
    assert_ne!(turn_id, RETAINED_TURN);
    assert_eq!(committed[2].1["request_id"], json!(request_id));

    // No decision wrote, no second open landed, no gap fired.
    let report = scenario.report();
    assert_eq!(report["decides"], json!(0));
    assert_eq!(report["pending_pulls"], json!(1));

    let stopped = stop_agent(&mut scenario.host, &agent_id, 2, false).await;
    assert_eq!(
        stopped
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["session.ended"]
    );
    // The re-issued open is receipted exactly once; the stale snapshot entry
    // never presents, so it needs no receipt.
    assert_eq!(scenario.report()["receipts"], json!(1));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

/// Committed request.opened details redact the approval subject (including
/// its command string), questions and available choices with the configured
/// secrets; the in-memory offer stays exact so replies still work.
async fn scenario_request_details_redact_secrets() {
    const SECRET: &str = "request-redact";
    let mut scenario = Scenario::start("request-redact", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (_send_id, turn_id, committed) =
        send_parked(&mut scenario.host, &agent_id, 1, "Deploy it.", 2).await;
    let opens: Vec<&Value> = committed
        .iter()
        .filter(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| data)
        .collect();
    assert_eq!(opens.len(), 2);
    let approval = opens
        .iter()
        .find(|data| data["kind"] == json!("approval"))
        .expect("approval opened");
    let prompt = opens
        .iter()
        .find(|data| data["kind"] == json!("user_input"))
        .expect("prompt opened");
    let approval_id = approval["request_id"]
        .as_str()
        .expect("approval id")
        .to_owned();
    let prompt_id = prompt["request_id"].as_str().expect("prompt id").to_owned();

    // The persisted offer redacts every secret-bearing free-text field.
    assert_eq!(
        approval["details"]["subject"]["command"],
        json!("deploy --token [REDACTED]")
    );
    for choice in approval["details"]["available_choices"]
        .as_array()
        .expect("offered choices")
    {
        let label = choice.get("label").and_then(Value::as_str).expect("label");
        assert!(
            label.contains("[REDACTED]"),
            "choice label redacted: {label}"
        );
        assert!(!label.contains(SECRET), "choice label hides the secret");
    }
    for question in prompt["details"]["questions"]
        .as_array()
        .expect("offered questions")
    {
        let text = question
            .get("question")
            .and_then(Value::as_str)
            .expect("question text");
        assert!(text.contains("[REDACTED]"), "question redacted: {text}");
        assert!(!text.contains(SECRET), "question hides the secret");
        for option in question
            .get("options")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let label = option
                .get("label")
                .and_then(Value::as_str)
                .expect("option label");
            assert!(label.contains("[REDACTED]"), "option redacted: {label}");
            assert!(!label.contains(SECRET), "option hides the secret");
        }
    }
    assert!(
        !approval.to_string().contains(SECRET),
        "persisted approval hides the secret"
    );
    assert!(
        !prompt.to_string().contains(SECRET),
        "persisted prompt hides the secret"
    );

    // Replies still decide from the exact in-memory offer.
    let (approve_id, approved, _) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        &approval_id,
        &turn_id,
        json!({}),
    )
    .await;
    assert_eq!(approved["state"], json!("applied"));
    let resolved = scenario
        .host
        .answer_events_put(&agent_id, 1, "request.resolved")
        .await;
    assert_eq!(resolved[0]["data"]["command_id"], json!(approve_id));

    let (answer_id, answered, _) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "answer",
        &prompt_id,
        &turn_id,
        json!({"answers": {
            "q1": {"selected_label": "staging request-redact"},
            "q2": {"selected_labels": ["dry-run request-redact", "force request-redact"]},
        }}),
    )
    .await;
    assert_eq!(answered["state"], json!("applied"));
    let resolved = scenario
        .host
        .answer_events_put(&agent_id, 1, "request.resolved")
        .await;
    assert_eq!(resolved[0]["data"]["command_id"], json!(answer_id));
    scenario
        .host
        .answer_events_put(&agent_id, 1, "message.completed")
        .await;
    scenario
        .host
        .answer_events_put(&agent_id, 1, "turn.completed")
        .await;

    let report = scenario.report();
    assert_eq!(report["decides"], json!(1));
    assert_eq!(report["answers"], json!(1));

    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    assert_eq!(scenario.report()["receipts"], json!(2));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

/// A pulled entry that fails describe is skipped with an explicit gap
/// instead of adopted, and the resume still proceeds: adoption never fails
/// the resume. The gap carries the standard source consequence (new input is
/// refused unknown), while the owned session still stops cleanly.
async fn scenario_resume_with_undecodable_pull_proceeds() {
    let agent_id = ulid::Ulid::new().to_string();
    let (resume_from, source_state) = retained_resume_subject(&agent_id);
    let (pending, rx) = Scenario::setup_start(
        "resume-undecodable-pull",
        muse_profile(),
        agent_id,
        2,
        resume_from,
        source_state,
    )
    .await;
    let (mut scenario, gap_reason) = pending
        .finish_with_snapshot_gap(rx, Some(RETAINED_SESSION))
        .await;
    let agent_id = scenario.agent_id.clone();
    assert_eq!(
        gap_reason,
        "Muse pulled request snapshot contained an undecodable entry"
    );
    assert_eq!(scenario.start_result["snapshot_pull"], json!("ok"));
    assert_eq!(scenario.report()["pending_pulls"], json!(1));

    // The foreign entry never opens, and the gapped source refuses new
    // input without dispatching it.
    let send = scenario
        .host
        .call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 2,
                "command_id": ulid::Ulid::new().to_string(),
                "verb": "send",
                "args": {"text": "Continue the work."},
                "turn_id": null,
                "request_id": null,
            }),
        )
        .await;
    assert_eq!(send, Err("unknown".to_owned()));
    assert_eq!(scenario.report()["turn_starts"], json!(0));

    let stopped = stop_agent(&mut scenario.host, &agent_id, 2, false).await;
    assert_eq!(
        stopped
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["session.ended"]
    );
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

/// Multi-stage approvals: the first decision applies with `terminal: false`,
/// the update resets the consumed reply, and the second stage decides
/// (here: a deny with feedback) to the terminal resolution.
async fn scenario_multistage_approval_decides_each_stage() {
    let mut scenario = Scenario::start("request-multistage", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();

    let (_send_id, turn_id, committed) =
        send_parked(&mut scenario.host, &agent_id, 1, "List files.", 1).await;
    let opened = committed
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| data)
        .expect("request.opened");
    let request_id = opened["request_id"]
        .as_str()
        .expect("request id")
        .to_owned();

    let (_, first, _) = reply(
        &mut scenario.host,
        &agent_id,
        1,
        "approve",
        &request_id,
        &turn_id,
        json!({}),
    )
    .await;
    assert_eq!(first["state"], json!("applied"));
    // The stage update commits nothing observable, so the second-stage reply
    // retries (state-based, never a fixed sleep) until the processor has
    // applied it: premature attempts are rejected locally while the consumed
    // first-stage reply is still outstanding, without touching the native lane.
    let (second_id, second) = reply_until_applied(
        &mut scenario.host,
        &agent_id,
        1,
        "deny",
        &request_id,
        &turn_id,
        json!({"feedback": "never touch prod"}),
    )
    .await;
    assert_eq!(second["state"], json!("applied"));
    let resolved = scenario
        .host
        .answer_events_put(&agent_id, 1, "request.resolved")
        .await;
    assert_eq!(resolved[0]["data"]["command_id"], json!(second_id));
    assert_eq!(resolved[0]["data"]["request_id"], json!(request_id));
    scenario
        .host
        .answer_events_put(&agent_id, 1, "message.completed")
        .await;
    scenario
        .host
        .answer_events_put(&agent_id, 1, "turn.completed")
        .await;

    let report = scenario.report();
    assert_eq!(report["decides"], json!(2));

    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}

async fn scenario_unknown_native_request_is_rejected_with_a_gap() {
    let mut scenario = Scenario::start("request-unknown-method", muse_profile()).await;
    let agent_id = scenario.agent_id.clone();
    // The unpresentable request is rejected with a typed error and gaps the
    // request family while the turn itself still completes.
    send_turn_with_stale_replay(&mut scenario.host, &agent_id, 1, "Say hello.").await;
    stop_agent(&mut scenario.host, &agent_id, 1, false).await;
    // The typed rejection is fire-and-forget: assert it only once the stop
    // roundtrip has flushed the native lane.
    assert_eq!(scenario.report()["receipts"], json!(1));
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
    let _ = std::fs::remove_dir_all(&scenario.root);
}
