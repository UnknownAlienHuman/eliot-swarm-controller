use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_client_protocol::{
    Client, Lines,
    schema::v1::{CloseSessionRequest, SessionId, SessionNotification},
};
use futures::StreamExt as _;
use serde_json::{Value, json};
use swarm_contracts::{RuntimeCommand, module_contract::ModuleContractClaim};
use swarm_process::process_group::Group;
use tokio::{
    process::Command,
    sync::{mpsc, oneshot},
    time,
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};
use uuid::Uuid;

use super::super as acp;

fn isolated_worktree_root() -> std::result::Result<PathBuf, String> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "Command adapter manifest is outside its expected workspace".to_owned())
}

fn route_and_workspace() -> std::result::Result<(PathBuf, Value, PathBuf), String> {
    let worktree = isolated_worktree_root()?;
    let qualification_root = worktree.join(".local").join("qualification");
    let qualification_root = qualification_root
        .canonicalize()
        .map_err(|error| format!("qualification root is unavailable: {error}"))?;
    let route_file = PathBuf::from(
        env::var_os("PR44_ACP_QUALIFICATION_ROUTE")
            .ok_or_else(|| "PR44_ACP_QUALIFICATION_ROUTE is required".to_owned())?,
    );
    let canonical_route_file = route_file
        .canonicalize()
        .map_err(|error| format!("qualification route is unavailable: {error}"))?;
    if !canonical_route_file.starts_with(&qualification_root) {
        return Err("qualification route escaped the isolated worktree run root".to_owned());
    }
    let route_document: Value = serde_json::from_slice(
        &fs::read(&route_file).map_err(|error| format!("route read failed: {error}"))?,
    )
    .map_err(|_| "qualification route JSON is malformed".to_owned())?;
    let route = route_document["route"].clone();
    acp::validate_acp_route(&route).map_err(|error| error.code)?;
    let options = &route["native_options"];
    if options["modelId"] != "deepseek/deepseek-v4-flash"
        || options["effort"] != "default"
        || options["mode"] != "default"
    {
        return Err(
            "qualification route differs from the already-observed installed values".to_owned(),
        );
    }
    let workspace = options["workspaceRoot"]
        .as_str()
        .map(PathBuf::from)
        .ok_or_else(|| "qualification workspaceRoot is missing".to_owned())?;
    let canonical_workspace = workspace
        .canonicalize()
        .map_err(|error| format!("qualification workspace is unavailable: {error}"))?;
    if !canonical_workspace.starts_with(&qualification_root) {
        return Err("qualification workspace escaped the isolated worktree run root".to_owned());
    }
    Ok((route_file, route, workspace))
}

fn fixture_owner(state_dir: &Path, binding_id: &str) -> acp::Owner {
    let claim: ModuleContractClaim = serde_json::from_value(json!({
        "schema_version":1,
        "module_id":"runtime.command",
        "artifact":{
            "artifact_id":acp::ACP_ARTIFACT_ID,
            "version":crate::ACP_ARTIFACT_VERSION,
            "build_id":null
        },
        "protocol":{"major":1,"minor":0},
        "capabilities":["native.command.close_session"],
        "config_schema":null,
        "command_schemas":[],
        "event_schemas":[]
    }))
    .expect("local qualification claim parses");
    acp::Owner {
        state_dir: state_dir.to_path_buf(),
        token: "pr44-installed-acp-qualification".to_owned(),
        record: json!({"qualification_only":true}),
        host: crate::module_host::ModuleHostIdentity {
            claim,
            profile: crate::Profile::AcpV1,
            boot_id: Uuid::new_v4().to_string(),
            binding_id: binding_id.to_owned(),
            generation: 1,
            module_client_id: "qualification-only".to_owned(),
            credential_file: state_dir.join("unused-qualification-credential"),
        },
    }
}

fn qualification_command(operation: &str, binding_id: &str, route: &Value) -> RuntimeCommand {
    RuntimeCommand {
        operation_id: format!("op-pr44-{operation}-{}", Uuid::new_v4()),
        method: "agent.open".to_owned(),
        created_at_ms: 1,
        binding_id: binding_id.to_owned(),
        generation: 1,
        native_root_id: None,
        route: route.clone(),
        input: json!({}),
        input_sha256: None,
        target_input_sha256: None,
    }
}

fn effect_records(journal: &acp::AcpJournal, operation_id: &str) -> Vec<Value> {
    let Ok(directory) = journal.operation_dir(operation_id) else {
        return Vec::new();
    };
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };
    let mut paths: Vec<_> = entries
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("effect-"))
        })
        .collect();
    paths.sort();
    paths
        .iter()
        .filter_map(|path| acp::read_json::<Value>(path).ok())
        .collect()
}

fn native_jsonrpc_code(message: &str) -> Option<i32> {
    let suffix = message.split_once("native JSON-RPC code ")?.1;
    suffix
        .split(|character: char| !character.is_ascii_digit() && character != '-')
        .next()?
        .parse()
        .ok()
}

fn bounded_stderr_summary(capture: &Arc<Mutex<acp::Capture>>) -> (u64, bool, String) {
    let Ok(capture) = capture.lock() else {
        return (0, false, "stderr capture unavailable".to_owned());
    };
    let text = String::from_utf8_lossy(&capture.stderr);
    let lower = text.to_ascii_lowercase();
    let auth_indicator = lower.contains("authentication required")
        || lower.contains("auth_required")
        || lower.contains("unauthorized")
        || lower.contains("not logged in");
    let sensitive_marker = [
        "token",
        "secret",
        "password",
        "credential",
        "bearer",
        "authorization",
        "api key",
        "apikey",
        "cookie",
        "oauth",
    ]
    .iter()
    .any(|marker| lower.contains(marker));
    let text = capture.native_session_id.as_deref().map_or_else(
        || text.to_string(),
        |session_id| text.replace(session_id, "<native-session-id>"),
    );
    let summary = if sensitive_marker {
        "[redacted credential-related stderr]".to_owned()
    } else {
        text.replace("C:\\Users\\kleym", "<user-home>")
            .chars()
            .filter(|character| !character.is_control() || *character == '\n' || *character == '\t')
            .take(320)
            .collect()
    };
    (capture.stderr_total_bytes, auth_indicator, summary)
}

fn summarize_selector_receipt(record: &Value) -> Value {
    let evidence = &record["evidence"];
    let session_id_hash = evidence["native_session_id"]
        .as_str()
        .map(|session_id| acp::digest(session_id.as_bytes()));
    json!({
        "operation_id":record["operation_id"],
        "method":record["effect_id"],
        "phase":record["phase"],
        "native_session_id_sha256":session_id_hash,
        "config_id":evidence["config_id"],
        "requested_value_id":evidence["requested_value_id"],
        "effective_value_id":evidence["effective_value_id"],
        "readback_verified":evidence["readback_verified"],
        "readback_source":evidence["readback_source"]
    })
}

fn summarize_initial_session_receipt(record: Option<&Value>) -> Value {
    let Some(record) = record else {
        return Value::Null;
    };
    let evidence = &record["evidence"];
    json!({
        "native_session_id_sha256":evidence["native_session_id"]
            .as_str()
            .map(|session_id| acp::digest(session_id.as_bytes())),
        "workspace":evidence["workspace"],
        "requested_model":evidence["requested_model"],
        "requested_effort":evidence["requested_effort"],
        "requested_mode":evidence["requested_mode"],
        "initial_config_options":evidence["initial_config_options"],
        "initial_mode_id":evidence["initial_mode_id"],
        "initial_mode_value_ids":evidence["initial_mode_value_ids"]
    })
}

async fn run_installed_qualification() -> std::result::Result<(PathBuf, Value), String> {
    let (route_file, route, workspace) = route_and_workspace()?;
    let qualification_root = isolated_worktree_root()?
        .join(".local")
        .join("qualification");
    let run_root = qualification_root
        .join("runs")
        .join(Uuid::new_v4().to_string());
    let state_dir = run_root.join("state");
    fs::create_dir_all(&state_dir).map_err(|error| error.to_string())?;
    let evidence_path = run_root.join("installed-acp-live-evidence.json");
    let journal = acp::AcpJournal::new(&state_dir).map_err(|error| error.code)?;
    let binding_id = format!("binding-pr44-qualification-{}", Uuid::new_v4());
    let owner = fixture_owner(&state_dir, &binding_id);
    let open_command = qualification_command("open", &binding_id, &route);
    let restore_command = qualification_command("restore", &binding_id, &route);
    let native_options = &route["native_options"];
    let session_route = acp::SessionRouteConfiguration {
        model_id: native_options["modelId"]
            .as_str()
            .ok_or_else(|| "route modelId is missing".to_owned())?
            .to_owned(),
        effort: native_options["effort"].as_str().map(str::to_owned),
        mode: native_options["mode"].as_str().map(str::to_owned),
    };
    let launcher = PathBuf::from(
        env::var_os("PR44_ACP_PWSH").ok_or_else(|| "PR44_ACP_PWSH is required".to_owned())?,
    );
    let canonical_launcher = launcher
        .canonicalize()
        .map_err(|error| format!("PowerShell launcher is unavailable: {error}"))?;
    let expected_launcher = PathBuf::from(r"C:\Program Files\PowerShell\7\pwsh.exe")
        .canonicalize()
        .map_err(|error| format!("expected PowerShell launcher is unavailable: {error}"))?;
    if canonical_launcher != expected_launcher {
        return Err("PowerShell launcher differs from the inspected installation".to_owned());
    }
    let shim = PathBuf::from(
        env::var_os("PR44_ACP_CMD_SHIM")
            .ok_or_else(|| "PR44_ACP_CMD_SHIM is required".to_owned())?,
    );
    let canonical_shim = shim
        .canonicalize()
        .map_err(|error| format!("installed Command shim is unavailable: {error}"))?;
    let expected_shim = PathBuf::from(r"C:\Users\kleym\AppData\Roaming\npm\cmd.ps1")
        .canonicalize()
        .map_err(|error| format!("expected installed Command shim is unavailable: {error}"))?;
    if canonical_shim != expected_shim {
        return Err("Command shim differs from the inspected installation".to_owned());
    }

    let process_group = Group::enter(&Uuid::new_v4().to_string().replace('-', ""))
        .map_err(|error| format!("qualification process owner failed: {}", error.code))?;
    let mut process = Command::new(&launcher);
    process
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(&shim)
        .arg("acp")
        .current_dir(&workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = match process.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = process_group.disarm();
            return Err(format!("installed ACP server did not start: {error}"));
        }
    };
    let child_stdin = child.stdin.take().expect("ACP stdin was piped");
    let child_stdout = child.stdout.take().expect("ACP stdout was piped");
    let child_stderr = child.stderr.take().expect("ACP stderr was piped");
    let outgoing = futures::SinkExt::<String>::sink_map_err(
        FramedWrite::new(
            child_stdin,
            LinesCodec::new_with_max_length(acp::MAX_ACP_FRAME_BYTES),
        ),
        |error| std::io::Error::other(error.to_string()),
    );
    let incoming = FramedRead::new(
        child_stdout,
        acp::AcpLineCodec::new(acp::MAX_ACP_FRAME_BYTES),
    )
    .map(|result| result.map_err(|error| std::io::Error::other(error.to_string())));
    let transport = Lines::new(outgoing, incoming);
    let capture = Arc::new(Mutex::new(acp::Capture::default()));
    let stderr_task = tokio::spawn(acp::drain_native_stderr(child_stderr, Arc::clone(&capture)));
    let route_file_text = route_file.to_string_lossy().into_owned();
    let callback_route_file = route_file_text.clone();
    let notification_capture = Arc::clone(&capture);
    let callback_capture = Arc::clone(&capture);
    let callback_journal = journal.clone();
    let (report_tx, mut report_rx) = oneshot::channel();
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let client = Client.builder()
        .name("pr44-installed-acp-qualification")
        .on_receive_notification(
            async move |notification: SessionNotification, _connection| {
                if let Ok(mut capture) = notification_capture.lock() {
                    acp::capture_session_notification(&mut capture, &notification);
                }
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_with(transport, move |connection| {
            let journal = callback_journal;
            let owner = owner;
            let route = route;
            let workspace = workspace;
            let session_route = session_route;
            let open_command = open_command;
            let restore_command = restore_command;
            let capture = callback_capture;
            let route_file_text = callback_route_file;
            let progress_tx = progress_tx;
            async move {
                let _ = progress_tx.send("callback_started".to_owned());
                let features = match acp::initialize(&connection).await {
                    Ok(features) => {
                        let _ = progress_tx.send("initialize_succeeded".to_owned());
                        features
                    }
                    Err(native_error) => {
                        let native_code = i32::from(native_error.code);
                        let _ = progress_tx.send("initialize_failed".to_owned());
                        let _ = report_tx.send(json!({
                            "initialize":{"status":"failed","native_jsonrpc_code":native_code,"auth_required":native_code == -32000},
                            "route_is_temporary_not_active_binding":true,
                            "prompt_sent":false,
                            "model_call_sent":false,
                            "login_or_account_mutation":false
                        }));
                        return Err(native_error);
                    }
                };
                let _ = progress_tx.send("session_open_started".to_owned());
                let open_recorded = journal.record_command(&open_command);
                let open_result = match open_recorded {
                    Ok(()) => {
                        acp::open_session_with_policy(
                            acp::SessionOpenContext {
                                connection: &connection,
                                journal: &journal,
                                command: &open_command,
                                workspace: &workspace,
                                owner: &owner,
                                features: &features,
                                capture: &capture,
                            },
                            &session_route,
                            true,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                let _ = progress_tx.send(if open_result.is_ok() {
                    "session_open_succeeded".to_owned()
                } else {
                    "session_open_failed".to_owned()
                });
                let opened_session_id = open_result
                    .as_ref()
                    .ok()
                    .map(|session| session.id.to_string())
                    .or_else(|| {
                        capture
                            .lock()
                            .ok()
                            .and_then(|capture| capture.native_session_id.clone())
                    });
                let open_effects = effect_records(&journal, &open_command.operation_id);
                let initial_session_receipt = open_effects.iter().find(|record| {
                    record["effect_id"] == "session/new" && record["phase"] == "receipt"
                });
                let selector_receipts: Vec<_> = open_effects
                    .iter()
                    .filter(|record| {
                        record["phase"] == "receipt"
                            && (record["effect_id"]
                                .as_str()
                                .is_some_and(|id| id.starts_with("session/config-option/"))
                                || record["effect_id"] == "session/set-mode")
                    })
                    .map(summarize_selector_receipt)
                    .collect();

                let mut restore_report = json!({"status":"not_attempted"});
                if let Ok(session) = &open_result {
                    if features.load_session_supported {
                        let record_result = journal.record_command(&restore_command);
                        let saved_result = journal.read_session();
                        match (record_result, saved_result) {
                            (Ok(()), Ok(Some(saved))) => {
                                let restore_result = acp::restore_session(
                                    acp::SessionRestoreContext {
                                        connection: &connection,
                                        journal: &journal,
                                        binding_id: &owner.host.binding_id,
                                        generation: owner.host.generation,
                                        route: &route,
                                        owner: &owner,
                                        features: &features,
                                        capture: &capture,
                                    },
                                    &restore_command,
                                    saved,
                                )
                                .await;
                                restore_report = match restore_result {
                                    Ok(restored) => json!({
                                        "status":"loaded",
                                        "operation_id":restore_command.operation_id,
                                        "same_session":restored.id.to_string() == session.id.to_string()
                                    }),
                                    Err(error) => json!({
                                        "status":"load_failed",
                                        "operation_id":restore_command.operation_id,
                                        "adapter_error_code":error.code,
                                        "native_jsonrpc_code":native_jsonrpc_code(&error.message),
                                        "message":error.message,
                                        "native_replay":false
                                    }),
                                };
                            }
                            (Err(error), _) => {
                                restore_report = json!({"status":"not_run","reason_code":error.code});
                            }
                            (_, Err(error)) => {
                                restore_report = json!({"status":"not_run","reason_code":error.code});
                            }
                            (_, Ok(None)) => {
                                restore_report = json!({"status":"not_run","reason":"session receipt missing"});
                            }
                        }
                    } else {
                        restore_report = json!({"status":"not_run","reason":"session/load not advertised"});
                    }
                } else if let Err(error) = &open_result {
                    restore_report = json!({"status":"not_run","open_error_code":error.code});
                }
                let restore_effects = effect_records(&journal, &restore_command.operation_id);
                let _ = progress_tx.send("restore_finished".to_owned());
                let load_target = restore_effects.iter().find(|record| {
                    record["effect_id"] == "session/load" && record["phase"] == "intent"
                });
                let same_session_load_target = opened_session_id.as_deref().is_some_and(|session_id| {
                    load_target
                        .and_then(|record| record["evidence"]["native_session_id"].as_str())
                        == Some(session_id)
                });
                let close_report = if let Some(session_id) = opened_session_id.as_deref() {
                    if features.close_supported
                        && owner.host.claim.capabilities.iter().any(|capability| {
                            capability.as_str() == "native.command.close_session"
                        })
                    {
                        match connection
                            .send_request(CloseSessionRequest::new(SessionId::new(session_id)))
                            .block_task()
                            .await
                        {
                            Ok(_) => json!({"advertised":true,"response_received":true}),
                            Err(native_error) => json!({
                                "advertised":true,
                                "response_received":false,
                                "native_jsonrpc_code":i32::from(native_error.code)
                            }),
                        }
                    } else {
                        json!({"advertised":features.close_supported,"response_received":false})
                    }
                } else {
                    json!({"advertised":features.close_supported,"response_received":false,"session_id_observed":false})
                };
                let open_report = match &open_result {
                    Ok(session) => json!({
                        "status":"verified",
                        "operation_id":open_command.operation_id,
                        "model_id":session.model_id,
                        "effective_effort":session.effective_effort,
                        "effective_mode":session.effective_mode,
                        "session_id_sha256":acp::digest(session.id.to_string().as_bytes()),
                        "initial_session_receipt":summarize_initial_session_receipt(initial_session_receipt),
                        "setter_receipts":selector_receipts
                    }),
                    Err(error) => json!({
                        "status":"failed",
                        "operation_id":open_command.operation_id,
                        "adapter_error_code":error.code,
                        "native_jsonrpc_code":native_jsonrpc_code(&error.message),
                        "message":error.message,
                        "setter_receipts":selector_receipts
                    }),
                };
                let no_prompt_effects = open_effects
                    .iter()
                    .chain(restore_effects.iter())
                    .all(|record| record["effect_id"] != "session/prompt");
                let _ = progress_tx.send("close_finished".to_owned());
                let agent_version = features
                    .agent_info
                    .as_ref()
                    .and_then(|info| info["version"].as_str());
                let _ = report_tx.send(json!({
                    "schema_version":1,
                    "route_file":route_file_text,
                    "route_is_temporary_not_active_binding":true,
                    "initialize":{
                        "status":"ok",
                        "agent_version":agent_version,
                        "load_session_supported":features.load_session_supported,
                        "close_supported":features.close_supported
                    },
                    "open":open_report,
                    "restore":restore_report,
                    "restore_load_target_matches_open_session":same_session_load_target,
                    "restore_load_request_sent":load_target.is_some(),
                    "close":close_report,
                    "prompt_sent":false,
                    "prompt_effect_count":if no_prompt_effects {0}else{1},
                    "model_call_sent":false,
                    "login_or_account_mutation":false,
                    "model_or_billing_choice_changed":false,
                    "native_family_departure_claimed":false,
                    "native_family_departure":"UNKNOWN"
                }));
                Ok::<(), agent_client_protocol::Error>(())
            }
        });
    let client_task = tokio::spawn(client);
    let mut callback_progress = Vec::new();
    let callback_result = match time::timeout(Duration::from_secs(30), async {
        loop {
            tokio::select! {
                result = &mut report_rx => {
                    break match result {
                        Ok(evidence) => Ok(evidence),
                        Err(_) => Err("ACP adapter callback ended without a qualification receipt".to_owned()),
                    };
                }
                progress = progress_rx.recv() => {
                    if let Some(progress) = progress {
                        callback_progress.push(progress);
                    }
                }
            }
        }
    }).await {
        Ok(result) => result,
        Err(_) => Err("bounded ACP adapter callback deadline expired".to_owned()),
    };
    client_task.abort();
    let _ = client_task.await;

    let mut direct_child_exit_code = None;
    let first_wait = time::timeout(Duration::from_secs(8), child.wait()).await;
    if let Ok(Ok(status)) = first_wait {
        direct_child_exit_code = status.code();
    } else {
        let _ = process_group.cancel_children();
        if let Ok(Ok(status)) = time::timeout(Duration::from_secs(4), child.wait()).await {
            direct_child_exit_code = status.code();
        }
    }
    let mut qualification_job_empty = false;
    for _ in 0..30 {
        if process_group.children_empty().unwrap_or(false) {
            qualification_job_empty = true;
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    if !qualification_job_empty {
        let _ = process_group.cancel_children();
        for _ in 0..30 {
            if process_group.children_empty().unwrap_or(false) {
                qualification_job_empty = true;
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    }
    if qualification_job_empty {
        process_group
            .disarm()
            .map_err(|error| format!("qualification job could not be disarmed: {}", error.code))?;
    }
    let _ = match time::timeout(Duration::from_secs(2), stderr_task).await {
        Ok(Ok(())) => capture
            .lock()
            .map(|capture| capture.stderr_total_bytes)
            .unwrap_or(0),
        _ => 0,
    };
    let mut evidence = callback_result.unwrap_or_else(|error| {
        json!({
            "schema_version":1,
            "probe_status":"no_adapter_receipt",
            "failure":error,
            "route_file":route_file_text,
            "route_is_temporary_not_active_binding":true,
            "prompt_sent":false,
            "model_call_sent":false,
            "login_or_account_mutation":false,
            "native_family_departure_claimed":false,
            "native_family_departure":"UNKNOWN"
        })
    });
    let (stderr_bytes, stderr_auth_indicator, stderr_summary) = bounded_stderr_summary(&capture);
    evidence["callback_progress"] = json!(callback_progress);
    evidence["stderr_auth_indicator"] = json!(stderr_auth_indicator);
    evidence["stderr_summary"] = json!(stderr_summary);
    evidence["process"] = json!({
        "launcher":launcher.to_string_lossy(),
        "shim":shim.to_string_lossy(),
        "direct_child_exit_code":direct_child_exit_code,
        "stderr_bytes_count":stderr_bytes,
        "qualification_job_empty":qualification_job_empty,
        "native_family_departure_claimed":false,
        "native_family_departure":"UNKNOWN"
    });
    fs::write(
        &evidence_path,
        serde_json::to_vec_pretty(&evidence).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("qualification evidence write failed: {error}"))?;
    Ok((evidence_path, evidence))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "bounded installed Command ACP qualification; requires the isolated operator-selected route"]
async fn pr44_command_acp_installed_open_setters_and_same_session_restore_without_prompt() {
    let (evidence_path, evidence) = run_installed_qualification()
        .await
        .unwrap_or_else(|error| panic!("installed ACP qualification could not run: {error}"));
    eprintln!(
        "sanitized installed ACP evidence: {}",
        evidence_path.display()
    );
    assert_eq!(evidence["prompt_sent"], false);
    assert_eq!(evidence["model_call_sent"], false);
    assert_eq!(evidence["native_family_departure"], "UNKNOWN");
    assert_eq!(evidence["native_family_departure_claimed"], false);
    assert_eq!(evidence["open"]["status"], "verified");
    assert_eq!(evidence["restore_load_target_matches_open_session"], true);
    assert_eq!(evidence["process"]["qualification_job_empty"], true);
    let selectors = evidence["open"]["setter_receipts"]
        .as_array()
        .expect("adapter setter receipts were retained");
    for field in ["model", "effort"] {
        assert!(selectors.iter().any(|receipt| {
            receipt["method"] == format!("session/config-option/{field}")
                && receipt["readback_verified"] == true
        }));
    }
    assert_eq!(evidence["open"]["model_id"], "deepseek/deepseek-v4-flash");
    assert_eq!(evidence["open"]["effective_effort"], "default");
    assert_eq!(evidence["open"]["effective_mode"], "default");
}
