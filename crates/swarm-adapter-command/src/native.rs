use crate::journal::{DispatchIdentity, RunStore, digest};
use crate::{ARTIFACT_ID, EXECUTION_SHAPE};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    io::Read,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};
use swarm_contracts::{
    EffectOutcome, RuntimeCommand, RuntimeOutcome,
    error::{Error, Result},
};
use swarm_process::{process_birth_identity, write_private_new};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    task::{AbortHandle, JoinHandle},
    time,
};

const MAX_PROMPT_BYTES: usize = 1_048_576;
const MAX_STDOUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_STDERR_BYTES: usize = 256 * 1024;
const MAX_LINE_BYTES: usize = 1_048_576;
const MAX_FRAME_COUNT: usize = 20_000;
const MAX_RESULT_PREVIEW_BYTES: usize = 8 * 1024;
const MAX_NATIVE_MODEL_BYTES: usize = 256;
const MAX_NATIVE_EVENT_TYPE_BYTES: usize = 128;
const MAX_NATIVE_SESSION_ID_BYTES: usize = 256;
const MAX_NATIVE_STOP_REASON_BYTES: usize = 256;
const MAX_NATIVE_USAGE_SUMMARY_BYTES: usize = 8 * 1024;
const STREAM_DRAIN_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct InvocationConfig {
    pub program: PathBuf,
    pub fixed_args: Vec<String>,
    pub mod_path: PathBuf,
    pub run_timeout: Duration,
    pub owner_token: String,
}

#[derive(Debug)]
pub struct InvocationResult {
    pub outcome: RuntimeOutcome,
    /// True only when the bridge must stop polling because the direct native
    /// child did not exit before its deadline. The host owner then verifies the
    /// complete inherited module group before allowing another bridge.
    pub stop_bridge: bool,
}

#[derive(Debug, Default)]
struct StreamCapture {
    prefix: Vec<u8>,
    total_bytes: u64,
    sha256: String,
    truncated: bool,
    read_error: bool,
}

#[derive(Debug, Default)]
struct StreamSummary {
    event_count: usize,
    event_types: BTreeMap<String, u64>,
    result: Option<Value>,
    result_subtype: Option<String>,
    result_payload_sha256: Option<String>,
    result_payload_bytes: Option<usize>,
    model_evidence: Vec<Value>,
    model_names: Vec<String>,
    session_id: Option<String>,
    gaps: Vec<String>,
    gap_count: usize,
    frames_after_result: usize,
    frame_count: usize,
    partial_final_line: bool,
}

pub async fn invoke(
    config: &InvocationConfig,
    store: &RunStore,
    command: &RuntimeCommand,
    identity: &DispatchIdentity,
    prompt: &str,
    workspace: &Path,
) -> Result<InvocationResult> {
    let run_dir = store.directory(&command.operation_id)?;
    let mod_control_dir = run_dir.join("mod");
    match std::fs::symlink_metadata(&mod_control_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "native mod control path is not a regular directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&mod_control_dir)?;
            swarm_process::private_permissions(&mod_control_dir, true)?;
        }
        Err(error) => return Err(error.into()),
    }
    if prompt.is_empty() || prompt.len() > MAX_PROMPT_BYTES {
        return Err(Error::new(
            "COMMAND_PROMPT_BOUNDARY",
            "frozen Command instruction is empty or exceeds one MiB",
        ));
    }
    let normalized_mod = normalize_lf(&read_bounded(&config.mod_path, 2 * 1024 * 1024)?);
    let mod_sha256 = digest(&normalized_mod);
    if mod_sha256 != crate::MOD_SHA256 {
        return Err(Error::new(
            "COMMAND_MOD_MISMATCH",
            "configured mod differs from the pinned adapter artifact",
        ));
    }
    // Hash and execute the same private, normalized bytes. The native CLI
    // never reopens a mutable operator path after this pin check.
    let pinned_mod_path = run_dir.join("pinned-eliot-command.ts");
    write_private_new(&pinned_mod_path, &normalized_mod)?;
    let workspace = std::fs::canonicalize(workspace).map_err(|_| {
        Error::new(
            "COMMAND_WORKSPACE_INVALID",
            "route workspace is unavailable",
        )
    })?;
    if !workspace.is_dir() {
        return Err(Error::new(
            "COMMAND_WORKSPACE_INVALID",
            "route workspace is not a directory",
        ));
    }

    let native_args = native_arguments(config, &identity.requested_model, &pinned_mod_path, prompt);
    if !native_argument_length_supported(config.program.as_os_str(), &native_args) {
        let mut outcome = launch_rejected(
            command,
            identity,
            &workspace,
            &mod_sha256,
            "NATIVE_COMMAND_LINE_LIMIT",
            false,
        );
        outcome.details["module_owner_token_sha256"] = json!(digest(config.owner_token.as_bytes()));
        let native_child = direct_child_facts(None, &Value::Null, false, Value::Null);
        outcome.details["native_child"] = native_child.clone();
        store.save_native_evidence(
            &command.operation_id,
            b"",
            b"",
            &json!({"spawn_attempted":false,"native_child":native_child}),
        )?;
        return Ok(InvocationResult {
            outcome,
            stop_bridge: false,
        });
    }

    let mut native = Command::new(&config.program);
    native
        .args(&native_args)
        .current_dir(&workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(false)
        .env_clear();
    install_native_environment(&mut native, &mod_control_dir);

    let mut child = match native.spawn() {
        Ok(child) => child,
        Err(_) => {
            let mut outcome = launch_rejected(
                command,
                identity,
                &workspace,
                &mod_sha256,
                "NATIVE_SPAWN_FAILED",
                true,
            );
            outcome.details["module_owner_token_sha256"] =
                json!(digest(config.owner_token.as_bytes()));
            let native_child = direct_child_facts(None, &Value::Null, false, Value::Null);
            outcome.details["native_child"] = native_child.clone();
            store.save_native_evidence(
                &command.operation_id,
                b"",
                b"",
                &json!({"spawn_error_observed":true,"native_child":native_child}),
            )?;
            return Ok(InvocationResult {
                outcome,
                stop_bridge: false,
            });
        }
    };

    // `Command::spawn` returned the direct child handle. The PID comes from
    // that handle; birth evidence is supplementary. No PID-only signal or
    // family-departure test is used here.
    let direct_pid = child.id();
    let birth = direct_pid.map(process_birth_identity).transpose();
    let birth_value = match birth {
        Ok(Some(Some(identity))) => json!({"status":"observed","identity":identity}),
        Ok(Some(None)) => json!({"status":"exited_before_birth_readback","identity":null}),
        Err(error) => json!({"status":"unavailable","code":safe_code(&error.code),"identity":null}),
        Ok(None) => json!({"status":"pid_unavailable","identity":null}),
    };

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("NATIVE_STREAM_UNAVAILABLE", "stdout pipe was not created"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::new("NATIVE_STREAM_UNAVAILABLE", "stderr pipe was not created"))?;
    let stdout_reader = tokio::spawn(read_stream(stdout, MAX_STDOUT_BYTES));
    let stderr_reader = tokio::spawn(read_stream(stderr, MAX_STDERR_BYTES));
    let stdout_abort = stdout_reader.abort_handle();
    let stderr_abort = stderr_reader.abort_handle();
    let status = match time::timeout(config.run_timeout, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => {
            let (stdout_capture, stderr_capture, capture_status) = drain_captures_after_stop(
                stdout_reader,
                stderr_reader,
                &stdout_abort,
                &stderr_abort,
            )
            .await;
            let mut outcome = unknown_outcome(
                command,
                identity,
                &workspace,
                &mod_sha256,
                "native_wait_failed",
                json!({
                    "timed_out":false,
                    "prompt_transport":"argv",
                    "native_stream_capture_status":format!("wait_error:{capture_status}"),
                    "stdout":capture_metadata(stdout_capture.as_ref()),
                    "stderr":capture_metadata(stderr_capture.as_ref()),
                    "spawn_error_observed":false,
                    "direct_child":direct_child_facts(direct_pid, &birth_value, false, Value::Null),
                    "native_session_state":"possibly_started"
                }),
            );
            outcome.details["module_owner_token_sha256"] =
                json!(digest(config.owner_token.as_bytes()));
            store.save_native_evidence(
                &command.operation_id,
                stdout_capture
                    .as_ref()
                    .map_or(&[][..], |capture| capture.prefix.as_slice()),
                stderr_capture
                    .as_ref()
                    .map_or(&[][..], |capture| capture.prefix.as_slice()),
                &json!({
                    "native_stream_capture_status":format!("wait_error:{capture_status}"),
                    "stdout":capture_metadata(stdout_capture.as_ref()),
                    "stderr":capture_metadata(stderr_capture.as_ref()),
                    "native_child":direct_child_facts(direct_pid, &birth_value, false, Value::Null)
                }),
            )?;
            return Ok(InvocationResult {
                outcome,
                stop_bridge: true,
            });
        }
        Err(_) => {
            // This bridge does not signal the child. It briefly drains
            // already-open output pipes, then leaves the exact group-drain
            // proof to the module owner.
            let (stdout_capture, stderr_capture, capture_status) = drain_captures_after_stop(
                stdout_reader,
                stderr_reader,
                &stdout_abort,
                &stderr_abort,
            )
            .await;
            let mut outcome = unknown_outcome(
                command,
                identity,
                &workspace,
                &mod_sha256,
                "native_deadline_exceeded",
                json!({
                    "timed_out":true,
                    "prompt_transport":"argv",
                    "native_stream_capture_status":format!("deadline:{capture_status}"),
                    "stdout":capture_metadata(stdout_capture.as_ref()),
                    "stderr":capture_metadata(stderr_capture.as_ref()),
                    "spawn_error_observed":false,
                    "direct_child":direct_child_facts(direct_pid, &birth_value, false, Value::Null),
                    "native_session_state":"possibly_started",
                    "family_departure_claimed":false,
                    "manager_group_drain_required":true
                }),
            );
            outcome.details["module_owner_token_sha256"] =
                json!(digest(config.owner_token.as_bytes()));
            store.save_native_evidence(
                &command.operation_id,
                stdout_capture
                    .as_ref()
                    .map_or(&[][..], |capture| capture.prefix.as_slice()),
                stderr_capture
                    .as_ref()
                    .map_or(&[][..], |capture| capture.prefix.as_slice()),
                &json!({
                    "timed_out":true,
                    "native_stream_capture_status":format!("deadline:{capture_status}"),
                    "stdout":capture_metadata(stdout_capture.as_ref()),
                    "stderr":capture_metadata(stderr_capture.as_ref()),
                    "native_child":direct_child_facts(direct_pid, &birth_value, false, Value::Null),
                    "family_departure_claimed":false,
                    "manager_group_drain_required":true
                }),
            )?;
            return Ok(InvocationResult {
                outcome,
                stop_bridge: true,
            });
        }
    };

    let (stdout_capture, stderr_capture, drain_timed_out) = match time::timeout(
        STREAM_DRAIN_GRACE,
        async {
            let stdout = join_capture(stdout_reader).await;
            let stderr = join_capture(stderr_reader).await;
            (stdout, stderr)
        },
    )
    .await
    {
        Ok((Ok(stdout), Ok(stderr))) => (stdout, stderr, false),
        Ok((stdout_result, stderr_result)) => {
            let stdout_joined = stdout_result.is_ok();
            let stderr_joined = stderr_result.is_ok();
            let stdout_capture = stdout_result.ok();
            let stderr_capture = stderr_result.ok();
            let mut outcome = unknown_outcome(
                command,
                identity,
                &workspace,
                &mod_sha256,
                "native_stream_reader_incomplete",
                json!({
                    "timed_out":false,
                    "prompt_transport":"argv",
                    "stdout_reader_joined":stdout_joined,
                    "stderr_reader_joined":stderr_joined,
                    "stdout_read_error":stdout_capture.as_ref().map(|capture| capture.read_error),
                    "stderr_read_error":stderr_capture.as_ref().map(|capture| capture.read_error),
                    "spawn_error_observed":false,
                    "direct_child":direct_child_facts(direct_pid, &birth_value, true, status_json(&status)),
                    "family_departure_claimed":false,
                    "manager_group_drain_required":true
                }),
            );
            outcome.details["module_owner_token_sha256"] =
                json!(digest(config.owner_token.as_bytes()));
            store.save_native_evidence(
                    &command.operation_id,
                    stdout_capture.as_ref().map_or(&[][..], |capture| capture.prefix.as_slice()),
                    stderr_capture.as_ref().map_or(&[][..], |capture| capture.prefix.as_slice()),
                    &json!({
                        "prompt_transport":"argv",
                        "stdout_reader_joined":stdout_joined,
                        "stderr_reader_joined":stderr_joined,
                        "stdout_bytes":stdout_capture.as_ref().map(|capture| capture.total_bytes),
                        "stdout_sha256":stdout_capture.as_ref().map(|capture| capture.sha256.as_str()),
                        "stdout_truncated":stdout_capture.as_ref().map(|capture| capture.truncated),
                        "stdout_read_error":stdout_capture.as_ref().map(|capture| capture.read_error),
                        "stderr_bytes":stderr_capture.as_ref().map(|capture| capture.total_bytes),
                        "stderr_sha256":stderr_capture.as_ref().map(|capture| capture.sha256.as_str()),
                        "stderr_truncated":stderr_capture.as_ref().map(|capture| capture.truncated),
                        "stderr_read_error":stderr_capture.as_ref().map(|capture| capture.read_error),
                        "native_child":direct_child_facts(direct_pid, &birth_value, true, status_json(&status))
                    }),
                )?;
            return Ok(InvocationResult {
                outcome,
                stop_bridge: true,
            });
        }
        Err(_) => {
            stdout_abort.abort();
            stderr_abort.abort();
            let mut outcome = unknown_outcome(
                command,
                identity,
                &workspace,
                &mod_sha256,
                "native_stream_drain_incomplete",
                json!({
                    "timed_out":false,
                    "stream_drain_timed_out":true,
                    "prompt_transport":"argv",
                    "native_stream_capture_status":"drain_deadline_exceeded",
                    "spawn_error_observed":false,
                    "direct_child":direct_child_facts(direct_pid, &birth_value, true, status_json(&status)),
                    "family_departure_claimed":false,
                    "manager_group_drain_required":true
                }),
            );
            outcome.details["module_owner_token_sha256"] =
                json!(digest(config.owner_token.as_bytes()));
            store.save_native_evidence(
                    &command.operation_id,
                    b"",
                    b"",
                    &json!({
                        "stream_drain_timed_out":true,
                        "native_stream_capture_status":"drain_deadline_exceeded",
                        "native_child":direct_child_facts(direct_pid, &birth_value, true, status_json(&status))
                    }),
                )?;
            return Ok(InvocationResult {
                outcome,
                stop_bridge: true,
            });
        }
    };

    let summary = summarize_stdout(&stdout_capture.prefix, stdout_capture.truncated);
    let signal = status_signal(&status);
    let mut anomalies = summary.gaps.clone();
    if summary.result.is_none() {
        anomalies.push("native_result_missing".to_owned());
    }
    if summary.frames_after_result > 0 {
        anomalies.push("frames_after_result_line".to_owned());
    }
    if summary.partial_final_line {
        anomalies.push("native_stream_partial_final_line".to_owned());
    }
    if stdout_capture.truncated || stderr_capture.truncated {
        anomalies.push("native_stream_truncated".to_owned());
    }
    if stdout_capture.read_error || stderr_capture.read_error {
        anomalies.push("native_stream_read_error".to_owned());
    }
    if drain_timed_out {
        anomalies.push("native_stream_drain_timed_out".to_owned());
    }
    let mut outcome = disposition(
        command,
        identity,
        &workspace,
        &mod_sha256,
        &summary,
        &stdout_capture,
        &stderr_capture,
        &status,
        anomalies,
        drain_timed_out,
        direct_child_facts(direct_pid, &birth_value, true, status_json(&status)),
    );
    outcome.details["prompt_transport"] = json!("argv");
    outcome.details["module_owner_token_sha256"] = json!(digest(config.owner_token.as_bytes()));
    let receipt = json!({
        "schema":1,
        "module_artifact_id":ARTIFACT_ID,
        "operation_id":command.operation_id,
        "input_sha256":identity.input_sha256,
        "requested_model":identity.requested_model,
        "prompt_sha256":identity.prompt_sha256,
        "prompt_bytes":identity.prompt_bytes,
        "stdout_bytes":stdout_capture.total_bytes,
        "stdout_stored_bytes":stdout_capture.prefix.len(),
        "stdout_sha256":stdout_capture.sha256,
        "stdout_stored_sha256":digest(&stdout_capture.prefix),
        "stdout_truncated":stdout_capture.truncated,
        "stdout_read_error":stdout_capture.read_error,
        "stderr_bytes":stderr_capture.total_bytes,
        "stderr_stored_bytes":stderr_capture.prefix.len(),
        "stderr_sha256":stderr_capture.sha256,
        "stderr_stored_sha256":digest(&stderr_capture.prefix),
        "stderr_truncated":stderr_capture.truncated,
        "stderr_read_error":stderr_capture.read_error,
        "native_result_subtype":summary.result_subtype,
        "prompt_transport":"argv",
        "native_event_count":summary.event_count,
        "native_event_types":summary.event_types,
        "protocol_gap_count":summary.gap_count,
        "frames_after_result":summary.frames_after_result,
        "exit":status_json(&status),
        "direct_child":direct_child_facts(direct_pid, &birth_value, true, status_json(&status)),
        "family_departure_claimed":false,
        "manager_group_drain_required":true
    });
    store.save_native_evidence(
        &command.operation_id,
        &stdout_capture.prefix,
        &stderr_capture.prefix,
        &receipt,
    )?;
    Ok(InvocationResult {
        outcome,
        stop_bridge: stdout_capture.read_error || stderr_capture.read_error || !signal.is_null(),
    })
}

// The receipt disposition consumes the full native capture tuple so the
// saved outcome retains each independently verified stream fact.
#[allow(clippy::too_many_arguments)]
fn disposition(
    command: &RuntimeCommand,
    identity: &DispatchIdentity,
    _workspace: &Path,
    mod_sha256: &str,
    summary: &StreamSummary,
    stdout: &StreamCapture,
    stderr: &StreamCapture,
    status: &ExitStatus,
    anomalies: Vec<String>,
    stream_drain_timed_out: bool,
    direct_child: Value,
) -> RuntimeOutcome {
    let exit_code = status.code();
    let signal = status_signal(status);
    let stream_complete = !stdout.truncated
        && !stderr.truncated
        && !stdout.read_error
        && !stderr.read_error
        && !summary.partial_final_line
        && summary.gap_count == 0
        && summary.frames_after_result == 0
        && !stream_drain_timed_out;
    let terminal = summary.result.is_some() && stream_complete && signal.is_null();
    let outcome = match (terminal, summary.result_subtype.as_deref(), exit_code) {
        (true, Some("success"), Some(0)) => EffectOutcome::Applied,
        (true, Some("error"), Some(1 | 3 | 4 | 5 | 6 | 7 | 9 | 10 | 130)) => {
            EffectOutcome::Rejected
        }
        (true, Some("max_turns"), Some(8)) => EffectOutcome::Rejected,
        _ => EffectOutcome::Unknown,
    };
    let completion = if summary.result.is_some() {
        "native_result_observed"
    } else {
        "native_result_unconfirmed"
    };
    let result = summary.result.as_ref();
    let final_text = result
        .and_then(|value| value.get("finalText"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let request_models: Vec<_> = summary.model_names.iter().collect();
    let (request_model, request_model_status) = match request_models.as_slice() {
        [only] => (json!(only), "observed"),
        [] => (Value::Null, "unknown"),
        _ => (Value::Null, "conflicting"),
    };
    let native_result = result.map(|value| {
        let session_id = bounded_native_text(value.get("sessionId"), MAX_NATIVE_SESSION_ID_BYTES);
        let stop_reason = bounded_native_text(value.get("stopReason"), MAX_NATIVE_STOP_REASON_BYTES);
        let usage = bounded_json_value(value.get("usage"), MAX_NATIVE_USAGE_SUMMARY_BYTES);
        let session_id_status = bounded_field_status(value.get("sessionId"), session_id.is_some());
        let stop_reason_status = bounded_field_status(value.get("stopReason"), stop_reason.is_some());
        let usage_status = bounded_field_status(value.get("usage"), usage.is_some());
        json!({
            "status":summary.result_subtype,
            "session_id":session_id,
            "session_id_status":session_id_status,
            "stop_reason":stop_reason,
            "stop_reason_status":stop_reason_status,
            "usage":usage,
            "usage_status":usage_status,
            "duration_ms":value.get("durationMs").filter(|value| value.is_u64() || value.is_i64()).cloned().unwrap_or(Value::Null),
            "error_present":value.get("error").is_some()
        })
    });
    let mut details = json!({
        "execution_shape":EXECUTION_SHAPE,
        "completion_condition":completion,
        "batch_run_id":identity.batch_run_id,
        "input_sha256":identity.input_sha256,
        "requested_model":identity.requested_model,
        "effective_model":Value::Null,
        "effective_model_status":"unknown",
        "native_request_model":request_model,
        "native_request_model_status":request_model_status,
        "native_request_model_evidence":summary.model_evidence,
        "native_session_id":result.and_then(|value| bounded_native_text(value.get("sessionId"), MAX_NATIVE_SESSION_ID_BYTES)).or_else(|| summary.session_id.clone()).map(Value::String).unwrap_or(Value::Null),
        "native_result":native_result.unwrap_or(Value::Null),
        "native_result_payload_sha256":summary.result_payload_sha256,
        "native_result_payload_bytes":summary.result_payload_bytes,
        "result_subtype":summary.result_subtype,
        "exit_code":exit_code,
        "signal":signal,
        "spawn_error_observed":false,
        "timed_out":false,
        "stream_drain_timed_out":stream_drain_timed_out,
        "prompt_sha256":identity.prompt_sha256,
        "prompt_bytes":identity.prompt_bytes,
        "task_snapshot_sha256":identity.task_snapshot_sha256,
        "native_event_count":summary.event_count,
        "native_event_types":summary.event_types,
        "native_frames_after_result":summary.frames_after_result,
        "native_protocol_gap_count":summary.gap_count,
        "stdout_bytes":stdout.total_bytes,
        "stdout_stored_bytes":stdout.prefix.len(),
        "stdout_sha256":stdout.sha256,
        "stdout_stored_sha256":digest(&stdout.prefix),
        "stdout_truncated":stdout.truncated,
        "stdout_read_error":stdout.read_error,
        "stderr_bytes":stderr.total_bytes,
        "stderr_stored_bytes":stderr.prefix.len(),
        "stderr_sha256":stderr.sha256,
        "stderr_stored_sha256":digest(&stderr.prefix),
        "stderr_truncated":stderr.truncated,
        "stderr_read_error":stderr.read_error,
        "result_text_sha256":digest(final_text.as_bytes()),
        "result_text_bytes":final_text.len(),
        "result_preview":truncate_utf8(final_text, MAX_RESULT_PREVIEW_BYTES),
        "artifact_refs":[],
        "output_artifact_refs":{},
        "result_page_available":false,
        "result_page_gap":"command_agent_result_not_admitted",
        "local_evidence_available":true,
        "native_child":direct_child,
        "family_departure_claimed":false,
        "manager_group_drain_required":true,
        "task_acceptance_claimed":false,
        "anomalies":anomalies
    });
    // Do not copy local paths, argv, or stderr into Store-visible Operation
    // details; the complete bounded stream remains in the private run store.
    details["mod_sha256"] = json!(mod_sha256);
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details,
    }
}

fn launch_rejected(
    command: &RuntimeCommand,
    identity: &DispatchIdentity,
    _workspace: &Path,
    mod_sha256: &str,
    diagnostic_code: &str,
    spawn_error_observed: bool,
) -> RuntimeOutcome {
    let mut details = base_details(identity);
    details["completion_condition"] = json!("executor_launch_rejected");
    details["native_session_state"] = json!("not_started");
    details["exit_code"] = Value::Null;
    details["signal"] = Value::Null;
    details["spawn_error_observed"] = json!(spawn_error_observed);
    details["timed_out"] = json!(false);
    details["mod_sha256"] = json!(mod_sha256);
    details["diagnostic_code"] = json!(diagnostic_code);
    details["artifact_refs"] = json!([]);
    details["output_artifact_refs"] = json!({});
    details["result_page_available"] = json!(false);
    details["task_acceptance_claimed"] = json!(false);
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Rejected,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details,
    }
}

fn unknown_outcome(
    command: &RuntimeCommand,
    identity: &DispatchIdentity,
    _workspace: &Path,
    mod_sha256: &str,
    diagnostic: &str,
    evidence: Value,
) -> RuntimeOutcome {
    let mut details = base_details(identity);
    details["completion_condition"] = json!("native_result_unconfirmed");
    details["native_session_state"] = evidence
        .get("native_session_state")
        .cloned()
        .unwrap_or(json!("possibly_started"));
    details["diagnostic_code"] = json!(diagnostic);
    details["mod_sha256"] = json!(mod_sha256);
    details["artifact_refs"] = json!([]);
    details["output_artifact_refs"] = json!({});
    details["result_page_available"] = json!(false);
    details["result_page_gap"] = json!("command_agent_result_not_admitted");
    details["task_acceptance_claimed"] = json!(false);
    if let Some(map) = evidence.as_object() {
        for (key, value) in map {
            details[key] = value.clone();
        }
    }
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details,
    }
}

pub fn core_bound_unknown(
    operation_id: &str,
    identity: &DispatchIdentity,
    diagnostic: &str,
) -> RuntimeOutcome {
    let mut details = base_details(identity);
    details["completion_condition"] = json!("native_result_unconfirmed");
    details["diagnostic_code"] = json!(diagnostic);
    details["native_session_state"] = json!("possibly_started");
    details["native_request_model"] = Value::Null;
    details["native_request_model_status"] = json!("unknown");
    details["native_request_model_evidence"] = json!([]);
    details["native_result"] = Value::Null;
    details["result_subtype"] = Value::Null;
    details["exit_code"] = Value::Null;
    details["signal"] = Value::Null;
    details["spawn_error_observed"] = json!(false);
    details["timed_out"] = json!(false);
    details["prompt_sha256"] = json!(identity.prompt_sha256);
    details["prompt_bytes"] = json!(identity.prompt_bytes);
    details["input_sha256"] = json!(identity.input_sha256);
    details["task_snapshot_sha256"] = json!(identity.task_snapshot_sha256);
    if identity.task_snapshot_sha256.is_empty() {
        details["task_snapshot_sha256"] = Value::Null;
        details["task_snapshot_sha256_status"] = json!("not_supplied_by_store_reconcile_contract");
    }
    details["artifact_refs"] = json!([]);
    details["output_artifact_refs"] = json!({});
    details["result_page_available"] = json!(false);
    details["result_page_gap"] = json!("command_agent_result_not_admitted");
    details["task_acceptance_claimed"] = json!(false);
    RuntimeOutcome {
        operation_id: operation_id.to_owned(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details,
    }
}

fn base_details(identity: &DispatchIdentity) -> Value {
    json!({
        "execution_shape":EXECUTION_SHAPE,
        "prompt_transport":"argv",
        "batch_run_id":identity.batch_run_id,
        "requested_model":identity.requested_model,
        "effective_model":Value::Null,
        "effective_model_status":"unknown",
        "native_session_id":Value::Null,
        "native_request_model":Value::Null,
        "native_request_model_status":"unknown",
        "native_request_model_evidence":[],
        "native_result":Value::Null,
        "result_subtype":Value::Null,
        "prompt_sha256":identity.prompt_sha256,
        "prompt_bytes":identity.prompt_bytes,
        "input_sha256":identity.input_sha256,
        "task_snapshot_sha256":identity.task_snapshot_sha256,
        "exit_code":Value::Null,
        "signal":Value::Null,
        "spawn_error_observed":false,
        "timed_out":false,
        "artifact_refs":[],
        "output_artifact_refs":{},
        "local_evidence_available":true,
        "family_departure_claimed":false,
        "manager_group_drain_required":true,
        "task_acceptance_claimed":false,
        "anomalies":[]
    })
}

fn summarize_stdout(bytes: &[u8], truncated: bool) -> StreamSummary {
    let mut summary = StreamSummary::default();
    let lines = bytes.split(|byte| *byte == b'\n');
    let has_final_newline = bytes.last() == Some(&b'\n');
    for raw_line in lines {
        if raw_line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        summary.frame_count += 1;
        if summary.frame_count > MAX_FRAME_COUNT {
            add_gap(&mut summary, "native_frame_count_limit");
            break;
        }
        if raw_line.len() > MAX_LINE_BYTES {
            add_gap(&mut summary, "native_frame_line_limit");
            continue;
        }
        let raw_line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        let parsed: Value = match serde_json::from_slice(raw_line) {
            Ok(value) => value,
            Err(_) => {
                add_gap(&mut summary, "native_frame_invalid_json");
                continue;
            }
        };
        match parsed["type"].as_str() {
            Some("event") => {
                if summary.result.is_some() {
                    summary.frames_after_result += 1;
                }
                let Some(event) = parsed.get("event").filter(|value| value.is_object()) else {
                    add_gap(&mut summary, "native_event_frame_invalid");
                    continue;
                };
                let Some(event_type) = event["type"].as_str().filter(|value| !value.is_empty())
                else {
                    add_gap(&mut summary, "native_event_type_invalid");
                    continue;
                };
                summary.event_count += 1;
                if event_type.len() > MAX_NATIVE_EVENT_TYPE_BYTES {
                    add_gap(&mut summary, "native_event_type_too_large");
                    continue;
                }
                if summary.event_types.len() < 64 || summary.event_types.contains_key(event_type) {
                    *summary
                        .event_types
                        .entry(event_type.to_owned())
                        .or_default() += 1;
                }
                if matches!(event_type, "model_request_start" | "model_request_end")
                    && let Some(model) = event["model"]
                        .as_str()
                        .filter(|value| !value.trim().is_empty())
                {
                    if model.len() > MAX_NATIVE_MODEL_BYTES {
                        add_gap(&mut summary, "native_request_model_too_large");
                        continue;
                    }
                    if summary.model_names.len() < 2
                        && !summary.model_names.iter().any(|saved| saved == model)
                    {
                        summary.model_names.push(model.to_owned());
                    }
                    if summary.model_evidence.len() < 32 {
                        summary.model_evidence.push(json!({
                            "seq":summary.frame_count,
                            "event_type":event_type,
                            "model":model
                        }));
                    }
                }
                if event_type == "run_start"
                    && summary.session_id.is_none()
                    && let Some(session) = event["sessionId"].as_str()
                    && session.len() <= MAX_NATIVE_SESSION_ID_BYTES
                {
                    summary.session_id = Some(session.to_owned());
                }
            }
            Some("result") => {
                if summary.result.is_some() {
                    add_gap(&mut summary, "native_duplicate_result_frame");
                    continue;
                }
                let subtype = parsed["subtype"].as_str();
                if !matches!(subtype, Some("success" | "error" | "max_turns")) {
                    add_gap(&mut summary, "native_result_subtype_invalid");
                    continue;
                }
                summary.result_subtype = subtype.map(str::to_owned);
                summary.result_payload_sha256 = Some(digest(raw_line));
                summary.result_payload_bytes = Some(raw_line.len());
                summary.result = Some(parsed);
            }
            _ => add_gap(&mut summary, "native_frame_kind_invalid"),
        }
    }
    if !bytes.is_empty() && !has_final_newline {
        summary.partial_final_line = true;
        add_gap(&mut summary, "native_stream_partial_final_line");
    }
    if truncated {
        add_gap(&mut summary, "native_stream_truncated");
    }
    summary
}

fn add_gap(summary: &mut StreamSummary, code: &str) {
    summary.gap_count += 1;
    if summary.gaps.len() < 64 && !summary.gaps.iter().any(|saved| saved == code) {
        summary.gaps.push(code.to_owned());
    }
}

fn bounded_native_text(value: Option<&Value>, limit: usize) -> Option<String> {
    let text = value?.as_str()?;
    (text.len() <= limit).then(|| text.to_owned())
}

fn bounded_json_value(value: Option<&Value>, limit: usize) -> Option<Value> {
    let value = value?;
    serde_json::to_vec(value)
        .ok()
        .filter(|bytes| bytes.len() <= limit)
        .map(|_| value.clone())
}

fn bounded_field_status(value: Option<&Value>, included: bool) -> &'static str {
    if value.is_none() {
        "absent"
    } else if included {
        "included"
    } else {
        "omitted_or_invalid"
    }
}

async fn read_stream<R: AsyncRead + Unpin>(mut reader: R, limit: usize) -> StreamCapture {
    let mut capture = StreamCapture::default();
    let mut hash = Sha256::new();
    let mut chunk = [0_u8; 16 * 1024];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => break,
            Ok(count) => {
                capture.total_bytes = capture.total_bytes.saturating_add(count as u64);
                hash.update(&chunk[..count]);
                let remaining = limit.saturating_sub(capture.prefix.len());
                let stored = remaining.min(count);
                capture.prefix.extend_from_slice(&chunk[..stored]);
                if stored < count {
                    capture.truncated = true;
                }
            }
            Err(_) => {
                capture.read_error = true;
                break;
            }
        }
    }
    capture.sha256 = format!("{:x}", hash.finalize());
    capture
}

async fn join_capture(task: JoinHandle<StreamCapture>) -> Result<StreamCapture> {
    task.await.map_err(|_| {
        Error::new(
            "NATIVE_STREAM_JOIN",
            "native stream reader did not complete",
        )
    })
}

async fn drain_captures_after_stop(
    stdout_task: JoinHandle<StreamCapture>,
    stderr_task: JoinHandle<StreamCapture>,
    stdout_abort: &AbortHandle,
    stderr_abort: &AbortHandle,
) -> (Option<StreamCapture>, Option<StreamCapture>, &'static str) {
    match time::timeout(STREAM_DRAIN_GRACE, async {
        (
            join_capture(stdout_task).await,
            join_capture(stderr_task).await,
        )
    })
    .await
    {
        Ok((stdout, stderr)) => {
            let joined = stdout.is_ok() && stderr.is_ok();
            if !joined {
                stdout_abort.abort();
                stderr_abort.abort();
            }
            (
                stdout.ok(),
                stderr.ok(),
                if joined {
                    "drained_to_eof"
                } else {
                    "reader_join_failed"
                },
            )
        }
        Err(_) => {
            stdout_abort.abort();
            stderr_abort.abort();
            (None, None, "drain_deadline_exceeded")
        }
    }
}

fn native_arguments(
    config: &InvocationConfig,
    model: &str,
    mod_path: &Path,
    prompt: &str,
) -> Vec<OsString> {
    let mut args = config
        .fixed_args
        .iter()
        .map(OsString::from)
        .collect::<Vec<_>>();
    args.extend([
        OsString::from("-p"),
        OsString::from("--output-format"),
        OsString::from("json"),
        OsString::from("--model"),
        OsString::from(model),
        OsString::from("--mod"),
        mod_path.as_os_str().to_os_string(),
        OsString::from(prompt),
    ]);
    args
}

#[cfg(windows)]
fn native_argument_length_supported(program: &OsStr, args: &[OsString]) -> bool {
    use std::os::windows::ffi::OsStrExt;

    fn quoted_units(value: &OsStr) -> Option<usize> {
        let units = value.encode_wide().collect::<Vec<_>>();
        if units.contains(&0) {
            return None;
        }
        let quote = units.is_empty()
            || units
                .iter()
                .any(|unit| *unit == 0x20 || *unit == 0x09 || *unit == 0x22);
        let mut length = if quote { 2_usize } else { 0 };
        let mut backslashes = 0_usize;
        for unit in units {
            if unit == b'\\' as u16 {
                backslashes = backslashes.checked_add(1)?;
            } else {
                let escaped_backslashes = if unit == b'"' as u16 {
                    backslashes.checked_mul(2)?
                } else {
                    backslashes
                };
                length = length.checked_add(escaped_backslashes)?;
                if unit == b'"' as u16 {
                    length = length.checked_add(2)?;
                } else {
                    length = length.checked_add(1)?;
                }
                backslashes = 0;
            }
        }
        let trailing_backslashes = if quote {
            backslashes.checked_mul(2)?
        } else {
            backslashes
        };
        length.checked_add(trailing_backslashes)
    }

    let Some(mut length) = quoted_units(program) else {
        return false;
    };
    for argument in args {
        let Some(argument_length) = quoted_units(argument) else {
            return false;
        };
        let Some(next) = length
            .checked_add(1)
            .and_then(|length| length.checked_add(argument_length))
        else {
            return false;
        };
        length = next;
    }
    // Leave room below CreateProcessW's 32,767 UTF-16 code-unit boundary.
    length.checked_add(1).is_some_and(|length| length <= 32_000)
}

#[cfg(not(windows))]
fn native_argument_length_supported(_program: &OsStr, _args: &[OsString]) -> bool {
    true
}

fn capture_metadata(capture: Option<&StreamCapture>) -> Value {
    capture.map_or(Value::Null, |capture| {
        json!({
            "bytes":capture.total_bytes,
            "stored_bytes":capture.prefix.len(),
            "sha256":capture.sha256,
            "stored_sha256":digest(&capture.prefix),
            "truncated":capture.truncated,
            "read_error":capture.read_error
        })
    })
}

fn install_native_environment(command: &mut Command, control_dir: &Path) {
    for (key, value) in std::env::vars_os() {
        let upper = key.to_string_lossy().to_ascii_uppercase();
        if upper.starts_with("ELIOT_") || upper.starts_with("SWARM_") || upper.contains("CAPTURE") {
            continue;
        }
        command.env(key, value);
    }
    command.env("ELIOT_COMMAND_CONTROL_DIR", control_dir);
}

fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "COMMAND_MOD_UNAVAILABLE",
            "configured Command mod is unavailable",
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > max {
        return Err(Error::new(
            "COMMAND_MOD_INVALID",
            "configured Command mod is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len().min(max) as usize);
    std::fs::File::open(path)?
        .take(max + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err(Error::new(
            "COMMAND_MOD_INVALID",
            "configured Command mod exceeded its read boundary",
        ));
    }
    Ok(bytes)
}

fn normalize_lf(bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
            output.push(b'\n');
            index += 2;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    output
}

fn status_json(status: &ExitStatus) -> Value {
    json!({"code":status.code(),"signal":status_signal(status)})
}

fn status_signal(status: &ExitStatus) -> Value {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status
            .signal()
            .map(|signal| json!(signal.to_string()))
            .unwrap_or(Value::Null)
    }
    #[cfg(not(unix))]
    {
        let _ = status;
        Value::Null
    }
}

fn direct_child_facts(pid: Option<u32>, birth: &Value, exit_observed: bool, exit: Value) -> Value {
    json!({
        "spawn_returned_pid":pid,
        "birth_identity":birth,
        "exit_observed_through_child_handle":exit_observed,
        "exit":exit,
        "family_departure_claimed":false,
        "manager_group_drain_required":true
    })
}

fn truncate_utf8(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let end = text
        .char_indices()
        .take_while(|(index, character)| index + character.len_utf8() <= max_bytes)
        .map(|(index, character)| index + character.len_utf8())
        .last()
        .unwrap_or(0);
    text[..end].to_owned()
}

fn safe_code(code: &str) -> &str {
    if code.is_empty()
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        "PROCESS_IDENTITY_ERROR"
    } else {
        code
    }
}

pub fn prompt_for(command: &RuntimeCommand) -> Result<(String, DispatchIdentity)> {
    if command.method != "task.dispatch" {
        return Err(Error::invalid("native prompt requires task.dispatch"));
    }
    let text = command.input["text"]
        .as_str()
        .ok_or_else(|| Error::invalid("dispatch text is missing"))?;
    let snapshot = command
        .input
        .get("task_snapshot")
        .filter(|value| value.is_object())
        .ok_or_else(|| Error::invalid("immutable task snapshot is missing"))?;
    let canonical_snapshot = canonical_snapshot(snapshot)?;
    if command.input["task_snapshot_canonical"].as_str() != Some(canonical_snapshot.as_str()) {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "provided canonical Task snapshot differs from the frozen snapshot",
        ));
    }
    let prompt = format!("{text}\n\nELIOT immutable task snapshot:\n{canonical_snapshot}");
    if prompt.is_empty() || prompt.len() > MAX_PROMPT_BYTES {
        return Err(Error::new(
            "COMMAND_PROMPT_BOUNDARY",
            "frozen Command instruction is empty or exceeds one MiB",
        ));
    }
    let model = command.route["native_options"]["modelId"]
        .as_str()
        .filter(|value| !value.is_empty() && *value == value.trim())
        .ok_or_else(|| Error::new("COMMAND_MODEL_REQUIRED", "route modelId is invalid"))?;
    let input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|value| is_sha256(value))
        .ok_or_else(|| {
            Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "Store input digest is missing or invalid",
            )
        })?;
    let digest_operation = digest(command.operation_id.as_bytes());
    let identity = DispatchIdentity {
        operation_id: command.operation_id.clone(),
        input_sha256: input_sha256.to_owned(),
        batch_run_id: format!("command-batch:{}", &digest_operation[..32]),
        requested_model: model.to_owned(),
        prompt_sha256: digest(prompt.as_bytes()),
        prompt_bytes: prompt.len(),
        task_snapshot_sha256: digest(canonical_snapshot.as_bytes()),
    };
    let supplied = &command.input["command_core_binding"];
    if supplied["batch_run_id"] != identity.batch_run_id
        || supplied["prompt_sha256"] != identity.prompt_sha256
        || supplied["prompt_bytes"].as_u64() != u64::try_from(identity.prompt_bytes).ok()
        || supplied.as_object().is_none_or(|object| {
            object.len() != 3
                || !object.contains_key("batch_run_id")
                || !object.contains_key("prompt_sha256")
                || !object.contains_key("prompt_bytes")
        })
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "Command instruction differs from the Store-frozen prompt receipt",
        ));
    }
    Ok((prompt, identity))
}

fn canonical_snapshot(snapshot: &Value) -> Result<String> {
    fn ordered(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> = map
                    .iter()
                    .map(|(key, child)| (key.clone(), ordered(child)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(ordered).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&ordered(snapshot)).map_err(Into::into)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn route_workspace(command: &RuntimeCommand) -> Result<PathBuf> {
    let workspace = command.route["native_options"]["workspaceRoot"]
        .as_str()
        .filter(|value| Path::new(value).is_absolute())
        .ok_or_else(|| {
            Error::new(
                "COMMAND_WORKSPACE_INVALID",
                "route workspaceRoot must be absolute",
            )
        })?;
    Ok(PathBuf::from(workspace))
}

pub fn result_identity_matches(value: &Value, identity: &DispatchIdentity) -> bool {
    value["operation_id"] == identity.operation_id
        && value["details"]["input_sha256"] == identity.input_sha256
        && value["details"]["batch_run_id"] == identity.batch_run_id
        && value["details"]["requested_model"] == identity.requested_model
        && value["details"]["prompt_sha256"] == identity.prompt_sha256
        && value["details"]["prompt_bytes"].as_u64() == u64::try_from(identity.prompt_bytes).ok()
}

pub fn open_preflight_outcome(
    command: &RuntimeCommand,
    requested_model: &str,
    ready: bool,
    code: Option<&str>,
) -> RuntimeOutcome {
    let outcome = if ready {
        EffectOutcome::Applied
    } else {
        EffectOutcome::Rejected
    };
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({
            "execution_shape":EXECUTION_SHAPE,
            "completion_condition":if ready {"executor_preflight_completed"} else {"executor_preflight_rejected"},
            "requested_model":requested_model,
            "effective_model":Value::Null,
            "effective_model_status":"unknown",
            "native_session_state":"not_started",
            "installed_runtime_verified":false,
            "native_version_probe_performed":false,
            "native_model_probe_performed":false,
            "mod_sha256":crate::MOD_SHA256,
            "diagnostic_code":code,
            "task_acceptance_claimed":false
        }),
    }
}
