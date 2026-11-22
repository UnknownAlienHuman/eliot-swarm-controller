pub(crate) mod auth;
pub(crate) mod edit;
pub(crate) mod freeze;
pub(crate) mod process;

pub(crate) use freeze::freeze_project_run;

use crate::{
    platform::CustodyGuarantee,
    router::Router,
    scripts::process::{ProcessCapture, ProcessFailure, ProcessOutcome},
};
use eliot_core::{
    Command, CommandState, Error, FrozenScriptRun, Result, ScriptActionRecord, ScriptCustody,
    ScriptDirectExit, ScriptInputContext, ScriptProcessResult, limits::MAX_FRAME_BYTES,
};
use serde_json::json;
use std::sync::Arc;

/// Execute a Run only after ordinary Store dispatch has committed its event.
pub(crate) async fn execute_dispatched(router: Router, command: Command) {
    let run_id = command.id.clone();
    let frozen: FrozenScriptRun = match serde_json::from_value(command.args.clone()) {
        Ok(frozen) => frozen,
        Err(_) => {
            settle_failed_before_process(&router, &run_id, "run_inputs_invalid").await;
            return;
        }
    };

    let (mut credential, saved_principal) = match router.issue_run_credential(&command).await {
        Ok(value) => value,
        Err(error) => {
            settle_failed_before_process(&router, &run_id, &error.code).await;
            return;
        }
    };
    let verified_child = match &saved_principal {
        auth::SavedRunPrincipal::Owner => router
            .verify_global_run(&frozen)
            .await
            .map_err(|error| error.code),
        auth::SavedRunPrincipal::ProjectRule { registration } => {
            if router.verify_run_registration(registration).await.is_err() {
                Err("project_registration_changed".to_owned())
            } else if !project_worktree_matches_registration(&frozen, registration).await {
                Err("script_worktree_repository_mismatch".to_owned())
            } else {
                let verification_policy = router.secret_policy_snapshot();
                freeze::verify_project_run(&frozen, registration, verification_policy.as_ref())
                    .map_err(|error| error.code)
            }
        }
    };
    let mut child = match verified_child {
        Ok(child) => child,
        Err(cause) => {
            credential.revoke();
            settle_failed_before_process(&router, &run_id, &cause).await;
            return;
        }
    };

    scrub_role_environment(&mut child);
    child
        .env("ELIOT_RUN_COMMAND_ID", &run_id)
        .env("ELIOT_RUN_TOKEN", credential.token());
    let policy = router.script_secret_policy(credential.token());

    let mut input = match ScriptInputContext::for_run_command(&frozen.context, run_id.clone()) {
        Ok(input) => input,
        Err(error) => {
            credential.revoke();
            settle_failed_before_process(&router, &run_id, &error.code).await;
            return;
        }
    };
    display_sanitize_workspace_paths(&mut input);
    let stdin = match serde_json::to_vec(&input) {
        Ok(stdin) if stdin.len() <= MAX_FRAME_BYTES => stdin,
        _ => {
            credential.revoke();
            settle_failed_before_process(&router, &run_id, "run_input_too_large").await;
            return;
        }
    };
    let timeout = match freeze::project_run_timeout(&frozen) {
        Ok(timeout) => timeout,
        Err(error) => {
            credential.revoke();
            settle_failed_before_process(&router, &run_id, &error.code).await;
            return;
        }
    };

    let outcome = process::run_owned_process_with_stderr(
        child,
        stdin,
        timeout,
        Arc::clone(&policy),
        router.run_cancellation_receiver(),
        process::StderrExcerpt::Prefix,
    )
    .await;
    credential.revoke();

    match outcome {
        ProcessOutcome::Completed(capture) => {
            let process_result = process_result_from_capture(&capture);
            if !capture.status.success()
                || !capture.stdout_complete
                || process_result.custody == ScriptCustody::Unknown
            {
                settle_unknown(
                    &router,
                    &run_id,
                    "script_completion_unproven",
                    process_result,
                )
                .await;
                return;
            }
            let records = match parse_script_output(&capture.stdout) {
                Ok(records) => records,
                Err(error) => {
                    settle_failed(&router, &run_id, &error.code, process_result).await;
                    return;
                }
            };
            let mut actions = Vec::with_capacity(records.len());
            for record in records {
                match record.into_submit_command() {
                    Ok(action) => actions.push(action),
                    Err(error) => {
                        settle_failed(&router, &run_id, &error.code, process_result).await;
                        return;
                    }
                }
            }
            let output = match serde_json::to_value(&actions) {
                Ok(output) => output,
                Err(_) => {
                    settle_failed(&router, &run_id, "script_output_invalid", process_result).await;
                    return;
                }
            };
            if policy.contains_value(&output) {
                settle_failed(&router, &run_id, "secret_in_script_output", process_result).await;
                return;
            }
            let prepared = match router
                .prepare_script_output(actions, &saved_principal, &frozen)
                .await
            {
                Ok(prepared) => prepared,
                Err(error) => {
                    settle_failed(&router, &run_id, &error.code, process_result).await;
                    return;
                }
            };
            if let Err(error) = router
                .admit_script_output(&run_id, prepared, process_result.clone())
                .await
                && error.code != "unknown"
            {
                settle_failed(
                    &router,
                    &run_id,
                    "script_output_admission_failed",
                    process_result,
                )
                .await;
            }
        }
        ProcessOutcome::Failed { reason, capture } => {
            let process_result = capture
                .as_ref()
                .map(process_result_from_capture)
                .unwrap_or_else(|| unobserved_process_result(true));
            settle_failed(
                &router,
                &run_id,
                process_failure_code(reason),
                process_result,
            )
            .await;
        }
        ProcessOutcome::NotSpawned(_) => {
            settle_unknown(
                &router,
                &run_id,
                process::ProcessUncertainty::Spawn.stable_code(),
                unobserved_process_result(false),
            )
            .await;
        }
        ProcessOutcome::Unknown { reason, quiescent } => {
            settle_unknown(
                &router,
                &run_id,
                reason.stable_code(),
                unobserved_process_result(quiescent),
            )
            .await;
        }
    }
}

fn process_result_from_capture(capture: &ProcessCapture) -> ScriptProcessResult {
    ScriptProcessResult {
        custody: script_custody(capture.custody),
        quiescent: true,
        direct_exit: ScriptDirectExit::Exited {
            success: capture.status.success(),
            exit_code: capture.status.code(),
        },
        stderr: Some(capture.stderr.clone()),
    }
}

pub(crate) fn unobserved_process_result(quiescent: bool) -> ScriptProcessResult {
    ScriptProcessResult {
        custody: ScriptCustody::Unknown,
        quiescent,
        direct_exit: ScriptDirectExit::Unknown,
        stderr: None,
    }
}

#[cfg(windows)]
fn script_custody(_: CustodyGuarantee) -> ScriptCustody {
    ScriptCustody::Exact
}

#[cfg(unix)]
fn script_custody(custody: CustodyGuarantee) -> ScriptCustody {
    match custody {
        CustodyGuarantee::Degraded => ScriptCustody::Degraded,
        CustodyGuarantee::Unknown => ScriptCustody::Unknown,
    }
}

#[cfg(not(any(windows, unix)))]
fn script_custody(_: CustodyGuarantee) -> ScriptCustody {
    ScriptCustody::Unknown
}
fn scrub_role_environment(command: &mut tokio::process::Command) {
    for name in [
        "ELIOT_AGENT_ID",
        "ELIOT_AGENT_GENERATION",
        "ELIOT_AGENT_TOKEN",
        crate::daemon::AGENT_DATA_DIR_ENV,
        "ELIOT_OWNER_ID",
        "ELIOT_OWNER_TOKEN",
        "ELIOT_OWNER_CREDENTIAL",
        "ELIOT_RUN_COMMAND_ID",
        "ELIOT_RUN_TOKEN",
    ] {
        command.env_remove(name);
    }
}

fn process_failure_code(reason: ProcessFailure) -> &'static str {
    match reason {
        ProcessFailure::InputLimit => "run_input_too_large",
        ProcessFailure::InvalidTimeout => "run_timeout_invalid",
        ProcessFailure::Exit => "script_exit_failed",
        ProcessFailure::Timeout => "script_timeout",
        ProcessFailure::Cancelled => "script_cancelled",
        ProcessFailure::OutputLimit => "script_output_too_large",
    }
}

async fn project_worktree_matches_registration(
    frozen: &FrozenScriptRun,
    registration: &eliot_core::api::ProjectRegistrationView,
) -> bool {
    let Some(workspace) = freeze::event_workspace(&frozen.context) else {
        return true;
    };
    let Ok(workspace_git) = crate::workspace_git::WorkspaceGit::new() else {
        return false;
    };
    let Ok((_, common_dir)) = workspace_git.repository_identity(workspace).await else {
        return false;
    };
    same_repository_path(&common_dir, &registration.repo_common_dir)
}

#[cfg(windows)]
fn same_repository_path(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(not(windows))]
fn same_repository_path(left: &str, right: &str) -> bool {
    left == right
}

fn display_sanitize_workspace_paths(input: &mut ScriptInputContext) {
    let ScriptInputContext::Event { event, subject, .. } = input else {
        return;
    };
    if let Some(workspace) = event
        .subject
        .as_mut()
        .and_then(|subject| subject.workspace.as_mut())
    {
        *workspace = eliot_core::display_path(workspace);
    }
    if let Some(workspace) = subject
        .as_mut()
        .and_then(|subject| subject.workspace.as_mut())
    {
        *workspace = eliot_core::display_path(workspace);
    }
}

fn parse_script_output(bytes: &[u8]) -> Result<Vec<ScriptActionRecord>> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "invalid",
            "script output exceeds the shared Core frame bound",
        ));
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| Error::new("invalid", "script output is not valid UTF-8"))?;
    if text.is_empty() {
        return Ok(Vec::new());
    }

    let bytes = text.as_bytes();
    let mut offset = 0;
    let mut records = Vec::new();
    while offset < bytes.len() {
        let end = bytes[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |relative| offset + relative);
        let terminated = end < bytes.len();
        let mut line = &bytes[offset..end];
        if terminated && line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        } else if !terminated && line.last() == Some(&b'\r') {
            return Err(Error::new(
                "invalid",
                "script output ends with an incomplete CRLF record",
            ));
        }
        if line.is_empty() {
            return Err(Error::new(
                "invalid",
                "script output contains an empty line",
            ));
        }
        let record: ScriptActionRecord = serde_json::from_slice(line)
            .map_err(|_| Error::new("invalid", "script output record is malformed"))?;

        records.push(record);
        offset = if terminated { end + 1 } else { end };
    }
    Ok(records)
}

async fn settle_failed_before_process(router: &Router, run_id: &str, cause: &str) {
    settle_failed(router, run_id, cause, unobserved_process_result(true)).await;
}

async fn settle_failed(router: &Router, run_id: &str, cause: &str, process: ScriptProcessResult) {
    let outcome = json!({"code":"failed", "cause":cause, "process":process});
    let _ = router
        .settle(run_id, CommandState::Failed, Some(outcome))
        .await;
}

async fn settle_unknown(router: &Router, run_id: &str, cause: &str, process: ScriptProcessResult) {
    let outcome = json!({"code":"unknown", "cause":cause, "process":process});
    let _ = router
        .settle(run_id, CommandState::Unknown, Some(outcome))
        .await;
}
