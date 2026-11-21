//! Replace a running daemon without replaying an Agent input.

use super::daemon::shutdown_daemon;
use super::operator::{
    agents, check_same_daemon, require_running, start_owned, status, wait_for_exit,
};
use super::support::{Failure, mutation_id, print_value};
use eliot_core::api::{DaemonShutdownRequest, ShutdownMode};
use eliot_daemon::operator::{
    DaemonInfo, binary_sha256, daemon_alive, lock_offline, lock_transition, replacement_data_dir,
    running_binary,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout};

pub(crate) async fn run(
    source: PathBuf,
    data_dir: PathBuf,
    wait_seconds: u64,
    json_output: bool,
) -> Result<u8, Failure> {
    let (old, _) = require_running(&data_dir).await?;
    let previous_binary = running_binary(&old).map_err(Failure::for_read)?;
    let previous_hash = binary_sha256(&previous_binary).map_err(Failure::for_read)?;
    let binary = release_binary(&source)?;
    let previous = release_info(&previous_binary).await?;
    let candidate = release_info(&binary).await?;
    if previous.store_schema != candidate.store_schema {
        return Err(Failure::new(
            2,
            format!(
                "update would change Store schema from {} to {}; reversible rollback requires the same schema",
                previous.store_schema, candidate.store_schema
            ),
        ));
    }
    let sha256 = binary_sha256(&binary).map_err(Failure::for_read)?;
    #[cfg(windows)]
    let desktop = desktop_binary(&binary)?;
    let _transition = lock_transition(&data_dir).map_err(Failure::for_read)?;
    check_same_daemon(&data_dir, &old)?;
    let startup_path = replacement_data_dir(&data_dir, &old).map_err(Failure::for_read)?;
    let before = agents(&data_dir).await?.into_values()
        .filter(|agent| !agent.state.is_terminal())
        .map(|agent| json!({"agent": agent.id, "generation": agent.generation, "state": agent.state}))
        .collect::<Vec<_>>();
    let prepare = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::PrepareRestart);
    let mut commit = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::Restart);
    commit.restart_id = Some(prepare.id.clone());
    let mut cancel = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::CancelRestart);
    cancel.restart_id = Some(prepare.id.clone());

    let committed = quiesce(&data_dir, &old, &prepare, &commit, wait_seconds).await;
    let commit_reply_uncertain = committed.is_err();
    if let Err(error) = committed {
        match reconcile_preparation(&data_dir, &old, &cancel).await {
            result @ (Preparation::Open | Preparation::Unconfirmed) => {
                let recovered = matches!(result, Preparation::Open);
                print_value(
                    &json!({
                        "state": if recovered { "update_cancelled" } else { "update_cancel_unconfirmed" },
                        "pid": old.pid, "error": error.message,
                        "admission": if recovered { Some("open") } else { None },
                        "agents_before": before,
                    }),
                    json_output,
                );
                return Ok(if recovered { error.exit_code } else { 4 });
            }
            Preparation::Exited => {
                return Ok(rollback(
                    &startup_path,
                    &previous_binary,
                    &previous_hash,
                    &old,
                    error.message,
                    before,
                    json_output,
                )
                .await);
            }
            Preparation::Restarting => {} // The old process confirmed commit despite the RPC error.
        }
    }

    if let Err(error) = wait_for_exit(&old, Duration::from_secs(120)).await {
        print_value(
            &json!({"state":"restart_exit_unknown","previous_pid":old.pid,
            "error":error.message,"agents_before":before}),
            json_output,
        );
        return Ok(error.exit_code);
    }
    let pid = match start_owned(&startup_path, &binary).await {
        Ok(pid) => pid,
        Err(error) => {
            return Ok(rollback(
                &startup_path,
                &previous_binary,
                &previous_hash,
                &old,
                error.message,
                before,
                json_output,
            )
            .await);
        }
    };

    #[cfg(windows)]
    let (desktop, desktop_failed) = start_companion(&data_dir, desktop);
    #[cfg(not(windows))]
    let (desktop, desktop_failed) = (None::<Value>, false);
    print_value(
        &json!({
            "state": if desktop_failed { "partial" } else { "ready" },
            "previous_pid": old.pid, "pid": pid,
            "commit_reply_uncertain": commit_reply_uncertain,
        "binary": eliot_core::display_path(&binary.to_string_lossy()), "version": candidate.version,
            "sha256": sha256, "desktop": desktop,
            "agents_before": before, "agents_after": observe_agents(&data_dir, &before).await,
            "continuation": "boot recovery applies the configured on_restart policy",
        }),
        json_output,
    );
    Ok(u8::from(desktop_failed))
}

#[derive(Clone, Copy)]
enum Preparation {
    Open,
    Restarting,
    Exited,
    Unconfirmed,
}

async fn reconcile_status(path: &Path, old: &DaemonInfo) -> Preparation {
    match daemon_alive(old) {
        Ok(false) => return Preparation::Exited,
        Ok(true) if check_same_daemon(path, old).is_ok() => {}
        _ => return Preparation::Unconfirmed,
    }
    match status(path)
        .await
        .as_ref()
        .map(|status| status.admission.as_str())
    {
        Ok("open") => Preparation::Open,
        Ok("restarting") => Preparation::Restarting,
        _ => Preparation::Unconfirmed,
    }
}

async fn reconcile_preparation(
    path: &Path,
    old: &DaemonInfo,
    cancel: &DaemonShutdownRequest,
) -> Preparation {
    // An absent reply cannot distinguish a late restart commit from an uncommitted
    // preparation. Follow the same epoch through cancellation or actual exit.
    let deadline = Instant::now() + Duration::from_secs(125);
    loop {
        if check_same_daemon(path, old).is_ok() && call_shutdown(path, cancel).await.is_ok() {
            return Preparation::Open;
        }
        match reconcile_status(path, old).await {
            Preparation::Unconfirmed if Instant::now() < deadline => {}
            state => return state,
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn quiesce(
    path: &Path,
    old: &DaemonInfo,
    prepare: &DaemonShutdownRequest,
    commit: &DaemonShutdownRequest,
    wait_seconds: u64,
) -> Result<(), Failure> {
    call_shutdown(path, prepare).await?;
    let deadline = Instant::now() + Duration::from_secs(wait_seconds);
    loop {
        check_same_daemon(path, old)?;
        match call_shutdown(path, commit).await {
            Ok(()) => return Ok(()),
            Err(error) if error.code.as_deref() == Some("restart_busy") => {
                if Instant::now() >= deadline {
                    return Err(Failure::new(
                        5,
                        "admitted work did not finish before the update deadline",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn call_shutdown(path: &Path, request: &DaemonShutdownRequest) -> Result<(), Failure> {
    timeout(Duration::from_secs(5), shutdown_daemon(path, request))
        .await
        .map_err(|_| Failure::timeout())??;
    Ok(())
}

async fn rollback(
    path: &Path,
    previous_binary: &Path,
    previous_hash: &str,
    old: &DaemonInfo,
    update_error: String,
    before: Vec<Value>,
    json_output: bool,
) -> u8 {
    let restored = async {
        // Do not race a surviving candidate or open a second writer.
        let offline = lock_offline(path).map_err(Failure::for_read)?;
        if binary_sha256(previous_binary).map_err(Failure::for_read)? != previous_hash {
            return Err(Failure::new(
                4,
                "the previous release binary changed on disk",
            ));
        }
        drop(offline);
        start_owned(path, previous_binary).await
    }
    .await;
    let (state, pid, rollback_error) = match restored {
        Ok(pid) => ("rolled_back", Some(pid), None),
        Err(error) => ("rollback_failed", None, Some(error.message)),
    };
    print_value(
        &json!({
            "state": state, "previous_pid": old.pid, "pid": pid, "update_error": update_error,
            "rollback_error": rollback_error, "binary": eliot_core::display_path(&previous_binary.to_string_lossy()),
            "sha256": previous_hash, "agents_before": before,
            "agents_after": if pid.is_some() { observe_agents(path, &before).await } else { Value::Null },
            "continuation": "boot recovery applies the configured on_restart policy",
        }),
        json_output,
    );
    if pid.is_some() { 1 } else { 4 }
}

async fn observe_agents(path: &Path, before: &[Value]) -> Value {
    match agents(path).await {
        Ok(current) => json!(before.iter().filter_map(|entry| {
            let observed = current.get(entry["agent"].as_str()?)?;
            Some(json!({"agent": observed.id, "generation": observed.generation,
                "state": observed.state, "last_turn": observed.last_turn_id, "last_outcome": observed.last_outcome}))
        }).collect::<Vec<_>>()),
        Err(error) => json!({"read_error": error.message}),
    }
}

fn release_binary(source: &Path) -> Result<PathBuf, Failure> {
    let source = if source.is_dir() {
        source.join(format!("swarm{}", std::env::consts::EXE_SUFFIX))
    } else {
        source.to_owned()
    };
    let path = source.canonicalize().map_err(|error| {
        Failure::new(
            2,
            format!("resolve release binary {}: {error}", source.display()),
        )
    })?;
    if !path.is_file() {
        return Err(Failure::new(
            2,
            format!("release binary is not a regular file: {}", path.display()),
        ));
    }
    Ok(path)
}

#[derive(serde::Deserialize)]
struct ReleaseInfo {
    version: String,
    store_schema: i64,
}

async fn release_info(binary: &Path) -> Result<ReleaseInfo, Failure> {
    let mut command = tokio::process::Command::new(binary);
    command
        .arg("--operator-release-info")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let output = timeout(Duration::from_secs(5), command.output())
        .await
        .map_err(|_| {
            Failure::new(
                2,
                "release binary did not report its update metadata within 5 seconds",
            )
        })?
        .map_err(|error| Failure::new(2, format!("read replacement binary version: {error}")))?;
    if !output.status.success() {
        return Err(Failure::new(
            2,
            format!(
                "release {} does not support reversible daemon updates",
                binary.display()
            ),
        ));
    }
    let info: ReleaseInfo = serde_json::from_slice(&output.stdout)
        .map_err(|_| Failure::new(2, "release metadata is invalid"))?;
    if info.store_schema <= 0
        || !info.version.starts_with("swarm ")
        || info.version.len() > 128
        || info.version.chars().any(char::is_control)
    {
        return Err(Failure::new(2, "release metadata is invalid"));
    }
    Ok(info)
}

#[cfg(windows)]
fn desktop_binary(binary: &Path) -> Result<Option<(PathBuf, String)>, Failure> {
    let desktop = binary.with_file_name("swarm-desktop.exe");
    if !desktop.is_file() {
        return Ok(None);
    }
    let hash = binary_sha256(&desktop).map_err(Failure::for_read)?;
    Ok(Some((desktop, hash)))
}

#[cfg(windows)]
fn start_companion(data_dir: &Path, desktop: Option<(PathBuf, String)>) -> (Option<Value>, bool) {
    let Some((binary, sha256)) = desktop else {
        return (
            Some(json!({"reason": "release contains no desktop companion"})),
            false,
        );
    };
    match eliot_daemon::operator::start_desktop(data_dir, &binary, sha256.clone()) {
        Ok(pid) => (Some(json!({"pid": pid, "sha256": sha256})), false),
        Err(error) => (Some(json!({"reason": error.to_string()})), true),
    }
}
