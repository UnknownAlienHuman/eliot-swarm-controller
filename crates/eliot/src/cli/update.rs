//! Replace a running daemon without replaying an Agent input.

use super::daemon::shutdown_daemon;
use super::operator::{
    agents, check_same_daemon, require_running, start_owned, start_update, status, wait_for_exit,
};
use super::support::{Failure, mutation_id, print_value_or_human};
use eliot_core::api::{DaemonShutdownRequest, ShutdownMode};
use eliot_daemon::operator::{
    DaemonInfo, binary_sha256, daemon_alive, daemon_info, discard_update_backup, lock_offline,
    lock_transition, replacement_data_dir, restore_update_backup, running_binary,
    update_backup_path,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout};

fn print_update_value(value: &Value, json_output: bool) {
    print_value_or_human(value, json_output, || {
        println!(
            "Daemon update: {}",
            value["state"].as_str().unwrap_or("outcome unavailable")
        );
        for (key, label) in [
            ("previous_pid", "Previous daemon PID"),
            ("pid", "Daemon PID"),
            ("admission", "Admission"),
            ("version", "Release"),
            ("binary", "Binary"),
            ("sha256", "Binary SHA-256"),
            ("backup_cleanup_error", "Backup cleanup error"),
            ("error", "Error"),
            ("update_error", "Update error"),
            ("rollback_error", "Rollback error"),
            ("message", "Note"),
            ("continuation", "Next"),
        ] {
            if let Some(field) = value.get(key) {
                if let Some(text) = field.as_str() {
                    println!("{label}: {text}");
                } else if let Some(number) = field.as_u64() {
                    println!("{label}: {number}");
                }
            }
        }
        if let Some(backup) = value.get("backup") {
            println!("Store backup retained: {}", backup.as_str().unwrap_or("no"));
        }
        for key in [
            "commit_reply_uncertain",
            "store_restored",
            "backup_retained",
        ] {
            if let Some(flag) = value.get(key).and_then(Value::as_bool) {
                println!("{}: {flag}", key.replace('_', " "));
            }
        }
        if let Some(agents) = value.get("agents_before").and_then(Value::as_array) {
            println!("Agents before restart: {}", agents.len());
            print_update_agents(agents);
        }
        if let Some(agents) = value.get("agents_after").and_then(Value::as_array) {
            println!("Agents observed after restart: {}", agents.len());
            print_update_agents(agents);
        } else if let Some(error) = value
            .get("agents_after")
            .and_then(|after| after.get("read_error"))
            .and_then(Value::as_str)
        {
            println!("Agent readback unavailable: {error}");
        }
        if let Some(desktop) = value.get("desktop") {
            if let Some(pid) = desktop.get("pid").and_then(Value::as_u64) {
                println!("Desktop companion PID: {pid}");
            }
            if let Some(hash) = desktop.get("sha256").and_then(Value::as_str) {
                println!("Desktop companion SHA-256: {hash}");
            }
            if let Some(reason) = desktop.get("reason").and_then(Value::as_str) {
                println!("Desktop companion: {reason}");
            }
        }
    });
}

fn print_update_agents(agents: &[Value]) {
    for agent in agents {
        let id = agent.get("agent").and_then(Value::as_str).unwrap_or("?");
        let generation = agent
            .get("generation")
            .map(Value::to_string)
            .unwrap_or_else(|| "?".to_owned());
        let state = agent
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        println!("  {id} · generation {generation} · state {state}");
    }
}

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
    if candidate.store_schema < previous.store_schema {
        return Err(Failure::new(
            2,
            format!(
                "Store downgrade refused: replacement schema {} is older than current schema {}",
                candidate.store_schema, previous.store_schema
            ),
        ));
    }
    if !previous.backup_update || !candidate.backup_update {
        return Err(Failure::new(
            2,
            "both releases must support updates with a Store backup",
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
    commit.backup = true;
    let backup = StoreBackup {
        id: &commit.id,
        path: update_backup_path(&startup_path, &commit.id).map_err(Failure::for_read)?,
    };
    let previous = PreviousRelease {
        binary: &previous_binary,
        sha256: &previous_hash,
        daemon: &old,
    };
    let mut cancel = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::CancelRestart);
    cancel.restart_id = Some(prepare.id.clone());

    let committed = quiesce(&data_dir, &old, &prepare, &commit, wait_seconds).await;
    let commit_reply_uncertain = committed.is_err();
    if let Err(error) = committed {
        match reconcile_preparation(&data_dir, &old, &cancel).await {
            result @ (Preparation::Open | Preparation::Unconfirmed) => {
                let recovered = matches!(result, Preparation::Open);
                let backup_path = backup.retained();
                let backup_cleanup_error = if recovered && backup_path.is_some() {
                    discard_update_backup(&startup_path, backup.id).err()
                } else {
                    None
                };
                print_update_value(
                    &json!({
                        "state": if recovered { "update_cancelled" } else { "update_cancel_unconfirmed" },
                        "pid": old.pid, "error": error.message,
                        "admission": if recovered { Some("open") } else { None },
                        "agents_before": before,
                        "backup": backup_path,
                        "backup_retained": backup.retained().is_some(),
                        "backup_cleanup_error": backup_cleanup_error.as_ref().map(ToString::to_string),
                    }),
                    json_output,
                );
                return Ok(if recovered && backup_cleanup_error.is_none() {
                    error.exit_code
                } else {
                    4
                });
            }
            Preparation::Exited => {
                return Ok(rollback(
                    &startup_path,
                    &previous,
                    &backup,
                    false,
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
        print_update_value(
            &json!({"state":"restart_exit_unknown","previous_pid":old.pid,
            "error":error.message,"agents_before":before,"backup":backup.retained()}),
            json_output,
        );
        return Ok(error.exit_code);
    }
    let pid = match start_update(&startup_path, &binary, backup.id).await {
        Ok(pid) => pid,
        Err(error) => {
            let admission = match daemon_info(&startup_path) {
                Ok(Some(info)) if info == old => None,
                Ok(Some(_)) => Some("failed_after_admission"),
                _ => Some("admission_unknown"),
            };
            if let Some(state) = admission {
                report_retained(state, &backup, error, json_output);
                return Ok(4);
            }
            return Ok(rollback(
                &startup_path,
                &previous,
                &backup,
                true,
                error.message,
                before,
                json_output,
            )
            .await);
        }
    };
    if let Err(error) = check_health(&startup_path, pid).await {
        report_retained("failed_after_admission", &backup, error, json_output);
        return Ok(4);
    }
    let backup_cleanup = discard_update_backup(&startup_path, backup.id).err();

    #[cfg(windows)]
    let (desktop, desktop_failed) = start_companion(&data_dir, desktop);
    #[cfg(not(windows))]
    let (desktop, desktop_failed) = (None::<Value>, false);
    print_update_value(
        &json!({
            "state": if desktop_failed || backup_cleanup.is_some() { "partial" } else { "ready" },
            "previous_pid": old.pid, "pid": pid,
            "commit_reply_uncertain": commit_reply_uncertain,
        "binary": eliot_core::display_path(&binary.to_string_lossy()), "version": candidate.version,
            "sha256": sha256, "desktop": desktop,
            "backup": backup.retained(), "backup_cleanup_error": backup_cleanup.as_ref().map(ToString::to_string),
            "agents_before": before, "agents_after": observe_agents(&data_dir, &before).await,
            "continuation": "boot recovery applies the configured on_restart policy",
        }),
        json_output,
    );
    Ok(u8::from(desktop_failed || backup_cleanup.is_some()))
}

struct PreviousRelease<'a> {
    binary: &'a Path,
    sha256: &'a str,
    daemon: &'a DaemonInfo,
}

struct StoreBackup<'a> {
    id: &'a str,
    path: PathBuf,
}

impl StoreBackup<'_> {
    fn retained(&self) -> Option<String> {
        self.path
            .exists()
            .then(|| eliot_core::display_path(&self.path.to_string_lossy()))
    }
}

fn report_retained(state: &str, backup: &StoreBackup<'_>, error: Failure, json_output: bool) {
    print_update_value(
        &json!({
            "state": state, "error": error.message, "backup": backup.retained(),
            "message": "The Store was preserved because replacement writes may exist",
        }),
        json_output,
    );
}

async fn check_health(path: &Path, pid: u32) -> Result<(), Failure> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (current, status) = require_running(path).await?;
        if current.pid != pid || status.admission != "open" || status.store_write_faulted {
            return Err(Failure::new(
                4,
                "replacement lost healthy admission during the health check",
            ));
        }
        if Instant::now() >= deadline {
            return Ok(());
        }
        sleep(Duration::from_millis(250)).await;
    }
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
    match status(path).await {
        Ok(status) if status.admission == "open" && !status.store_write_faulted => {
            Preparation::Open
        }
        Ok(status) if status.admission == "restarting" => Preparation::Restarting,
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
    let wait = Duration::from_secs(if request.backup { 155 } else { 5 });
    timeout(wait, shutdown_daemon(path, request))
        .await
        .map_err(|_| Failure::timeout())??;
    Ok(())
}

async fn rollback(
    path: &Path,
    previous: &PreviousRelease<'_>,
    backup: &StoreBackup<'_>,
    restore: bool,
    update_error: String,
    before: Vec<Value>,
    json_output: bool,
) -> u8 {
    let mut store_restored = false;
    let restored = async {
        // Do not race a surviving candidate or open a second writer.
        let offline = lock_offline(path).map_err(Failure::for_read)?;
        if binary_sha256(previous.binary).map_err(Failure::for_read)? != previous.sha256 {
            return Err(Failure::new(
                4,
                "the previous release binary changed on disk",
            ));
        }
        if restore {
            restore_update_backup(path, backup.id, previous.daemon, &offline)
                .map_err(Failure::for_read)?;
            store_restored = true;
        }
        drop(offline);
        start_owned(path, previous.binary).await
    }
    .await;
    let (state, pid, rollback_error) = match restored {
        Ok(pid) => ("rolled_back", Some(pid), None),
        Err(error) => ("rollback_failed", None, Some(error.message)),
    };
    let cleanup = if pid.is_some() && !restore {
        discard_update_backup(path, backup.id)
            .err()
            .map(|error| error.to_string())
    } else {
        None
    };
    print_update_value(
        &json!({
            "state": state, "previous_pid": previous.daemon.pid, "pid": pid, "update_error": update_error,
            "rollback_error": rollback_error, "binary": eliot_core::display_path(&previous.binary.to_string_lossy()),
            "sha256": previous.sha256, "agents_before": before,
            "store_restored": store_restored, "backup": backup.retained(), "backup_cleanup_error": cleanup,
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
    #[serde(default)]
    backup_update: bool,
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
