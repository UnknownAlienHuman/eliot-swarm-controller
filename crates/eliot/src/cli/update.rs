//! Replace a running daemon with an operator-supplied release.

use super::operator::{StoppedAgent, require_running, start_owned, stop_and_drain};
use super::support::{Failure, print_value, submit_command, submit_request, wait_for_command};
use eliot_core::AgentState;
use eliot_core::CommandState;
use eliot_daemon::operator::{binary_sha256, lock_transition, replacement_data_dir};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::time::{Instant, timeout, timeout_at};

pub(crate) async fn run(
    source: PathBuf,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let (old, _) = require_running(&data_dir).await?;
    let binary = release_binary(&source)?;
    let version = release_version(&binary).await?;
    let sha256 = binary_sha256(&binary).map_err(Failure::for_read)?;
    #[cfg(windows)]
    let desktop = desktop_binary(&binary)?;
    let _transition = lock_transition(&data_dir).map_err(Failure::for_read)?;
    let (current, _) = require_running(&data_dir).await?;
    if old != current {
        return Err(Failure::new(
            1,
            "the daemon changed during release preflight",
        ));
    }
    let startup_path = replacement_data_dir(&data_dir, &old).map_err(Failure::for_read)?;
    let stopped = stop_and_drain(&data_dir).await?;
    let pid = start_owned(&startup_path, &binary)
        .await
        .map_err(|mut error| {
            error.message = format!(
                "old daemon PID {} drained; replacement startup did not complete: {}",
                old.pid, error.message
            );
            error
        })?;
    let (_, status) = require_running(&data_dir).await?;
    if status.admission != "open" || status.store_write_faulted {
        return Err(Failure::new(
            4,
            "the replacement is running with Agent admission unavailable",
        ));
    }

    #[cfg(windows)]
    let (desktop, desktop_failed) = start_companion(&data_dir, desktop);
    #[cfg(not(windows))]
    let (desktop, desktop_failed) = (None::<Value>, false);

    let mut resumed = Vec::new();
    let mut skipped = Vec::new();
    let mut failed = desktop_failed;
    for StoppedAgent {
        id,
        generation,
        skip_reason,
    } in stopped
    {
        if let Some(reason) = skip_reason {
            skipped.push(json!({"agent": id, "generation": generation, "reason": reason}));
            continue;
        }
        match resume(&data_dir, &id, generation).await {
            Ok(generation) => resumed.push(json!({"agent": id, "generation": generation})),
            Err(error) => {
                failed = true;
                skipped
                    .push(json!({"agent": id, "generation": generation, "reason": error.message}));
            }
        }
    }

    let binary_path = eliot_core::display_path(&binary.to_string_lossy());
    let result = json!({
        "state": if failed { "partial" } else { "ready" },
        "pid": pid,
        "previous_pid": old.pid,
        "binary": binary_path,
        "version": version,
        "sha256": sha256,
        "desktop": desktop,
        "resumed": resumed,
        "skipped": skipped,
    });
    if json_output {
        print_value(&result, true);
    } else {
        println!("{version} ready as PID {pid} (previous PID {})", old.pid);
        println!("{binary_path} SHA-256 {sha256}");
        if let Some(desktop) = desktop {
            if let Some(pid) = desktop["pid"].as_u64() {
                println!("Desktop PID {pid}");
            } else if let Some(reason) = desktop["reason"].as_str() {
                println!("Desktop: {reason}");
            }
        }
        for agent in resumed {
            println!(
                "Resumed {} generation {}",
                agent["agent"].as_str().unwrap_or_default(),
                agent["generation"]
            );
        }
        for agent in skipped {
            println!(
                "Skipped {}: {}",
                agent["agent"].as_str().unwrap_or_default(),
                agent["reason"].as_str().unwrap_or_default()
            );
        }
    }
    Ok(u8::from(failed))
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

async fn release_version(binary: &Path) -> Result<String, Failure> {
    let mut command = tokio::process::Command::new(binary);
    command
        .arg("--version")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let output = timeout(Duration::from_secs(5), command.output())
        .await
        .map_err(|_| {
            Failure::new(
                2,
                "replacement binary did not report its version within 5 seconds",
            )
        })?
        .map_err(|error| Failure::new(2, format!("read replacement binary version: {error}")))?;
    let version = String::from_utf8_lossy(&output.stdout);
    let version = version.trim();
    if !output.status.success()
        || !version.starts_with("swarm ")
        || version.len() > 128
        || version.chars().any(char::is_control)
    {
        return Err(Failure::new(
            2,
            "replacement binary did not identify itself as swarm",
        ));
    }
    Ok(version.to_owned())
}

async fn resume(data_dir: &Path, agent: &str, generation: i64) -> Result<i64, Failure> {
    let current = super::operator::read_agent(data_dir, agent).await?.agent;
    if current.generation != generation || current.state != AgentState::Stopped {
        return Err(Failure::new(
            1,
            format!(
                "Agent changed after restart (generation {}, state {:?})",
                current.generation, current.state
            ),
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut request = submit_request(None, Some(agent.to_owned()), "resume", json!({}))?;
    request.expected_generation = Some(generation);
    let submitted = timeout_at(deadline, submit_command(data_dir, &request))
        .await
        .map_err(|_| Failure::timeout())??;
    if submitted.state == CommandState::Unknown {
        return Err(Failure::new(
            4,
            format!("resume {} outcome is unknown", request.id),
        ));
    }
    let completed = wait_for_command(data_dir, &request.id, deadline).await?;
    completed
        .generation
        .filter(|new| *new > generation)
        .ok_or_else(|| {
            Failure::new(
                4,
                format!("resume {} did not confirm a new generation", request.id),
            )
        })
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
