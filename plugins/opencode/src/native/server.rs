//! Owned per-Agent `opencode serve` lifecycle.
//!
//! One server per Agent generation, on loopback, under the existing process
//! custody: the plugin process runs inside the generation's Windows Job, so
//! the server child (spawned directly, never through a `.cmd` wrapper) and
//! anything it spawns die with the generation. The server never sees the
//! owner's global config or data (verified on the installed v2.0.32
//! server). Authentication is a generated per-generation `OPENCODE_PASSWORD`;
//! a preset password also suppresses the `server password` stdout line.
//! The child inherits the daemon-allowlisted plugin environment plus the
//! shared target environment, exactly like the Codex/Muse stdio servers
//! (`native_stdio.rs`): there is no third allowlist here.
//!
//! Store layout: the generated `opencode.jsonc` is per generation (under the
//! plugin instance directory), but the session store (`XDG_DATA_HOME`) is
//! per Agent — `opencode-{agent_id}` next to the instance directories — so
//! a resumed generation serves the exact retained native session instead of
//! an empty store. Generations of one Agent never run concurrently (the old
//! one is terminal before resume), so one server owns the store at a time;
//! a stale server from a lost generation can only idle (its plugin is dead)
//! and the resume preflight refuses a non-idle retained session.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{StderrCapture, session::paths_equal, spawn_stderr};
use tokio::{
    net::TcpListener,
    process::{Child, Command},
    time::{Instant, sleep, timeout_at},
};

use crate::native::client::OpenCodeHttp;

const READINESS_POLL: Duration = Duration::from_millis(250);

/// The generated per-Agent config: the frozen profile model plus an
/// explicitly empty MCP set. No permissions, providers, plugins or
/// credentials: the Agent runs the server defaults, and provider keys can
/// only arrive through the target environment the host passes on top of the
/// daemon-allowlisted plugin environment the child inherits.
fn agent_config_json(model: &str) -> String {
    serde_json::json!({
        "$schema": "https://opencode.ai/config.json",
        "model": model,
        "mcp": {},
    })
    .to_string()
}

pub struct ServerLaunch {
    pub program: OsString,
    pub workspace: PathBuf,
    pub instance_dir: PathBuf,
    pub store_root: PathBuf,
    pub agent_id: String,
    pub environment: Vec<(OsString, OsString)>,
    pub model: String,
}

/// A running owned server: its child, HTTP client, and addresses.
pub struct OwnedServer {
    child: Child,
    stdout: Option<StderrCapture>,
    stderr: Option<StderrCapture>,
    http: OpenCodeHttp,
    config_dir: PathBuf,
    data_dir: PathBuf,
    stop_trace_path: Option<PathBuf>,
}

impl OwnedServer {
    /// Write the per-generation config, reserve a loopback port, spawn the
    /// server on the per-Agent store, and wait for readiness. Any failure
    /// leaves no running child behind. The child inherits the
    /// daemon-allowlisted plugin environment plus the shared target
    /// environment the host built, the generated password, and the XDG
    /// roots — exactly like the Codex/Muse stdio servers, with no
    /// `env_clear` and no third allowlist.
    pub async fn spawn(launch: ServerLaunch, secrets: &[String]) -> Result<Self> {
        let stop_trace_path = crate::host::stop_trace_path_from_environment(&launch.environment);
        let config_dir = launch.instance_dir.join("config").join("opencode");
        let data_dir = stable_store_dir(&launch.store_root, &launch.agent_id)?;
        std::fs::create_dir_all(&config_dir).map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode per-Agent config directory could not be created: {error}"),
            )
        })?;
        std::fs::create_dir_all(&data_dir).map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode per-Agent data directory could not be created: {error}"),
            )
        })?;
        let config_path = config_dir.join("opencode.jsonc");
        std::fs::write(&config_path, agent_config_json(&launch.model)).map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode per-Agent config could not be written: {error}"),
            )
        })?;

        // Reserve a loopback port, then hand it to the server. A transient
        // bind race resolves as a spawn/readiness failure with no process
        // kept, never as a connection to a foreign listener: readiness
        // requires our password and our config document path.
        let port = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| {
                Error::new(
                    "native_spawn",
                    format!("OpenCode loopback port could not be reserved: {error}"),
                )
            })?
            .local_addr()
            .map_err(|error| {
                Error::new(
                    "native_spawn",
                    format!("OpenCode loopback port could not be read: {error}"),
                )
            })?
            .port();
        let password = format!("eliot-{}-{}", ulid::Ulid::new(), ulid::Ulid::new());
        // The capture policy covers the host secrets plus the generated
        // password before any pipe is read: a compromised server must not
        // leak its own credential through the retained tails.
        let mut capture = secrets.to_vec();
        capture.push(password.clone());
        let capture = SecretPolicy::new(capture);

        let mut command = Command::new(&launch.program);
        command
            .arg("serve")
            .arg("--hostname")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .current_dir(&launch.workspace)
            .envs(launch.environment)
            .env("OPENCODE_PASSWORD", &password)
            .env("XDG_CONFIG_HOME", launch.instance_dir.join("config"))
            .env("XDG_DATA_HOME", &data_dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            Error::new(
                "native_spawn",
                capture.redact_text(&format!(
                    "could not start the configured OpenCode serve: {error}"
                )),
            )
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::new("native_spawn", "OpenCode serve stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::new("native_spawn", "OpenCode serve stderr was not piped"))?;
        // Both pipes drain continuously with the shared redacting tail;
        // stdout carries the `server listening` line and MCP spawn notes.
        let capture = std::sync::Arc::new(capture);
        let stdout = spawn_stderr(stdout, capture.clone());
        let stderr = spawn_stderr(stderr, capture);

        let http = OpenCodeHttp::new(port, password)?;
        let mut server = Self {
            child,
            stdout: Some(stdout),
            stderr: Some(stderr),
            http,
            config_dir,
            data_dir,
            stop_trace_path,
        };
        if let Err(error) = server.await_ready(Instant::now() + READINESS_TIMEOUT).await {
            server
                .terminate(true, Instant::now() + READINESS_TIMEOUT)
                .await?;
            return Err(error);
        }
        Ok(server)
    }

    /// Poll `GET /api/config` until it proves the live server is ours: the
    /// password authenticates it and the config documents name our
    /// per-Agent config file. An early child exit fails fast with the
    /// retained stderr tail attached.
    async fn await_ready(&mut self, deadline: Instant) -> Result<()> {
        loop {
            if let Some(status) = self.child.try_wait().map_err(|error| {
                Error::new(
                    "native_spawn",
                    format!("OpenCode serve exit readback failed: {error}"),
                )
            })? {
                let tail = self
                    .stderr
                    .as_ref()
                    .map(|stderr| stderr.snapshot().text)
                    .unwrap_or_default();
                return Err(Error::new(
                    "native_spawn",
                    format!(
                        "OpenCode serve exited during startup ({status}){}",
                        if tail.trim().is_empty() {
                            String::new()
                        } else {
                            format!(": {}", tail.lines().last().unwrap_or_default())
                        }
                    ),
                ));
            }
            match self.http.get_config(deadline).await {
                Ok(documents) if serves_our_config(&self.config_dir, &documents) => return Ok(()),
                Ok(_) => {}
                Err(_) => {}
            }
            if Instant::now() + READINESS_POLL >= deadline {
                return Err(Error::new(
                    "native_spawn",
                    "OpenCode serve did not become ready before its deadline",
                ));
            }
            sleep(READINESS_POLL).await;
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn http(&self) -> &OpenCodeHttp {
        &self.http
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn stderr_snapshot(&self) -> eliot_core::StderrSnapshot {
        self.stderr
            .as_ref()
            .map(|capture| capture.snapshot())
            .unwrap_or(eliot_core::StderrSnapshot {
                text: String::new(),
                truncated: false,
                eof: true,
                read_error: false,
            })
    }

    /// Attach the harness stderr tail to an error as its second diagnostic
    /// source. Stdout stays out of the error: it carries only the
    /// `server listening` line and spawn notes.
    pub fn diagnostic_error(&self, error: Error) -> Error {
        if self.stderr.is_none() {
            return error;
        }
        error.with_stderr(eliot_core::StderrSource::Harness, self.stderr_snapshot())
    }

    /// End the owned server after native session-control work. `serve` exposes
    /// no shutdown RPC, so both paths end the process; the generation Job ends
    /// any grandchildren. Ordinary Stop retains the native session.
    pub async fn terminate(&mut self, force: bool, deadline: Instant) -> Result<()> {
        let exit = self.terminate_leader(force, deadline).await;
        if exit.is_ok() {
            self.finish_diagnostics(deadline).await;
        }
        exit
    }

    /// Prove the exact server leader exited without waiting for its diagnostic
    /// pipe readers. Stop commits the native EndAck before finishing those pipes.
    pub(crate) async fn terminate_leader(&mut self, force: bool, deadline: Instant) -> Result<()> {
        let leader_check_started = Instant::now();
        let leader_check = self.child.try_wait();
        crate::host::record_stop_stage(
            self.stop_trace_path.as_deref(),
            "leader_try_wait",
            match &leader_check {
                Ok(Some(status)) if status.success() => "already_exited_success",
                Ok(Some(_)) => "already_exited_nonzero",
                Ok(None) => "running",
                Err(_) => "read_error",
            },
            leader_check_started.elapsed(),
        );
        let leader_status = leader_check.map_err(|error| {
            Error::new(
                "native_process",
                format!("OpenCode serve exit readback failed: {error}"),
            )
        })?;
        if let Some(_status) = leader_status {
            return Ok(());
        }
        if !force {
            // No graceful RPC exists; a short grace lets an in-flight native
            // close settle before the process ends.
            let grace = (Instant::now() + Duration::from_secs(2)).min(deadline);
            let grace_started = Instant::now();
            let grace_result = timeout_at(grace, self.child.wait()).await;
            crate::host::record_stop_stage(
                self.stop_trace_path.as_deref(),
                "leader_grace_wait",
                match &grace_result {
                    Ok(Ok(status)) if status.success() => "exited_success",
                    Ok(Ok(_)) => "exited_nonzero",
                    Ok(Err(_)) => "wait_error",
                    Err(_) => "deadline",
                },
                grace_started.elapsed(),
            );
            if matches!(&grace_result, Ok(Ok(_))) {
                return Ok(());
            }
        }
        let kill_started = Instant::now();
        let kill_result = timeout_at(deadline, self.child.kill()).await;
        crate::host::record_stop_stage(
            self.stop_trace_path.as_deref(),
            "leader_kill",
            match &kill_result {
                Ok(Ok(())) => "ok",
                Ok(Err(_)) => "io_error",
                Err(_) => "deadline",
            },
            kill_started.elapsed(),
        );
        if let Err(error) = kill_result {
            // Kill outcome is ambiguous without the wait proof below.
            let proof_started = Instant::now();
            let proof = timeout_at(deadline, self.child.wait()).await;
            crate::host::record_stop_stage(
                self.stop_trace_path.as_deref(),
                "leader_wait_proof",
                match &proof {
                    Ok(Ok(status)) if status.success() => "exited_success",
                    Ok(Ok(_)) => "exited_nonzero",
                    Ok(Err(_)) => "wait_error",
                    Err(_) => "deadline",
                },
                proof_started.elapsed(),
            );
            if !matches!(&proof, Ok(Ok(_))) {
                return Err(Error::new(
                    "unknown",
                    "OpenCode serve termination outcome is unknown",
                ));
            }
            let _ = error;
            return Ok(());
        }
        let proof_started = Instant::now();
        let proof = timeout_at(deadline, self.child.wait()).await;
        crate::host::record_stop_stage(
            self.stop_trace_path.as_deref(),
            "leader_wait_proof",
            match &proof {
                Ok(Ok(status)) if status.success() => "exited_success",
                Ok(Ok(_)) => "exited_nonzero",
                Ok(Err(_)) => "wait_error",
                Err(_) => "deadline",
            },
            proof_started.elapsed(),
        );
        if !matches!(&proof, Ok(Ok(_))) {
            return Err(Error::new(
                "unknown",
                "OpenCode serve exit proof is unknown",
            ));
        }
        Ok(())
    }

    pub(crate) async fn finish_diagnostics(&mut self, deadline: Instant) {
        let drain_started = Instant::now();
        if let Some(stdout) = self.stdout.take() {
            let stage_started = Instant::now();
            let snapshot = stdout.finish(deadline).await;
            crate::host::record_stop_stage(
                self.stop_trace_path.as_deref(),
                "stdout_drain",
                capture_drain_class(snapshot.eof, snapshot.read_error, deadline),
                stage_started.elapsed(),
            );
        }
        if let Some(stderr) = self.stderr.take() {
            let stage_started = Instant::now();
            let snapshot = stderr.finish(deadline).await;
            crate::host::record_stop_stage(
                self.stop_trace_path.as_deref(),
                "stderr_drain",
                capture_drain_class(snapshot.eof, snapshot.read_error, deadline),
                stage_started.elapsed(),
            );
        }
        crate::host::record_stop_stage(
            self.stop_trace_path.as_deref(),
            "pipe_drain_total",
            "returned",
            drain_started.elapsed(),
        );
    }
}

fn capture_drain_class(eof: bool, read_error: bool, deadline: Instant) -> &'static str {
    if Instant::now() >= deadline && !eof {
        "deadline"
    } else if read_error {
        "read_error"
    } else if eof {
        "eof"
    } else {
        "incomplete"
    }
}

const READINESS_TIMEOUT: Duration = Duration::from_secs(30);

/// The per-Agent session store: `opencode-{agent_id}/data` next to the
/// host-owned `store_root` (the old `instance_dir` parent, `{data_root}/plugins`).
/// The per-generation `instance_dir` does not affect this path. The agent id
/// allowlist keeps the derived segment a single safe path component.
fn stable_store_dir(store_root: &Path, agent_id: &str) -> Result<PathBuf> {
    if !store_root.is_absolute() || store_root.as_os_str().is_empty() {
        return Err(Error::new(
            "invalid",
            "OpenCode store root must be an absolute path",
        ));
    }
    if agent_id.is_empty()
        || agent_id.len() > 64
        || !agent_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(Error::new(
            "invalid",
            "OpenCode Agent id cannot name a stable session store",
        ));
    }
    Ok(store_root.join(format!("opencode-{agent_id}")).join("data"))
}

/// Our config document path must appear among the effective documents:
/// this proves the server honors the per-Agent `XDG_CONFIG_HOME` and is
/// not serving the owner's global config.
fn serves_our_config(config_dir: &Path, documents: &[crate::native::types::ConfigEntry]) -> bool {
    let wanted = config_dir.join("opencode.jsonc");
    documents.iter().any(|entry| {
        entry.entry_type == "document"
            && entry
                .path
                .as_deref()
                .is_some_and(|path| paths_equal(Path::new(path), &wanted))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_config_pins_model_and_empty_mcp() {
        let parsed: serde_json::Value =
            serde_json::from_str(&agent_config_json("opencode/big-pickle")).expect("valid JSON");
        assert_eq!(parsed["model"], "opencode/big-pickle");
        assert_eq!(parsed["mcp"], serde_json::json!({}));
        assert_eq!(
            parsed.as_object().expect("object").len(),
            3,
            "no other keys: {parsed}"
        );
    }

    #[test]
    fn stable_store_is_per_agent_beside_the_instance_dirs() {
        // Platform-absolute root without I/O: `is_absolute` on Windows
        // rejects root-relative `/eliot/plugins`, so build from temp_dir.
        let store_root = std::env::temp_dir().join(format!("eliot-plugins-{}", std::process::id()));
        let store = stable_store_dir(&store_root, "a12").expect("stable store");
        assert_eq!(store, store_root.join("opencode-a12").join("data"));
        // Another Agent gets another store.
        assert_ne!(
            stable_store_dir(&store_root, "a13").expect("stable store"),
            store
        );
        // Uppercase ULID agent ids (the plugin-test shape) are safe too.
        assert!(
            stable_store_dir(&store_root, "01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok(),
            "ULID agent id"
        );
        for bad in ["", "a/12", "a\\12", "..", "a 12", &"a".repeat(65)] {
            assert!(
                stable_store_dir(&store_root, bad).is_err(),
                "unsafe agent id {bad:?}"
            );
        }
        // Malformed (relative/empty) store roots are rejected before I/O.
        assert!(stable_store_dir(Path::new("plugins"), "a12").is_err());
        assert!(stable_store_dir(Path::new(""), "a12").is_err());
    }

    #[test]
    fn readiness_requires_our_config_document() {
        // Platform separators on both sides: production always compares
        // same-OS paths.
        let config_dir = std::env::temp_dir().join("eliot-opencode-config-test");
        let document = config_dir.join("opencode.jsonc");
        let ours =
            serde_json::from_value::<Vec<crate::native::types::ConfigEntry>>(serde_json::json!([
                {"type": "document", "path": document.to_string_lossy()},
                {"type": "directory", "path": config_dir.to_string_lossy()},
            ]))
            .expect("entries");
        assert!(serves_our_config(&config_dir, &ours));
        let elsewhere = std::env::temp_dir().join("eliot-opencode-owner.jsonc");
        let foreign =
            serde_json::from_value::<Vec<crate::native::types::ConfigEntry>>(serde_json::json!([
                {"type": "document", "path": elsewhere.to_string_lossy()},
            ]))
            .expect("entries");
        assert!(!serves_our_config(&config_dir, &foreign));
    }
}
