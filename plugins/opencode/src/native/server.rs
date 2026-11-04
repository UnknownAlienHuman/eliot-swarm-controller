//! Owned per-Agent `opencode serve` lifecycle.
//!
//! One server per Agent generation, on loopback, under the existing process
//! custody: the plugin process runs inside the generation's Windows Job, so
//! the server child (spawned directly, never through a `.cmd` wrapper) and
//! anything it spawns die with the generation. The server never sees the
//! owner's global config or data: `XDG_CONFIG_HOME` and `XDG_DATA_HOME`
//! point into the plugin instance directory (verified on the installed
//! v2.0.32 server), which holds a generated per-Agent `opencode.jsonc`.
//! Authentication is a generated per-generation `OPENCODE_PASSWORD`; a
//! preset password also suppresses the `server password` stdout line.
//! The child inherits the daemon-allowlisted plugin environment plus the
//! shared target environment, exactly like the Codex/Muse stdio servers
//! (`native_stdio.rs`): there is no third allowlist here.

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
}

impl OwnedServer {
    /// Write the per-Agent config, reserve a loopback port, spawn the
    /// server, and wait for readiness. Any failure leaves no running
    /// child behind. The child inherits the daemon-allowlisted plugin
    /// environment plus the shared target environment the host built, the
    /// generated password, and the per-Agent XDG roots — exactly like the
    /// Codex/Muse stdio servers, with no `env_clear` and no third
    /// allowlist.
    pub async fn spawn(launch: ServerLaunch, secrets: &[String]) -> Result<Self> {
        let config_dir = launch.instance_dir.join("config").join("opencode");
        let data_dir = launch.instance_dir.join("data");
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

    /// End the owned server. `serve` exposes no shutdown RPC, so both paths
    /// end the process; the generation Job ends any grandchildren. The
    /// native session is closed separately with `DELETE /api/session/{id}`
    /// before this runs.
    pub async fn terminate(&mut self, force: bool, deadline: Instant) -> Result<()> {
        if let Some(_status) = self.child.try_wait().map_err(|error| {
            Error::new(
                "native_process",
                format!("OpenCode serve exit readback failed: {error}"),
            )
        })? {
            self.drain_pipes(deadline).await;
            return Ok(());
        }
        if !force {
            // No graceful RPC exists; a short grace lets an in-flight native
            // close settle before the process ends.
            let grace = (Instant::now() + Duration::from_secs(2)).min(deadline);
            if timeout_at(grace, self.child.wait()).await.is_ok() {
                self.drain_pipes(deadline).await;
                return Ok(());
            }
        }
        timeout_at(deadline, self.child.kill())
            .await
            .map_err(|_| Error::new("unknown", "OpenCode serve termination outcome is unknown"))?
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!("OpenCode serve termination failed: {error}"),
                )
            })?;
        let _ = timeout_at(deadline, self.child.wait()).await;
        self.drain_pipes(deadline).await;
        Ok(())
    }

    async fn drain_pipes(&mut self, deadline: Instant) {
        if let Some(stdout) = self.stdout.take() {
            let _ = stdout.finish(deadline).await;
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.finish(deadline).await;
        }
    }
}

const READINESS_TIMEOUT: Duration = Duration::from_secs(30);

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
