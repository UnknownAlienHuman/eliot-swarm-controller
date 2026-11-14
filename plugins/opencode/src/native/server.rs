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

use crate::native::{
    AuthSetup, AuthTransfer,
    client::{OpenCodeHttp, server_native_directory},
    ensure_no_credential_for_provider, transfer_selected_integration,
};

const READINESS_POLL: Duration = Duration::from_millis(250);

/// Embedded vendor-native context-hook producer shim. Written verbatim into
/// the isolated per-Agent config root's auto-discovered `opencode/plugins/`
/// directory before `serve` starts, so the existing per-Agent server loads
/// it with no `plugins` config entry (the loader skips configured
/// single-file paths). The shim opens no listener and calls no Eliot
/// endpoint: Rust reaches it over the existing authenticated HTTP transport
/// as RPC `eliot-context`. Original code against the installed OpenCode
/// 2.0.32 `@opencode/plugin` Promise API; no vendor source copied and no
/// dependency installed.
const ELIOT_CONTEXT_PLUGIN: &str = include_str!("eliot-context.ts");
const ELIOT_CONTEXT_PLUGIN_FILE: &str = "eliot-context.ts";

/// Frozen adopted-backend provider inputs (nonsecret): the dedicated
/// provider id, the exact API model id, the explicit base URL, the optional
/// key reference name (never its value), the explicit context/output
/// limits, the backend preset kind, and the expected native package.
#[derive(Debug, Clone)]
pub struct LocalProvider {
    pub provider_id: String,
    pub api_model_id: String,
    pub base_url: String,
    pub api_key_env: Option<String>,
    pub context_limit: i64,
    pub output_limit: i64,
    pub backend: String,
    pub package: String,
}

/// The generated per-Agent config: the frozen profile model when one was
/// requested, otherwise no `model` key so the configured native default
/// applies. An explicitly empty MCP set. On the cloud path there are no
/// providers or credentials in this document: the Agent runs the server
/// defaults, and the selected native credential arrives through the pre-serve
/// auth transfer into the isolated store, never through this document. On the
/// adopted local path the frozen provider block below renders the installed
/// native schema (`providers`, `package`, `settings`, model capabilities
/// and `limit.context/output`) with `{env:NAME}` references only, so key
/// values stay runtime-only; no-auth mode carries no `apiKey` key at all,
/// never an empty string. A local-backend profile always carries an explicit
/// model (the eliot-local segment): absent model with a local provider is a
/// profile error raised by the caller, never rendered here.
fn agent_config_json(model: Option<&str>, local: Option<&LocalProvider>) -> String {
    let mut document = match local {
        None => serde_json::json!({
            "$schema": "https://opencode.ai/config.json",
            "mcp": {},
        }),
        Some(provider) => {
            let mut settings = serde_json::Map::new();
            settings.insert(
                "baseURL".to_owned(),
                serde_json::Value::String(provider.base_url.clone()),
            );
            if let Some(name) = &provider.api_key_env {
                settings.insert(
                    "apiKey".to_owned(),
                    serde_json::Value::String(format!("{{env:{name}}}")),
                );
            }
            let mut models = serde_json::Map::new();
            models.insert(
                provider.api_model_id.clone(),
                serde_json::json!({
                    "name": provider.api_model_id,
                    "tools": true,
                    "limit": {
                        "context": provider.context_limit,
                        "output": provider.output_limit,
                    },
                }),
            );
            serde_json::json!({
                "$schema": "https://opencode.ai/config.json",
                "providers": {
                    &provider.provider_id: {
                        "package": provider.package,
                        "settings": settings,
                        "models": models,
                    },
                },
                "mcp": {},
            })
        }
    };
    if let Some(model) = model {
        document["model"] = serde_json::Value::String(model.to_owned());
    }
    document.to_string()
}

pub struct ServerLaunch {
    pub program: OsString,
    pub workspace: PathBuf,
    pub instance_dir: PathBuf,
    pub store_root: PathBuf,
    pub agent_id: String,
    pub environment: Vec<(OsString, OsString)>,
    pub model: Option<String>,
    pub auth: Option<AuthSetup>,
    pub local: Option<LocalProvider>,
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
    /// Await the owned server handle, never infer death from the SSE source.
    pub async fn wait_for_process_exit(&mut self) {
        if self.child.wait().await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// Write the per-generation config, prepare the selected native
    /// credential in the isolated store, reserve a loopback port, spawn the
    /// server on the per-Agent store, and wait for readiness. Any failure
    /// leaves no running child behind. The child inherits the
    /// daemon-allowlisted plugin environment plus the shared target
    /// environment the host built, the generated password, and the XDG
    /// roots — exactly like the Codex/Muse stdio servers, with no
    /// `env_clear` and no third allowlist.
    pub async fn spawn(
        launch: ServerLaunch,
        secrets: &[String],
        auth_deadline: Instant,
    ) -> Result<Self> {
        // OpenCode keeps plugin and session contexts per directory. Use the
        // same server-native spelling for cwd, session location, and HTTP
        // directory headers so a Windows verbatim alias cannot split routing.
        let native_directory = server_native_directory(&launch.workspace).map_err(|_| {
            Error::new(
                "native_spawn",
                "OpenCode workspace directory could not be canonicalized",
            )
        })?;
        let config_root = launch.instance_dir.join("config");
        let config_dir = config_root.join("opencode");
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
        if launch.local.is_some() && launch.model.is_none() {
            return Err(Error::new(
                "profile_error",
                "OpenCode local backend requires an explicit model with the dedicated eliot-local provider segment",
            ));
        }
        std::fs::write(
            &config_path,
            agent_config_json(launch.model.as_deref(), launch.local.as_ref()),
        )
        .map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode per-Agent config could not be written: {error}"),
            )
        })?;
        // Vendor-native context-hook producer: one auto-discovered plugin
        // file under the isolated root, written before `serve` starts. This
        // adds nothing to `opencode.jsonc` (model/mcp/auth behavior
        // unchanged) and touches no global user file: only this directory
        // gains one file, rewritten verbatim on every start.
        let plugins_dir = config_dir.join("plugins");
        std::fs::create_dir_all(&plugins_dir).map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode per-Agent plugins directory could not be created: {error}"),
            )
        })?;
        std::fs::write(
            plugins_dir.join(ELIOT_CONTEXT_PLUGIN_FILE),
            ELIOT_CONTEXT_PLUGIN,
        )
        .map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode per-Agent plugin could not be written: {error}"),
            )
        })?;
        // Credential preparation before `serve` starts, under the exact XDG
        // roots the server will use. Cloud path: the isolated store sees no
        // canonical credential, so the selected integration moves in opaquely
        // (a refused or ambiguous transfer fails the start with no server
        // child; resume reuses a correctly present credential). Local path:
        // no canonical credential is ever imported; instead the required key
        // reference must resolve to a non-empty runtime value (a missing or
        // empty key refuses before startup, never silently becoming no-auth
        // mode) and the isolated store must hold no credential for the
        // dedicated local provider ID.
        if let Some(local) = launch.local.as_ref() {
            if let Some(name) = local.api_key_env.as_deref() {
                let present = launch.environment.iter().any(|(key, value)| {
                    let matches = if cfg!(windows) {
                        key.to_string_lossy().eq_ignore_ascii_case(name)
                    } else {
                        key.to_string_lossy() == *name
                    };
                    matches && !value.to_string_lossy().trim().is_empty()
                });
                if !present {
                    return Err(Error::new(
                        "profile_error",
                        "OpenCode local backend requires a non-empty API key value for its configured reference; refusing to start without credentials",
                    ));
                }
            }
            ensure_no_credential_for_provider(
                &launch.program,
                &launch.workspace,
                &launch.environment,
                &config_root,
                &data_dir,
                &local.provider_id,
                auth_deadline,
            )
            .await?;
        } else {
            let auth = launch.auth.as_ref().ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "OpenCode profile requires an explicit auth_integration selector",
                )
            })?;
            transfer_selected_integration(
                &AuthTransfer {
                    program: &launch.program,
                    selector: &auth.selector,
                    workspace: &launch.workspace,
                    export_environment: &[],
                    import_environment: &launch.environment,
                    isolated_config_home: &config_root,
                    isolated_data_home: &data_dir,
                    reuse_existing: auth.reuse_existing,
                },
                auth_deadline,
            )
            .await?;
        }

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
        let http = OpenCodeHttp::new(port, password.clone(), &native_directory)?;

        let mut command = Command::new(&launch.program);
        command
            .arg("serve")
            .arg("--hostname")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .current_dir(&native_directory)
            .envs(launch.environment)
            .env("OPENCODE_PASSWORD", &password)
            .env("XDG_CONFIG_HOME", &config_root)
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
        if self
            .child
            .try_wait()
            .map_err(|error| {
                Error::new(
                    "native_process",
                    format!("OpenCode serve exit readback failed: {error}"),
                )
            })?
            .is_some()
        {
            return Ok(());
        }
        if !force {
            // No graceful RPC exists; a short grace lets an in-flight native
            // close settle before the process ends.
            let grace = (Instant::now() + Duration::from_secs(2)).min(deadline);
            if matches!(timeout_at(grace, self.child.wait()).await, Ok(Ok(_))) {
                return Ok(());
            }
        }
        if let Err(error) = timeout_at(deadline, self.child.kill()).await {
            // Kill outcome is ambiguous without the wait proof below.
            if !matches!(timeout_at(deadline, self.child.wait()).await, Ok(Ok(_))) {
                return Err(Error::new(
                    "unknown",
                    "OpenCode serve termination outcome is unknown",
                ));
            }
            let _ = error;
            return Ok(());
        }
        if !matches!(timeout_at(deadline, self.child.wait()).await, Ok(Ok(_))) {
            return Err(Error::new(
                "unknown",
                "OpenCode serve exit proof is unknown",
            ));
        }
        Ok(())
    }

    pub(crate) async fn finish_diagnostics(&mut self, deadline: Instant) {
        if let Some(stdout) = self.stdout.take() {
            let _ = stdout.finish(deadline).await;
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.finish(deadline).await;
        }
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
            serde_json::from_str(&agent_config_json(Some("opencode/big-pickle"), None))
                .expect("valid JSON");
        assert_eq!(parsed["model"], "opencode/big-pickle");
        assert_eq!(parsed["mcp"], serde_json::json!({}));
        assert_eq!(
            parsed.as_object().expect("object").len(),
            3,
            "no other keys: {parsed}"
        );
    }

    fn local_provider_fixture(api_key_env: Option<&str>) -> LocalProvider {
        LocalProvider {
            provider_id: "eliot-local".to_owned(),
            api_model_id: "qwen3-8b".to_owned(),
            base_url: "http://127.0.0.1:1234/v1".to_owned(),
            api_key_env: api_key_env.map(str::to_owned),
            context_limit: 131072,
            output_limit: 8192,
            backend: "lmstudio".to_owned(),
            package: "@opencode/ai/providers/openai-compatible".to_owned(),
        }
    }

    #[test]
    fn local_config_renders_provider_package_settings_limits_and_env_key_reference() {
        let parsed: serde_json::Value = serde_json::from_str(&agent_config_json(
            Some("eliot-local/qwen3-8b"),
            Some(&local_provider_fixture(Some("LOCAL_LLM_KEY"))),
        ))
        .expect("valid JSON");
        assert_eq!(parsed["model"], "eliot-local/qwen3-8b");
        assert_eq!(parsed["mcp"], serde_json::json!({}));
        let provider = &parsed["providers"]["eliot-local"];
        assert_eq!(
            provider["package"],
            "@opencode/ai/providers/openai-compatible"
        );
        assert_eq!(provider["settings"]["baseURL"], "http://127.0.0.1:1234/v1");
        // The key travels as a reference only; the resolved value is never
        // rendered into the document.
        assert_eq!(provider["settings"]["apiKey"], "{env:LOCAL_LLM_KEY}");
        let model = &provider["models"]["qwen3-8b"];
        assert_eq!(model["name"], "qwen3-8b");
        assert_eq!(model["tools"], true);
        assert_eq!(model["limit"]["context"], 131072);
        assert_eq!(model["limit"]["output"], 8192);
    }

    #[test]
    fn local_no_auth_config_carries_no_key_not_an_empty_string() {
        // Missing key must never become no-auth mode with an empty string:
        // no-auth mode omits the key entirely.
        let rendered = agent_config_json(
            Some("eliot-local/qwen3-8b"),
            Some(&local_provider_fixture(None)),
        );
        let parsed: serde_json::Value = serde_json::from_str(&rendered).expect("valid JSON");
        let settings = &parsed["providers"]["eliot-local"]["settings"];
        assert_eq!(
            settings.as_object().expect("object").len(),
            1,
            "only baseURL: {settings}"
        );
        assert_eq!(settings["baseURL"], "http://127.0.0.1:1234/v1");
        assert!(settings.get("apiKey").is_none());
        assert!(!rendered.contains("apiKey"));
    }

    #[test]
    fn absent_model_omits_the_key_for_the_native_default() {
        // Requested-absent: no `model` key, so the configured native default
        // applies; the effective value is read back from the session.
        let parsed: serde_json::Value =
            serde_json::from_str(&agent_config_json(None, None)).expect("valid JSON");
        assert!(parsed.get("model").is_none(), "native default applies");
        assert_eq!(parsed["mcp"], serde_json::json!({}));
    }

    #[test]
    fn config_covers_absent_cloud_model_and_explicit_local_model() {
        // Ad-hoc cloud launch without a model: no `model` key and no
        // providers block, so the configured native default applies.
        let cloud: serde_json::Value =
            serde_json::from_str(&agent_config_json(None, None)).expect("valid JSON");
        assert!(cloud.get("model").is_none());
        assert!(cloud.get("providers").is_none());
        assert_eq!(cloud["mcp"], serde_json::json!({}));
        // Adopted local-backend profile: the explicit eliot-local model plus
        // the frozen provider document.
        let local: serde_json::Value = serde_json::from_str(&agent_config_json(
            Some("eliot-local/qwen3-8b"),
            Some(&local_provider_fixture(Some("LOCAL_LLM_KEY"))),
        ))
        .expect("valid JSON");
        assert_eq!(local["model"], "eliot-local/qwen3-8b");
        assert_eq!(
            local["providers"]["eliot-local"]["settings"]["baseURL"],
            "http://127.0.0.1:1234/v1"
        );
        assert_eq!(local["mcp"], serde_json::json!({}));
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
