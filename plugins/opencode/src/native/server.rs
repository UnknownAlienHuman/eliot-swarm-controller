//! Owned per-Agent `opencode serve` lifecycle.
//!
//! One server per Agent generation, on loopback, under the existing process
//! custody: the plugin process runs inside the generation's Windows Job, so
//! the server child (spawned directly, never through a `.cmd` wrapper) and
//! anything it spawns die with the generation. The server never sees the
//! owner's global config or data (verified on the installed v2.2.7
//! server). Authentication is a generated per-generation `OPENCODE_PASSWORD`;
//! a preset password also suppresses the `server password` stdout line.
//! The child inherits the daemon-allowlisted plugin environment plus the
//! shared target environment, exactly like the Codex/Muse stdio servers
//! (`native_stdio.rs`): there is no third allowlist here.
//!
//! Store layout: the generated `opencode.jsonc` is per generation (under the
//! plugin instance directory), but the session store (`XDG_DATA_HOME`) is
//! per Agent â€” `opencode-{agent_id}` next to the instance directories â€” so
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

use eliot_core::local_provider::LocalProvider;
use eliot_core::{Error, FailureOrigin, Result, SecretPolicy};
use eliot_plugin::{StderrCapture, session::paths_equal, skill, spawn_stderr};
use tokio::{
    net::TcpListener,
    process::{Child, Command},
    time::{Instant, sleep, timeout_at},
};

use super::apply_environment;
use crate::native::{
    AuthSetup, AuthTransfer,
    client::{OpenCodeHttp, server_native_directory},
    transfer_selected_integration,
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

/// Use the frozen allowlist independently of the assembled environment.
pub(crate) fn validate_local_provider(
    provider: &LocalProvider,
    frozen_pass_env: Option<&serde_json::Value>,
) -> Result<()> {
    let pass_env: Vec<String> = frozen_pass_env
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|_| {
            Error::new(
                "profile_error",
                "OpenCode retained pass_env must be an array of names",
            )
        })?
        .unwrap_or_default();
    provider
        .validate(&pass_env)
        .map_err(|message| Error::new("profile_error", message))
}

pub(crate) fn validate_local_provider_environment(
    provider: &LocalProvider,
    frozen_pass_env: Option<&serde_json::Value>,
    environment: &[(OsString, OsString)],
) -> Result<()> {
    validate_local_provider(provider, frozen_pass_env)?;
    if let Some(name) = provider.api_key_env.as_deref() {
        let value = environment
            .iter()
            .find_map(|(key, value)| {
                let key = key.to_str()?;
                (key == name || (cfg!(windows) && key.eq_ignore_ascii_case(name))).then_some(value)
            })
            .and_then(|value| value.to_str());
        // OpenCode substitutes env values into JSON text before parsing,
        // then looks for file references in the result.
        if !value.is_some_and(|text| {
            !text.trim().is_empty()
                && !text.contains(['"', '\\', '{', '}'])
                && !text.chars().any(char::is_control)
        }) {
            return Err(Error::new(
                "profile_error",
                "OpenCode local provider requires a non-empty API key safe for native config substitution",
            ));
        }
    }
    Ok(())
}

/// Name of the Agent-scoped swarm MCP server in the per-Agent config.
/// Fixed per generation: the config document itself is per generation, so
/// no ULID suffix is needed (unlike the Codex per-run override name).
pub const SWARM_MCP_SERVER_NAME: &str = eliot_plugin::native_cli::WORKER_SWARM_MCP_SERVER_NAME;

/// The generated per-Agent config: the frozen profile model when one was
/// requested, otherwise no `model` key so the configured native default
/// applies. The Agent-scoped swarm MCP server (below). On the cloud path there are no
/// providers or credentials in this document: the Agent runs the server
/// defaults, and the selected native credential arrives through the pre-serve
/// auth transfer into the isolated store, never through this document. On the
/// local-provider path the typed provider renders the installed
/// native schema (`providers`, `package`, `settings`, model limits)
/// with `{env:NAME}` references only, so key values stay runtime-only;
/// no-auth mode carries no `apiKey` key at all, never an empty string.
/// A fragment profile always carries an explicit model: absent model with a
/// fragment is a profile error raised by the caller, never rendered here.
///
/// `swarm_executable` and `swarm_data_dir` are the absolute active Swarm
/// executable and the Agent's owned Eliot data root, validated by the
/// caller (host start admission mirrors the Codex `args_with_agent_mcp`
/// guards). They render as public `command` arguments only: Agent
/// authentication stays in the inherited server environment and is never
/// copied into this file (the Claude `mcp_config.rs` model), and
/// `mcp --require-agent` fails closed without it. Shape verified against
/// the installed 2.2.7 server: `mcp add` writes
/// `mcp.servers.<name> = {type: "local", command: [...], environment?}`,
/// and `GET /api/config` echoes the document while `GET /api/mcp`
/// (`mcp.list`) reports the servers with connection status.
/// Per-generation native-subagent config mapping for the installed
/// OpenCode server (verified on 2.2.7: `opencode --version` reports v2.2.7
/// and the agent docs allow per-agent `tools`/`permission` control with
/// `task` as the subagent tool): `deny` sets the isolated per-Agent config
/// `permission.task` to `deny`; `allow` omits the key and preserves today's
/// behavior. This config layer alone cannot enforce `deny`: the server
/// loads project configuration after the global layer with last-match-wins
/// evaluation, so a project `subagent` allow would beat it. The launch path
/// therefore also stores a per-session `subagent` deny ruleset on the
/// session itself (merged after every agent/project rule and verified by
/// readback on create and resume); that session layer is authoritative and
/// this config entry is only a second layer.
fn native_subagents_permission(native_subagents: Option<&str>) -> Option<serde_json::Value> {
    if native_subagents == Some("deny") {
        Some(serde_json::json!({"task": "deny"}))
    } else {
        None
    }
}

fn agent_config_json(
    model: Option<&str>,
    fragment: Option<&LocalProvider>,
    swarm_executable: &str,
    swarm_data_dir: &str,
    native_subagents: Option<&str>,
) -> String {
    let mut servers = serde_json::Map::new();
    let mut command = vec![swarm_executable.to_owned()];
    command.extend(eliot_plugin::native_cli::worker_swarm_mcp_args(
        swarm_data_dir,
    ));
    servers.insert(
        SWARM_MCP_SERVER_NAME.to_owned(),
        serde_json::json!({
            "type": "local",
            "command": command,
        }),
    );
    let mcp = serde_json::Value::Object({
        let mut mcp = serde_json::Map::new();
        mcp.insert("servers".to_owned(), serde_json::Value::Object(servers));
        mcp
    });
    let mut document = provider_config(model, fragment);
    document["mcp"] = mcp;
    if let Some(permission) = native_subagents_permission(native_subagents) {
        document["permission"] = permission;
    }
    document.to_string()
}

pub(crate) fn provider_config(
    model: Option<&str>,
    fragment: Option<&LocalProvider>,
) -> serde_json::Value {
    let mut document = match fragment {
        None => serde_json::json!({
            "$schema": "https://opencode.ai/config.json",
        }),
        Some(fragment) => serde_json::json!({
            "$schema": "https://opencode.ai/config.json",
            "providers": (typed_provider_json(fragment)),
        }),
    };
    if let Some(model) = model {
        document["model"] = serde_json::Value::String(model.to_owned());
    }
    document
}

fn typed_provider_json(provider: &LocalProvider) -> serde_json::Value {
    let mut models = serde_json::Map::new();
    for model in &provider.models {
        let mut entry = serde_json::Map::new();
        entry.insert(
            "name".to_owned(),
            serde_json::Value::String(model.id.clone()),
        );
        let mut limit = serde_json::Map::new();
        if let Some(context) = model.context_limit {
            limit.insert("context".to_owned(), serde_json::json!(context));
        }
        if let Some(output) = model.output_limit {
            limit.insert("output".to_owned(), serde_json::json!(output));
        }
        if !limit.is_empty() {
            entry.insert("limit".to_owned(), serde_json::Value::Object(limit));
        }
        models.insert(model.id.clone(), serde_json::Value::Object(entry));
    }
    let mut settings = serde_json::Map::new();
    settings.insert(
        "baseURL".to_owned(),
        serde_json::Value::String(provider.base_url.clone()),
    );
    if let Some(name) = provider.api_key_env.as_deref() {
        settings.insert(
            "apiKey".to_owned(),
            serde_json::Value::String(format!("{{env:{name}}}")),
        );
    }
    serde_json::json!({
        provider.id.clone(): {
            "package": provider.package.clone(),
            "settings": settings,
            "models": models,
        }
    })
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
    pub local_provider: Option<LocalProvider>,
    pub frozen_pass_env: Option<serde_json::Value>,
    /// Active Swarm executable for the Agent-scoped MCP server command.
    /// Absolute; start admission resolves it from the running executable.
    pub swarm_executable: PathBuf,
    /// The Agent's owned Eliot data root for the MCP `--data-dir` argument.
    /// Absolute; start admission resolves it from the launch environment.
    pub swarm_data_dir: PathBuf,
    /// Frozen native-subagent switch (`allow` or `deny`): `deny` keeps the
    /// `task` deny in the isolated per-Agent config as a second layer next
    /// to the authoritative per-session `subagent` deny ruleset.
    pub native_subagents: Option<String>,
}

/// Launch of a short-lived archive reader server: the same owned `serve`
/// lifecycle on the exact retained per-Agent store, with a minimal
/// per-reader config (no model turn, no inference, no MCP server, no
/// context-hook plugin, no skill copy). Credentials already live in the
/// retained store from the Agent's own launches, so no auth transfer runs.
pub struct ArchiveServerLaunch {
    pub program: OsString,
    pub workspace: PathBuf,
    pub instance_dir: PathBuf,
    pub store_root: PathBuf,
    pub agent_id: String,
    pub environment: Vec<(OsString, OsString)>,
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
    /// server on the per-Agent store, and wait for readiness. Every failure
    /// from this function happens before any native session or prompt
    /// exists, so the caller settles it as a definite failed start, except
    /// an uncertain auth-import outcome (code `unknown`): the isolated
    /// credential store may already have been written and reconciliation
    /// failed, so that error propagates for the caller to settle unknown per
    /// the authentication contract. Any failure leaves no running child
    /// behind. The child gets the host-built target environment, generated
    /// password and XDG roots. Local providers use isolated config discovery.
    pub async fn spawn(
        launch: ServerLaunch,
        secrets: &[String],
        envelope: Instant,
    ) -> Result<Self> {
        let prepare_timing = super::startup::Step::new("config_environment");
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
        // Empty per-generation directory for canonical-store transfer
        // children (list, export): pointing their `XDG_CONFIG_HOME` here
        // keeps owner config, plugins, and MCP servers from loading during
        // the transfer, while the canonical `XDG_DATA_HOME` still resolves
        // the true credential store.
        let canonical_config_dir = launch.instance_dir.join("auth-config");
        std::fs::create_dir_all(&canonical_config_dir).map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode per-Agent auth config directory could not be created: {error}"),
            )
        })?;
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
        if launch.local_provider.is_some() && launch.model.is_none() {
            return Err(Error::new(
                "profile_error",
                "OpenCode provider fragment requires an explicit model with the fragment provider segment",
            ));
        }
        // Refuse a retained fragment that fails the shared strict allowlist
        // BEFORE any isolated-config write below: a literal credential or
        // unadmitted key must never reach disk first.
        if let Some(fragment) = launch.local_provider.as_ref() {
            validate_local_provider_environment(
                fragment,
                launch.frozen_pass_env.as_ref(),
                &launch.environment,
            )?;
        }
        // The MCP server command carries only public arguments; absolute
        // paths are re-checked here so a malformed launch can never render
        // a relative executable or data root into the per-Agent document.
        if !launch.swarm_executable.is_absolute() || !launch.swarm_data_dir.is_absolute() {
            return Err(Error::new(
                "invalid",
                "OpenCode Agent MCP requires the absolute Swarm executable and data directory",
            ));
        }
        let swarm_executable = launch.swarm_executable.to_str().ok_or_else(|| {
            Error::new("invalid", "OpenCode Agent MCP executable path is not UTF-8")
        })?;
        let swarm_data_dir = launch.swarm_data_dir.to_str().ok_or_else(|| {
            Error::new("invalid", "OpenCode Agent MCP data-root path is not UTF-8")
        })?;
        std::fs::write(
            &config_path,
            agent_config_json(
                launch.model.as_deref(),
                launch.local_provider.as_ref(),
                swarm_executable,
                swarm_data_dir,
                launch.native_subagents.as_deref(),
            ),
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
        // Bundled coordination skill: copy the daemon-materialized
        // canonical files into this isolated root's global skills
        // directory, so the per-Agent server loads `eliot-swarm` natively
        // with no project or user file touched. Rewritten verbatim on
        // every start, like the context-hook plugin above. A missing
        // canonical copy fails the start: the Agent must not start
        // believing it has no skill.
        let skill_dir = config_dir.join("skills").join(skill::SKILL_NAME);
        std::fs::create_dir_all(&skill_dir).map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode per-Agent skills directory could not be created: {error}"),
            )
        })?;
        for file in [skill::SKILL_FILE, skill::SKILL_REFERENCE_FILE] {
            let text = skill::read_skill_file(&skill::skill_dir(&launch.swarm_data_dir).join(file))
                .map_err(|_| {
                    Error::new(
                        "native_spawn",
                        "OpenCode skill source is unavailable; the daemon did not materialize it",
                    )
                })?;
            std::fs::write(skill_dir.join(file), text).map_err(|error| {
                Error::new(
                    "native_spawn",
                    format!("OpenCode per-Agent skill could not be written: {error}"),
                )
                .with_where("plugin.opencode.start.spawn")
                .with_origin(
                    FailureOrigin::Environment,
                    "isolated instance directory write failed",
                )
            })?;
        }
        // Credential preparation before `serve` starts, under the exact XDG
        // roots the server will use. Cloud path: the isolated store sees no
        // canonical credential, so the selected integration moves in opaquely
        // (a refused or ambiguous transfer fails the start with no server
        // child; resume reuses a correctly present credential). Fragment path:
        // no canonical credential is ever imported; the fragment was already
        // refused by the shared allowlist before any isolated-config write
        // above (shape plus non-empty runtime values for every `{env:}`
        // reference, never silently becoming no-auth mode).
        drop(prepare_timing);
        // Fragment path: validated before the config write above and imports
        // no canonical credential, so only the cloud path transfers.
        if launch.local_provider.is_none() {
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
                    canonical_config_home: &canonical_config_dir,
                    import_environment: &launch.environment,
                    isolated_config_home: &config_root,
                    isolated_data_home: &data_dir,
                    reuse_existing: auth.reuse_existing,
                },
                // The transfer is the known slow pole (two concurrent
                // canonical readers followed by the isolated importer):
                // it shares the whole start envelope
                // instead of a shorter private budget so loaded cold starts
                // fail definite, never unknown-by-deadline.
                envelope,
            )
            .await
            .map_err(|error| {
                // A definite no-effect transfer failure can belong to the OS,
                // credentials, native CLI or our validation. Preserve typed
                // evidence; uncertainty about an import remains explicit.
                let mut error = error.with_where("plugin.opencode.start.auth_transfer");
                if error.origin.is_none() {
                    if error.code == "unknown" {
                        error = error.with_origin(
                            FailureOrigin::Unknown,
                            "credential transfer outcome uncertain; isolated store may be written",
                        );
                    } else {
                        error = eliot_plugin::session::classify_start_failure(
                            error,
                            "opencode",
                            "auth_transfer",
                        );
                    }
                }
                error
            })?;
        }

        // Reserve a loopback port, then hand it to the server. A transient
        // bind race resolves as a spawn/readiness failure with no process
        // kept, never as a connection to a foreign listener: readiness
        // requires our password and our config document path.
        let port_timing = super::startup::Step::new("port_selection");
        let port = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| {
                Error::new(
                    "native_spawn",
                    format!("OpenCode loopback port could not be reserved: {error}"),
                )
                .with_where("plugin.opencode.start.spawn")
                .with_origin(
                    FailureOrigin::Environment,
                    "loopback port reservation failed",
                )
            })?
            .local_addr()
            .map_err(|error| {
                Error::new(
                    "native_spawn",
                    format!("OpenCode loopback port could not be read: {error}"),
                )
                .with_where("plugin.opencode.start.spawn")
                .with_origin(FailureOrigin::Environment, "loopback port readback failed")
            })?
            .port();
        drop(port_timing);
        let spawn_timing = super::startup::Step::new("serve_spawn");
        let password = format!("eliot-{}-{}", ulid::Ulid::new(), ulid::Ulid::new());
        // The capture policy covers the host secrets plus the generated
        // password before any pipe is read: a compromised server must not
        // leak its own credential through the retained tails.
        let mut capture = secrets.to_vec();
        capture.push(password.clone());
        let capture = SecretPolicy::new(capture);
        let http = OpenCodeHttp::new(port, password.clone(), &native_directory)?;

        let mut command = Command::new(&launch.program);
        apply_environment(
            &mut command,
            &launch.environment,
            launch.local_provider.is_some(),
        );
        command
            .arg("serve")
            .arg("--hostname")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .current_dir(&native_directory)
            .env("OPENCODE_PASSWORD", &password)
            .env("XDG_CONFIG_HOME", &config_root)
            .env("XDG_DATA_HOME", &data_dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // Normal Agent stop preserves the native session and hands any
            // lingering serve process to the daemon's generation Job. The
            // plugin's explicit startup cleanup still terminates failures;
            // dropping this handle must not perform a second force stop.
            .kill_on_drop(false);
        let mut child = command.spawn().map_err(|error| {
            Error::new(
                "native_spawn",
                capture.redact_text(&format!(
                    "could not start the configured OpenCode serve: {error}"
                )),
            )
            .with_where("plugin.opencode.start.spawn")
            .with_origin(
                FailureOrigin::Environment,
                "OS could not start the native serve binary",
            )
        })?;
        drop(spawn_timing);
        let ready_timing = super::startup::Step::new("serve_readiness");
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::new("native_spawn", "OpenCode serve stdout was not piped")
                .with_where("plugin.opencode.start.spawn")
        })?;
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
        // Readiness shares the start envelope: a loaded cold start gets the
        // full 60 s bound, but the whole launch still answers inside the
        // daemon agent.start budget.
        let ready_deadline = (Instant::now() + READINESS_TIMEOUT).min(envelope);
        if let Err(error) = server.await_ready(ready_deadline).await {
            // Bounded best-effort cleanup: the generation Job reaps anything
            // left once the daemon retires the plugin on the failed value,
            // so a wedged child cannot stack another minute here. The
            // readiness cause is returned, never masked by a cleanup outcome.
            let _ = server
                .terminate(true, Instant::now() + Duration::from_secs(10))
                .await;
            return Err(error);
        }
        drop(ready_timing);
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
                )
                .with_where("plugin.opencode.start.spawn")
                .with_exit(status.code())
                .with_origin(
                    FailureOrigin::Environment,
                    "native serve process exited during startup",
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

    /// Spawn the archive reader server on the retained per-Agent store.
    /// Readiness proves our password and our per-reader config document,
    /// exactly like a start; the caller additionally proves the served store
    /// derives the retained native scope before any delete. Every failure
    /// leaves no running child behind.
    pub async fn spawn_archive_reader(
        launch: ArchiveServerLaunch,
        secrets: &[String],
        envelope: Instant,
    ) -> Result<Self> {
        let native_directory = server_native_directory(&launch.workspace).map_err(|_| {
            Error::new(
                "native_spawn",
                "OpenCode archive workspace directory could not be canonicalized",
            )
        })?;
        let config_root = launch.instance_dir.join("config");
        let config_dir = config_root.join("opencode");
        let data_dir = stable_store_dir(&launch.store_root, &launch.agent_id)?;
        // The retained store must already exist: the reader never creates a
        // fresh store a delete could then be pointed at.
        if !data_dir.is_dir() {
            return Err(Error::new(
                "native_spawn",
                "OpenCode archive reader has no retained per-Agent store",
            ));
        }
        std::fs::create_dir_all(&config_dir).map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode archive reader config directory could not be created: {error}"),
            )
        })?;
        std::fs::write(
            config_dir.join("opencode.jsonc"),
            provider_config(None, None).to_string(),
        )
        .map_err(|error| {
            Error::new(
                "native_spawn",
                format!("OpenCode archive reader config could not be written: {error}"),
            )
        })?;
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
        let mut capture = secrets.to_vec();
        capture.push(password.clone());
        let capture = SecretPolicy::new(capture);
        let http = OpenCodeHttp::new(port, password.clone(), &native_directory)?;
        let mut command = Command::new(&launch.program);
        apply_environment(&mut command, &launch.environment, false);
        command
            .arg("serve")
            .arg("--hostname")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .current_dir(&native_directory)
            .env("OPENCODE_PASSWORD", &password)
            .env("XDG_CONFIG_HOME", &config_root)
            .env("XDG_DATA_HOME", &data_dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(false);
        let mut child = command.spawn().map_err(|error| {
            Error::new(
                "native_spawn",
                capture.redact_text(&format!(
                    "could not start the configured OpenCode serve: {error}"
                )),
            )
            .with_where("plugin.opencode.archive.spawn")
            .with_origin(
                FailureOrigin::Environment,
                "OS could not start the native serve binary",
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::new("native_spawn", "OpenCode serve stdout was not piped")
                .with_where("plugin.opencode.archive.spawn")
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            Error::new("native_spawn", "OpenCode serve stderr was not piped")
                .with_where("plugin.opencode.archive.spawn")
        })?;
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
        let ready_deadline = (Instant::now() + READINESS_TIMEOUT).min(envelope);
        if let Err(error) = server.await_ready(ready_deadline).await {
            let _ = server
                .terminate(true, Instant::now() + Duration::from_secs(5))
                .await;
            return Err(error);
        }
        Ok(server)
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

    /// End the owned server during cleanup. The generation Job owns any
    /// remaining descendants.
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

/// Bound for one `serve` readiness wait: poll the real signal (`GET
/// /api/config` proving the live server is ours) until this deadline. Under
/// CPU contention a cold `serve` needs well over the old 30 s, so the bound
/// is 60 s; the caller start envelope still caps the whole launch under the
/// daemon `agent.start` budget, and a slow start settles as a definite
/// failed start (no native session exists yet), never `unknown`.
pub(crate) const READINESS_TIMEOUT: Duration = Duration::from_secs(60);

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
