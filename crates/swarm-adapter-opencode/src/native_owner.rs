//! Adapter-owned lifecycle for one fresh OpenCode child.
//!
//! The host still admits the binding, operation, workspace and native route.
//! This module owns only the exact Bun/`serve.mjs` child and its private
//! readiness receipts.  It deliberately has no Store, SQL, Task, or retry
//! authority.  An uncertain start is retained as an unknown native effect;
//! this module never kills or silently replaces a child.

use crate::{
    config::{NativeOptions, OwnedNativeOptions, is_loopback_endpoint},
    mcp_plugin,
    native::NativeClient,
    provider_auth,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{OwnedServiceProcessIdentity, OwnedServiceReadyReceipt},
};
use swarm_process::{
    private_permissions, process_birth_identity, process_image_identity, write_private_new,
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, ChildStdin, Command},
    time::{sleep, timeout},
};

const READY_TIMEOUT: Duration = Duration::from_secs(120);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_OWNER_BYTES: usize = 64 * 1024;
const MAX_CONNECTION_BYTES: usize = 64 * 1024;
const MAX_STDERR_BYTES: usize = 16 * 1024;
const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;
const SERVER_FILE_BYTES: u64 = 512 * 1024;
const PINNED_BUN_VERSION: &str = "1.4.0";

/// A bounded startup failure that lets the existing RuntimeOutcome path report
/// whether native execution was attempted without exposing process stderr.
#[derive(Debug)]
pub struct OwnerStartFailure {
    pub error: Error,
    pub effect_attempted: bool,
    pub details: Value,
}

/// Optional owner kept by the long-lived adapter loop.  It has one launch
/// attempt per fresh state root and no restart/adoption path.
pub struct NativeOwnerController {
    config: Option<OwnedNativeOptions>,
    active: Option<NativeOwner>,
    ready: Option<OwnedServiceReadyReceipt>,
}

impl NativeOwnerController {
    pub fn new(config: Option<OwnedNativeOptions>) -> Self {
        Self {
            config,
            active: None,
            ready: None,
        }
    }

    /// Start the exact configured native child after the caller has persisted
    /// the operation intent.  External-attach routes return immediately.
    pub async fn ensure_started(
        &mut self,
        options: &NativeOptions,
    ) -> std::result::Result<Option<OwnedServiceReadyReceipt>, OwnerStartFailure> {
        let Some(config) = self.config.clone() else {
            return Ok(None);
        };
        if self.active.is_some() {
            let receipt = self.ready.clone().ok_or_else(|| OwnerStartFailure {
                error: Error::new(
                    "NATIVE_OWNER_RECEIPT_MISSING",
                    "active native owner has no retained readiness proof",
                ),
                effect_attempted: true,
                details: Value::Null,
            })?;
            if receipt.service_id != options.service_id
                || receipt.service_version != options.expected_version
            {
                return Err(OwnerStartFailure {
                    error: Error::new(
                        "NATIVE_OWNER_ROUTE_CHANGED",
                        "active native owner proof differs from the selected route",
                    ),
                    effect_attempted: true,
                    details: Value::Null,
                });
            }
            return Ok(Some(receipt));
        }
        let (owner, receipt) = NativeOwner::start(&config, options).await?;
        self.active = Some(owner);
        self.ready = Some(receipt.clone());
        Ok(Some(receipt))
    }

    /// Send only the documented stdin EOF stop signal to a ready owner.  A
    /// timeout remains an error; dropping the child never claims departure.
    pub async fn shutdown(&mut self) -> Result<()> {
        let Some(owner) = self.active.take() else {
            self.ready = None;
            return Ok(());
        };
        self.ready = None;
        owner.shutdown().await
    }

    /// Bootstrap provider auth only for a route that also supplied a fresh
    /// owner plan. External-attach configurations never receive a key path.
    pub async fn bootstrap_provider_auth(
        &self,
        native: &NativeClient,
        options: &NativeOptions,
    ) -> Result<Option<Value>> {
        let Some(config) = self.config.as_ref() else {
            return Ok(None);
        };
        let Some(auth) = config.provider_auth.as_ref() else {
            return Ok(None);
        };
        provider_auth::bootstrap_once(native, options, auth)
            .await
            .map(Some)
    }
}

struct NativeOwner {
    child: Child,
    stdin: Option<ChildStdin>,
}

#[derive(Clone)]
struct OwnerPlan {
    config: OwnedNativeOptions,
    options: NativeOptions,
    workspace: PathBuf,
    config_digest: String,
    plugin_module_sha256: String,
    plugin_entrypoint_sha256: String,
}

#[derive(Debug, Default)]
struct StderrCapture {
    total_bytes: u64,
    prefix: Vec<u8>,
    truncated: bool,
    hasher: Sha256,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionRecord {
    schema_version: u32,
    endpoint: String,
    pid: u32,
    username: String,
    password: String,
}

impl NativeOwner {
    async fn start(
        config: &OwnedNativeOptions,
        options: &NativeOptions,
    ) -> std::result::Result<(Self, OwnedServiceReadyReceipt), OwnerStartFailure> {
        if let Err(error) = config.validate_for(options) {
            return Err(failure(
                error,
                "preflight",
                false,
                None,
                Value::Null,
                Value::Null,
            ));
        }
        let plan = match OwnerPlan::prepare(config, options) {
            Ok(plan) => plan,
            Err(error) => {
                return Err(failure(
                    error,
                    "preflight",
                    false,
                    None,
                    Value::Null,
                    Value::Null,
                ));
            }
        };
        let stderr = Arc::new(Mutex::new(StderrCapture::default()));
        let mut command = bun_command(&plan);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                return Err(failure(
                    Error::new("NATIVE_OWNER_SPAWN", "pinned OpenCode owner could not start"),
                    "spawn",
                    true,
                    None,
                    Value::Null,
                    stderr_snapshot(&stderr),
                )
                .with_io(error.kind().to_string()));
            }
        };
        let pid = match child.id() {
            Some(pid) => pid,
            None => {
                return Err(failure(
                    Error::new(
                        "NATIVE_OWNER_PID_UNAVAILABLE",
                        "OpenCode owner started without an observable process identity",
                    ),
                    "identity",
                    true,
                    None,
                    Value::Null,
                    stderr_snapshot(&stderr),
                ));
            }
        };
        let birth_identity = match process_birth_identity(pid) {
            Ok(Some(identity)) => json!({"status":"observed","identity":identity}),
            Ok(None) => json!({"status":"exited_before_readback","identity":null}),
            Err(error) => json!({
                "status":"unavailable",
                "code":safe_code(&error.code),
                "identity":null
            }),
        };
        let image_identity = match process_image_identity(pid) {
            Ok(identity) => identity,
            Err(error) => json!({"status":"unavailable","code":safe_code(&error.code)}),
        };
        if let Some(stderr_stream) = child.stderr.take() {
            spawn_stderr_reader(stderr_stream, stderr.clone());
        }
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    return Err(failure(
                        Error::new(
                            "NATIVE_OWNER_EXITED_BEFORE_READY",
                            "OpenCode owner exited before publishing exact readiness receipts",
                        ),
                        "ready",
                        true,
                        Some(pid),
                        json!({
                            "birth_identity":birth_identity,
                            "image_identity":image_identity,
                            "exit_code":status.code()
                        }),
                        stderr_snapshot(&stderr),
                    ));
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(failure(
                        Error::new(
                            "NATIVE_OWNER_WAIT_FAILED",
                            "owner process status could not be observed",
                        ),
                        "ready",
                        true,
                        Some(pid),
                        json!({
                            "birth_identity":birth_identity,
                            "image_identity":image_identity,
                            "wait_error":safe_code(&error.to_string())
                        }),
                        stderr_snapshot(&stderr),
                    ));
                }
            }
            if let Some(receipt) = ready_receipt(&plan, pid, &birth_identity["identity"]) {
                if let Err(error) = validate_live_bun_identity(&plan, &image_identity) {
                    return Err(failure(
                        error,
                        "identity",
                        true,
                        Some(pid),
                        json!({
                            "birth_identity":birth_identity,
                            "image_identity":image_identity,
                            "ready_receipt":receipt
                        }),
                        stderr_snapshot(&stderr),
                    ));
                }
                return Ok((
                    Self {
                        stdin: child.stdin.take(),
                        child,
                    },
                    receipt,
                ));
            }
            if Instant::now() >= deadline {
                return Err(failure(
                    Error::new(
                        "NATIVE_OWNER_READY_TIMEOUT",
                        "OpenCode owner did not publish exact readiness receipts",
                    ),
                    "ready",
                    true,
                    Some(pid),
                    json!({
                        "birth_identity":birth_identity,
                        "image_identity":image_identity
                    }),
                    stderr_snapshot(&stderr),
                ));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn shutdown(mut self) -> Result<()> {
        self.stdin.take();
        let status = timeout(STOP_TIMEOUT, self.child.wait()).await.map_err(|_| {
            Error::new(
                "NATIVE_OWNER_STOP_UNKNOWN",
                "OpenCode owner did not confirm a graceful stop before its deadline",
            )
        })??;
        if status.success() {
            Ok(())
        } else {
            Err(Error::new(
                "NATIVE_OWNER_STOP_UNKNOWN",
                "OpenCode owner exited without a successful stop receipt",
            ))
        }
    }
}

impl OwnerPlan {
    fn prepare(config: &OwnedNativeOptions, options: &NativeOptions) -> Result<Self> {
        let plugin_config = mcp_plugin::prepare_plugin_config(options)?;
        let plugin_identity = plugin_config.identity().clone();
        let bun = canonical_regular_file(&config.bun_executable, MAX_FILE_BYTES)?;
        let server = canonical_regular_file(&config.server_program, SERVER_FILE_BYTES)?;
        if digest_file(&bun, MAX_FILE_BYTES)? != config.bun_sha256
            || digest_file(&server, SERVER_FILE_BYTES)? != config.server_program_sha256
        {
            return Err(Error::new(
                "NATIVE_OWNER_PIN_MISMATCH",
                "configured Bun or serve.mjs bytes differ from the descriptor pin",
            ));
        }
        let workspace = canonical_directory(&options.directory)?;
        ensure_private_state_root(&config.state_root)?;
        let password_parent = config
            .password_file
            .parent()
            .ok_or_else(|| Error::new("NATIVE_OWNER_STATE", "owner password path has no parent"))?;
        if password_parent != config.state_root {
            return Err(Error::new(
                "NATIVE_OWNER_STATE",
                "owner password path must be directly below the fresh state root",
            ));
        }
        for directory in [
            "data",
            "cache",
            "config",
            "state",
            "tmp",
            "home",
            "appdata",
            "localappdata",
            "workspace",
        ] {
            let path = config.state_root.join(directory);
            fs::create_dir(&path)?;
            private_permissions(&path, true)?;
        }
        let config_dir = config.state_root.join("config").join("opencode");
        fs::create_dir(&config_dir)?;
        private_permissions(&config_dir, true)?;
        let config_path = config_dir.join("opencode.json");
        write_private_new(&config_path, plugin_config.bytes())?;
        let verified_plugin = mcp_plugin::verify_plugin_config_file(&config_path, options)?;
        if &verified_plugin != plugin_config.identity() {
            return Err(Error::new(
                "NATIVE_MCP_PLUGIN_SOURCE",
                "private OpenCode plugin config changed after preparation",
            ));
        }
        let password = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        write_private_new(&config.password_file, password.as_bytes())?;
        Ok(Self {
            config: config.clone(),
            options: options.clone(),
            workspace,
            config_digest: plugin_identity.config_sha256,
            plugin_module_sha256: plugin_identity.module_sha256,
            plugin_entrypoint_sha256: plugin_identity.entrypoint_sha256,
        })
    }
}

fn bun_command(plan: &OwnerPlan) -> Command {
    let mut command = Command::new(&plan.config.bun_executable);
    command
        .arg(&plan.config.server_program)
        .arg("--state-root")
        .arg(&plan.config.state_root)
        .arg("--password-file")
        .arg(&plan.config.password_file)
        .arg("--port")
        .arg(plan.config.port.to_string())
        .arg("--model-catalog")
        .arg(&plan.config.model_catalog)
        .arg("--owner-nonce")
        .arg(&plan.config.owner_nonce)
        .arg("--workspace-directory")
        .arg(&plan.workspace)
        .arg("--stop-on-stdin-eof")
        .current_dir(&plan.workspace)
        .env_clear()
        .envs(safe_environment(&plan.config.state_root))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(false);
    command
}

fn ready_receipt(
    plan: &OwnerPlan,
    pid: u32,
    birth_identity: &Value,
) -> Option<OwnedServiceReadyReceipt> {
    let owner_path = plan.config.state_root.join("owner.json");
    let connection_path = &plan.options.connection_file;
    let owner = read_json(&owner_path, MAX_OWNER_BYTES).ok()?;
    let connection_bytes = read_bounded(connection_path, MAX_CONNECTION_BYTES).ok()?;
    let connection: ConnectionRecord = serde_json::from_slice(&connection_bytes).ok()?;
    let endpoint = reqwest::Url::parse(&connection.endpoint).ok()?;
    if owner["schema_version"] != 1
        || owner["status"] != "ready"
        || owner["owner_nonce"] != plan.config.owner_nonce
        || owner["runtime"] != "bun"
        || owner["runtime_version"] != PINNED_BUN_VERSION
        || owner["native_server"] != "@opencode/server"
        || owner["native_server_version"] != plan.options.expected_version
        || owner["pid"].as_u64() != Some(u64::from(pid))
        || owner["native_reported_pid"].as_u64() != Some(u64::from(pid))
        || owner["endpoint"] != connection.endpoint
        || owner["state_root"] != path_text(&plan.config.state_root).ok()?
        || owner["connection_file"] != path_text(connection_path).ok()?
        || connection.schema_version != 1
        || connection.pid != pid
        || connection.username != "opencode"
        || connection.password.len() < 32
        || connection.password.len() > 4096
        || !is_loopback_endpoint(&endpoint)
        || endpoint.port_or_known_default() != Some(u16::from(plan.config.port)) && plan.config.port != 0
        || owner["connection_sha256"] != digest_bytes(&connection_bytes)
    {
        return None;
    }
    let birth_token = process_birth_token(birth_identity).ok()?;
    let receipt = OwnedServiceReadyReceipt {
        schema_version: 1,
        status: "ready".to_owned(),
        service_id: plan.options.service_id.clone(),
        service_version: plan.options.expected_version.clone(),
        owner_nonce: plan.config.owner_nonce.clone(),
        process: OwnedServiceProcessIdentity {
            pid,
            birth_token,
            binary_sha256: plan.config.bun_sha256.clone(),
        },
        endpoint_digest: digest_bytes(connection.endpoint.as_bytes()),
        connection_digest: digest_bytes(&connection_bytes),
        config_digest: plan.config_digest.clone(),
        plugin_module_sha256: plan.plugin_module_sha256.clone(),
        plugin_entrypoint_sha256: plan.plugin_entrypoint_sha256.clone(),
        server_program_sha256: plan.config.server_program_sha256.clone(),
        bun_sha256: plan.config.bun_sha256.clone(),
        readiness_observed: true,
        plugin_loaded: "unknown".to_owned(),
        dispatch_permitted: false,
    };
    receipt.validate().ok()?;
    Some(receipt)
}

fn process_birth_token(identity: &Value) -> Result<String> {
    let birth = if let Some(value) = identity["creation_filetime"].as_str() {
        json!({
            "platform":"windows",
            "pid":identity["pid"],
            "creation_filetime":value
        })
    } else {
        let boot = identity["boot_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new("NATIVE_OWNER_IDENTITY", "process boot identity is unavailable"))?;
        let ticks = identity["start_ticks"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new("NATIVE_OWNER_IDENTITY", "process start identity is unavailable"))?;
        json!({
            "platform":"linux",
            "pid":identity["pid"],
            "boot_id":boot,
            "start_ticks":ticks
        })
    };
    let canonical = crate::native::canonical_json(&birth)?;
    Ok(digest_bytes(canonical.as_bytes()))
}

fn validate_live_bun_identity(plan: &OwnerPlan, image: &Value) -> Result<()> {
    if image["image_sha256"] != plan.config.bun_sha256 {
        return Err(Error::new(
            "NATIVE_OWNER_IMAGE_MISMATCH",
            "spawned owner image differs from the pinned Bun executable",
        ));
    }
    let observed = image["image_path"]
        .as_str()
        .ok_or_else(|| Error::new("NATIVE_OWNER_IMAGE_MISMATCH", "owner image path is unavailable"))?;
    if !same_path(Path::new(observed), &plan.config.bun_executable)? {
        return Err(Error::new(
            "NATIVE_OWNER_IMAGE_MISMATCH",
            "spawned owner image path differs from the pinned Bun executable",
        ));
    }
    Ok(())
}

fn ensure_private_state_root(path: &Path) -> Result<()> {
    if !absolute_plain_path(path) {
        return Err(Error::new(
            "NATIVE_OWNER_STATE",
            "native owner state root must be an absolute plain path",
        ));
    }
    match fs::symlink_metadata(path) {
        Ok(_) => Err(Error::new(
            "NATIVE_OWNER_STATE_EXISTS",
            "fresh native owner state already exists; uncertain launches are never replayed",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or_else(|| Error::new("NATIVE_OWNER_STATE", "state root has no parent"))?;
            fs::create_dir_all(parent)?;
            private_permissions(parent, true)?;
            fs::create_dir(path)?;
            private_permissions(path, true)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn spawn_stderr_reader(mut stderr: tokio::process::ChildStderr, target: Arc<Mutex<StderrCapture>>) {
    tokio::spawn(async move {
        let mut buffer = [0_u8; 8192];
        loop {
            let count = match stderr.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(count) => count,
            };
            let Ok(mut capture) = target.lock() else {
                return;
            };
            capture.total_bytes = capture.total_bytes.saturating_add(count as u64);
            capture.hasher.update(&buffer[..count]);
            if capture.prefix.len() < MAX_STDERR_BYTES {
                let remaining = MAX_STDERR_BYTES - capture.prefix.len();
                capture.prefix.extend_from_slice(&buffer[..count.min(remaining)]);
            }
            capture.truncated |= capture.total_bytes > MAX_STDERR_BYTES as u64;
        }
    });
}

fn failure(
    error: Error,
    stage: &str,
    effect_attempted: bool,
    pid: Option<u32>,
    identity: Value,
    stderr: Value,
) -> OwnerStartFailure {
    OwnerStartFailure {
        error,
        effect_attempted,
        details: json!({
            "native_owner":{
                "stage":stage,
                "effect_attempted":effect_attempted,
                "native_session_state":if effect_attempted {"possibly_started"} else {"not_started"},
                "native_child":{
                    "spawn_returned_pid":pid,
                    "identity":identity,
                    "family_departure_claimed":false,
                    "manager_group_drain_required":true
                },
                "stderr":stderr
            },
            "native_replay":false
        }),
    }
}

impl OwnerStartFailure {
    fn with_io(mut self, value: String) -> Self {
        self.details["native_owner"]["io_kind"] = json!(safe_code(&value));
        self
    }
}

fn stderr_snapshot(target: &Arc<Mutex<StderrCapture>>) -> Value {
    let Ok(capture) = target.lock() else {
        return json!({"status":"unavailable"});
    };
    let digest = capture.hasher.clone().finalize();
    json!({
        "status":"bounded_digest",
        "bytes":capture.total_bytes,
        "stored_bytes":capture.prefix.len(),
        "sha256":hex_digest(&digest),
        "truncated":capture.truncated
    })
}

fn safe_environment(state_root: &Path) -> Vec<(String, PathBuf)> {
    let mut values = Vec::new();
    for name in [
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "HOMEDRIVE",
        "HOMEPATH",
        "PROCESSOR_ARCHITECTURE",
        "NUMBER_OF_PROCESSORS",
        "OS",
        "PUBLIC",
        "SYSTEMDRIVE",
        "LANG",
        "LC_ALL",
        "TZ",
    ] {
        if let Some(value) = std::env::var_os(name) {
            values.push((name.to_owned(), PathBuf::from(value)));
        }
    }
    let home = state_root.join("home");
    let tmp = state_root.join("tmp");
    let config = state_root.join("config");
    let data = state_root.join("data");
    let cache = state_root.join("cache");
    for (name, path) in [
        ("HOME", home.clone()),
        ("USERPROFILE", home.clone()),
        ("APPDATA", home.clone()),
        ("LOCALAPPDATA", home),
        ("TMP", tmp.clone()),
        ("TEMP", tmp.clone()),
        ("TMPDIR", tmp),
        ("XDG_CONFIG_HOME", config),
        ("XDG_DATA_HOME", data),
        ("XDG_CACHE_HOME", cache),
    ] {
        values.push((name.to_owned(), path));
    }
    values
}

fn canonical_regular_file(path: &Path, maximum: u64) -> Result<PathBuf> {
    if !absolute_plain_path(path) {
        return Err(Error::new("NATIVE_OWNER_PATH", "owner file path is invalid"));
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(Error::new("NATIVE_OWNER_PATH", "owner file is not a bounded regular file"));
    }
    let canonical = fs::canonicalize(path)?;
    if !same_path(&canonical, path)? {
        return Err(Error::new("NATIVE_OWNER_PATH", "owner file path is redirected"));
    }
    Ok(canonical)
}

fn canonical_directory(path: &Path) -> Result<PathBuf> {
    if !absolute_plain_path(path) {
        return Err(Error::new("NATIVE_OWNER_PATH", "workspace path is invalid"));
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new("NATIVE_OWNER_PATH", "workspace is not a plain directory"));
    }
    let canonical = fs::canonicalize(path)?;
    if !same_path(&canonical, path)? {
        return Err(Error::new("NATIVE_OWNER_PATH", "workspace path is redirected"));
    }
    Ok(canonical)
}

fn absolute_plain_path(path: &Path) -> bool {
    path.is_absolute()
        && !path.components().any(|component| {
            matches!(component, Component::CurDir | Component::ParentDir)
        })
}

fn same_path(left: &Path, right: &Path) -> Result<bool> {
    let left = fs::canonicalize(left)?;
    let right = fs::canonicalize(right)?;
    #[cfg(windows)]
    {
        Ok(left.to_string_lossy().replace('/', "\\").eq_ignore_ascii_case(
            &right.to_string_lossy().replace('/', "\\"),
        ))
    }
    #[cfg(not(windows))]
    {
        Ok(left == right)
    }
}

fn read_json(path: &Path, maximum: usize) -> Result<Value> {
    let bytes = read_bounded(path, maximum)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("NATIVE_OWNER_RECEIPT", "native owner receipt is invalid"))
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(Error::new(
            "NATIVE_OWNER_RECEIPT",
            "native owner receipt is not a bounded regular file",
        ));
    }
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.by_ref().take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(Error::new(
            "NATIVE_OWNER_RECEIPT",
            "native owner receipt exceeds its size bound",
        ));
    }
    Ok(bytes)
}

fn digest_file(path: &Path, maximum: u64) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total = total.saturating_add(count as u64);
        if total > maximum {
            return Err(Error::new("NATIVE_OWNER_PATH", "owner file exceeds its size bound"));
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex_digest(&hasher.finalize()))
}

fn digest_bytes(bytes: &[u8]) -> String {
    hex_digest(&Sha256::digest(bytes))
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| Error::new("NATIVE_OWNER_PATH", "owner path is not valid UTF-8"))
}

fn safe_code(value: &str) -> &str {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        "PROCESS_IDENTITY_ERROR"
    } else {
        value
    }
}
