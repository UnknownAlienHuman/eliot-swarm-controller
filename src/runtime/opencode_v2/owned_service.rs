//! Fresh, controller-owned OpenCode 2.0.7 service lifecycle.
//!
//! This boundary is deliberately separate from `Options::connect`: external
//! routes attach to an operator-owned service, while this module starts only a
//! Store-admitted fresh service through the pinned repository owner script.
//! A process effect is one-shot; recovery reads exact owner evidence and never
//! starts a replacement.

use super::{ModelRef, Options, Service, mcp_plugin};
use crate::{
    config::OwnedOpenCodeServiceConfig,
    error::{Error, Result},
    model,
    platform::{
        private_permissions,
        process_group::{Group, process_birth_identity, process_image_identity},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader as AsyncBufReader},
    process::{Child, ChildStdin, Command as AsyncCommand},
    time::timeout,
};

const VERSION: &str = "2.0.7";
const BUN_VERSION: &str = "1.4.0";
const MAX_PLAN_BYTES: usize = 64 * 1024;
const MAX_OWNER_BYTES: usize = 64 * 1024;
const MAX_HANDSHAKE_BYTES: usize = 32 * 1024;
const MAX_CONFIG_BYTES: usize = 16 * 1024;
const MAX_OBSERVATION_BYTES: usize = 4096;
const MAX_PATH_BYTES: usize = 4096;
const START_TIMEOUT: Duration = Duration::from_secs(120);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) use super::mcp_plugin::{OwnedServiceIntent, OwnedServiceOrigin, OwnedServiceSeed};

/// Typed route whose origin is explicitly fresh-owned. It is never parsed from
/// `native_options` and keeps every path private to the runtime/Store boundary.
#[derive(Debug, Clone)]
pub(crate) struct OwnedServiceRoute {
    origin: OwnedServiceOrigin,
    base_service_id: String,
    service_id: String,
    model: ModelRef,
    model_catalog: String,
    bun_executable: PathBuf,
    bun_sha256: String,
    server_program: PathBuf,
    server_program_sha256: String,
    base_state_root: PathBuf,
    state_root: PathBuf,
    port: u16,
    owner_nonce: String,
    workspace_directory: PathBuf,
    password_file: PathBuf,
    connection_file: PathBuf,
    config_file: PathBuf,
}

impl OwnedServiceRoute {
    pub(crate) fn from_config(config: &OwnedOpenCodeServiceConfig) -> Result<Self> {
        if config.origin != "fresh_owned_service"
            || config.model_catalog != "offline" && config.model_catalog != "refresh"
            || !config.model.valid()
        {
            return Err(config_error("invalid fresh-owned OpenCode declaration"));
        }
        validate_identity("service ID", &config.service_id, 128)?;
        if !config
            .service_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        {
            return Err(config_error("invalid owned OpenCode service ID"));
        }
        let bun_executable = lexical_absolute(&config.bun_executable)?;
        let server_program = lexical_absolute(&config.server_program)?;
        let base_state_root = lexical_absolute(&config.state_root)?;
        let bun_sha256 = normalize_sha256(&config.bun_sha256)?;
        let server_program_sha256 = normalize_sha256(&config.server_program_sha256)?;
        let expected_server = lexical_absolute(
            &Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("modules")
                .join("opencode")
                .join("serve.mjs"),
        )?;
        if !same_lexical_path(&server_program, &expected_server)? {
            return Err(config_error(
                "owned OpenCode must use the repository-pinned serve.mjs",
            ));
        }
        Ok(Self {
            origin: OwnedServiceOrigin::FreshOwnedService,
            base_service_id: config.service_id.clone(),
            service_id: config.service_id.clone(),
            model: config.model.clone(),
            model_catalog: config.model_catalog.clone(),
            bun_executable,
            bun_sha256,
            server_program,
            server_program_sha256,
            base_state_root: base_state_root.clone(),
            state_root: base_state_root,
            port: config.port,
            owner_nonce: String::new(),
            workspace_directory: PathBuf::new(),
            password_file: PathBuf::new(),
            connection_file: PathBuf::new(),
            config_file: PathBuf::new(),
        })
    }

    /// Derive the one per-launch child state from the Store nonce and exact
    /// held workspace. The child directory is never reused after uncertainty.
    pub(crate) fn for_launch(&self, owner_nonce: &str, workspace_directory: &Path) -> Result<Self> {
        if self.origin != OwnedServiceOrigin::FreshOwnedService || !valid_uuid(owner_nonce) {
            return Err(scope_error(
                "fresh-owned launch needs a canonical owner nonce",
            ));
        }
        let workspace_directory = lexical_absolute(workspace_directory)?;
        let suffix = &model::digest(owner_nonce.as_bytes())[..16];
        let mut route = self.clone();
        route.owner_nonce = owner_nonce.to_owned();
        route.service_id = format!(
            "{}-{suffix}",
            self.base_service_id.chars().take(108).collect::<String>()
        );
        route.state_root = self.base_state_root.join("launches").join(owner_nonce);
        route.workspace_directory = workspace_directory;
        route.password_file = route.state_root.join("server.password");
        route.connection_file = route.state_root.join("connection.json");
        route.config_file = route
            .state_root
            .join("config")
            .join("opencode")
            .join("opencode.json");
        Ok(route)
    }

    pub(crate) fn service_id(&self) -> &str {
        &self.service_id
    }
    pub(crate) fn version(&self) -> &str {
        VERSION
    }
    pub(crate) fn owner_nonce(&self) -> &str {
        &self.owner_nonce
    }
    pub(crate) fn state_root(&self) -> &Path {
        &self.state_root
    }
    pub(crate) fn workspace_directory(&self) -> &Path {
        &self.workspace_directory
    }
    pub(crate) fn bun_sha256(&self) -> &str {
        &self.bun_sha256
    }
    pub(crate) fn server_program_sha256(&self) -> &str {
        &self.server_program_sha256
    }

    pub(crate) fn options(&self) -> Options {
        Options {
            service_id: self.service_id.clone(),
            connection_file: self.connection_file.clone(),
            expected_version: VERSION.to_owned(),
            directory: self.workspace_directory.clone(),
            model: self.model.clone(),
        }
    }

    pub(crate) fn route_digest(&self) -> Result<String> {
        if self.owner_nonce.is_empty() || self.workspace_directory.as_os_str().is_empty() {
            return Err(scope_error(
                "unbound owned-service base route has no launch digest",
            ));
        }
        let value = json!({
            "origin":"fresh_owned_service", "service_id":self.service_id,
            "version":VERSION, "model":self.model, "model_catalog":self.model_catalog,
            "bun_executable":path_text(&self.bun_executable)?, "bun_sha256":self.bun_sha256,
            "server_program":path_text(&self.server_program)?, "server_program_sha256":self.server_program_sha256,
            "state_root":path_text(&self.state_root)?, "base_state_root":path_text(&self.base_state_root)?,
            "port":self.port, "owner_nonce":self.owner_nonce,
            "workspace_directory":path_text(&self.workspace_directory)?,
            "password_file":path_text(&self.password_file)?,
            "connection_file":path_text(&self.connection_file)?, "config_file":path_text(&self.config_file)?,
        });
        Ok(model::digest(model::canonical(&value)?.as_bytes()))
    }

    fn verify_files(&self) -> Result<()> {
        let bun = canonical_regular_file(&self.bun_executable, 512 * 1024 * 1024)?;
        let server = canonical_regular_file(&self.server_program, 512 * 1024)?;
        let workspace = canonical_directory(&self.workspace_directory)?;
        if !same_lexical_path(&bun, &self.bun_executable)?
            || !same_lexical_path(&server, &self.server_program)?
            || !same_lexical_path(&workspace, &self.workspace_directory)?
            || hash_file(&bun, 512 * 1024 * 1024)? != self.bun_sha256
            || hash_file(&server, 512 * 1024)? != self.server_program_sha256
        {
            return Err(config_error(
                "owned service path or executable changed after route validation",
            ));
        }
        validate_directory_components(&self.base_state_root)?;
        validate_directory_components(&self.state_root)?;
        Ok(())
    }
}

/// Pure config and executable/source evidence assembled before the Store's
/// durable `outcome_unknown` transition.
pub(crate) struct PreparedOwnedService {
    route: OwnedServiceRoute,
    plugin: mcp_plugin::PreparedPluginConfig,
    config_digest: String,
    intent: OwnedServiceIntent,
}

impl PreparedOwnedService {
    pub(crate) fn config_digest(&self) -> &str {
        &self.config_digest
    }
}

pub(crate) fn prepare_owned_service(
    route: &OwnedServiceRoute,
    intent: &OwnedServiceIntent,
) -> Result<PreparedOwnedService> {
    route.verify_files()?;
    validate_directory_components(&route.state_root)?;
    match fs::symlink_metadata(&route.state_root) {
        Ok(_) => {
            return Err(Error::new(
                "OWNED_SERVICE_STATE_EXISTS",
                "fresh owned service state already exists; uncertain launches are never replayed",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(config_error(
                "fresh owned service state cannot be safely inspected",
            ));
        }
    }
    let plugin = mcp_plugin::prepare_plugin_config(route, intent)?;
    let config_digest = plugin.config_digest().to_owned();
    Ok(PreparedOwnedService {
        route: route.clone(),
        plugin,
        config_digest,
        intent: intent.clone(),
    })
}

/// Single-use in-process permit built from the exact Store admission. Store
/// keeps it local until its `outcome_unknown` write-ahead state commits, then
/// consumes it here. It has no Serde or Clone path.
pub(crate) struct OwnedServiceStartPermit {
    owner_nonce: String,
    route_digest: String,
    config_digest: String,
    scope_digest: String,
    consumed: bool,
}

impl OwnedServiceStartPermit {
    pub(crate) fn from_store_admission(
        intent: &OwnedServiceIntent,
        prepared: &PreparedOwnedService,
    ) -> Result<Self> {
        let route_digest = prepared.route.route_digest()?;
        if intent.owner_nonce() != prepared.route.owner_nonce()
            || intent.route_digest() != route_digest
            || intent.scope_digest() != prepared.intent.scope_digest()
            || prepared.intent.owner_nonce() != intent.owner_nonce()
            || prepared.intent.route_digest() != intent.route_digest()
            || prepared.intent.scope_digest() != intent.scope_digest()
            || prepared.plugin.owner_nonce() != intent.owner_nonce()
        {
            return Err(scope_error(
                "start permit does not match the exact Store-owned route",
            ));
        }
        Ok(Self {
            owner_nonce: intent.owner_nonce().to_owned(),
            route_digest,
            config_digest: prepared.config_digest.clone(),
            scope_digest: intent.scope_digest().to_owned(),
            consumed: false,
        })
    }
}

/// The only recoverable start failure. Its private identity was copied from
/// the same single-use Store permit; callers cannot manufacture or deserialize
/// proof that the exact operation stopped before any helper spawn attempt.
pub(crate) struct OwnedServiceNoEffect {
    owner_nonce: String,
    route_digest: String,
    config_digest: String,
    scope_digest: String,
}

impl OwnedServiceNoEffect {
    fn from_permit(permit: &OwnedServiceStartPermit) -> Self {
        Self {
            owner_nonce: permit.owner_nonce.clone(),
            route_digest: permit.route_digest.clone(),
            config_digest: permit.config_digest.clone(),
            scope_digest: permit.scope_digest.clone(),
        }
    }

    pub(crate) fn matches(&self, intent: &OwnedServiceIntent, config_digest: &str) -> bool {
        self.owner_nonce == intent.owner_nonce()
            && self.route_digest == intent.route_digest()
            && self.config_digest == config_digest
            && self.scope_digest == intent.scope_digest()
    }

    pub(crate) fn safe_code(&self) -> &'static str {
        "OWNED_SERVICE_PRE_SPAWN_NO_EFFECT"
    }
}

pub(crate) enum OwnedServiceStartFailure {
    ProvenNoEffect(OwnedServiceNoEffect),
    Unknown(Error),
}

/// Live, bounded process and readiness proof. Private endpoint and credentials
/// are retained only in memory; `store_proof()` never includes them.
pub(crate) struct OwnedServiceReadback {
    proof: Value,
}

impl OwnedServiceReadback {
    pub(crate) fn store_proof(&self) -> Value {
        self.proof.clone()
    }
    pub(crate) fn owner_nonce(&self) -> &str {
        self.proof["owner_nonce"].as_str().unwrap_or("")
    }
    pub(crate) fn service_id(&self) -> &str {
        self.proof["service_id"].as_str().unwrap_or("")
    }
    pub(crate) fn version(&self) -> &str {
        self.proof["service_version"].as_str().unwrap_or("")
    }
    pub(crate) fn pid(&self) -> u32 {
        self.proof["process"]["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())
            .unwrap_or(0)
    }
    pub(crate) fn birth_token(&self) -> &str {
        self.proof["process"]["birth_token"].as_str().unwrap_or("")
    }
    pub(crate) fn connection_digest(&self) -> &str {
        self.proof["connection_digest"].as_str().unwrap_or("")
    }
    pub(crate) fn binary_sha256(&self) -> &str {
        self.proof["process"]["binary_sha256"]
            .as_str()
            .unwrap_or("")
    }
    pub(crate) fn config_digest(&self) -> &str {
        self.proof["config_digest"].as_str().unwrap_or("")
    }
    pub(crate) fn route_digest(&self) -> &str {
        self.proof["route_digest"].as_str().unwrap_or("")
    }

    /// Strict constructor for proof retained by Store. It intentionally does
    /// not recover credentials, local paths, or an endpoint from serialized data.
    pub(crate) fn from_store_value(
        value: &Value,
        route: &OwnedServiceRoute,
        intent: &OwnedServiceIntent,
    ) -> Result<Self> {
        Self::from_route_value(value, route, intent.owner_nonce())
    }

    fn from_route_value(
        value: &Value,
        route: &OwnedServiceRoute,
        owner_nonce: &str,
    ) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| readback_error("owned service proof is not an object"))?;
        let expected = [
            "schema_version",
            "status",
            "service_id",
            "service_version",
            "owner_nonce",
            "route_digest",
            "process",
            "endpoint_digest",
            "connection_digest",
            "config_digest",
            "plugin_module_sha256",
            "plugin_entrypoint_sha256",
            "server_program_sha256",
            "bun_sha256",
            "readiness_observed",
            "plugin_loaded",
            "dispatch_permitted",
        ];
        if object.len() != expected.len()
            || object.keys().any(|key| !expected.contains(&key.as_str()))
            || value["schema_version"] != 1
            || value["status"] != "ready"
            || value["service_id"] != route.service_id
            || value["service_version"] != VERSION
            || owner_nonce != route.owner_nonce()
            || value["owner_nonce"] != owner_nonce
            || value["route_digest"] != route.route_digest()?
            || value["readiness_observed"] != true
            || value["plugin_loaded"] != "unknown"
            || value["dispatch_permitted"] != false
            || value["config_digest"]
                .as_str()
                .is_none_or(|hash| !is_sha256(hash))
            || value["plugin_module_sha256"]
                .as_str()
                .is_none_or(|hash| !is_sha256(hash))
            || value["plugin_entrypoint_sha256"]
                .as_str()
                .is_none_or(|hash| !is_sha256(hash))
            || value["server_program_sha256"].as_str() != Some(route.server_program_sha256())
            || value["bun_sha256"].as_str() != Some(route.bun_sha256())
            || value["endpoint_digest"]
                .as_str()
                .is_none_or(|hash| !is_sha256(hash))
            || value["connection_digest"]
                .as_str()
                .is_none_or(|hash| !is_sha256(hash))
        {
            return Err(readback_error(
                "retained owned service proof does not match its exact route",
            ));
        }
        let process = value["process"]
            .as_object()
            .ok_or_else(|| readback_error("process proof is missing"))?;
        if process.len() != 3
            || process
                .keys()
                .any(|key| !["pid", "birth_token", "binary_sha256"].contains(&key.as_str()))
            || process
                .get("pid")
                .and_then(Value::as_u64)
                .is_none_or(|pid| pid == 0 || pid > u32::MAX as u64)
            || process
                .get("birth_token")
                .and_then(Value::as_str)
                .is_none_or(|token| !is_sha256(token))
            || process.get("binary_sha256").and_then(Value::as_str) != Some(route.bun_sha256())
        {
            return Err(readback_error("process proof is malformed"));
        }
        Ok(Self {
            proof: value.clone(),
        })
    }
}

/// Positive no-kill evidence that one exact owner process incarnation has
/// departed after the pinned server wrote matching clean-stop receipts.
pub(crate) struct OwnedServiceDeparture {
    proof: Value,
}

impl OwnedServiceDeparture {
    pub(crate) fn store_proof(&self) -> Value {
        self.proof.clone()
    }
}

/// One active owned service. A recovery-created handle has no helper child and
/// therefore can read/observe but cannot claim graceful-stop control.
pub(crate) struct OwnedServiceHandle {
    route: OwnedServiceRoute,
    options: Options,
    service: Service,
    readback: OwnedServiceReadback,
    helper: Option<Child>,
    helper_stdin: Option<ChildStdin>,
}

impl OwnedServiceHandle {
    pub(crate) fn service(&self) -> &Service {
        &self.service
    }
    pub(crate) fn options(&self) -> Options {
        self.options.clone()
    }
    pub(crate) fn readback(&self) -> &OwnedServiceReadback {
        &self.readback
    }

    pub(crate) async fn close_gracefully(mut self) -> Result<OwnedServiceDeparture> {
        let Some(mut helper) = self.helper.take() else {
            return Err(Error::new(
                "OWNED_SERVICE_OWNER_UNAVAILABLE",
                "no retained foreground owner is available to stop this service",
            ));
        };
        self.helper_stdin.take(); // EOF is the only graceful stop signal; never kill the service process.
        let status = timeout(STOP_TIMEOUT, helper.wait()).await.map_err(|_| {
            Error::new(
                "OWNED_SERVICE_STOP_UNKNOWN",
                "owned service graceful stop is not yet confirmed",
            )
        })??;
        if !status.success() {
            return Err(Error::new(
                "OWNED_SERVICE_STOP_UNKNOWN",
                "owned service helper did not confirm graceful stop",
            ));
        }
        observe_departure(&self.route, &self.readback.store_proof())?
            .ok_or_else(|| readback_error("owned service departure is not yet proven"))
    }
}

/// Start one foreground helper only after Store has durably authorized the
/// effect. The helper enters the non-killing process Job before spawning Bun.
pub(crate) async fn start_foreground(
    prepared: PreparedOwnedService,
    mut permit: OwnedServiceStartPermit,
) -> std::result::Result<OwnedServiceHandle, OwnedServiceStartFailure> {
    if permit.consumed
        || permit.owner_nonce != prepared.route.owner_nonce()
        || permit.route_digest != prepared.intent.route_digest()
        || permit.config_digest != prepared.config_digest
        || permit.scope_digest != prepared.intent.scope_digest()
    {
        return Err(OwnedServiceStartFailure::Unknown(scope_error(
            "owned-service start permit is stale or mismatched",
        )));
    }
    permit.consumed = true;
    if prepared.route.verify_files().is_err() {
        return Err(pre_spawn_failure(&prepared.route, &permit, false));
    }
    let plan_path = match materialize_launch(&prepared) {
        Ok(plan_path) => plan_path,
        Err(failure) => {
            return Err(pre_spawn_failure(
                &prepared.route,
                &permit,
                failure.state_root_created,
            ));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(_) => {
            return Err(pre_spawn_failure(&prepared.route, &permit, true));
        }
    };
    let child = AsyncCommand::new(executable)
        .arg("owned-opencode-service")
        .arg("--file")
        .arg(&plan_path)
        .env_clear()
        .envs(safe_helper_environment())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(false)
        .spawn()
        .map_err(|error| {
            // An OS spawn error is still an attempted effect. The process may
            // have started even when the parent could not observe its handle.
            OwnedServiceStartFailure::Unknown(error.into())
        })?;
    complete_foreground_start(prepared, child)
        .await
        .map_err(OwnedServiceStartFailure::Unknown)
}

async fn complete_foreground_start(
    prepared: PreparedOwnedService,
    mut child: Child,
) -> Result<OwnedServiceHandle> {
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| readback_error("owned service helper input is unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| readback_error("owned service helper output is unavailable"))?;
    let mut reader = AsyncBufReader::new(stdout);
    let mut line = Vec::with_capacity(MAX_HANDSHAKE_BYTES);
    let read_result = timeout(START_TIMEOUT, reader.read_until(b'\n', &mut line)).await;
    let count = match read_result {
        Ok(Ok(count)) if count > 0 && count <= MAX_HANDSHAKE_BYTES => count,
        _ => {
            drop(stdin);
            return Err(readback_error(
                "owned service helper did not publish a bounded ready receipt",
            ));
        }
    };
    if line[count - 1] != b'\n' {
        drop(stdin);
        return Err(readback_error(
            "owned service helper receipt is unterminated",
        ));
    }
    let value: Value = serde_json::from_slice(&line[..count - 1])
        .map_err(|_| readback_error("owned service helper receipt is invalid"))?;
    let readback =
        OwnedServiceReadback::from_store_value(&value, &prepared.route, &prepared.intent)?;
    let options = prepared.route.options();
    if readback.config_digest() != prepared.config_digest
        || value["plugin_module_sha256"].as_str() != Some(prepared.plugin.module_sha256())
        || value["plugin_entrypoint_sha256"].as_str() != Some(prepared.plugin.entrypoint_sha256())
    {
        drop(stdin);
        return Err(readback_error(
            "ready receipt differs from the exact prepared plugin config",
        ));
    }
    let service = match Service::connect_owned(
        &options,
        readback.pid(),
        readback.birth_token(),
        readback.binary_sha256(),
    )
    .await
    {
        Ok(service) => service,
        Err(error) => {
            drop(stdin);
            return Err(error);
        }
    };
    prepared.route.verify_files()?;
    Ok(OwnedServiceHandle {
        route: prepared.route,
        options,
        service,
        readback,
        helper: Some(child),
        helper_stdin: Some(stdin),
    })
}

fn pre_spawn_failure(
    route: &OwnedServiceRoute,
    permit: &OwnedServiceStartPermit,
    state_root_created: bool,
) -> OwnedServiceStartFailure {
    if no_helper_effect_evidence(route)
        && (state_root_created || path_is_absent(&route.state_root).unwrap_or(false))
    {
        OwnedServiceStartFailure::ProvenNoEffect(OwnedServiceNoEffect::from_permit(permit))
    } else {
        OwnedServiceStartFailure::Unknown(readback_error(
            "owned service start outcome is uncertain",
        ))
    }
}

fn no_helper_effect_evidence(route: &OwnedServiceRoute) -> bool {
    if validate_directory_components(&route.state_root).is_err() {
        return false;
    }
    [
        ".owned-launch-consumed",
        "process-observation.json",
        "helper-observation.json",
        "helper-family-stop.json",
        "owner.json",
        "connection.json",
        "stop-receipt.json",
        "data",
        "home",
        "tmp",
        "cache",
    ]
    .iter()
    .all(|name| path_is_absent(&route.state_root.join(name)).unwrap_or(false))
}

/// Strict no-spawn readback for a Store row already in an uncertain/observed
/// state. It may connect to the exact retained process but never creates one.
pub(crate) async fn readback_existing(
    route: &OwnedServiceRoute,
    intent: &OwnedServiceIntent,
    expected_proof: &Value,
) -> Result<Option<OwnedServiceHandle>> {
    route.verify_files()?;
    if intent.owner_nonce() != route.owner_nonce()
        || intent.route_digest() != route.route_digest()?
    {
        return Err(readback_error(
            "retained intent does not match the exact owned route",
        ));
    }
    let expected = if expected_proof
        .as_object()
        .is_some_and(serde_json::Map::is_empty)
    {
        None
    } else {
        Some(OwnedServiceReadback::from_store_value(
            expected_proof,
            route,
            intent,
        )?)
    };
    let Some(readback) = read_owned_files(route)? else {
        return Ok(None);
    };
    let checked = OwnedServiceReadback::from_store_value(&readback.store_proof(), route, intent)?;
    if let Some(expected) = expected
        && model::canonical(&checked.store_proof())? != model::canonical(&expected.store_proof())?
    {
        return Ok(None);
    }
    let pid = checked.pid();
    let identity = process_image_identity(pid)?;
    verify_process_identity(
        route,
        &identity,
        pid,
        checked.binary_sha256(),
        checked.birth_token(),
    )?;
    let options = route.options();
    let service = Service::connect_owned(
        &options,
        pid,
        checked.birth_token(),
        checked.binary_sha256(),
    )
    .await?;
    let identity = process_image_identity(pid)?;
    verify_process_identity(
        route,
        &identity,
        pid,
        checked.binary_sha256(),
        checked.birth_token(),
    )?;
    Ok(Some(OwnedServiceHandle {
        route: route.clone(),
        options,
        service,
        readback,
        helper: None,
        helper_stdin: None,
    }))
}

/// Read-only recovery for an already observed binding after controller
/// restart. It has no LaunchActor/start permit and never creates a process.
pub(crate) async fn readback_retained(
    route: &OwnedServiceRoute,
    stored_proof: &Value,
) -> Result<OwnedServiceHandle> {
    route.verify_files()?;
    let owner_nonce = route.owner_nonce();
    let expected = OwnedServiceReadback::from_route_value(stored_proof, route, owner_nonce)?;
    let readback = read_owned_files(route)?
        .ok_or_else(|| readback_error("retained owned service owner is unavailable"))?;
    let checked =
        OwnedServiceReadback::from_route_value(&readback.store_proof(), route, owner_nonce)?;
    if model::canonical(&checked.store_proof())? != model::canonical(&expected.store_proof())? {
        return Err(readback_error(
            "live owned service differs from its retained startup proof",
        ));
    }
    let pid = checked.pid();
    let identity = process_image_identity(pid)?;
    verify_process_identity(
        route,
        &identity,
        pid,
        checked.binary_sha256(),
        checked.birth_token(),
    )?;
    let options = route.options();
    let service = Service::connect_owned(
        &options,
        pid,
        checked.birth_token(),
        checked.binary_sha256(),
    )
    .await?;
    let identity = process_image_identity(pid)?;
    verify_process_identity(
        route,
        &identity,
        pid,
        checked.binary_sha256(),
        checked.birth_token(),
    )?;
    Ok(OwnedServiceHandle {
        route: route.clone(),
        options,
        service,
        readback,
        helper: None,
        helper_stdin: None,
    })
}

// The Store-facing route allows only the one exact origin after resolving the
// configured alias. No conversion exists from external `Options`.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HelperPlan {
    schema_version: u32,
    owner_nonce: String,
    service_id: String,
    version: String,
    model: ModelRef,
    model_catalog: String,
    bun_executable: PathBuf,
    bun_sha256: String,
    server_program: PathBuf,
    server_program_sha256: String,
    state_root: PathBuf,
    password_file: PathBuf,
    connection_file: PathBuf,
    config_file: PathBuf,
    workspace_directory: PathBuf,
    port: u16,
    route_digest: String,
    config_digest: String,
    module_sha256: String,
    entrypoint_sha256: String,
}

struct MaterializeFailure {
    state_root_created: bool,
}

fn materialize_launch(
    prepared: &PreparedOwnedService,
) -> std::result::Result<PathBuf, MaterializeFailure> {
    let route = &prepared.route;
    (|| -> Result<()> {
        ensure_directory_tree(&route.base_state_root)?;
        let launch_parent = route.base_state_root.join("launches");
        ensure_directory_tree(&launch_parent)?;
        match fs::symlink_metadata(&route.state_root) {
            Ok(_) => Err(Error::new(
                "OWNED_SERVICE_STATE_EXISTS",
                "fresh owned service state already exists; uncertain launches are never replayed",
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    })()
    .map_err(|_| MaterializeFailure {
        state_root_created: false,
    })?;
    fs::create_dir(&route.state_root).map_err(|_| MaterializeFailure {
        state_root_created: false,
    })?;

    let after_root = (|| -> Result<PathBuf> {
        private_permissions(&route.state_root, true)?;
        let config_dir = route
            .config_file
            .parent()
            .ok_or_else(|| config_error("owned config path is invalid"))?;
        ensure_directory_tree(config_dir)?;
        let config = model::canonical(prepared.plugin.config_value())?;
        if model::digest(config.as_bytes()) != prepared.config_digest {
            return Err(source_error("prepared plugin config digest changed"));
        }
        write_private_new(&route.config_file, format!("{config}\n").as_bytes())?;
        let password = format!(
            "{}{}",
            model::new_id().replace('-', ""),
            model::new_id().replace('-', "")
        );
        write_private_new(&route.password_file, password.as_bytes())?;
        let plan = HelperPlan {
            schema_version: 1,
            owner_nonce: route.owner_nonce.clone(),
            service_id: route.service_id.clone(),
            version: VERSION.into(),
            model: route.model.clone(),
            model_catalog: route.model_catalog.clone(),
            bun_executable: route.bun_executable.clone(),
            bun_sha256: route.bun_sha256.clone(),
            server_program: route.server_program.clone(),
            server_program_sha256: route.server_program_sha256.clone(),
            state_root: route.state_root.clone(),
            password_file: route.password_file.clone(),
            connection_file: route.connection_file.clone(),
            config_file: route.config_file.clone(),
            workspace_directory: route.workspace_directory.clone(),
            port: route.port,
            route_digest: route.route_digest()?,
            config_digest: prepared.config_digest.clone(),
            module_sha256: prepared.plugin.module_sha256().to_owned(),
            entrypoint_sha256: prepared.plugin.entrypoint_sha256().to_owned(),
        };
        let plan_path = route.state_root.join(".owned-launch-plan.json");
        let canonical_plan = model::canonical(&serde_json::to_value(plan)?)?;
        write_private_new(&plan_path, canonical_plan.as_bytes())?;
        Ok(plan_path)
    })();
    after_root.map_err(|_| MaterializeFailure {
        state_root_created: true,
    })
}

/// Hidden CLI entrypoint. It creates a non-killing per-service Job in this
/// helper process before Bun is spawned; the root host is never assigned to it.
pub(crate) fn run_owned_service_helper(plan_path: &Path) -> Result<()> {
    let plan = read_plan(plan_path)?;
    validate_plan(&plan, plan_path)?;
    let consumed = plan.state_root.join(".owned-launch-consumed");
    write_private_new(&consumed, plan.owner_nonce.as_bytes())?;
    let _group = Group::enter_module(&plan.owner_nonce)?;
    let helper_observation = write_helper_observation(&plan)?;
    let mut command = bun_command(&plan);
    let mut bun = command.spawn().map_err(|_| {
        Error::new(
            "OWNED_SERVICE_START",
            "pinned OpenCode owner process could not start",
        )
    })?;
    let observation = ProcessObservation::for_spawned_process(&plan, bun.id())?;
    write_private_new(
        &plan.state_root.join("process-observation.json"),
        model::canonical(&serde_json::to_value(&observation)?)?.as_bytes(),
    )?;
    let ready = wait_for_ready(&plan, &mut bun)?;
    if ready.pid != observation.pid {
        return Err(readback_error(
            "ready owner PID differs from the spawned process",
        ));
    }
    let identity = process_image_identity(ready.pid)?;
    verify_image_identity(&identity, ready.pid, &plan.bun_sha256)?;
    let readback = proof_from_ready(&plan, &ready, &identity)?;
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    output.write_all(model::canonical(&readback)?.as_bytes())?;
    output.write_all(b"\n")?;
    output.flush()?;

    // Parent EOF is the only stop request. No signal/kill fallback is used.
    let stdin = std::io::stdin();
    let mut discard = [0_u8; 1024];
    loop {
        match stdin.lock().read(&mut discard) {
            Ok(0) => break,
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    drop(bun.stdin.take());
    let deadline = Instant::now() + STOP_TIMEOUT;
    loop {
        if let Some(status) = bun.try_wait()? {
            if !status.success() {
                return Err(Error::new(
                    "OWNED_SERVICE_STOP_UNKNOWN",
                    "pinned OpenCode owner exited without a confirmed stop",
                ));
            }
            break;
        }
        if Instant::now() >= deadline {
            return Err(Error::new(
                "OWNED_SERVICE_STOP_UNKNOWN",
                "pinned OpenCode owner stop receipt is not confirmed",
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    verify_stopped_plan(&plan)?;
    let family_deadline = Instant::now() + STOP_TIMEOUT;
    while !_group.children_empty()? {
        if Instant::now() >= family_deadline {
            return Err(Error::new(
                "OWNED_SERVICE_STOP_UNKNOWN",
                "owned helper did not confirm its process family has exited",
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    write_helper_family_stop(&plan, &helper_observation)?;
    Ok(())
}

fn bun_command(plan: &HelperPlan) -> Command {
    let mut command = Command::new(&plan.bun_executable);
    command
        .arg(&plan.server_program)
        .arg("--state-root")
        .arg(&plan.state_root)
        .arg("--password-file")
        .arg(&plan.password_file)
        .arg("--port")
        .arg(plan.port.to_string())
        .arg("--model-catalog")
        .arg(&plan.model_catalog)
        .arg("--owner-nonce")
        .arg(&plan.owner_nonce)
        .arg("--workspace-directory")
        .arg(&plan.workspace_directory)
        .arg("--stop-on-stdin-eof")
        .current_dir(&plan.workspace_directory)
        .env_clear()
        .envs(safe_bun_environment(&plan.state_root))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

fn wait_for_ready(plan: &HelperPlan, bun: &mut std::process::Child) -> Result<ReadyRecord> {
    let owner_path = plan.state_root.join("owner.json");
    let connection_path = &plan.connection_file;
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = bun.try_wait()? {
            return Err(Error::new(
                "OWNED_SERVICE_START",
                format!("pinned OpenCode owner exited before readiness ({status})"),
            ));
        }
        if regular_file(&owner_path, MAX_OWNER_BYTES).is_ok()
            && regular_file(connection_path, MAX_OWNER_BYTES).is_ok()
            && let Ok((owner, connection, connection_digest)) =
                load_owner_connection(plan, &owner_path, connection_path)
            && owner.status == "ready"
            && owner.owner_nonce == plan.owner_nonce
            && owner.native_server_version == VERSION
            && owner.pid == connection.pid
            && owner.connection_sha256.as_deref() == Some(&connection_digest)
            && connection.username == "opencode"
            && !connection.password.is_empty()
        {
            let endpoint = validate_endpoint(&connection.endpoint, plan.port)?;
            return Ok(ReadyRecord {
                pid: owner.pid,
                endpoint,
                connection_digest,
            });
        }
        if Instant::now() >= deadline {
            return Err(Error::new(
                "OWNED_SERVICE_START_UNKNOWN",
                "pinned service did not publish matching owner and connection receipts",
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[derive(Debug, Deserialize)]
struct OwnerRecord {
    schema_version: u32,
    owner_nonce: String,
    status: String,
    runtime: String,
    runtime_version: String,
    native_server: String,
    native_server_version: String,
    model_catalog_mode: String,
    pid: u32,
    endpoint: Option<String>,
    bound_port: Option<u16>,
    connection_file: String,
    stop_receipt: String,
    database_path: String,
    connection_sha256: Option<String>,
    server_fiber: Option<String>,
    listener_closed: Option<bool>,
    runtime_disposed: Option<bool>,
    connection_absent: Option<bool>,
    process_exit_code: Option<i32>,
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
struct ReadyRecord {
    pid: u32,
    endpoint: String,
    connection_digest: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessObservation {
    schema_version: u32,
    owner_nonce: String,
    route_digest: String,
    pid: u32,
    birth_token: String,
    binary_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HelperObservation {
    schema_version: u32,
    owner_nonce: String,
    route_digest: String,
    helper_pid: u32,
    helper_birth_token: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HelperFamilyStopReceipt {
    schema_version: u32,
    owner_nonce: String,
    route_digest: String,
    helper_pid: u32,
    helper_birth_token: String,
    children_empty: bool,
}

impl HelperObservation {
    fn current(plan: &HelperPlan) -> Result<Self> {
        let helper_pid = std::process::id();
        let identity = process_birth_identity(helper_pid)?
            .ok_or_else(|| readback_error("owned helper process identity is unavailable"))?;
        Ok(Self {
            schema_version: 1,
            owner_nonce: plan.owner_nonce.clone(),
            route_digest: plan.route_digest.clone(),
            helper_pid,
            helper_birth_token: process_birth_token(&identity)?,
        })
    }

    fn validate(&self, owner_nonce: &str, route_digest: &str) -> Result<()> {
        if self.schema_version != 1
            || self.owner_nonce != owner_nonce
            || self.route_digest != route_digest
            || self.helper_pid == 0
            || !is_sha256(&self.helper_birth_token)
        {
            return Err(readback_error(
                "private helper observation differs from its exact launch",
            ));
        }
        Ok(())
    }
}

fn write_helper_observation(plan: &HelperPlan) -> Result<HelperObservation> {
    let observation = HelperObservation::current(plan)?;
    write_private_new(
        &plan.state_root.join("helper-observation.json"),
        model::canonical(&serde_json::to_value(&observation)?)?.as_bytes(),
    )?;
    Ok(observation)
}

fn write_helper_family_stop(plan: &HelperPlan, observation: &HelperObservation) -> Result<()> {
    observation.validate(&plan.owner_nonce, &plan.route_digest)?;
    if observation.helper_pid != std::process::id() {
        return Err(readback_error(
            "helper family-stop receipt is being written by a different process",
        ));
    }
    let receipt = HelperFamilyStopReceipt {
        schema_version: 1,
        owner_nonce: observation.owner_nonce.clone(),
        route_digest: observation.route_digest.clone(),
        helper_pid: observation.helper_pid,
        helper_birth_token: observation.helper_birth_token.clone(),
        children_empty: true,
    };
    write_private_new(
        &plan.state_root.join("helper-family-stop.json"),
        model::canonical(&serde_json::to_value(receipt)?)?.as_bytes(),
    )
}

fn helper_family_departed(route: &OwnedServiceRoute) -> Result<Option<String>> {
    let route_digest = route.route_digest()?;
    let Some(observation_bytes) = read_optional_regular(
        &route.state_root.join("helper-observation.json"),
        MAX_OBSERVATION_BYTES,
    )?
    else {
        return Ok(None);
    };
    let observation: HelperObservation = serde_json::from_slice(&observation_bytes)
        .map_err(|_| readback_error("private helper observation is invalid"))?;
    observation.validate(route.owner_nonce(), &route_digest)?;
    let Some(receipt_bytes) = read_optional_regular(
        &route.state_root.join("helper-family-stop.json"),
        MAX_OBSERVATION_BYTES,
    )?
    else {
        return Ok(None);
    };
    let receipt: HelperFamilyStopReceipt = serde_json::from_slice(&receipt_bytes)
        .map_err(|_| readback_error("helper family-stop receipt is invalid"))?;
    if receipt.schema_version != 1
        || receipt.owner_nonce != observation.owner_nonce
        || receipt.route_digest != observation.route_digest
        || receipt.helper_pid != observation.helper_pid
        || receipt.helper_birth_token != observation.helper_birth_token
        || !receipt.children_empty
    {
        return Err(readback_error(
            "helper family-stop receipt differs from its exact process observation",
        ));
    }
    let Some(current_identity) = process_birth_identity(observation.helper_pid)? else {
        return Ok(Some(model::digest(&receipt_bytes)));
    };
    if process_birth_token(&current_identity)? == observation.helper_birth_token {
        Ok(None)
    } else {
        Ok(Some(model::digest(&receipt_bytes)))
    }
}

impl ProcessObservation {
    fn for_spawned_process(plan: &HelperPlan, pid: u32) -> Result<Self> {
        if pid == 0 {
            return Err(readback_error("spawned owner process has no PID"));
        }
        let identity = process_image_identity(pid)?;
        verify_image_identity(&identity, pid, &plan.bun_sha256)?;
        let observed_image = identity["image_path"]
            .as_str()
            .ok_or_else(|| readback_error("spawned process image path is unavailable"))?;
        let observed_image = canonical_regular_file(Path::new(observed_image), 512 * 1024 * 1024)?;
        if !same_lexical_path(&observed_image, &plan.bun_executable)? {
            return Err(readback_error(
                "spawned process image differs from the configured Bun executable",
            ));
        }
        Ok(Self {
            schema_version: 1,
            owner_nonce: plan.owner_nonce.clone(),
            route_digest: plan.route_digest.clone(),
            pid,
            birth_token: process_birth_token(&identity)?,
            binary_sha256: plan.bun_sha256.clone(),
        })
    }

    fn validate_expected(
        &self,
        owner_nonce: &str,
        route_digest: &str,
        binary_sha256: &str,
    ) -> Result<()> {
        if self.schema_version != 1
            || self.owner_nonce != owner_nonce
            || self.route_digest != route_digest
            || self.pid == 0
            || !is_sha256(&self.birth_token)
            || self.binary_sha256 != binary_sha256
        {
            return Err(readback_error(
                "private process observation differs from its exact owned route",
            ));
        }
        Ok(())
    }
}

fn read_process_observation(
    path: &Path,
    owner_nonce: &str,
    route_digest: &str,
    binary_sha256: &str,
) -> Result<Option<ProcessObservation>> {
    let Some(bytes) = read_optional_regular(path, MAX_OBSERVATION_BYTES)? else {
        return Ok(None);
    };
    let observation: ProcessObservation = serde_json::from_slice(&bytes)
        .map_err(|_| readback_error("private process observation is invalid"))?;
    observation.validate_expected(owner_nonce, route_digest, binary_sha256)?;
    Ok(Some(observation))
}

fn load_owner_connection(
    plan: &HelperPlan,
    owner_path: &Path,
    connection_path: &Path,
) -> Result<(OwnerRecord, ConnectionRecord, String)> {
    let owner_bytes = regular_file(owner_path, MAX_OWNER_BYTES)?;
    let connection_bytes = regular_file(connection_path, MAX_OWNER_BYTES)?;
    let owner: OwnerRecord = serde_json::from_slice(&owner_bytes)
        .map_err(|_| readback_error("owned service owner receipt is invalid"))?;
    let connection: ConnectionRecord = serde_json::from_slice(&connection_bytes)
        .map_err(|_| readback_error("owned service connection receipt is invalid"))?;
    let digest = model::digest(&connection_bytes);
    if owner.schema_version != 1
        || owner.owner_nonce != plan.owner_nonce
        || owner.runtime != "bun"
        || owner.runtime_version != BUN_VERSION
        || owner.native_server != "@opencode/server"
        || owner.native_server_version != VERSION
        || owner.model_catalog_mode != plan.model_catalog
        || owner.pid == 0
        || connection.schema_version != 1
        || connection.pid != owner.pid
        || owner.connection_file != path_text(connection_path)?
        || owner.stop_receipt != path_text(&plan.state_root.join("stop-receipt.json"))?
        || owner.database_path != path_text(&plan.state_root.join("data").join("opencode.sqlite"))?
        || owner.endpoint.as_deref() != Some(connection.endpoint.as_str())
        || owner.bound_port != endpoint_port(&connection.endpoint)
        || !connection.endpoint.starts_with("http://127.0.0.1:")
    {
        return Err(readback_error(
            "owner receipt does not match the exact private launch plan",
        ));
    }
    Ok((owner, connection, digest))
}

fn proof_from_ready(plan: &HelperPlan, ready: &ReadyRecord, image: &Value) -> Result<Value> {
    verify_image_identity(image, ready.pid, &plan.bun_sha256)?;
    let birth = process_birth_token(image)?;
    let observation = read_process_observation(
        &plan.state_root.join("process-observation.json"),
        &plan.owner_nonce,
        &plan.route_digest,
        &plan.bun_sha256,
    )?
    .ok_or_else(|| readback_error("spawned process observation is missing"))?;
    let image_path = image["image_path"]
        .as_str()
        .ok_or_else(|| readback_error("owned process image path is unavailable"))?;
    let image_path = canonical_regular_file(Path::new(image_path), 512 * 1024 * 1024)?;
    if observation.pid != ready.pid
        || observation.birth_token != birth
        || !same_lexical_path(&image_path, &plan.bun_executable)?
    {
        return Err(readback_error(
            "ready process does not match the first-spawn observation",
        ));
    }
    let endpoint_digest = model::digest(ready.endpoint.as_bytes());
    let proof = json!({
        "schema_version":1,"status":"ready","service_id":plan.service_id,"service_version":VERSION,
        "owner_nonce":plan.owner_nonce,"route_digest":plan.route_digest,
        "process":{"pid":ready.pid,"birth_token":birth,"binary_sha256":plan.bun_sha256},
        "endpoint_digest":endpoint_digest,"connection_digest":ready.connection_digest,
        "config_digest":plan.config_digest,"plugin_module_sha256":plan.module_sha256,
        "plugin_entrypoint_sha256":plan.entrypoint_sha256,"server_program_sha256":plan.server_program_sha256,
        "bun_sha256":plan.bun_sha256,"readiness_observed":true,"plugin_loaded":"unknown","dispatch_permitted":false
    });
    Ok(proof)
}

#[derive(Deserialize)]
struct StopReceipt {
    schema_version: u32,
    owner_nonce: String,
    pid: u32,
    status: String,
    listener_closed: Option<bool>,
    server_fiber: Option<String>,
    runtime_disposed: Option<bool>,
    connection_absent: Option<bool>,
    process_exit_code: Option<i32>,
    completed_at: Option<String>,
}

struct VerifiedStopRecords {
    owner: OwnerRecord,
    owner_sha256: String,
    stop_sha256: String,
}

fn clean_stop_records(
    state_root: &Path,
    owner_nonce: &str,
    model_catalog: &str,
    port: u16,
    connection_file: &Path,
) -> Result<Option<VerifiedStopRecords>> {
    let owner_path = state_root.join("owner.json");
    let stop_path = state_root.join("stop-receipt.json");
    let Some(owner_bytes) = read_optional_regular(&owner_path, MAX_OWNER_BYTES)? else {
        return Ok(None);
    };
    let Some(stop_bytes) = read_optional_regular(&stop_path, MAX_OWNER_BYTES)? else {
        return Ok(None);
    };
    let owner: OwnerRecord = serde_json::from_slice(&owner_bytes)
        .map_err(|_| readback_error("owner receipt is invalid"))?;
    let stop: StopReceipt = serde_json::from_slice(&stop_bytes)
        .map_err(|_| readback_error("stop receipt is invalid"))?;
    let endpoint = owner.endpoint.as_deref();
    let clean_server_fiber =
        |value: Option<&str>| matches!(value, Some("interrupted" | "completed"));
    if owner.schema_version != 1
        || owner.owner_nonce != owner_nonce
        || owner.status != "stopped"
        || owner.runtime != "bun"
        || owner.runtime_version != BUN_VERSION
        || owner.native_server != "@opencode/server"
        || owner.native_server_version != VERSION
        || owner.model_catalog_mode != model_catalog
        || owner.pid == 0
        || owner.connection_file != path_text(connection_file)?
        || owner.stop_receipt != path_text(&stop_path)?
        || owner.database_path != path_text(&state_root.join("data").join("opencode.sqlite"))?
        || !clean_server_fiber(owner.server_fiber.as_deref())
        || owner.listener_closed != Some(true)
        || owner.runtime_disposed != Some(true)
        || owner.connection_absent != Some(true)
        || owner.process_exit_code != Some(0)
        || endpoint.is_none_or(|value| validate_endpoint(value, port).is_err())
        || owner.bound_port != endpoint.and_then(endpoint_port)
        || owner
            .connection_sha256
            .as_deref()
            .is_none_or(|hash| !is_sha256(hash))
        || stop.schema_version != 1
        || stop.owner_nonce != owner_nonce
        || stop.pid != owner.pid
        || stop.status != "stopped"
        || !clean_server_fiber(stop.server_fiber.as_deref())
        || stop.listener_closed != Some(true)
        || stop.runtime_disposed != Some(true)
        || stop.connection_absent != Some(true)
        || stop.process_exit_code != Some(0)
        || stop.completed_at.as_deref().is_none_or(|value| {
            value.len() < 20
                || value.len() > 64
                || value.bytes().any(|byte| byte.is_ascii_control())
        })
        || !path_is_absent(connection_file)?
    {
        return Ok(None);
    }
    Ok(Some(VerifiedStopRecords {
        owner,
        owner_sha256: model::digest(&owner_bytes),
        stop_sha256: model::digest(&stop_bytes),
    }))
}

fn verify_stopped_plan(plan: &HelperPlan) -> Result<()> {
    let Some(receipts) = clean_stop_records(
        &plan.state_root,
        &plan.owner_nonce,
        &plan.model_catalog,
        plan.port,
        &plan.connection_file,
    )?
    else {
        return Err(Error::new(
            "OWNED_SERVICE_STOP_UNKNOWN",
            "stop receipts do not prove the exact owned service stopped",
        ));
    };
    let observation = read_process_observation(
        &plan.state_root.join("process-observation.json"),
        &plan.owner_nonce,
        &plan.route_digest,
        &plan.bun_sha256,
    )?
    .ok_or_else(|| readback_error("private process observation is missing at stop"))?;
    if receipts.owner.pid != observation.pid {
        return Err(readback_error(
            "clean-stop owner PID differs from the spawned process",
        ));
    }
    Ok(())
}

/// Verify departure using only immutable launch observations and the pinned
/// server's exact clean-stop receipts. It never signals or starts a process.
pub(crate) fn observe_departure(
    route: &OwnedServiceRoute,
    stored_proof: &Value,
) -> Result<Option<OwnedServiceDeparture>> {
    // Departure reconciliation is a cheap PID/receipt read. It deliberately
    // does not rehash historical Bun or server binaries on each host sweep.
    validate_directory_components(&route.state_root)?;
    let is_empty = stored_proof
        .as_object()
        .is_some_and(serde_json::Map::is_empty);
    let expected = if is_empty {
        None
    } else {
        Some(OwnedServiceReadback::from_route_value(
            stored_proof,
            route,
            route.owner_nonce(),
        )?)
    };
    let Some(observation) = read_process_observation(
        &route.state_root.join("process-observation.json"),
        route.owner_nonce(),
        &route.route_digest()?,
        route.bun_sha256(),
    )?
    else {
        return Ok(None);
    };
    if let Some(expected) = expected.as_ref()
        && (expected.pid() != observation.pid
            || expected.birth_token() != observation.birth_token
            || expected.binary_sha256() != observation.binary_sha256)
    {
        return Err(readback_error(
            "retained service proof differs from its first-spawn observation",
        ));
    }
    let Some(receipts) = clean_stop_records(
        &route.state_root,
        route.owner_nonce(),
        &route.model_catalog,
        route.port,
        &route.connection_file,
    )?
    else {
        return Ok(None);
    };
    if receipts.owner.pid != observation.pid
        || expected.as_ref().is_some_and(|proof| {
            receipts.owner.connection_sha256.as_deref() != Some(proof.connection_digest())
        })
    {
        return Err(readback_error(
            "clean-stop owner receipt differs from the exact launched process",
        ));
    }
    if let Some(current_birth) = process_birth_identity(observation.pid)?
        && process_birth_token(&current_birth)? == observation.birth_token
    {
        return Ok(None);
    }
    let Some(helper_family_stop_sha256) = helper_family_departed(route)? else {
        return Ok(None);
    };
    Ok(Some(departure_from(
        &observation,
        route,
        &receipts,
        &helper_family_stop_sha256,
    )?))
}

fn departure_from(
    observation: &ProcessObservation,
    route: &OwnedServiceRoute,
    receipts: &VerifiedStopRecords,
    helper_family_stop_sha256: &str,
) -> Result<OwnedServiceDeparture> {
    let stop_receipts_sha256 = model::digest(
        model::canonical(&json!({
            "server_stop_receipt_sha256":receipts.stop_sha256,
            "helper_family_stop_receipt_sha256":helper_family_stop_sha256,
        }))?
        .as_bytes(),
    );
    let proof = json!({
        "schema_version":1,
        "status":"departed",
        "service_id":route.service_id,
        "service_version":VERSION,
        "owner_nonce":route.owner_nonce,
        "route_digest":route.route_digest()?,
        "process":{"pid":observation.pid,"birth_token":observation.birth_token,"binary_sha256":observation.binary_sha256},
        "owner_receipt_sha256":receipts.owner_sha256,
        "stop_receipts_sha256":stop_receipts_sha256,
        "listener_closed":true,
        "runtime_disposed":true,
        "connection_absent":true,
        "process_absent":true
    });
    if model::canonical(&proof)?.len() > MAX_OBSERVATION_BYTES {
        return Err(readback_error(
            "owned service departure proof exceeds its bound",
        ));
    }
    Ok(OwnedServiceDeparture { proof })
}

fn read_owned_files(route: &OwnedServiceRoute) -> Result<Option<OwnedServiceReadback>> {
    let owner_path = route.state_root.join("owner.json");
    if !owner_path.exists() {
        return Ok(None);
    }
    let plan = HelperPlan {
        schema_version: 1,
        owner_nonce: route.owner_nonce.clone(),
        service_id: route.service_id.clone(),
        version: VERSION.into(),
        model: route.model.clone(),
        model_catalog: route.model_catalog.clone(),
        bun_executable: route.bun_executable.clone(),
        bun_sha256: route.bun_sha256.clone(),
        server_program: route.server_program.clone(),
        server_program_sha256: route.server_program_sha256.clone(),
        state_root: route.state_root.clone(),
        password_file: route.password_file.clone(),
        connection_file: route.connection_file.clone(),
        config_file: route.config_file.clone(),
        workspace_directory: route.workspace_directory.clone(),
        port: route.port,
        route_digest: route.route_digest()?,
        config_digest: String::new(),
        module_sha256: String::new(),
        entrypoint_sha256: String::new(),
    };
    let (owner, connection, digest) =
        match load_owner_connection(&plan, &owner_path, &route.connection_file) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
    if owner.status != "ready"
        || connection.username != "opencode"
        || connection.password.len() < 32
    {
        return Ok(None);
    }
    let endpoint = validate_endpoint(&connection.endpoint, route.port)?;
    let identity = process_image_identity(owner.pid)?;
    verify_image_identity(&identity, owner.pid, &route.bun_sha256)?;
    let birth_token = process_birth_token(&identity)?;
    let observation = read_process_observation(
        &route.state_root.join("process-observation.json"),
        &route.owner_nonce,
        &route.route_digest()?,
        &route.bun_sha256,
    )?
    .ok_or_else(|| readback_error("private process observation is missing"))?;
    if observation.pid != owner.pid || observation.birth_token != birth_token {
        return Err(readback_error(
            "owner process differs from its first-spawn observation",
        ));
    }
    let (_, config_digest, module_sha, entrypoint_sha) = verify_config_file(route)?;
    let proof = json!({
        "schema_version":1,"status":"ready","service_id":route.service_id,"service_version":VERSION,
        "owner_nonce":route.owner_nonce,"route_digest":route.route_digest()?,
        "process":{"pid":owner.pid,"birth_token":birth_token,"binary_sha256":route.bun_sha256},
        "endpoint_digest":model::digest(endpoint.as_bytes()),"connection_digest":digest,"config_digest":config_digest,
        "plugin_module_sha256":module_sha,"plugin_entrypoint_sha256":entrypoint_sha,
        "server_program_sha256":route.server_program_sha256,"bun_sha256":route.bun_sha256,
        "readiness_observed":true,"plugin_loaded":"unknown","dispatch_permitted":false
    });
    Ok(Some(OwnedServiceReadback { proof }))
}

fn verify_config_file(route: &OwnedServiceRoute) -> Result<(Value, String, String, String)> {
    let bytes = regular_file(&route.config_file, MAX_CONFIG_BYTES)?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| source_error("owned OpenCode config is invalid"))?;
    let canonical = model::canonical(&value)?;
    let config_digest = model::digest(canonical.as_bytes());
    let (module_sha, entry_sha) =
        mcp_plugin::verify_plugin_config_value(&value, &route.service_id)?;
    Ok((value, config_digest, module_sha, entry_sha))
}

fn read_plan(path: &Path) -> Result<HelperPlan> {
    if !path.is_absolute() {
        return Err(config_error("owned helper plan must be absolute"));
    }
    let bytes = regular_file(path, MAX_PLAN_BYTES)?;
    serde_json::from_slice(&bytes).map_err(|_| config_error("owned helper plan is invalid"))
}

fn validate_plan(plan: &HelperPlan, plan_path: &Path) -> Result<()> {
    if plan.schema_version != 1
        || plan.version != VERSION
        || !valid_uuid(&plan.owner_nonce)
        || plan.service_id.is_empty()
        || plan.service_id.len() > 128
        || plan.model_catalog != "offline" && plan.model_catalog != "refresh"
        || !plan.model.valid()
        || !is_sha256(&plan.route_digest)
        || !is_sha256(&plan.config_digest)
        || !is_sha256(&plan.module_sha256)
        || !is_sha256(&plan.entrypoint_sha256)
    {
        return Err(config_error(
            "owned helper plan failed its bounded schema checks",
        ));
    }
    let expected_plan = plan.state_root.join(".owned-launch-plan.json");
    if !same_lexical_path(plan_path, &expected_plan)? {
        return Err(config_error(
            "owned helper plan is outside the exact private state root",
        ));
    }
    if !same_lexical_path(
        &plan.connection_file,
        &plan.state_root.join("connection.json"),
    )? || !same_lexical_path(
        &plan.password_file,
        &plan.state_root.join("server.password"),
    )? || !same_lexical_path(
        &plan.config_file,
        &plan
            .state_root
            .join("config")
            .join("opencode")
            .join("opencode.json"),
    )? || plan.workspace_directory.is_relative()
    {
        return Err(config_error(
            "owned helper paths do not match its exact plan",
        ));
    }
    let bun = canonical_regular_file(&plan.bun_executable, 512 * 1024 * 1024)?;
    let server = canonical_regular_file(&plan.server_program, 512 * 1024)?;
    if !same_lexical_path(&bun, &plan.bun_executable)?
        || !same_lexical_path(&server, &plan.server_program)?
        || hash_file(&bun, 512 * 1024 * 1024)? != plan.bun_sha256
        || hash_file(&server, 512 * 1024)? != plan.server_program_sha256
    {
        return Err(config_error(
            "owned helper executable identity changed before spawn",
        ));
    }
    let workspace = canonical_directory(&plan.workspace_directory)?;
    let state_root = canonical_directory(&plan.state_root)?;
    if !same_lexical_path(&workspace, &plan.workspace_directory)?
        || !same_lexical_path(&state_root, &plan.state_root)?
    {
        return Err(config_error(
            "owned helper workspace or state root was redirected",
        ));
    }
    let (_, digest, module_sha, entrypoint_sha) = verify_plan_config(plan)?;
    if digest != plan.config_digest
        || module_sha != plan.module_sha256
        || entrypoint_sha != plan.entrypoint_sha256
        || route_digest_for_plan(plan)? != plan.route_digest
    {
        return Err(config_error(
            "owned helper plan no longer matches its prepared files",
        ));
    }
    Ok(())
}

fn verify_plan_config(plan: &HelperPlan) -> Result<(Value, String, String, String)> {
    let bytes = regular_file(&plan.config_file, MAX_CONFIG_BYTES)?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| source_error("owned config is invalid"))?;
    let config_digest = model::digest(model::canonical(&value)?.as_bytes());
    let (module_sha, entry_sha) = mcp_plugin::verify_plugin_config_value(&value, &plan.service_id)?;
    Ok((value, config_digest, module_sha, entry_sha))
}

fn route_digest_for_plan(plan: &HelperPlan) -> Result<String> {
    let value = json!({
        "origin":"fresh_owned_service","service_id":plan.service_id,"version":VERSION,"model":plan.model,
        "model_catalog":plan.model_catalog,"bun_executable":path_text(&plan.bun_executable)?,"bun_sha256":plan.bun_sha256,
        "server_program":path_text(&plan.server_program)?,"server_program_sha256":plan.server_program_sha256,
        "state_root":path_text(&plan.state_root)?,"base_state_root":path_text(plan.state_root.parent().and_then(Path::parent).ok_or_else(||config_error("state root parent missing"))?)?,
        "port":plan.port,"owner_nonce":plan.owner_nonce,"workspace_directory":path_text(&plan.workspace_directory)?,
        "password_file":path_text(&plan.password_file)?,"connection_file":path_text(&plan.connection_file)?,"config_file":path_text(&plan.config_file)?,
    });
    Ok(model::digest(model::canonical(&value)?.as_bytes()))
}

fn verify_process_identity(
    route: &OwnedServiceRoute,
    identity: &Value,
    pid: u32,
    expected_hash: &str,
    expected_birth: &str,
) -> Result<()> {
    verify_image_identity(identity, pid, expected_hash)?;
    if process_birth_token(identity)? != expected_birth {
        return Err(readback_error("owned process birth identity changed"));
    }
    let observed = identity["image_path"]
        .as_str()
        .ok_or_else(|| readback_error("owned process image path is unavailable"))?;
    let observed = canonical_regular_file(Path::new(observed), 512 * 1024 * 1024)?;
    if !same_lexical_path(&observed, &route.bun_executable)? {
        return Err(readback_error(
            "owned process executable path differs from its configured route",
        ));
    }
    Ok(())
}
fn verify_image_identity(identity: &Value, pid: u32, expected_hash: &str) -> Result<()> {
    if identity["pid"].as_u64() != Some(pid as u64)
        || identity["image_sha256"].as_str() != Some(image_digest(expected_hash).as_str())
        || identity["image_path"]
            .as_str()
            .is_none_or(|path| path.is_empty() || path.len() > MAX_PATH_BYTES)
    {
        return Err(readback_error(
            "owned process image does not match the configured Bun executable",
        ));
    }
    Ok(())
}
pub(crate) fn process_birth_token(identity: &Value) -> Result<String> {
    let birth = if let Some(value) = identity["creation_filetime"].as_str() {
        json!({"platform":"windows","pid":identity["pid"],"creation_filetime":value})
    } else {
        let boot = identity["boot_id"]
            .as_str()
            .ok_or_else(|| readback_error("process boot identity is unavailable"))?;
        let ticks = identity["start_ticks"]
            .as_str()
            .ok_or_else(|| readback_error("process start identity is unavailable"))?;
        if boot.is_empty() || ticks.is_empty() {
            return Err(readback_error("process birth token is invalid"));
        }
        json!({"platform":"linux","pid":identity["pid"],"boot_id":boot,"start_ticks":ticks})
    };
    Ok(model::digest(model::canonical(&birth)?.as_bytes()))
}
fn image_digest(config_hash: &str) -> String {
    format!(
        "sha256:{}",
        config_hash.strip_prefix("sha256:").unwrap_or(config_hash)
    )
}

fn validate_endpoint(text: &str, requested_port: u16) -> Result<String> {
    if text.len() > 128 || !text.starts_with("http://127.0.0.1:") || text.contains(['@', '?', '#'])
    {
        return Err(readback_error(
            "owned service endpoint is not exact loopback HTTP",
        ));
    }
    let url = super::http::endpoint(text)?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| readback_error("owned endpoint has no port"))?;
    if requested_port != 0 && requested_port != port {
        return Err(readback_error("owned service bound a different port"));
    }
    Ok(text.to_owned())
}
fn endpoint_port(text: &str) -> Option<u16> {
    text.parse::<reqwest::Url>().ok()?.port_or_known_default()
}

fn ensure_directory_tree(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(config_error("private owned state path must be absolute"));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::Normal(part) => {
                current.push(part);
                match fs::symlink_metadata(&current) {
                    Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {
                        let canonical = fs::canonicalize(&current)?;
                        if !same_lexical_path(&canonical, &current)? {
                            return Err(config_error("private state path was redirected"));
                        }
                    }
                    Ok(_) => {
                        return Err(config_error(
                            "private state path contains a non-directory component",
                        ));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        fs::create_dir(&current)?;
                        private_permissions(&current, true)?;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(config_error(
                    "private state path cannot traverse parent directories",
                ));
            }
        }
    }
    Ok(())
}

fn safe_helper_environment() -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let allowed = [
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
    ];
    std::env::vars_os()
        .filter(|(key, _)| {
            key.to_str().is_some_and(|key| {
                allowed
                    .iter()
                    .any(|allowed| key.eq_ignore_ascii_case(allowed))
            })
        })
        .collect()
}
fn safe_bun_environment(state_root: &Path) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let mut values = safe_helper_environment();
    let private_home = state_root.join("home");
    let private_tmp = state_root.join("tmp");
    let private_config = state_root.join("config");
    let private_data = state_root.join("data");
    let private_cache = state_root.join("cache");
    for (key, path) in [
        ("HOME", &private_home),
        ("USERPROFILE", &private_home),
        ("APPDATA", &private_home),
        ("LOCALAPPDATA", &private_home),
        ("TMP", &private_tmp),
        ("TEMP", &private_tmp),
        ("TMPDIR", &private_tmp),
        ("XDG_CONFIG_HOME", &private_config),
        ("XDG_DATA_HOME", &private_data),
        ("XDG_CACHE_HOME", &private_cache),
    ] {
        values.push((key.into(), path.as_os_str().into()));
    }
    values
}

fn canonical_regular_file(path: &Path, maximum: u64) -> Result<PathBuf> {
    let lexical = lexical_absolute(path)?;
    let metadata = fs::symlink_metadata(&lexical)
        .map_err(|_| config_error("configured executable is unavailable"))?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(config_error(
            "configured executable is not a bounded regular file",
        ));
    }
    let canonical = fs::canonicalize(&lexical)?;
    if !same_lexical_path(&canonical, &lexical)? {
        return Err(config_error("configured executable path was redirected"));
    }
    Ok(canonical)
}
fn canonical_directory(path: &Path) -> Result<PathBuf> {
    let lexical = lexical_absolute(path)?;
    let metadata = fs::symlink_metadata(&lexical)
        .map_err(|_| config_error("configured owned directory must already exist"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(config_error(
            "configured directory is not a plain directory",
        ));
    }
    let canonical = fs::canonicalize(&lexical)?;
    if !same_lexical_path(&canonical, &lexical)? {
        return Err(config_error("configured directory path was redirected"));
    }
    Ok(canonical)
}
fn same_lexical_path(left: &Path, right: &Path) -> Result<bool> {
    let left = lexical_absolute(left)?;
    let right = lexical_absolute(right)?;
    #[cfg(windows)]
    {
        let key = |path: &Path| {
            let text = path.to_string_lossy().replace('/', "\\");
            if let Some(unc) = text.strip_prefix("\\\\?\\UNC\\") {
                format!("\\\\{unc}")
            } else if let Some(local) = text.strip_prefix("\\\\?\\") {
                local.to_owned()
            } else {
                text
            }
        };
        Ok(key(&left).eq_ignore_ascii_case(&key(&right)))
    }
    #[cfg(not(windows))]
    {
        Ok(left == right)
    }
}
fn lexical_absolute(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(config_error("path must be absolute"));
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(config_error("path cannot traverse parent directories"));
            }
            Component::Normal(part) => out.push(part),
        }
    }
    Ok(out)
}

fn validate_directory_components(path: &Path) -> Result<()> {
    let path = lexical_absolute(path)?;
    let mut current = PathBuf::new();
    let mut missing = false;
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(config_error("path cannot traverse parent directories"));
            }
            Component::Normal(part) => {
                current.push(part);
                if missing {
                    continue;
                }
                match fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                        let canonical = fs::canonicalize(&current)?;
                        if !same_lexical_path(&canonical, &current)? {
                            return Err(config_error(
                                "owned directory path contains a redirected component",
                            ));
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => missing = true,
                    _ => {
                        return Err(config_error(
                            "owned directory path contains a non-directory or reparse component",
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
fn hash_file(path: &Path, maximum: u64) -> Result<String> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(config_error("file exceeds the owned service hash boundary"));
    }
    let bytes = fs::read(path)?;
    if bytes.len() as u64 != metadata.len() || fs::metadata(path)?.len() != metadata.len() {
        return Err(config_error("file changed while its digest was checked"));
    }
    Ok(model::digest(&bytes))
}
fn regular_file(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() as usize > maximum
    {
        return Err(readback_error(
            "private receipt is not a bounded regular file",
        ));
    }
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(readback_error("private receipt changed while opening"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum || bytes.len() as u64 != metadata.len() {
        return Err(readback_error("private receipt exceeds its read bound"));
    }
    Ok(bytes)
}
fn read_optional_regular(path: &Path, maximum: usize) -> Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path) {
        Ok(_) => regular_file(path, maximum).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
fn path_is_absent(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error.into()),
    }
}
fn path_text(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .ok_or_else(|| config_error("owned path is not valid Unicode"))?;
    if value.len() > MAX_PATH_BYTES || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(config_error("owned path exceeds its retained bound"));
    }
    Ok(value.to_owned())
}
fn normalize_sha256(value: &str) -> Result<String> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(config_error("configured SHA-256 is malformed"));
    }
    Ok(value.to_ascii_lowercase())
}
fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn valid_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23].iter().all(|i| bytes[*i] == b'-')
        && bytes.iter().enumerate().all(|(i, b)| {
            [8, 13, 18, 23].contains(&i) || b.is_ascii_hexdigit() && !b.is_ascii_uppercase()
        })
        && bytes[14] == b'4'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
}
fn validate_identity(field: &str, value: &str, limit: usize) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > limit
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(config_error(&format!("invalid owned service {field}")));
    }
    Ok(())
}
fn write_private_new(path: &Path, body: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    private_permissions(path, false)?;
    file.write_all(body)?;
    file.sync_all()?;
    Ok(())
}
fn config_error(message: &str) -> Error {
    Error::new("OWNED_SERVICE_CONFIG", message)
}
fn scope_error(message: &str) -> Error {
    Error::new("OWNED_SERVICE_SCOPE", message)
}
fn source_error(message: &str) -> Error {
    Error::new("OWNED_SERVICE_SOURCE", message)
}
fn readback_error(message: &str) -> Error {
    Error::new("OWNED_SERVICE_READBACK", message)
}
