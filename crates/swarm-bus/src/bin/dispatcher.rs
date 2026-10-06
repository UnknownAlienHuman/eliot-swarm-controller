//! Standalone ScriptRun consumer. It keeps only its scoped Module credential
//! while polling; the Manager credential is used by `enroll`/`revoke` only.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    env,
    fs::{self, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};
use swarm_bus::service_scope::{managed_service_state_path, managed_worker_config_path};
use swarm_client::{Client, IpcConfig};
use swarm_contracts::{
    Credential, DeclaredServicePurpose, DeclaredServiceScope,
    error::{Error, Result},
};
#[cfg(windows)]
use swarm_process::private_permissions;
use swarm_process::{Group, process_image_identity, write_private_new};
use tokio::time::sleep;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

const MAX_CONFIG_BYTES: u64 = 8 * 1024;
const DEFAULT_POLL_MS: u64 = 1_000;
const MIN_POLL_MS: u64 = 100;
const MAX_POLL_MS: u64 = 60_000;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    schema_version: u32,
    store_root: PathBuf,
    manager_id: String,
    project_id: String,
    automation_id: String,
    register_request_id: String,
    poll_interval_ms: u64,
    credential: Credential,
    #[serde(default)]
    managed_service: bool,
}

impl Drop for WorkerConfig {
    fn drop(&mut self) {
        self.credential.token.zeroize();
    }
}

struct SecretCredential(Credential);

impl Drop for SecretCredential {
    fn drop(&mut self) {
        self.0.token.zeroize();
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_cli().await {
        eprintln!("swarm-bus-dispatcher: {}", error.code);
        std::process::exit(2);
    }
}

async fn run_cli() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let mode = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| Error::invalid("expected enroll, run, or revoke"))?;
    let options = parse_options(args.collect())?;
    match mode.as_str() {
        "enroll" => enroll(&options).await,
        "run" => run_worker(&options).await,
        "revoke" => revoke(&options).await,
        _ => Err(Error::invalid("expected enroll, run, or revoke")),
    }
}

fn parse_options(
    args: Vec<std::ffi::OsString>,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut result = std::collections::BTreeMap::new();
    let mut index = 0;
    while index < args.len() {
        let key = args[index]
            .to_str()
            .filter(|value| value.starts_with("--"))
            .ok_or_else(|| Error::invalid("options must use --name value form"))?;
        let value = args
            .get(index + 1)
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::invalid("option is missing its value"))?;
        if result.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err(Error::invalid("duplicate option"));
        }
        index += 2;
    }
    Ok(result)
}

fn required<'a>(
    options: &'a std::collections::BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str> {
    options
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| Error::invalid(format!("missing {name}")))
}

fn reject_unknown_options(
    options: &std::collections::BTreeMap<String, String>,
    allowed: &[&str],
) -> Result<()> {
    if options.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(Error::invalid("unknown command option"));
    }
    Ok(())
}

async fn enroll(options: &std::collections::BTreeMap<String, String>) -> Result<()> {
    reject_unknown_options(
        options,
        &[
            "--root",
            "--manager-credential",
            "--project",
            "--automation",
            "--worker-config",
            "--poll-ms",
            "--managed-service",
            "--consumer-client-id",
        ],
    )?;
    if parse_bool_option(options.get("--managed-service"), false)? {
        return enroll_managed(options).await;
    }
    if options.contains_key("--consumer-client-id") {
        return Err(Error::invalid(
            "--consumer-client-id is available only with --managed-service true",
        ));
    }
    let root = PathBuf::from(required(options, "--root")?);
    let project_id = required(options, "--project")?.to_owned();
    let automation_id = required(options, "--automation")?.to_owned();
    let output_path = PathBuf::from(required(options, "--worker-config")?);
    let poll_interval_ms = options
        .get("--poll-ms")
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| Error::invalid("poll interval is invalid"))?
        .unwrap_or(DEFAULT_POLL_MS);
    if !(MIN_POLL_MS..=MAX_POLL_MS).contains(&poll_interval_ms) {
        return Err(Error::invalid("poll interval must be 100..=60000 ms"));
    }

    let manager_path = PathBuf::from(required(options, "--manager-credential")?);
    let mut manager = SecretCredential(read_credential(&manager_path)?);
    let mut worker = if output_path.exists() {
        let worker = read_worker_config(&output_path)?;
        if worker.schema_version != 1
            || worker.managed_service
            || worker.store_root != root
            || worker.manager_id != manager.0.client_id
            || worker.project_id != project_id
            || worker.automation_id != automation_id
            || worker.poll_interval_ms != poll_interval_ms
        {
            return Err(Error::new(
                "BUS_WORKER_CONFIG_CONFLICT",
                "existing private worker config does not match the requested scope",
            ));
        }
        worker
    } else {
        let consumer_client_id = format!("bus-script-{}", Uuid::new_v4().simple());
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let register_request_id = format!("bus-register-{consumer_client_id}");
        let worker = WorkerConfig {
            schema_version: 1,
            store_root: root,
            manager_id: manager.0.client_id.clone(),
            project_id,
            automation_id,
            register_request_id,
            poll_interval_ms,
            credential: Credential {
                client_id: consumer_client_id,
                token,
            },
            managed_service: false,
        };
        write_worker_config(&output_path, &worker)?;
        worker
    };
    let token_hash = hex_sha256(worker.credential.token.as_bytes());
    let params = json!({
        "client_request_id":worker.register_request_id,
        "project_id":worker.project_id,
        "automation_id":worker.automation_id,
        "consumer_client_id":worker.credential.client_id,
        "token_hash":token_hash,
        "managed_service":false,
    });
    let result = call_once(
        &worker.store_root,
        &manager.0,
        "bus.consumer.register",
        params,
    )
    .await;
    manager.0.token.zeroize();
    match result {
        Ok(value) if value["registered"] == true => {
            println!("consumer registered; private credential saved at the requested path");
            worker.credential.token.zeroize();
            Ok(())
        }
        Ok(_) => Err(Error::new(
            "BUS_REGISTER_RECEIPT_INVALID",
            "kernel returned no registered consumer receipt",
        )),
        Err(error) => Err(error),
    }
}

/// Create an opt-in enrollment under a host-derived private path. The selected
/// consumer receives the same exact two-method Module scope as a normal
/// enrollment; only its lifecycle demand bit and pinned config digest differ.
async fn enroll_managed(options: &std::collections::BTreeMap<String, String>) -> Result<()> {
    reject_unknown_options(
        options,
        &[
            "--root",
            "--manager-credential",
            "--project",
            "--automation",
            "--poll-ms",
            "--managed-service",
            "--consumer-client-id",
        ],
    )?;
    let root = fs::canonicalize(required(options, "--root")?)?;
    let project_id = required(options, "--project")?.to_owned();
    let automation_id = required(options, "--automation")?.to_owned();
    let poll_interval_ms = options
        .get("--poll-ms")
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| Error::invalid("poll interval is invalid"))?
        .unwrap_or(DEFAULT_POLL_MS);
    if !(MIN_POLL_MS..=MAX_POLL_MS).contains(&poll_interval_ms) {
        return Err(Error::invalid("poll interval must be 100..=60000 ms"));
    }

    let manager_path = PathBuf::from(required(options, "--manager-credential")?);
    let mut manager = SecretCredential(read_credential(&manager_path)?);
    let consumer_client_id = options
        .get("--consumer-client-id")
        .cloned()
        .unwrap_or_else(|| format!("bus-script-{}", Uuid::new_v4().simple()));
    validate_consumer_client_id(&consumer_client_id)?;
    let output_path = managed_worker_config_path(&root, &consumer_client_id)?;
    ensure_private_directory(
        &root,
        output_path
            .parent()
            .ok_or_else(|| Error::invalid("managed worker config path has no parent directory"))?,
    )?;

    let mut worker = if output_path.exists() {
        let worker = read_worker_config(&output_path)?;
        if worker.schema_version != 1
            || !worker.managed_service
            || fs::canonicalize(&worker.store_root)? != root
            || worker.manager_id != manager.0.client_id
            || worker.project_id != project_id
            || worker.automation_id != automation_id
            || worker.credential.client_id != consumer_client_id
            || worker.poll_interval_ms != poll_interval_ms
        {
            return Err(Error::new(
                "BUS_WORKER_CONFIG_CONFLICT",
                "existing managed private config does not match the requested scope",
            ));
        }
        worker
    } else {
        let worker = WorkerConfig {
            schema_version: 1,
            store_root: root.clone(),
            manager_id: manager.0.client_id.clone(),
            project_id,
            automation_id,
            register_request_id: format!(
                "bus-register-{}",
                hex_sha256(consumer_client_id.as_bytes())
            ),
            poll_interval_ms,
            credential: Credential {
                client_id: consumer_client_id.clone(),
                token: format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple()),
            },
            managed_service: true,
        };
        write_worker_config(&output_path, &worker)?;
        worker
    };
    let worker_config_sha256 = worker_file_sha256(&output_path)?;
    let token_hash = hex_sha256(worker.credential.token.as_bytes());
    let params = json!({
        "client_request_id":worker.register_request_id,
        "project_id":worker.project_id,
        "automation_id":worker.automation_id,
        "consumer_client_id":worker.credential.client_id,
        "token_hash":token_hash,
        "managed_service":true,
        "worker_config_sha256":worker_config_sha256,
    });
    let result = call_once(&root, &manager.0, "bus.consumer.register", params).await;
    manager.0.token.zeroize();
    match result {
        Ok(value) if value["registered"] == true => {
            let generation = value["service_generation"].as_u64().ok_or_else(|| {
                Error::new(
                    "BUS_SERVICE_RECEIPT_INVALID",
                    "managed enrollment omitted its persisted generation",
                )
            })?;
            println!(
                "managed consumer {} enrolled as service generation {}; private config: {}",
                worker.credential.client_id,
                generation,
                output_path.display()
            );
            worker.credential.token.zeroize();
            Ok(())
        }
        Ok(_) => Err(Error::new(
            "BUS_REGISTER_RECEIPT_INVALID",
            "kernel returned no registered consumer receipt",
        )),
        Err(error) => Err(error),
    }
}

fn validate_consumer_client_id(value: &str) -> Result<()> {
    if !value.starts_with("bus-script-")
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
    {
        return Err(Error::invalid("consumer client ID is invalid"));
    }
    Ok(())
}

fn parse_bool_option(value: Option<&String>, default: bool) -> Result<bool> {
    match value.map(String::as_str) {
        None => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(Error::invalid("managed-service must be true or false")),
    }
}

fn ensure_private_directory(root: &Path, path: &Path) -> Result<()> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| Error::invalid("managed worker directory must stay under the Store root"))?;
    if !root.is_absolute() || !path.is_absolute() || relative.as_os_str().is_empty() {
        return Err(Error::invalid("managed worker directory must be absolute"));
    }
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(Error::invalid("managed worker directory path is invalid"));
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if !metadata.is_dir() || is_link_or_reparse(&metadata) => {
                return Err(Error::new(
                    "BUS_WORKER_CONFIG_INVALID",
                    "managed worker directory traverses a link or non-directory",
                ));
            }
            Ok(_metadata) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if _metadata.permissions().mode() & 0o077 != 0 {
                        return Err(Error::new(
                            "BUS_WORKER_CONFIG_NOT_PRIVATE",
                            "managed worker directory is not owner-only",
                        ));
                    }
                }
                #[cfg(windows)]
                private_permissions(&current, true)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&current, fs::Permissions::from_mode(0o700))?;
                }
                #[cfg(windows)]
                private_permissions(&current, true)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn worker_file_sha256(path: &Path) -> Result<String> {
    let mut bytes = read_private_file(path)?;
    let digest = hex_sha256(&bytes);
    bytes.zeroize();
    Ok(digest)
}

async fn revoke(options: &std::collections::BTreeMap<String, String>) -> Result<()> {
    reject_unknown_options(
        options,
        &["--root", "--manager-credential", "--worker-config"],
    )?;
    let root = PathBuf::from(required(options, "--root")?);
    let manager_path = PathBuf::from(required(options, "--manager-credential")?);
    let worker_path = PathBuf::from(required(options, "--worker-config")?);
    let mut manager = SecretCredential(read_credential(&manager_path)?);
    let worker = read_worker_config(&worker_path)?;
    if worker.store_root != root || worker.manager_id != manager.0.client_id {
        return Err(Error::new(
            "BUS_WORKER_CONFIG_CONFLICT",
            "manager credential does not own this worker config",
        ));
    }
    let params = json!({
        "client_request_id":format!("bus-revoke-{}", worker.credential.client_id),
        "project_id":worker.project_id,
        "automation_id":worker.automation_id,
        "consumer_client_id":worker.credential.client_id,
    });
    let result = call_once(&root, &manager.0, "bus.consumer.revoke", params).await;
    manager.0.token.zeroize();
    let value = result?;
    if value["revoked"] != true {
        return Err(Error::new(
            "BUS_REVOKE_RECEIPT_INVALID",
            "kernel returned no revoked consumer receipt",
        ));
    }
    println!("scoped bus consumer revoked");
    Ok(())
}

async fn run_worker(options: &std::collections::BTreeMap<String, String>) -> Result<()> {
    reject_unknown_options(
        options,
        &[
            "--worker-config",
            "--managed-owner-dir",
            "--service-generation",
            "--worker-config-sha256",
            "--credential-token-sha256",
            "--scope-digest",
        ],
    )?;
    let (worker, config_sha256) =
        read_worker_config_with_digest(Path::new(required(options, "--worker-config")?))?;
    if worker.schema_version != 1
        || !(MIN_POLL_MS..=MAX_POLL_MS).contains(&worker.poll_interval_ms)
        || worker.manager_id.is_empty()
        || worker.project_id.is_empty()
        || worker.automation_id.is_empty()
        || !worker.credential.client_id.starts_with("bus-script-")
        || worker.credential.token.is_empty()
    {
        return Err(Error::new(
            "BUS_WORKER_CONFIG_INVALID",
            "private worker config identity or bounds are invalid",
        ));
    }

    let owner_dir = options.get("--managed-owner-dir").map(PathBuf::from);
    let generation = options
        .get("--service-generation")
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| Error::invalid("service generation is invalid"))?;
    let expected_config_sha256 = options.get("--worker-config-sha256");
    let expected_token_sha256 = options.get("--credential-token-sha256");
    let expected_scope_digest = options.get("--scope-digest");
    match (
        worker.managed_service,
        owner_dir,
        generation,
        expected_config_sha256,
        expected_token_sha256,
        expected_scope_digest,
    ) {
        (
            true,
            Some(owner_dir),
            Some(generation),
            Some(config_hash),
            Some(token_hash),
            Some(scope_digest),
        ) if generation > 0
            && is_sha256(config_hash)
            && is_sha256(token_hash)
            && is_sha256(scope_digest)
            && config_sha256.eq_ignore_ascii_case(config_hash)
            && hex_sha256(worker.credential.token.as_bytes()).eq_ignore_ascii_case(token_hash) =>
        {
            return run_managed_worker(
                worker,
                owner_dir,
                generation,
                config_sha256,
                token_hash.to_ascii_lowercase(),
                scope_digest.to_ascii_lowercase(),
            )
            .await;
        }
        (true, _, _, _, _, _) => {
            return Err(Error::new(
                "BUS_SERVICE_OWNER_REQUIRED",
                "managed worker requires its exact host-provided owner and integrity metadata",
            ));
        }
        (false, None, None, None, None, None) => {}
        (false, _, _, _, _, _) => {
            return Err(Error::new(
                "BUS_SERVICE_OWNER_UNEXPECTED",
                "unmanaged worker cannot join a host-owned service scope",
            ));
        }
    }

    run_unmanaged_worker(worker).await
}

async fn run_unmanaged_worker(worker: WorkerConfig) -> Result<()> {
    loop {
        match poll_once(&worker).await {
            Ok(true) => continue,
            Ok(false) => sleep(Duration::from_millis(worker.poll_interval_ms)).await,
            Err(error) if retryable_transport_code(&error.code) => {
                eprintln!(
                    "swarm-bus-dispatcher: {}; retrying by rereading cursor",
                    error.code
                );
                sleep(Duration::from_millis(worker.poll_interval_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn run_managed_worker(
    worker: WorkerConfig,
    owner_dir: PathBuf,
    generation: u64,
    worker_config_sha256: String,
    credential_token_sha256: String,
    scope_digest: String,
) -> Result<()> {
    let scope = DeclaredServiceScope::new(
        DeclaredServicePurpose::BusConsumer,
        worker.credential.client_id.clone(),
        generation,
    )?;
    let expected_owner_dir = managed_service_state_path(&worker.store_root, &scope)?;
    if owner_dir != expected_owner_dir {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_SCOPE_MISMATCH",
            "managed owner directory does not match the declared service scope",
        ));
    }
    let metadata = fs::symlink_metadata(&owner_dir)?;
    if !metadata.is_dir() || is_link_or_reparse(&metadata) {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
            "managed service owner directory is not a regular directory",
        ));
    }
    let owner_path = owner_dir.join("owner.json");
    if owner_path.exists() {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_EXISTS",
            "managed service owner receipt must be reconciled before start",
        ));
    }
    let token = Uuid::new_v4().simple().to_string();
    let group = Group::enter_service(&token, scope.purpose)?;
    let worker_image = process_image_identity(std::process::id())?;
    let receipt = json!({
        "schema_version":2,
        "scope":scope,
        "owner":{"version":1,"token":token,"process":group.identity},
        "worker_image":worker_image,
        "worker_config_sha256":worker_config_sha256,
        "credential_token_sha256":credential_token_sha256,
        "scope_digest":scope_digest,
    });
    write_private_new(
        &owner_path,
        Zeroizing::new(serde_json::to_vec(&receipt)?).as_slice(),
    )?;

    loop {
        if stop_requested(&owner_dir)? {
            if !group.children_empty()? {
                return Err(Error::new(
                    "BUS_SERVICE_DESCENDANTS_ACTIVE",
                    "managed dispatcher still has an untracked process-family member",
                ));
            }
            return Ok(());
        }
        match poll_once(&worker).await {
            Ok(true) => continue,
            Ok(false) => sleep(Duration::from_millis(worker.poll_interval_ms)).await,
            Err(error) if retryable_transport_code(&error.code) => {
                eprintln!(
                    "swarm-bus-dispatcher: {}; retrying by rereading cursor",
                    error.code
                );
                sleep(Duration::from_millis(worker.poll_interval_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn stop_requested(owner_dir: &Path) -> Result<bool> {
    let path = owner_dir.join("stop.request");
    match fs::symlink_metadata(&path) {
        Ok(metadata)
            if metadata.is_file() && !is_link_or_reparse(&metadata) && metadata.len() == 0 =>
        {
            Ok(true)
        }
        Ok(_) => Err(Error::new(
            "BUS_SERVICE_STOP_REQUEST_INVALID",
            "managed service stop marker is not a private empty file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Returns true when a page cut was attempted, so the next turn immediately
/// rereads the authoritative cursor instead of assuming the reply committed.
async fn poll_once(worker: &WorkerConfig) -> Result<bool> {
    let page = call_once(
        &worker.store_root,
        &worker.credential,
        "bus.events.page",
        json!({
            "project_id":worker.project_id,
            "consumer_id":worker.automation_id,
            "limit":32,
        }),
    )
    .await?;
    let expected_cursor = page["expected_cursor"]
        .as_i64()
        .filter(|value| *value >= 0)
        .ok_or_else(|| Error::new("BUS_PAGE_INVALID", "page cursor is invalid"))?;
    let through = page["scanned_through"]
        .as_i64()
        .filter(|value| *value >= 0)
        .ok_or_else(|| Error::new("BUS_PAGE_INVALID", "page scan cut is invalid"))?;
    if through <= expected_cursor {
        return Ok(false);
    }
    let revision = page["automation_revision"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| Error::new("BUS_PAGE_INVALID", "automation revision is invalid"))?;
    let occurrences = page["items"]
        .as_array()
        .ok_or_else(|| Error::new("BUS_PAGE_INVALID", "page items are invalid"))?;
    let request_id = admission_request_id(
        &worker.credential.client_id,
        &worker.project_id,
        &worker.automation_id,
        revision,
        expected_cursor,
        through,
        occurrences,
    )?;
    let params = json!({
        "client_request_id":request_id,
        "project_id":worker.project_id,
        "consumer_id":worker.automation_id,
        "automation_revision":revision,
        "expected_cursor":expected_cursor,
        "through_observation_id":through,
        "occurrences":occurrences,
    });
    // The Store commits the durable cursor and pending journal together. If
    // the reply is unknown, the next loop rereads the cursor first; an unchanged
    // page reuses this canonical request ID, while a committed cursor produces
    // a different page and is never admitted twice.
    match call_once(
        &worker.store_root,
        &worker.credential,
        "bus.consumer.admit",
        params,
    )
    .await
    {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.code.as_str(),
                "OUTCOME_UNKNOWN" | "BUS_CURSOR_CONFLICT" | "BUS_AUTOMATION_REVISION_CONFLICT"
            ) =>
        {
            Ok(false)
        }
        // A full shared pending journal is backpressure, not a terminal
        // worker error. Keep the cursor unchanged and reread after the normal
        // poll delay so the existing ScriptRun continuation can drain it.
        Err(error) if error.code == "BUS_PENDING_CAPACITY" => Ok(false),
        Err(error) => Err(error),
    }
}

async fn call_once(
    root: &Path,
    credential: &Credential,
    method: &str,
    params: Value,
) -> Result<Value> {
    let mut client = Client::connect(root, credential, &IpcConfig::default()).await?;
    client.request(method, params).await
}

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn admission_request_id(
    consumer_client_id: &str,
    project_id: &str,
    automation_id: &str,
    automation_revision: i64,
    expected_cursor: i64,
    through: i64,
    occurrences: &[Value],
) -> Result<String> {
    let canonical = serde_json::to_vec(&json!({
        "consumer_client_id":consumer_client_id,
        "project_id":project_id,
        "automation_id":automation_id,
        "automation_revision":automation_revision,
        "expected_cursor":expected_cursor,
        "through_observation_id":through,
        "occurrences":occurrences,
    }))?;
    Ok(format!("bus-admit-{}", hex_sha256(&canonical)))
}

fn read_credential(path: &Path) -> Result<Credential> {
    let mut bytes = read_private_file(path)?;
    let parsed = serde_json::from_slice(&bytes);
    bytes.zeroize();
    let credential: Credential =
        parsed.map_err(|_| Error::invalid("credential file is malformed"))?;
    if credential.client_id.is_empty() || credential.token.is_empty() {
        return Err(Error::invalid("credential file is incomplete"));
    }
    Ok(credential)
}

fn read_worker_config(path: &Path) -> Result<WorkerConfig> {
    read_worker_config_with_digest(path).map(|(worker, _)| worker)
}

fn read_worker_config_with_digest(path: &Path) -> Result<(WorkerConfig, String)> {
    let mut bytes = read_private_file(path)?;
    let digest = hex_sha256(&bytes);
    let parsed = serde_json::from_slice(&bytes);
    bytes.zeroize();
    parsed
        .map(|worker| (worker, digest))
        .map_err(|_| Error::invalid("private worker config is malformed"))
}

fn write_worker_config(path: &Path, worker: &WorkerConfig) -> Result<()> {
    if path.file_name().is_none() {
        return Err(Error::invalid("worker config path must name a file"));
    }
    let bytes = Zeroizing::new(serde_json::to_vec(worker)?);
    write_private_new(path, bytes.as_slice())
}

fn read_private_file(path: &Path) -> Result<Vec<u8>> {
    let before = fs::symlink_metadata(path)?;
    if !before.is_file() || is_link_or_reparse(&before) || before.len() > MAX_CONFIG_BYTES {
        return Err(Error::new(
            "BUS_CONFIG_FILE_INVALID",
            "credential/config must be a bounded regular private file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if before.permissions().mode() & 0o077 != 0 {
            return Err(Error::new(
                "BUS_CONFIG_FILE_NOT_PRIVATE",
                "credential/config file permissions must be owner-only",
            ));
        }
    }
    #[cfg(windows)]
    private_permissions(path, false)?;

    let file = OpenOptions::new().read(true).open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() > MAX_CONFIG_BYTES {
        return Err(Error::new(
            "BUS_CONFIG_FILE_INVALID",
            "credential/config must be a bounded regular file",
        ));
    }
    let mut bytes = Zeroizing::new(Vec::with_capacity(opened.len() as usize));
    file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(Error::new(
            "BUS_CONFIG_FILE_TOO_LARGE",
            "credential/config exceeds the bounded file size",
        ));
    }
    Ok(std::mem::take(&mut *bytes))
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn retryable_transport_code(code: &str) -> bool {
    matches!(
        code,
        "HOST_UNAVAILABLE" | "IO_ERROR" | "OUTCOME_UNKNOWN" | "STORE_CLOSED" | "DISCONNECTED"
    )
}
