//! Independent timer/queue for the Store's existing durable scheduler facts.
//!
//! The worker can read only the bounded Store page and submit its opaque cut
//! back for admission. It cannot select a Store method, supply an actor, or
//! carry Task/Attempt action parameters. The Store remains the sole owner of
//! schedule, calendar, Goal, Operation, authorization, and cursor decisions.

use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
};
use swarm_contracts::{DeclaredServicePurpose, DeclaredServiceScope};

pub const PAGE_METHOD: &str = "automation.scheduler.page";
pub const ADMIT_METHOD: &str = "automation.scheduler.admit";
/// Fixed local pipe frame emitted only after the worker joins its declared OS
/// service group. It contains no credential, endpoint, or process data.
pub const READY_FRAME: &[u8] = b"ELIOT_AUTOMATION_WORKER_READY_V1\n";
pub const MAX_SOURCES: usize = 3;
pub const MAX_POLL_DELAY_MS: u64 = 60_000;
const MAX_WORKER_CONFIG_BYTES: usize = 8 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub schema_version: u32,
    pub store_root: String,
    pub scope: DeclaredServiceScope,
    pub credential: swarm_contracts::Credential,
    pub service_owner_token: String,
    /// Digest of the canonical identity fields, retained in both the file
    /// and Store registration so restart recovery can detect config drift.
    pub config_sha256: String,
}

#[derive(Serialize)]
struct WorkerConfigIdentity<'a> {
    schema_version: u32,
    store_root: &'a str,
    scope: &'a DeclaredServiceScope,
    credential: &'a swarm_contracts::Credential,
    service_owner_token: &'a str,
}

impl WorkerConfig {
    fn computed_sha256(&self) -> swarm_contracts::error::Result<String> {
        use zeroize::Zeroize;
        let identity = WorkerConfigIdentity {
            schema_version: self.schema_version,
            store_root: &self.store_root,
            scope: &self.scope,
            credential: &self.credential,
            service_owner_token: &self.service_owner_token,
        };
        let mut bytes = serde_json::to_vec(&identity)?;
        let digest = hex_sha256(&bytes);
        bytes.zeroize();
        Ok(digest)
    }
}

/// Compute the digest that the Store retains for the immutable worker config.
/// The credential and owner token are included in the hash input, and the
/// temporary serialized copy is zeroed before this function returns.
pub fn worker_config_sha256(
    store_root: &str,
    scope: &DeclaredServiceScope,
    credential: &swarm_contracts::Credential,
    service_owner_token: &str,
) -> swarm_contracts::error::Result<String> {
    let config = WorkerConfig {
        schema_version: 1,
        store_root: store_root.to_owned(),
        scope: scope.clone(),
        credential: credential.clone(),
        service_owner_token: service_owner_token.to_owned(),
        config_sha256: String::new(),
    };
    config.computed_sha256()
}

impl Drop for WorkerConfig {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.credential.token.zeroize();
        self.service_owner_token.zeroize();
    }
}

pub fn worker_config_path(data_root: &Path) -> PathBuf {
    data_root
        .join("automation-scheduler-v1")
        .join("worker.json")
}

/// Persist a Store-issued scheduler Module credential in a stable private
/// config. It is create-only: restarts reuse this exact scope and credential,
/// and a retained registration is never silently rotated around a live worker.
pub fn write_worker_config(
    data_root: &Path,
    scope: &DeclaredServiceScope,
    credential: &swarm_contracts::Credential,
    service_owner_token: &str,
) -> swarm_contracts::error::Result<PathBuf> {
    use swarm_contracts::error::Error;
    scope.validate()?;
    if !data_root.is_absolute()
        || scope.purpose != DeclaredServicePurpose::AutomationScheduler
        || scope.service_id != "automation-scheduler-v1"
        || credential.client_id != scope.service_id
        || credential.token.len() < 32
        || !is_service_owner_token(service_owner_token)
    {
        return Err(Error::invalid("scheduler worker identity is invalid"));
    }
    let store_root = data_root
        .to_str()
        .ok_or_else(|| Error::invalid("scheduler data root must be valid UTF-8"))?;
    let root_metadata = fs::symlink_metadata(data_root)?;
    if !root_metadata.is_dir() || is_link_or_reparse(&root_metadata) {
        return Err(Error::invalid(
            "scheduler data root must be a regular directory",
        ));
    }
    let service_dir = data_root.join("automation-scheduler-v1");
    ensure_private_directory(&service_dir)?;
    let path = worker_config_path(data_root);
    let config = WorkerConfig {
        schema_version: 1,
        store_root: store_root.to_owned(),
        scope: scope.clone(),
        credential: credential.clone(),
        service_owner_token: service_owner_token.to_owned(),
        config_sha256: String::new(),
    };
    let mut config = config;
    config.config_sha256 = config.computed_sha256()?;
    let mut bytes = serde_json::to_vec(&config)?;
    if bytes.len() > MAX_WORKER_CONFIG_BYTES {
        use zeroize::Zeroize;
        bytes.zeroize();
        return Err(Error::invalid(
            "scheduler worker config exceeds its size bound",
        ));
    }
    let write = swarm_process::write_private_new(&path, &bytes);
    use zeroize::Zeroize;
    bytes.zeroize();
    write?;
    Ok(path)
}

/// Load the immutable bootstrap credential for the fixed scheduler service.
/// `None` is valid only before the Store has ever registered the service;
/// callers must not replace a retained DB registration when it is absent.
pub fn load_worker_config(
    data_root: &Path,
) -> swarm_contracts::error::Result<Option<WorkerConfig>> {
    use swarm_contracts::error::Error;
    if !data_root.is_absolute() {
        return Err(Error::invalid("scheduler data root must be absolute"));
    }
    let root_metadata = fs::symlink_metadata(data_root)?;
    if !root_metadata.is_dir() || is_link_or_reparse(&root_metadata) {
        return Err(Error::invalid(
            "scheduler data root must be a regular directory",
        ));
    }
    let service_dir = data_root.join("automation-scheduler-v1");
    match fs::symlink_metadata(&service_dir) {
        Ok(metadata) if metadata.is_dir() && !is_link_or_reparse(&metadata) => {
            swarm_process::private_permissions(&service_dir, false)?;
        }
        Ok(_) => return Err(Error::invalid("scheduler service directory is unsafe")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let path = worker_config_path(data_root);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let config = read_worker_config_file(&path)?;
    let expected_root = data_root
        .to_str()
        .ok_or_else(|| Error::invalid("scheduler data root must be valid UTF-8"))?;
    if config.store_root != expected_root {
        return Err(Error::new(
            "AUTOMATION_CONFIG_STORE_MISMATCH",
            "scheduler worker config belongs to another Store root",
        ));
    }
    Ok(Some(config))
}

/// Read and validate a bounded private worker config without exposing its
/// credential through logs or a Store projection.
pub fn read_worker_config_file(path: &Path) -> swarm_contracts::error::Result<WorkerConfig> {
    use swarm_contracts::error::Error;
    let before = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "AUTOMATION_CONFIG_MISSING",
            "private worker config is unavailable",
        )
    })?;
    if !before.is_file()
        || is_link_or_reparse(&before)
        || before.len() > MAX_WORKER_CONFIG_BYTES as u64
    {
        return Err(Error::new(
            "AUTOMATION_CONFIG_INVALID",
            "worker config must be a bounded regular private file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if before.permissions().mode() & 0o077 != 0 {
            return Err(Error::new(
                "AUTOMATION_CONFIG_NOT_PRIVATE",
                "worker config permissions must be owner-only",
            ));
        }
    }
    #[cfg(windows)]
    swarm_process::private_permissions(path, false)?;

    let file = fs::OpenOptions::new().read(true).open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_WORKER_CONFIG_BYTES as u64 {
        return Err(Error::new(
            "AUTOMATION_CONFIG_INVALID",
            "worker config changed outside its size bound",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_WORKER_CONFIG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_WORKER_CONFIG_BYTES {
        use zeroize::Zeroize;
        bytes.zeroize();
        return Err(Error::new(
            "AUTOMATION_CONFIG_TOO_LARGE",
            "worker config exceeds its size bound",
        ));
    }
    let parsed = serde_json::from_slice(&bytes);
    use zeroize::Zeroize;
    bytes.zeroize();
    let config: WorkerConfig = parsed
        .map_err(|_| Error::new("AUTOMATION_CONFIG_INVALID", "worker config is malformed"))?;
    if config.schema_version != 1
        || !Path::new(&config.store_root).is_absolute()
        || config.scope.purpose != DeclaredServicePurpose::AutomationScheduler
        || config.scope.service_id != "automation-scheduler-v1"
        || config.scope.generation == 0
        || config.credential.client_id != config.scope.service_id
        || config.credential.token.len() < 32
        || !is_service_owner_token(&config.service_owner_token)
        || !is_sha256(&config.config_sha256)
        || config
            .computed_sha256()
            .map_or(true, |digest| digest != config.config_sha256)
    {
        return Err(Error::new(
            "AUTOMATION_CONFIG_INVALID",
            "worker configuration identity is invalid",
        ));
    }
    config.scope.validate().map_err(|_| {
        Error::new(
            "AUTOMATION_WORKER_SCOPE_INVALID",
            "worker service scope is invalid",
        )
    })?;
    Ok(config)
}

/// Convert the exact identity returned immediately after host spawn into the
/// service-specific group identity used by the existing departure reader.
/// The worker starts no child processes; after it enters its declared group,
/// any descendants remain inside that group's non-killing scope.
pub fn service_owner_group_identity(
    spawned_identity: &serde_json::Value,
    service_owner_token: &str,
) -> swarm_contracts::error::Result<serde_json::Value> {
    use swarm_contracts::error::Error;
    if !is_service_owner_token(service_owner_token)
        || spawned_identity["scope"] != "launcher_spawned_process"
        || spawned_identity["purpose"] != "check"
    {
        return Err(Error::invalid("scheduler process birth proof is invalid"));
    }
    let pid = spawned_identity["pid"]
        .as_u64()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| Error::invalid("scheduler process PID is invalid"))?;
    #[cfg(windows)]
    {
        let creation_filetime = spawned_identity["creation_filetime"]
            .as_u64()
            .filter(|value| *value > 0)
            .ok_or_else(|| Error::invalid("scheduler process birth time is invalid"))?;
        Ok(serde_json::json!({
            "pid":pid,
            "creation_filetime":creation_filetime,
            "scope":"windows_job",
            "purpose":"automation_scheduler",
            "disposition_source":"job_accounting",
        }))
    }
    #[cfg(target_os = "linux")]
    {
        let start_ticks = spawned_identity["start_ticks"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::invalid("scheduler process birth time is invalid"))?;
        let boot_id = spawned_identity["boot_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::invalid("scheduler host boot identity is invalid"))?;
        Ok(serde_json::json!({
            "pid":pid,
            "pgid":pid,
            "start_ticks":start_ticks,
            "boot_id":boot_id,
            "scope":"linux_process_group",
            "purpose":"automation_scheduler",
            "disposition_source":"proc_group_members",
        }))
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = pid;
        Err(Error::new(
            "AUTOMATION_WORKER_PLATFORM_UNSUPPORTED",
            "scheduler process ownership is unavailable on this platform",
        ))
    }
}

pub fn service_owner_family_empty(
    identity: &serde_json::Value,
    service_owner_token: &str,
) -> swarm_contracts::error::Result<bool> {
    if !is_service_owner_token(service_owner_token)
        || identity["purpose"] != "automation_scheduler"
        || !service_owner_group_identity_shape(identity)
    {
        return Err(swarm_contracts::error::Error::invalid(
            "scheduler owner family identity is invalid",
        ));
    }
    let mut identity = identity.clone();
    #[cfg(windows)]
    {
        identity["job_name"] = serde_json::json!(format!(
            "Global\\EliotSwarmService-AutomationScheduler-{service_owner_token}"
        ));
    }
    swarm_process::departed_empty(&identity, service_owner_token)
}

fn service_owner_group_identity_shape(identity: &serde_json::Value) -> bool {
    #[cfg(windows)]
    {
        identity.as_object().is_some_and(|fields| {
            fields.len() == 5
                && [
                    "pid",
                    "creation_filetime",
                    "scope",
                    "purpose",
                    "disposition_source",
                ]
                .into_iter()
                .all(|key| fields.contains_key(key))
                && identity["pid"].as_u64().is_some_and(|value| value > 0)
                && identity["creation_filetime"]
                    .as_u64()
                    .is_some_and(|value| value > 0)
                && identity["scope"] == "windows_job"
                && identity["purpose"] == "automation_scheduler"
                && identity["disposition_source"] == "job_accounting"
        })
    }
    #[cfg(target_os = "linux")]
    {
        identity.as_object().is_some_and(|fields| {
            fields.len() == 7
                && [
                    "pid",
                    "pgid",
                    "start_ticks",
                    "boot_id",
                    "scope",
                    "purpose",
                    "disposition_source",
                ]
                .into_iter()
                .all(|key| fields.contains_key(key))
                && identity["pid"].as_u64().is_some_and(|value| value > 0)
                && identity["pgid"] == identity["pid"]
                && identity["start_ticks"].as_str().is_some_and(|value| {
                    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
                })
                && identity["boot_id"].as_str().is_some_and(|value| {
                    !value.is_empty()
                        && value.len() <= 64
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
                })
                && identity["scope"] == "linux_process_group"
                && identity["purpose"] == "automation_scheduler"
                && identity["disposition_source"] == "proc_group_members"
        })
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = identity;
        false
    }
}

fn is_service_owner_token(token: &str) -> bool {
    (32..=128).contains(&token.len())
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn ensure_private_directory(path: &Path) -> swarm_contracts::error::Result<()> {
    use swarm_contracts::error::Error;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !is_link_or_reparse(&metadata) => {}
        Ok(_) => {
            return Err(Error::invalid(
                "scheduler config path is not a private directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(path)?,
        Err(error) => return Err(error.into()),
    }
    swarm_process::private_permissions(path, true)
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

/// Closed names for the existing durable due readers. Adding a kind requires
/// a corresponding Store reader and typed admission path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DueSourceKind {
    IntervalSchedule,
    ManagerCalendar,
    GoalReminder,
}

/// A safe projection of one Store-owned due-index family. `cursor_digest`
/// binds the page cut to current Store facts without exporting action payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DueSource {
    pub kind: DueSourceKind,
    pub next_due_at_ms: Option<i64>,
    pub due_count: u32,
    pub cursor_digest: String,
}

/// Read-only projection. Its digest excludes wall-clock observation time so a
/// stable uncommitted cut keeps the same idempotency key across reconnects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuePage {
    pub schema_version: u32,
    pub scope: DeclaredServiceScope,
    pub observed_at_ms: i64,
    pub next_due_at_ms: Option<i64>,
    pub sources: Vec<DueSource>,
    pub snapshot_sha256: String,
}

impl DuePage {
    pub fn validate(&self, expected_scope: &DeclaredServiceScope) -> Result<(), &'static str> {
        expected_scope
            .validate()
            .map_err(|_| "expected automation scope is invalid")?;
        if expected_scope.purpose != DeclaredServicePurpose::AutomationScheduler
            || self.schema_version != 1
            || &self.scope != expected_scope
            || self.observed_at_ms < 0
            || self.sources.len() != MAX_SOURCES
            || !is_sha256(&self.snapshot_sha256)
        {
            return Err("automation scheduler page identity is invalid");
        }
        let mut kinds = std::collections::BTreeSet::new();
        let mut earliest = None;
        for source in &self.sources {
            if !kinds.insert(source.kind)
                || source.next_due_at_ms.is_some_and(|due| due < 0)
                || source.due_count > 32
                || !is_sha256(&source.cursor_digest)
                || (source.due_count > 0 && source.next_due_at_ms.is_none())
                || source
                    .next_due_at_ms
                    .is_some_and(|due| due <= self.observed_at_ms)
                    != (source.due_count > 0)
            {
                return Err("automation scheduler source projection is invalid");
            }
            if let Some(due) = source.next_due_at_ms {
                earliest = Some(earliest.map_or(due, |current: i64| current.min(due)));
            }
        }
        if kinds.len() != MAX_SOURCES
            || ![
                DueSourceKind::IntervalSchedule,
                DueSourceKind::ManagerCalendar,
                DueSourceKind::GoalReminder,
            ]
            .into_iter()
            .all(|kind| kinds.contains(&kind))
        {
            return Err("automation scheduler page is missing a source family");
        }
        if self.next_due_at_ms != earliest {
            return Err("automation scheduler wake does not match its source page");
        }
        Ok(())
    }

    /// A pulse is authorized only against the exact page the worker observed.
    /// The server rechecks the digest before it invokes existing source
    /// reconcilers; each source's ordinary Store transaction remains its
    /// durable cursor/action boundary.
    pub fn admit_params(&self) -> Result<serde_json::Value, &'static str> {
        if self.schema_version != 1 || !is_sha256(&self.snapshot_sha256) {
            return Err("automation scheduler page cannot be admitted");
        }
        let identity = serde_json::json!({
            "schema_version":1,
            "scope":self.scope,
            "snapshot_sha256":self.snapshot_sha256,
        });
        let canonical = serde_json::to_vec(&identity)
            .map_err(|_| "automation scheduler pulse identity cannot be encoded")?;
        let request_id = format!("automation-pulse-{}", hex_sha256(&canonical));
        Ok(serde_json::json!({
            "client_request_id":request_id,
            "scope":self.scope,
            "expected_snapshot_sha256":self.snapshot_sha256,
            "observed_at_ms":self.observed_at_ms,
        }))
    }
}

/// Return one shared bounded wake delay. A configured far-future schedule does
/// not create its own timer, and an empty page goes dormant for the same cap.
pub fn wait_ms(page: &DuePage) -> u64 {
    match page.next_due_at_ms {
        Some(due) if due <= page.observed_at_ms => 1,
        Some(due) => due
            .saturating_sub(page.observed_at_ms)
            .clamp(1, MAX_POLL_DELAY_MS as i64) as u64,
        None => MAX_POLL_DELAY_MS,
    }
}

pub fn has_due_source(page: &DuePage) -> bool {
    page.sources.iter().any(|source| {
        source
            .next_due_at_ms
            .is_some_and(|due| due <= page.observed_at_ms)
    })
}

pub fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}
