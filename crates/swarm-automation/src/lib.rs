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
    #[cfg(windows)]
    {
        let mut identity = identity.clone();
        identity["job_name"] = serde_json::json!(format!(
            "Global\\EliotSwarmService-AutomationScheduler-{service_owner_token}"
        ));
        swarm_process::departed_empty(&identity, service_owner_token)
    }
    #[cfg(not(windows))]
    {
        swarm_process::departed_empty(identity, service_owner_token)
    }
}

/// Validate the closed, platform-specific automation-scheduler owner identity.
/// This pure data check requires no owner token and performs no OS inspection;
/// a valid shape does not establish process liveness or authorize admission.
pub fn service_owner_group_identity_shape(identity: &serde_json::Value) -> bool {
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

/// Bounded qualification-only witness that one admit reply was withheld.
///
/// Written by the live, Store-owned scheduler worker at the moment the
/// qualification fault fires, into the same private directory that already
/// holds the worker config. It is the only observable trace of the applied
/// fault, because that launch pipes stdout only as far as the host readiness
/// frame and sends stderr to a null sink, so worker logging is unreachable.
///
/// The witness binds the PID of the Store-validated worker process, not the host
/// process, together with the exact worker config digest
/// that the Store issued, and the method whose reply was withheld. It carries
/// no credential, no request parameters, no Store decision content, and no
/// response body: only the length of the discarded frame, so a reader can tell
/// a real reply was withheld without ever seeing it.
///
/// It also binds the withheld frame length against the transport frame bound, so
/// an implausible count is refused rather than recorded as evidence.
///
/// The file is create-only and bounded. A second write never succeeds, which
/// keeps the single-drop property observable as well as enforceable.
#[cfg(feature = "qualification-faults")]
pub struct QualificationFaultWitness {
    /// PID of the process the Store launched for this worker.
    pub owner_launch_pid: u32,
    /// SHA-256 of the private worker config the Store issued for this launch.
    pub owner_config_sha256: String,
    /// Method whose single reply was withheld.
    pub method: &'static str,
    /// Byte length of the withheld response frame. No content is retained.
    pub withheld_frame_bytes: usize,
}

/// Maximum permitted witness size. A larger file is refused rather than
/// accepted, so a corrupted or oversized witness is never mistaken for
/// evidence.
///
/// This bounds the serialized witness document itself, not the withheld reply
/// frame. A withheld frame is legitimately 8 KiB to 512 KiB and is bounded
/// separately against the transport frame limit by the code that parses the
/// seam message.
#[cfg(feature = "qualification-faults")]
const MAX_QUALIFICATION_FAULT_WITNESS_BYTES: usize = 512;

/// Write the bounded witness for one withheld reply.
///
/// Called only from the qualification fault path, which is itself compiled out
/// of any build that did not request the qualification-faults feature. The
/// path is derived from the worker config location, so the witness can never
/// be written outside the private service directory the Store already owns.
#[cfg(feature = "qualification-faults")]
pub fn write_qualification_fault_witness(
    data_root: &Path,
    witness: &QualificationFaultWitness,
) -> swarm_contracts::error::Result<()> {
    use swarm_contracts::error::Error;
    if !is_sha256(&witness.owner_config_sha256) {
        return Err(Error::invalid(
            "qualification witness requires the exact owner config digest",
        ));
    }
    if witness.owner_launch_pid == 0
        || witness.method != ADMIT_METHOD
        || witness.withheld_frame_bytes == 0
        || witness.withheld_frame_bytes > swarm_client::IpcConfig::default().max_frame_bytes
    {
        return Err(Error::invalid("qualification witness identity is invalid"));
    }
    let value = serde_json::json!({
        "schema":"eliot.swarm.r48.qualification-fault-witness.v1",
        "owner_launch_pid":witness.owner_launch_pid,
        "owner_config_sha256":witness.owner_config_sha256,
        "method":witness.method,
        "withheld_frame_bytes":witness.withheld_frame_bytes,
        "replay":"withheld_before_delivery",
    });
    let bytes = serde_json::to_vec(&value)?;
    if bytes.len() > MAX_QUALIFICATION_FAULT_WITNESS_BYTES {
        return Err(Error::invalid(
            "qualification witness exceeds its bounded size",
        ));
    }
    let path = data_root
        .join("automation-scheduler-v1")
        .join("qualification-fault-witness.json");
    // Create-only: an existing witness means this process already fired once,
    // so a second application is refused instead of overwriting evidence.
    swarm_process::write_private_new(&path, &bytes).map_err(|_| {
        Error::new(
            "AUTOMATION_QUALIFICATION_WITNESS_EXISTS",
            "a qualification fault witness is already recorded for this service directory",
        )
    })
}

///
/// Bounded, qualification-only direct admission-intent marker.
///
/// The fault witness is proof that an injected fault fired; this marker is written
/// before every real admit request, which is what makes a replay observable. The
/// consumer rejects a second marker rather than inferring replay from a single
/// durable receipt.
///
/// The ordinal comes from the retained service directory rather than an in-process
/// counter, because a counter resets on restart and would let a second worker
/// lifetime restart the sequence. A fresh process continues the sequence the
/// previous lifetime established.
///
/// The caller must treat any error as a refusal: the request is not sent.
#[cfg(feature = "qualification-faults")]
pub fn write_qualification_admit_attempt(
    data_root: &Path,
    pid: u32,
    config_sha256: &str,
) -> swarm_contracts::error::Result<u8> {
    use swarm_contracts::error::Error;

    let service_dir = data_root.join("automation-scheduler-v1");

    // The ordinal is whatever the retained directory already shows. A retained
    // first marker means this attempt is the second, so the sequence survives a
    // restart instead of resetting with the process.
    let sequence = if service_dir.join(ATTEMPT_ONE_MARKER).exists() {
        2u8
    } else {
        1u8
    };

    if pid == 0 {
        return Err(Error::invalid(
            "admission attempt marker requires a live worker PID",
        ));
    }
    if config_sha256.len() != 64 || !config_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::invalid(
            "admission attempt marker requires the exact worker config digest",
        ));
    }

    let marker = if sequence == 2 {
        ATTEMPT_TWO_MARKER
    } else {
        ATTEMPT_ONE_MARKER
    };
    let value = serde_json::json!({
        "schema":ATTEMPT_SCHEMA,
        "owner_launch_pid":pid,
        "owner_config_sha256":config_sha256,
        "method":"automation.scheduler.admit",
        "sequence":sequence,
    });
    let bytes = serde_json::to_vec(&value)?;
    if bytes.len() > MAX_ADMIT_ATTEMPT_BYTES {
        return Err(Error::invalid(
            "admission attempt marker exceeds its bounded size",
        ));
    }

    // Create-only. A retained marker from an earlier lifetime makes this create
    // fail, which is the terminal signal that bounds the observer: a third attempt
    // can create nothing and is therefore refused rather than sent.
    swarm_process::write_private_new(&service_dir.join(marker), &bytes).map_err(|_| {
        Error::new(
            "AUTOMATION_ADMIT_ATTEMPT_MARKER_WRITE_FAILED",
            "admission intent could not be recorded; request was not sent",
        )
    })?;
    Ok(sequence)
}

/// Closed schema for an admission attempt marker.
#[cfg(feature = "qualification-faults")]
pub const ATTEMPT_SCHEMA: &str = "eliot.swarm.r48.qualification-admit-attempt.v1";

/// File name for the first admission attempt marker.
#[cfg(feature = "qualification-faults")]
pub const ATTEMPT_ONE_MARKER: &str = "qualification-admit-attempt-1.json";

/// File name for the second admission attempt marker.
#[cfg(feature = "qualification-faults")]
pub const ATTEMPT_TWO_MARKER: &str = "qualification-admit-attempt-2.json";

/// Maximum permitted marker size.
///
/// The marker binds identity, method, and sequence only, so a small bound is
/// generous. A larger serialization is refused rather than written.
#[cfg(feature = "qualification-faults")]
const MAX_ADMIT_ATTEMPT_BYTES: usize = 512;

/// Closed diagnosis of the first marked admit RPC result, never admission proof.
#[cfg(feature = "qualification-faults")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationAdmitTerminalCategory {
    Ok,
    ReplyWithheld,
    TransportUncertain,
    OtherRejection,
}

/// Actual result of the existing applied-witness writer.
#[cfg(feature = "qualification-faults")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationWitnessWriterOutcome {
    Applied,
    Rejected,
}

/// A missing parsed count cannot claim that the witness writer was attempted.
#[cfg(feature = "qualification-faults")]
#[derive(Serialize)]
#[serde(tag = "frame_length_state", rename_all = "snake_case")]
pub enum QualificationWitnessDiagnostic {
    Absent,
    Valid {
        withheld_frame_bytes: usize,
        witness_writer_outcome: QualificationWitnessWriterOutcome,
    },
}

#[cfg(feature = "qualification-faults")]
#[derive(Serialize)]
struct QualificationAdmitDiagnostic<'a> {
    schema: &'static str,
    owner_launch_pid: u32,
    owner_config_sha256: &'a str,
    method: &'static str,
    sequence: u8,
    #[serde(flatten)]
    details: QualificationAdmitDiagnosticDetails,
}

#[cfg(feature = "qualification-faults")]
#[derive(Serialize)]
#[serde(untagged)]
enum QualificationAdmitDiagnosticDetails {
    Terminal {
        terminal_category: QualificationAdmitTerminalCategory,
    },
    Witness(QualificationWitnessDiagnostic),
}

#[cfg(feature = "qualification-faults")]
const MAX_ADMIT_DIAGNOSTIC_BYTES: usize = 512;
#[cfg(feature = "qualification-faults")]
const ADMIT_TERMINAL_MARKER: &str = "qualification-admit-terminal-1.json";
#[cfg(feature = "qualification-faults")]
const ADMIT_WITNESS_DIAGNOSTIC_MARKER: &str = "qualification-admit-witness-diagnostic-1.json";

/// Retain the terminal category before retry logging or applied-witness handling.
/// Only the first marked request calls this producer; no result body is accepted.
#[cfg(feature = "qualification-faults")]
pub fn write_qualification_admit_terminal(
    data_root: &Path,
    pid: u32,
    config_sha256: &str,
    terminal_category: QualificationAdmitTerminalCategory,
) -> swarm_contracts::error::Result<()> {
    write_qualification_admit_diagnostic(
        data_root,
        ADMIT_TERMINAL_MARKER,
        QualificationAdmitDiagnostic {
            schema: "eliot.swarm.r48.qualification-admit-terminal.v1",
            owner_launch_pid: pid,
            owner_config_sha256: config_sha256,
            method: ADMIT_METHOD,
            sequence: 1,
            details: QualificationAdmitDiagnosticDetails::Terminal { terminal_category },
        },
    )
}

/// Retain only the actual parser and writer results after witness handling.
/// This separate immutable marker cannot replace the applied-fault witness.
#[cfg(feature = "qualification-faults")]
pub fn write_qualification_witness_diagnostic(
    data_root: &Path,
    pid: u32,
    config_sha256: &str,
    diagnostic: QualificationWitnessDiagnostic,
) -> swarm_contracts::error::Result<()> {
    if let QualificationWitnessDiagnostic::Valid {
        withheld_frame_bytes,
        ..
    } = &diagnostic
        && (*withheld_frame_bytes == 0
            || *withheld_frame_bytes > swarm_client::IpcConfig::default().max_frame_bytes)
    {
        return Err(swarm_contracts::error::Error::invalid(
            "qualification diagnosis requires a bounded parsed frame length",
        ));
    }
    write_qualification_admit_diagnostic(
        data_root,
        ADMIT_WITNESS_DIAGNOSTIC_MARKER,
        QualificationAdmitDiagnostic {
            schema: "eliot.swarm.r48.qualification-admit-witness-diagnostic.v1",
            owner_launch_pid: pid,
            owner_config_sha256: config_sha256,
            method: ADMIT_METHOD,
            sequence: 1,
            details: QualificationAdmitDiagnosticDetails::Witness(diagnostic),
        },
    )
}

#[cfg(feature = "qualification-faults")]
fn write_qualification_admit_diagnostic(
    data_root: &Path,
    name: &'static str,
    diagnostic: QualificationAdmitDiagnostic<'_>,
) -> swarm_contracts::error::Result<()> {
    use swarm_contracts::error::Error;
    if diagnostic.owner_launch_pid == 0 || !is_sha256(diagnostic.owner_config_sha256) {
        return Err(Error::invalid(
            "qualification diagnosis requires the exact worker identity",
        ));
    }
    let bytes = serde_json::to_vec(&diagnostic)?;
    if bytes.len() > MAX_ADMIT_DIAGNOSTIC_BYTES {
        return Err(Error::invalid(
            "qualification diagnosis exceeds its bounded size",
        ));
    }
    swarm_process::write_private_new(
        &data_root.join("automation-scheduler-v1").join(name),
        &bytes,
    )
    .map_err(|_| {
        Error::new(
            "AUTOMATION_QUALIFICATION_DIAGNOSTIC_WRITE_FAILED",
            "qualification admission diagnosis could not be recorded",
        )
    })
}

#[cfg(all(test, feature = "qualification-faults"))]
mod qualification_admit_diagnostic_tests {
    use super::*;

    fn fresh_service_root() -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "r48-terminal-diagnosis-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("automation-scheduler-v1")).unwrap();
        root
    }

    #[test]
    fn terminal_diagnosis_is_closed_bound_and_create_only() {
        let root = fresh_service_root();
        let digest = "b".repeat(64);
        let pid = std::process::id();
        write_qualification_admit_terminal(
            &root,
            pid,
            &digest,
            QualificationAdmitTerminalCategory::ReplyWithheld,
        )
        .unwrap();
        let path = root
            .join("automation-scheduler-v1")
            .join(ADMIT_TERMINAL_MARKER);
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.len() <= MAX_ADMIT_DIAGNOSTIC_BYTES);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let fields = value.as_object().unwrap();
        let mut keys = fields.keys().map(String::as_str).collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "method",
                "owner_config_sha256",
                "owner_launch_pid",
                "schema",
                "sequence",
                "terminal_category",
            ]
        );
        assert_eq!(value["owner_launch_pid"], pid);
        assert_eq!(value["owner_config_sha256"], digest);
        assert_eq!(
            value["schema"],
            "eliot.swarm.r48.qualification-admit-terminal.v1"
        );
        assert_eq!(value["method"], ADMIT_METHOD);
        assert_eq!(value["sequence"], 1);
        assert_eq!(value["terminal_category"], "reply_withheld");
        for (category, expected) in [
            (QualificationAdmitTerminalCategory::Ok, "ok"),
            (
                QualificationAdmitTerminalCategory::ReplyWithheld,
                "reply_withheld",
            ),
            (
                QualificationAdmitTerminalCategory::TransportUncertain,
                "transport_uncertain",
            ),
            (
                QualificationAdmitTerminalCategory::OtherRejection,
                "other_rejection",
            ),
        ] {
            assert_eq!(serde_json::to_value(category).unwrap(), expected);
        }
        assert!(
            write_qualification_admit_terminal(
                &root,
                pid,
                &digest,
                QualificationAdmitTerminalCategory::Ok,
            )
            .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert!(
            !root
                .join("automation-scheduler-v1/qualification-fault-witness.json")
                .exists()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn diagnosis_refuses_invalid_identity_or_count_before_create() {
        let root = fresh_service_root();
        let digest = "b".repeat(64);
        let pid = std::process::id();
        for (pid, digest) in [(0, digest.as_str()), (pid, "private-non-digest")] {
            assert!(
                write_qualification_admit_terminal(
                    &root,
                    pid,
                    digest,
                    QualificationAdmitTerminalCategory::Ok,
                )
                .is_err()
            );
            assert!(
                write_qualification_witness_diagnostic(
                    &root,
                    pid,
                    digest,
                    QualificationWitnessDiagnostic::Absent,
                )
                .is_err()
            );
        }
        for withheld_frame_bytes in [0, swarm_client::IpcConfig::default().max_frame_bytes + 1] {
            assert!(
                write_qualification_witness_diagnostic(
                    &root,
                    pid,
                    &digest,
                    QualificationWitnessDiagnostic::Valid {
                        withheld_frame_bytes,
                        witness_writer_outcome: QualificationWitnessWriterOutcome::Applied,
                    },
                )
                .is_err()
            );
        }
        assert_eq!(
            fs::read_dir(root.join("automation-scheduler-v1"))
                .unwrap()
                .count(),
            0
        );
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod qualification_admit_attempt_tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn valid_digest() -> String {
        "a".repeat(64)
    }

    /// A fresh private service directory, standing in for a fresh worker lifetime.
    fn fresh_service_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after the epoch")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("r48-admit-{label}-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&root)
            .expect("a new fixture root is created without replacing evidence");
        std::fs::create_dir(root.join("automation-scheduler-v1"))
            .expect("a fresh private service directory is created");
        root
    }

    fn marker_one(root: &Path) -> PathBuf {
        root.join("automation-scheduler-v1")
            .join(ATTEMPT_ONE_MARKER)
    }

    fn marker_two(root: &Path) -> PathBuf {
        root.join("automation-scheduler-v1")
            .join(ATTEMPT_TWO_MARKER)
    }

    /// A fresh lifetime records the first marker and reports the sequence.
    ///
    /// The retained directory is published so the consumer-side integration test
    /// can read the real bytes, and a real fault witness is written alongside so
    /// one fixture root carries both artifact kinds.
    #[test]
    fn a_fresh_lifetime_records_marker_one() {
        let root = fresh_service_root("first");
        let sequence = write_qualification_admit_attempt(&root, 6001, &valid_digest())
            .expect("a create-only write into a fresh directory succeeds");
        assert_eq!(sequence, 1, "a fresh lifetime records the first attempt");

        write_qualification_fault_witness(
            &root,
            &QualificationFaultWitness {
                owner_launch_pid: 6001,
                owner_config_sha256: valid_digest(),
                method: "automation.scheduler.admit",
                withheld_frame_bytes: 2048,
            },
        )
        .expect("the immutable fault witness is written beside the worker config");

        let marker = std::fs::read_to_string(marker_one(&root)).expect("marker one is written");
        assert!(marker.contains(ATTEMPT_SCHEMA));
        assert!(marker.contains("automation.scheduler.admit"));
        assert!(marker.contains("6001"));
        assert!(marker.contains(&valid_digest()));
        assert!(marker.contains("\"sequence\":1"));

        // Fixture root for the consumer-side integration test. Test-only, printed
        // only after the real bytes exist on disk.
        println!("R48_WITNESS_FIXTURE_ROOT={}", root.display());
    }

    /// A retained directory advances the sequence across a restart.
    ///
    /// This is the restart case. An in-process counter would reset to one here and
    /// let a replay begin a new first attempt, which is what the ordinal must not
    /// do.
    #[test]
    fn a_retained_directory_advances_the_sequence_across_a_restart() {
        let root = fresh_service_root("retained");
        assert_eq!(
            write_qualification_admit_attempt(&root, 6002, &valid_digest())
                .expect("marker one is written"),
            1
        );
        // A later lifetime sees the retained marker one and derives sequence two.
        assert_eq!(
            write_qualification_admit_attempt(&root, 6003, &valid_digest())
                .expect("marker two is written"),
            2,
            "a retained marker one makes the next attempt the second"
        );
        assert!(
            marker_two(&root).exists(),
            "marker two is written for the harness to refute"
        );

        println!("R48_WITNESS_FIXTURE_ROOT={}", root.display());
    }

    /// A third attempt cannot create a marker and is refused, because a
    /// create-only marker two always fails.
    #[test]
    fn a_third_attempt_is_refused_rather_than_recorded() {
        let root = fresh_service_root("third-refused");
        assert_eq!(
            write_qualification_admit_attempt(&root, 6004, &valid_digest())
                .expect("marker one is written"),
            1
        );
        assert_eq!(
            write_qualification_admit_attempt(&root, 6004, &valid_digest())
                .expect("marker two is written"),
            2
        );
        let third = write_qualification_admit_attempt(&root, 6004, &valid_digest());
        assert!(
            third.is_err(),
            "a third attempt is refused before its request is sent"
        );
        let preserved = std::fs::read_to_string(marker_two(&root)).expect("marker two survives");
        assert!(
            preserved.contains("\"sequence\":2"),
            "the refused third attempt never overwrites marker two"
        );
    }

    /// A write failure refuses the request rather than falling back.
    ///
    /// A read-only service directory stands in for a permissions failure: the
    /// caller must not send the request on the theory that the marker was optional.
    #[cfg(unix)]
    #[test]
    fn a_write_failure_refuses_the_request_without_a_fallback() {
        let root = fresh_service_root("unwritable");
        let service_dir = root.join("automation-scheduler-v1");
        let mut permissions = std::fs::metadata(&service_dir)
            .expect("directory metadata is readable")
            .permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o500);
        std::fs::set_permissions(&service_dir, permissions)
            .expect("the service directory is made read-only");

        let outcome = write_qualification_admit_attempt(&root, 6006, &valid_digest());
        assert!(
            outcome.is_err(),
            "a marker create failure must refuse the request"
        );
        assert!(!marker_one(&root).exists());

        let mut restored = std::fs::metadata(&service_dir)
            .expect("directory metadata is readable")
            .permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut restored, 0o700);
        let _ = std::fs::set_permissions(&service_dir, restored);
    }

    /// Identity is validated before any marker is created.
    #[test]
    fn an_invalid_or_zero_identity_fails_before_any_write() {
        let root = fresh_service_root("invalid-identity");

        let zero_pid = write_qualification_admit_attempt(&root, 0, &valid_digest());
        assert!(zero_pid.is_err(), "a zero PID is refused");

        let short_digest = write_qualification_admit_attempt(&root, 6007, "abc");
        assert!(short_digest.is_err(), "a short config digest is refused");

        assert!(
            !marker_one(&root).exists(),
            "no marker is written for a rejected identity"
        );
    }

    /// The marker schema is exactly the contracted five fields and carries no
    /// credential, which is why it is closed rather than a passthrough of request
    /// content.
    #[test]
    fn a_written_marker_has_exactly_the_closed_fields() {
        let root = fresh_service_root("closed-schema");
        write_qualification_admit_attempt(&root, 6008, &valid_digest())
            .expect("marker one is written");
        let marker = std::fs::read_to_string(marker_one(&root)).expect("marker one is written");
        for forbidden in ["token", "credential", "service_owner_token", "store_root"] {
            assert!(
                !marker.contains(forbidden),
                "a marker must never carry a credential field such as {forbidden}"
            );
        }
        let parsed: serde_json::Value =
            serde_json::from_str(&marker).expect("the marker is valid JSON");
        let fields = parsed.as_object().expect("the marker is an object");
        assert_eq!(
            fields.len(),
            5,
            "the marker schema is exactly the five contracted fields"
        );
    }
}

pub fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}
