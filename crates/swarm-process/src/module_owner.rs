//! Generic per-scope module owner bootstrap.
//!
//! This is deliberately a process-package primitive.  It owns the module
//! lock, enters the non-killing module group, publishes the existing v1
//! `owner.json` envelope, and launches exactly the absolute executable from a
//! private plan.  It has no vendor, controller, Task, database, or restart
//! policy knowledge.

use crate::{
    Group, StateMarkerError, acquire_state_marker, departed_empty, module_child_belongs_to_owner,
    private_permissions, process_image_identity, write_private_new,
};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeSet,
    env,
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Command, ExitStatus},
    time::Duration,
};
use swarm_contracts::error::{Error, Result};

const PLAN_VERSION: u64 = 1;
const RESOLVER_MAP_VERSION: u64 = 2;
const MAX_PLAN_BYTES: u64 = 65_536;
const MAX_ARGV: usize = 128;
const MAX_ARG_BYTES: usize = 4_096;
const MAX_REFS: usize = 32;
const MAX_REF_BYTES: usize = 4_096;
const MAX_CREDENTIAL_FILE_BYTES: u64 = 4_096;
const MAX_PROTECTED_REF_FILE_BYTES: u64 = 65_536;
const MODULE_CREDENTIAL_FILE_ENV: &str = "ELIOT_SWARM_MODULE_CREDENTIAL_FILE";
const OWNER_MAX_BYTES: u64 = 65_536;
const MARKER: &str = "ELIOT_SWARM_MODULE_V1\n";

/// Only these non-secret Windows values are carried into the adapter child.
/// Descriptor literals and late protected-reference paths are installed after
/// this baseline; arbitrary provider, API, bearer, or user environment values
/// are deliberately excluded.
#[cfg(windows)]
const WINDOWS_LAUNCH_ENVIRONMENT: &[&str] = &[
    "SystemRoot",
    "WINDIR",
    "ComSpec",
    "PATH",
    "PATHEXT",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "APPDATA",
    "LOCALAPPDATA",
    "ProgramData",
    "HOMEDRIVE",
    "HOMEPATH",
];

fn reject_link_components(path: &Path, allow_missing_tail: bool) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::invalid("path must be absolute"));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        if matches!(component, Component::CurDir | Component::ParentDir) {
            return Err(Error::invalid("path contains traversal components"));
        }
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || is_reparse(&metadata) {
                    return Err(Error::new(
                        "MODULE_OWNER_PATH_LINK",
                        "module owner path cannot traverse a symlink or reparse point",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && allow_missing_tail => {
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn is_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_reparse(_metadata: &fs::Metadata) -> bool {
    false
}

/// Opaque protected material reference. The plan never interprets this value
/// as a filesystem path.
#[derive(Debug, Clone)]
pub struct ProtectedRef {
    pub name: String,
    pub reference: String,
}

/// Private plan consumed by the helper.  Unknown JSON keys are rejected so a
/// caller cannot smuggle an unreviewed command, shell, or environment field.
#[derive(Debug, Clone)]
pub struct ModuleOwnerPlan {
    pub state_dir: PathBuf,
    pub executable: PathBuf,
    pub argv: Vec<String>,
    pub protected_refs: Vec<ProtectedRef>,
    pub environment: Vec<(String, String)>,
    pub module: String,
    pub binding: String,
    pub generation: String,
    pub artifact_id: String,
    pub artifact_version: String,
    pub build_id: Option<String>,
    pub protocol: String,
    /// Descriptor-derived compatibility claim. Data only; Store validation
    /// remains the authority before any admission or prompt write.
    pub module_contract: String,
    pub module_client_id: String,
    pub boot_id: String,
}

#[derive(Debug, Clone)]
struct ResolverEntry {
    reference: String,
    credential_file: PathBuf,
    sha256: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProtectedResolverMap {
    version: u64,
    module: String,
    binding: String,
    generation: String,
    artifact_id: String,
    artifact_version: String,
    build_id: Option<String>,
    protocol: String,
    module_contract: String,
    entries: Vec<ResolverEntry>,
}

/// Typed adapter attachment proof. The owner envelope is the exact bounded
/// `owner.json` value, `boot_id` is the supervisor-generated per-start UUID,
/// and `process_image_identity` is the fresh receipt for this distinct child.
#[derive(Debug, Clone)]
pub struct VerifiedModuleWorker {
    pub owner_record: Value,
    pub boot_id: String,
    pub process_image_identity: Value,
}

/// Launch values returned by a trusted late resolver.  `executable` and
/// `argv` must remain byte-for-byte equal to the pinned plan values.  Only
/// ephemeral environment values may be added by the resolver.
#[derive(Clone)]
pub struct ResolvedLaunch {
    pub executable: PathBuf,
    pub argv: Vec<String>,
    pub environment: Vec<(String, String)>,
}

/// Resolution happens after lock acquisition and owner publication, directly
/// before spawn. Implementations must not start processes or persist values.
pub trait ProtectedRefResolver {
    fn resolve(
        &self,
        plan: &ModuleOwnerPlan,
        owner_record: &Path,
        owner: &Value,
    ) -> Result<ResolvedLaunch>;
}

/// The explicit resolver map binds opaque references to existing absolute
/// credential files. It returns the file path to the adapter, never a token.
impl ProtectedResolverMap {
    pub fn load(path: &Path) -> Result<Self> {
        reject_link_components(path, false)?;
        let file = File::open(path)?;
        let mut bytes = Vec::new();
        file.take(MAX_PLAN_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_PLAN_BYTES {
            return Err(Error::invalid("resolver map exceeds the bounded envelope"));
        }
        parse_resolver_map(serde_json::from_slice(&bytes)?)
    }
}

impl ProtectedRefResolver for ProtectedResolverMap {
    fn resolve(
        &self,
        plan: &ModuleOwnerPlan,
        _owner_record: &Path,
        _owner: &Value,
    ) -> Result<ResolvedLaunch> {
        if plan.module != self.module
            || plan.binding != self.binding
            || plan.generation != self.generation
            || plan.artifact_id != self.artifact_id
            || plan.artifact_version != self.artifact_version
            || plan.build_id != self.build_id
            || plan.protocol != self.protocol
            || plan.module_contract != self.module_contract
        {
            return Err(Error::new(
                "MODULE_OWNER_RESOLUTION_FAILED",
                "resolver map is bound to a different module generation or artifact",
            ));
        }
        let mut environment = plan.environment.clone();
        environment.reserve(plan.protected_refs.len());
        let mut credential_refs = 0usize;
        if self.version == 1
            && plan
                .protected_refs
                .iter()
                .any(|reference| reference.name == MODULE_CREDENTIAL_FILE_ENV)
        {
            return Err(Error::new(
                "MODULE_OWNER_CREDENTIAL_DIGEST_MISSING",
                "legacy path-only resolver maps cannot authorize a binding credential",
            ));
        }
        for reference in &plan.protected_refs {
            let entry = self
                .entries
                .iter()
                .find(|entry| entry.reference.as_str() == reference.reference.as_str())
                .ok_or_else(|| {
                    Error::new(
                        "MODULE_OWNER_RESOLUTION_FAILED",
                        format!("unknown protected reference {}", reference.reference),
                    )
                })?;
            reject_link_components(&entry.credential_file, false)?;
            if !entry.credential_file.is_file() {
                return Err(Error::new(
                    "MODULE_OWNER_RESOLUTION_FAILED",
                    format!("credential file for {} is missing", reference.reference),
                ));
            }
            let credential_file = fs::canonicalize(&entry.credential_file).map_err(|error| {
                Error::new(
                    "MODULE_OWNER_RESOLUTION_FAILED",
                    format!(
                        "credential file for {} cannot be canonicalized: {error}",
                        reference.reference
                    ),
                )
            })?;
            if self.version == 2 {
                let expected_sha256 = entry.sha256.as_deref().ok_or_else(|| {
                    Error::new(
                        "MODULE_OWNER_RESOLUTION_FAILED",
                        "versioned resolver entry has no protected-file digest",
                    )
                })?;
                let max_bytes = if reference.name == MODULE_CREDENTIAL_FILE_ENV {
                    MAX_CREDENTIAL_FILE_BYTES
                } else {
                    MAX_PROTECTED_REF_FILE_BYTES
                };
                let actual_sha256 = hash_bounded_file_sha256(&credential_file, max_bytes)?;
                if !actual_sha256.eq_ignore_ascii_case(expected_sha256) {
                    return Err(Error::new(
                        if reference.name == MODULE_CREDENTIAL_FILE_ENV {
                            "MODULE_OWNER_CREDENTIAL_DIGEST_MISMATCH"
                        } else {
                            "MODULE_OWNER_PROTECTED_REF_DIGEST_MISMATCH"
                        },
                        "protected file differs from its resolver-map SHA-256 pin",
                    ));
                }
            }
            if reference.name == MODULE_CREDENTIAL_FILE_ENV {
                credential_refs += 1;
            }
            environment.push((
                reference.name.clone(),
                credential_file.to_string_lossy().into_owned(),
            ));
        }
        if (self.version == 2 && credential_refs != 1)
            || (self.version == 1 && credential_refs != 0)
        {
            return Err(Error::new(
                "MODULE_OWNER_CREDENTIAL_DIGEST_MISSING",
                "versioned resolver requires exactly one Store credential; legacy path-only maps cannot carry one",
            ));
        }
        Ok(ResolvedLaunch {
            executable: plan.executable.clone(),
            argv: plan.argv.clone(),
            environment,
        })
    }
}

/// Path-backed bootstrap used by `swarm-module-owner`.
pub struct ModuleOwnerBootstrap;

impl ModuleOwnerBootstrap {
    /// Read one private JSON plan and explicit resolver map, then resolve
    /// protected references immediately before the exact child spawn.
    pub fn run(plan_path: &Path, resolver_map_path: &Path) -> Result<ExitStatus> {
        let plan = read_plan(plan_path)?;
        let resolver = ProtectedResolverMap::load(resolver_map_path)?;
        run_module_with_resolver(plan, &resolver)
    }

    /// Host seam for a trusted resolver supplied by an embedding supervisor.
    pub fn run_with_resolver<R: ProtectedRefResolver>(
        plan_path: &Path,
        resolver: &R,
    ) -> Result<ExitStatus> {
        let plan = read_plan(plan_path)?;
        run_module_with_resolver(plan, resolver)
    }
}

/// Run one exact module plan with an explicit resolver map. Registration
/// starts no worker until the late resolver succeeds and the single exact
/// child is spawned.
pub fn run_module(plan: ModuleOwnerPlan, resolver: ProtectedResolverMap) -> Result<ExitStatus> {
    run_module_with_resolver(plan, &resolver)
}

pub fn run_module_with_resolver<R: ProtectedRefResolver>(
    plan: ModuleOwnerPlan,
    resolver: &R,
) -> Result<ExitStatus> {
    validate_plan(&plan)?;
    reject_link_components(&plan.state_dir, true)?;
    reject_link_components(&plan.executable, false)?;
    fs::create_dir_all(&plan.state_dir)?;
    let dir = fs::canonicalize(&plan.state_dir)?;
    let lock = match acquire_state_marker(&dir, "module.lock", MARKER.as_bytes()) {
        Ok(lock) => lock,
        Err(StateMarkerError::Busy) => {
            return Err(Error::new(
                "MODULE_OWNER_ACTIVE",
                "module marker lock is already held",
            ));
        }
        Err(StateMarkerError::ForeignDirectory | StateMarkerError::InvalidMarker) => {
            return Err(Error::new(
                "FOREIGN_STATE_DIRECTORY",
                "use a dedicated empty module state directory",
            ));
        }
        Err(StateMarkerError::System(error)) => return Err(error),
    };
    private_permissions(&dir, true)?;

    let record_path = dir.join("owner.json");
    if record_path.try_exists()? {
        let previous = read_record(&record_path);
        match previous {
            Ok(owner) => {
                if let Err(error) = verify_departed(&owner) {
                    if error.code == "INVALID_PARAMS" {
                        record_gap(&dir, "incomplete_owner_identity", &error.to_string());
                    }
                    return Err(error);
                }
            }
            Err(error) => {
                record_gap(&dir, "corrupt_owner_identity", &error.to_string());
                return Err(Error::new(
                    "MODULE_OWNER_IDENTITY_INVALID",
                    format!(
                        "recorded module-owner identity is unreadable; recovery gap recorded, checkpoint is not adopted: {error}"
                    ),
                ));
            }
        }
    } else if dir.join("checkpoint.json").try_exists()? {
        record_gap(
            &dir,
            "missing_owner_identity",
            "checkpoint exists without an ownership record",
        );
        return Err(Error::new(
            "MODULE_OWNER_IDENTITY_MISSING",
            "checkpoint without ownership evidence is not safe to resume; recovery gap recorded",
        ));
    }

    // worker.json is helper-owned evidence. Remove only this prior worker
    // receipt after the owner departure proof above; unknown checkpoints are
    // never removed or adopted.
    let worker_path = dir.join("worker.json");
    if worker_path.try_exists()? {
        fs::remove_file(&worker_path)?;
    }

    let token = uuid::Uuid::new_v4().to_string();
    let group = Group::enter_module(&token)?;
    let owner = json!({"version":1,"token":token,"process":group.identity.clone()});
    publish(&record_path, &owner)?;
    let resolved = match resolver.resolve(&plan, &record_path, &owner) {
        Ok(resolved) => resolved,
        Err(error) => {
            publish_pre_spawn_failure(&dir, &plan, &owner, &group, "resolve_refs", &error)?;
            return Err(error);
        }
    };
    if let Err(error) = validate_resolved(&plan, &resolved) {
        publish_pre_spawn_failure(&dir, &plan, &owner, &group, "validate_launch", &error)?;
        return Err(error);
    }

    let mut command = Command::new(&resolved.executable);
    apply_launch_environment(&mut command);
    command
        .args(&resolved.argv)
        .env_remove("ELIOT_SWARM_MODULE_BOOT_ID")
        .env("ELIOT_SWARM_MODULE_OWNER", &record_path)
        .env("ELIOT_SWARM_MODULE_STATE", &dir)
        .env("ELIOT_SWARM_MODULE_BOOT_ID", &plan.boot_id)
        .env("ELIOT_SWARM_MODULE_ID", &plan.module)
        .env("ELIOT_SWARM_MODULE_BINDING_ID", &plan.binding)
        .env("ELIOT_SWARM_MODULE_BINDING_GENERATION", &plan.generation)
        .env("ELIOT_SWARM_MODULE_CLIENT_ID", &plan.module_client_id)
        .env("ELIOT_SWARM_MODULE_ARTIFACT_ID", &plan.artifact_id)
        .env(
            "ELIOT_SWARM_MODULE_ARTIFACT_VERSION",
            &plan.artifact_version,
        )
        .env("ELIOT_SWARM_MODULE_PROTOCOL", &plan.protocol)
        .env("ELIOT_SWARM_MODULE_CONTRACT", &plan.module_contract);
    if let Some(build_id) = &plan.build_id {
        command.env("ELIOT_SWARM_MODULE_BUILD_ID", build_id);
    } else {
        command.env_remove("ELIOT_SWARM_MODULE_BUILD_ID");
    }
    for (name, value) in &resolved.environment {
        command.env(name, value);
    }
    let mut child = command.spawn()?;
    let child_identity = process_image_identity(child.id());
    let worker_error = if let Ok(identity) = &child_identity {
        publish(
            &worker_path,
            &json!({
                "version":1,
                "boot_id":plan.boot_id.clone(),
                "module":plan.module.clone(),
                "binding":plan.binding.clone(),
                "generation":plan.generation.clone(),
                "artifact_id":plan.artifact_id.clone(),
                "artifact_version":plan.artifact_version.clone(),
                "build_id":plan.build_id.clone(),
                "protocol":plan.protocol.clone(),
                "module_contract":plan.module_contract.clone(),
                "module_client_id":plan.module_client_id.clone(),
                "process":identity
            }),
        )
        .err()
    } else {
        None
    };
    let status_result = child.wait();
    // Module groups are deliberately non-killing. Keep the OS lock and group
    // alive until the bridge and every native descendant have departed.
    while !group.children_empty()? {
        std::thread::sleep(Duration::from_millis(500));
    }
    drop(group);
    drop(lock);
    let status = status_result?;
    if let Some(error) = worker_error {
        return Err(Error::new(
            "MODULE_WORKER_IDENTITY",
            format!("spawned adapter identity receipt could not be published: {error}"),
        ));
    }
    if let Err(error) = child_identity {
        return Err(Error::new(
            "MODULE_WORKER_IDENTITY",
            format!("spawned adapter identity could not be recorded: {error}"),
        ));
    }
    if status.success() {
        Ok(status)
    } else {
        Err(Error::new(
            "MODULE_EXITED",
            format!("bridge ended: {status}; native group is empty"),
        ))
    }
}

/// Start the adapter from a deliberately bounded environment. The Windows
/// baseline is limited to OS launch, home, and configuration discovery values;
/// all module-specific literals and protected-reference file paths come from
/// the validated plan/resolver below. This is an environment boundary, not a
/// same-user sandbox or a provider/account authorization boundary.
fn apply_launch_environment(command: &mut Command) {
    command.env_clear();
    #[cfg(windows)]
    for name in WINDOWS_LAUNCH_ENVIRONMENT {
        if let Some(value) = env::var_os(name) {
            command.env(name, value);
        }
    }
}

/// Adapter-side proof: the current process is a distinct live member of the
/// exact non-killing owner group and has the current image receipt. This does
/// not adopt, launch, stop, or authorize work.
pub fn verify_current_adapter(owner_record: &Path) -> Result<VerifiedModuleWorker> {
    let owner = read_record(owner_record)?;
    if owner.get("version").and_then(Value::as_u64) != Some(1)
        || owner["process"]["purpose"] != "module"
        || owner
            .get("token")
            .and_then(Value::as_str)
            .is_none_or(|token| uuid::Uuid::parse_str(token).is_err())
    {
        return Err(Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            "owner record is not a valid v1 module envelope",
        ));
    }
    let boot_id = env::var("ELIOT_SWARM_MODULE_BOOT_ID").map_err(|_| {
        Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            "ELIOT_SWARM_MODULE_BOOT_ID is missing",
        )
    })?;
    if uuid::Uuid::parse_str(&boot_id).is_err() {
        return Err(Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            "ELIOT_SWARM_MODULE_BOOT_ID is not a UUID",
        ));
    }
    let identity = process_image_identity(std::process::id())?;
    if !module_child_belongs_to_owner(&owner, &identity)? {
        return Err(Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            "current adapter is not a distinct member of the recorded module owner",
        ));
    }
    Ok(VerifiedModuleWorker {
        owner_record: owner,
        boot_id,
        process_image_identity: identity,
    })
}

pub fn verify_current_adapter_from_env() -> Result<VerifiedModuleWorker> {
    let path = env::var_os("ELIOT_SWARM_MODULE_OWNER").ok_or_else(|| {
        Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            "ELIOT_SWARM_MODULE_OWNER is missing",
        )
    })?;
    verify_current_adapter(Path::new(&path))
}

/// Read the helper-owned child receipt used by a supervisor to re-query the
/// exact adapter image and prove membership with
/// `module_child_belongs_to_owner`. The receipt is bounded and never an
/// authorization by itself.
pub fn read_worker_record(state_dir: &Path) -> Result<Value> {
    read_record(&state_dir.join("worker.json"))
}

fn read_plan(path: &Path) -> Result<ModuleOwnerPlan> {
    reject_link_components(path, false)?;
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(MAX_PLAN_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_PLAN_BYTES {
        return Err(Error::invalid("module plan exceeds the bounded envelope"));
    }
    let value: Value = serde_json::from_slice(&bytes)?;
    parse_plan(value)
}

fn parse_plan(value: Value) -> Result<ModuleOwnerPlan> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("module plan must be an object"))?;
    let expected = [
        "version",
        "state_dir",
        "executable",
        "argv",
        "protected_refs",
        "environment",
        "module",
        "binding",
        "generation",
        "artifact_id",
        "artifact_version",
        "build_id",
        "protocol",
        "module_contract",
        "module_client_id",
        "boot_id",
    ];
    let keys: BTreeSet<&str> = object.keys().map(String::as_str).collect();
    let expected_keys: BTreeSet<&str> = expected.into_iter().collect();
    if keys != expected_keys {
        return Err(Error::invalid("module plan has unknown or missing fields"));
    }
    if object.get("version").and_then(Value::as_u64) != Some(PLAN_VERSION) {
        return Err(Error::new(
            "MODULE_PLAN_VERSION",
            "unsupported module plan version",
        ));
    }
    let state_dir = absolute_path(object, "state_dir")?;
    let executable = absolute_path(object, "executable")?;
    let argv = object
        .get("argv")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("argv must be an array"))?
        .iter()
        .map(|value| {
            let value = value
                .as_str()
                .ok_or_else(|| Error::invalid("argv entries must be strings"))?;
            bounded_string(value, MAX_ARG_BYTES, "argv entry")
        })
        .collect::<Result<Vec<_>>>()?;
    let protected_refs = object
        .get("protected_refs")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("protected_refs must be an array"))?
        .iter()
        .map(parse_ref)
        .collect::<Result<Vec<_>>>()?;
    let environment = object
        .get("environment")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("environment must be an array"))?
        .iter()
        .map(parse_environment)
        .collect::<Result<Vec<_>>>()?;
    let module = bounded_string(text_field(object, "module")?, MAX_REF_BYTES, "module")?;
    let binding = bounded_string(text_field(object, "binding")?, MAX_REF_BYTES, "binding")?;
    let generation = bounded_string(
        text_field(object, "generation")?,
        MAX_REF_BYTES,
        "generation",
    )?;
    let artifact_id = bounded_string(
        text_field(object, "artifact_id")?,
        MAX_REF_BYTES,
        "artifact_id",
    )?;
    let artifact_version = bounded_string(
        text_field(object, "artifact_version")?,
        MAX_REF_BYTES,
        "artifact_version",
    )?;
    let build_id = optional_text_field(object, "build_id")?;
    let protocol = bounded_string(text_field(object, "protocol")?, MAX_REF_BYTES, "protocol")?;
    let module_contract = bounded_string(
        text_field(object, "module_contract")?,
        MAX_REF_BYTES,
        "module_contract",
    )?;
    let module_client_id = bounded_string(
        text_field(object, "module_client_id")?,
        MAX_REF_BYTES,
        "module_client_id",
    )?;
    let boot_id = bounded_string(text_field(object, "boot_id")?, 128, "boot_id")?;
    let plan = ModuleOwnerPlan {
        state_dir,
        executable,
        argv,
        protected_refs,
        environment,
        module,
        binding,
        generation,
        artifact_id,
        artifact_version,
        build_id,
        protocol,
        module_contract,
        module_client_id,
        boot_id,
    };
    validate_plan(&plan)?;
    Ok(plan)
}

fn parse_ref(value: &Value) -> Result<ProtectedRef> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("protected reference must be an object"))?;
    let keys: BTreeSet<&str> = object.keys().map(String::as_str).collect();
    if keys != BTreeSet::from(["name", "reference"]) {
        return Err(Error::invalid(
            "protected reference has unknown or missing fields",
        ));
    }
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid("protected reference name must be a string"))?;
    let name = bounded_string(name, 128, "protected reference name")?;
    if name.is_empty()
        || !name.chars().enumerate().all(|(index, ch)| {
            ch == '_' || ch.is_ascii_uppercase() || (index > 0 && ch.is_ascii_digit())
        })
    {
        return Err(Error::invalid(
            "protected reference name must be an environment key",
        ));
    }
    let reference = object
        .get("reference")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid("protected reference must be a string"))?;
    let reference = bounded_string(reference, MAX_REF_BYTES, "protected reference")?;
    Ok(ProtectedRef { name, reference })
}

fn parse_environment(value: &Value) -> Result<(String, String)> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("environment entry must be an object"))?;
    let keys: BTreeSet<&str> = object.keys().map(String::as_str).collect();
    if keys != BTreeSet::from(["name", "value"]) {
        return Err(Error::invalid(
            "environment entry has unknown or missing fields",
        ));
    }
    let name = bounded_string(text_field(object, "name")?, 128, "environment name")?;
    let value = bounded_string(
        text_field(object, "value")?,
        MAX_REF_BYTES,
        "environment value",
    )?;
    validate_environment_name(&name)?;
    Ok((name, value))
}

fn text_field<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid(format!("{field} must be a string")))
}

fn optional_text_field(object: &Map<String, Value>, field: &str) -> Result<Option<String>> {
    let value = object
        .get(field)
        .ok_or_else(|| Error::invalid(format!("{field} is missing")))?;
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(bounded_string(
        text_field(object, field)?,
        MAX_REF_BYTES,
        field,
    )?))
}

fn parse_resolver_map(value: Value) -> Result<ProtectedResolverMap> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("resolver map must be an object"))?;
    let expected = [
        "version",
        "module",
        "binding",
        "generation",
        "artifact_id",
        "artifact_version",
        "build_id",
        "protocol",
        "module_contract",
        "refs",
    ];
    let keys: BTreeSet<&str> = object.keys().map(String::as_str).collect();
    if keys != expected.into_iter().collect() {
        return Err(Error::invalid("resolver map has unknown or missing fields"));
    }
    let version = object
        .get("version")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::new("MODULE_RESOLVER_VERSION", "resolver map version is missing"))?;
    if !matches!(version, 1 | RESOLVER_MAP_VERSION) {
        return Err(Error::new(
            "MODULE_RESOLVER_VERSION",
            "unsupported resolver map version",
        ));
    }
    let refs = object
        .get("refs")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("resolver refs must be an array"))?;
    if refs.len() > MAX_REFS {
        return Err(Error::invalid("resolver map has too many refs"));
    }
    let mut entries = Vec::with_capacity(refs.len());
    for value in refs {
        let item = value
            .as_object()
            .ok_or_else(|| Error::invalid("resolver entry must be an object"))?;
        let entry_keys: BTreeSet<&str> = item.keys().map(String::as_str).collect();
        let expected_entry_keys = if version == RESOLVER_MAP_VERSION {
            BTreeSet::from(["reference", "credential_file", "sha256"])
        } else {
            BTreeSet::from(["reference", "credential_file"])
        };
        if entry_keys != expected_entry_keys {
            return Err(Error::invalid(
                "resolver entry has unknown or missing fields",
            ));
        }
        let reference = bounded_string(
            text_field(item, "reference")?,
            MAX_REF_BYTES,
            "resolver reference",
        )?;
        let credential_file = absolute_path(item, "credential_file")?;
        let sha256 = if version == RESOLVER_MAP_VERSION {
            Some(text_field(item, "sha256")?.to_owned())
        } else {
            None
        };
        if sha256
            .as_deref()
            .is_some_and(|digest| !is_sha256_hex(digest))
        {
            return Err(Error::invalid(
                "resolver credential digest must be a 64-character SHA-256 hex value",
            ));
        }
        if !entries
            .iter()
            .all(|entry: &ResolverEntry| entry.reference.as_str() != reference.as_str())
        {
            return Err(Error::invalid("resolver references must be unique"));
        }
        entries.push(ResolverEntry {
            reference,
            credential_file,
            sha256,
        });
    }
    Ok(ProtectedResolverMap {
        version,
        module: bounded_string(text_field(object, "module")?, MAX_REF_BYTES, "module")?,
        binding: bounded_string(text_field(object, "binding")?, MAX_REF_BYTES, "binding")?,
        generation: bounded_string(
            text_field(object, "generation")?,
            MAX_REF_BYTES,
            "generation",
        )?,
        artifact_id: bounded_string(
            text_field(object, "artifact_id")?,
            MAX_REF_BYTES,
            "artifact_id",
        )?,
        artifact_version: bounded_string(
            text_field(object, "artifact_version")?,
            MAX_REF_BYTES,
            "artifact_version",
        )?,
        build_id: optional_text_field(object, "build_id")?,
        protocol: bounded_string(text_field(object, "protocol")?, MAX_REF_BYTES, "protocol")?,
        module_contract: bounded_string(
            text_field(object, "module_contract")?,
            MAX_REF_BYTES,
            "module_contract",
        )?,
        entries,
    })
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn hash_bounded_file_sha256(path: &Path, max_bytes: u64) -> Result<String> {
    use sha2::{Digest, Sha256};

    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(Error::new(
            "MODULE_OWNER_PROTECTED_FILE_INVALID",
            "protected file exceeds the regular-file size bound",
        ));
    }
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024];
    let mut total = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if total > max_bytes {
            return Err(Error::new(
                "MODULE_OWNER_PROTECTED_FILE_INVALID",
                "protected file exceeds the regular-file size bound",
            ));
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn absolute_path(object: &Map<String, Value>, field: &str) -> Result<PathBuf> {
    let text = object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid(format!("{field} must be a string")))?;
    bounded_string(text, MAX_REF_BYTES, field)?;
    let path = PathBuf::from(text);
    if !path.is_absolute() {
        return Err(Error::invalid(format!("{field} must be an absolute path")));
    }
    Ok(path)
}

fn bounded_string(value: &str, max: usize, label: &str) -> Result<String> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(Error::invalid(format!(
            "{label} is outside the supported boundary"
        )));
    }
    Ok(value.to_owned())
}

fn validate_plan(plan: &ModuleOwnerPlan) -> Result<()> {
    if plan.argv.len() > MAX_ARGV
        || plan.protected_refs.len() > MAX_REFS
        || plan.environment.len() > MAX_REFS
    {
        return Err(Error::invalid(
            "module plan list exceeds the supported boundary",
        ));
    }
    if !plan.state_dir.is_absolute() || !plan.executable.is_absolute() {
        return Err(Error::invalid("module paths must be absolute"));
    }
    if plan
        .executable
        .file_stem()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name.to_ascii_lowercase().as_str(),
                "sh" | "bash"
                    | "dash"
                    | "zsh"
                    | "fish"
                    | "ksh"
                    | "cmd"
                    | "powershell"
                    | "pwsh"
                    | "wsl"
            )
        })
    {
        return Err(Error::invalid("module executable must not be a shell"));
    }
    #[cfg(windows)]
    if plan
        .executable
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext.to_ascii_lowercase().as_str(), "cmd" | "bat" | "com"))
    {
        return Err(Error::invalid(
            "select a native executable, not a shell wrapper",
        ));
    }
    for arg in &plan.argv {
        bounded_string(arg, MAX_ARG_BYTES, "argv entry")?;
    }
    for value in [
        &plan.module,
        &plan.binding,
        &plan.generation,
        &plan.artifact_id,
        &plan.artifact_version,
        &plan.protocol,
        &plan.module_contract,
        &plan.module_client_id,
    ] {
        bounded_string(value, MAX_REF_BYTES, "module binding field")?;
    }
    if let Some(build_id) = &plan.build_id {
        bounded_string(build_id, MAX_REF_BYTES, "build_id")?;
    }
    if uuid::Uuid::parse_str(&plan.boot_id).is_err() {
        return Err(Error::invalid("boot_id must be a UUID"));
    }
    let mut names = BTreeSet::new();
    for reference in &plan.protected_refs {
        if !names.insert(&reference.name) {
            return Err(Error::invalid("protected reference names must be unique"));
        }
        bounded_string(&reference.reference, MAX_REF_BYTES, "protected reference")?;
        validate_protected_environment_name(&reference.name)?;
    }
    for (name, value) in &plan.environment {
        if !names.insert(name) {
            return Err(Error::invalid(
                "environment names must be unique across literals and refs",
            ));
        }
        validate_environment_name(name)?;
        bounded_string(value, MAX_REF_BYTES, "environment value")?;
    }
    Ok(())
}

fn validate_resolved(plan: &ModuleOwnerPlan, resolved: &ResolvedLaunch) -> Result<()> {
    if resolved.executable != plan.executable || resolved.argv != plan.argv {
        return Err(Error::new(
            "MODULE_OWNER_RESOLUTION_FAILED",
            "trusted resolver changed the pinned executable or argv",
        ));
    }
    if resolved.environment.len() != plan.protected_refs.len() + plan.environment.len() {
        return Err(Error::new(
            "MODULE_OWNER_RESOLUTION_FAILED",
            "trusted resolver returned an unexpected environment set",
        ));
    }
    let allowed: BTreeSet<&str> = plan
        .protected_refs
        .iter()
        .map(|item| item.name.as_str())
        .chain(plan.environment.iter().map(|(name, _)| name.as_str()))
        .collect();
    let mut seen = BTreeSet::new();
    for (name, value) in &resolved.environment {
        if !allowed.contains(name.as_str()) || !seen.insert(name.as_str()) {
            return Err(Error::new(
                "MODULE_OWNER_RESOLUTION_FAILED",
                "resolver returned an unauthorized environment key",
            ));
        }
        bounded_string(name, 128, "resolved environment name")?;
        if name == "ELIOT_SWARM_MODULE_OWNER"
            || name == "ELIOT_SWARM_MODULE_STATE"
            || name == "ELIOT_SWARM_MODULE_BOOT_ID"
            || name == "ELIOT_SWARM_MODULE_CONTRACT"
            || name == "ELIOT_SWARM_MODULE_CLIENT_ID"
            || name == "ELIOT_SWARM_MODULE_BINDING_ID"
            || name == "ELIOT_SWARM_MODULE_BINDING_GENERATION"
            || name == "ELIOT_SWARM_MODULE_ARTIFACT_ID"
            || name == "ELIOT_SWARM_MODULE_ARTIFACT_VERSION"
            || name == "ELIOT_SWARM_MODULE_BUILD_ID"
        {
            return Err(Error::new(
                "MODULE_OWNER_RESOLUTION_FAILED",
                "resolver cannot override owner environment",
            ));
        }
        if value.len() > MAX_REF_BYTES || value.chars().any(char::is_control) {
            return Err(Error::new(
                "MODULE_OWNER_RESOLUTION_FAILED",
                "resolved environment value is outside the supported boundary",
            ));
        }
    }
    Ok(())
}

fn validate_environment_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name.chars().enumerate().all(|(index, ch)| {
            ch == '_' || ch.is_ascii_uppercase() || (index > 0 && ch.is_ascii_digit())
        })
        || name.starts_with("ELIOT_SWARM_MODULE_")
    {
        return Err(Error::invalid(
            "environment name is outside the protected module boundary",
        ));
    }
    Ok(())
}

fn validate_protected_environment_name(name: &str) -> Result<()> {
    if name == "ELIOT_SWARM_MODULE_CREDENTIAL_FILE" {
        return Ok(());
    }
    validate_environment_name(name)
}

fn read_record(path: &Path) -> Result<Value> {
    reject_link_components(path, false)?;
    let mut body = Vec::new();
    File::open(path)?
        .take(OWNER_MAX_BYTES + 1)
        .read_to_end(&mut body)?;
    if body.len() as u64 > OWNER_MAX_BYTES {
        return Err(Error::invalid("module ownership record exceeds envelope"));
    }
    Ok(serde_json::from_slice(&body)?)
}

fn verify_departed(owner: &Value) -> Result<()> {
    let token = owner
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid("a recorded module-owner identity is required"))?;
    if owner.get("version").and_then(Value::as_u64) != Some(1)
        || uuid::Uuid::parse_str(token).is_err()
        || owner["process"]["purpose"] != "module"
    {
        return Err(Error::invalid(
            "a recorded module-owner identity is required",
        ));
    }
    if !departed_empty(&owner["process"], token)? {
        return Err(Error::new(
            "MODULE_OWNER_ACTIVE",
            "previous bridge or native descendants remain; no replacement started",
        ));
    }
    Ok(())
}

/// An exact negative launch receipt, created only before any child spawn.
/// Supervisor must still prove wrapper/family departure before replacement.
fn publish_pre_spawn_failure(
    dir: &Path,
    plan: &ModuleOwnerPlan,
    owner: &Value,
    group: &Group,
    stage: &'static str,
    error: &Error,
) -> Result<()> {
    if !group.children_empty()? {
        return Err(Error::new(
            "MODULE_START_STATUS_UNKNOWN",
            "pre-spawn failure has no empty-family proof",
        ));
    }
    publish(
        &dir.join("launch-result.json"),
        &json!({
            "version":1,
            "boot_id":plan.boot_id,
            "module":plan.module,
            "module_client_id":plan.module_client_id,
            "binding":plan.binding,
            "generation":plan.generation,
            "artifact_id":plan.artifact_id,
            "artifact_version":plan.artifact_version,
            "build_id":plan.build_id,
            "protocol":plan.protocol,
            "owner":owner,
            "disposition":"not_started",
            "worker_started":false,
            "family_empty":true,
            "stage":stage,
            "error_code":error.code
        }),
    )
}

fn publish(path: &Path, value: &Value) -> Result<()> {
    let temp = path.with_file_name(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let data = canonical_json(value)?;
        write_private_new(&temp, data.as_bytes())?;
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        File::open(
            path.parent()
                .ok_or_else(|| Error::invalid("no owner directory"))?,
        )?
        .sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn record_gap(dir: &Path, kind: &str, detail: &str) {
    let path = dir.join("recovery-gap.json");
    if let Ok(existing) = read_record(&path)
        && existing["kind"] == kind
    {
        return;
    }
    let recorded_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    let _ = publish(
        &path,
        &json!({"version":1,"kind":kind,"detail":detail,"disposition":"recovery_not_authorized","recorded_at_ms":recorded_at_ms}),
    );
}

fn canonical_json(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok("null".into()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(serde_json::to_string(value)?),
        Value::Array(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Result<Vec<_>>>()?
                .join(",")
        )),
        Value::Object(values) => {
            let mut keys: Vec<&String> = values.keys().collect();
            keys.sort();
            let mut fields = Vec::with_capacity(keys.len());
            for key in keys {
                let child = values
                    .get(key)
                    .ok_or_else(|| Error::invalid("canonical JSON key disappeared"))?;
                fields.push(format!(
                    "{}:{}",
                    serde_json::to_string(key)?,
                    canonical_json(child)?
                ));
            }
            Ok(format!("{{{}}}", fields.join(",")))
        }
    }
}
