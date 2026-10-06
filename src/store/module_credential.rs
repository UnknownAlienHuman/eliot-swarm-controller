//! Binding-scoped credentials for trusted selected module adapters.
//!
//! This is a host-only path. It accepts an already-admitted Operation and
//! binds one stable module client identity to that Operation's exact binding
//! generation and retained descriptor. Token bytes exist only in the private
//! credential file and short-lived file-I/O memory; Store metadata contains
//! their hash and exact scope only.

use crate::{
    error::{Error, Result},
    model::{self, Credential},
    platform,
};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};
use swarm_contracts::module_catalog::{ArtifactIdentity, ModuleId, ProtectedRef, ProtocolVersion};

const PROVISION_KEY: &str = "module_credential_provision";
const MAX_CREDENTIAL_FILE_BYTES: u64 = 4 * 1024;
const PENDING_OPERATION_STATES: [&str; 4] =
    ["queued", "sending", "native_accepted", "outcome_unknown"];
const MODULE_OPERATIONS: [&str; 15] = [
    "agent.open",
    "task.dispatch",
    "agent.send",
    "agent.reply",
    "agent.configure",
    "agent.goal",
    "agent.background",
    "agent.refresh",
    "agent.reconcile",
    "agent.result",
    "agent.recover",
    "native.mcp.install",
    "native.mcp.observe",
    "native.mcp.arm",
    "native.mcp.read",
];

/// A host-only proof that the exact retained binding credential exists and is
/// registered. This DTO deliberately has no token or token-hash field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProvisionedModuleCredential {
    pub operation_id: String,
    pub binding_id: String,
    pub generation: i64,
    pub module_id: ModuleId,
    pub artifact_id: String,
    pub artifact_version: String,
    pub build_id: Option<String>,
    pub descriptor_revision: u64,
    pub protocol: ProtocolVersion,
    pub module_client_id: String,
    pub credential_ref: ProtectedRef,
    pub credential_file: PathBuf,
    pub credential_file_sha256: String,
    pub ready: bool,
}

#[derive(Debug, Clone)]
struct ProvisionScope {
    operation_id: String,
    binding_id: String,
    generation: i64,
    identity: Value,
    module_id: ModuleId,
    artifact: ArtifactIdentity,
    descriptor_revision: u64,
    protocol: ProtocolVersion,
    module_client_id: String,
    credential_ref: ProtectedRef,
    scope_digest: String,
    expected_token_hash: Option<String>,
    expected_file_sha256: Option<String>,
}

#[derive(Debug, Clone)]
struct FileEvidence {
    credential_file: PathBuf,
    token_hash: String,
    file_sha256: String,
}

impl super::Store {
    /// Prepare the private credential for one already-admitted native
    /// Operation. Repeated calls for the same binding reuse the exact identity
    /// and file; neither manager authority nor another registration is
    /// requested after Operation admission.
    pub(crate) async fn ensure_module_binding_credential(
        &self,
        operation_id: &str,
        binding_id: &str,
        generation: i64,
    ) -> Result<ProvisionedModuleCredential> {
        validate_scope_text(operation_id, "operation_id")?;
        validate_scope_text(binding_id, "binding_id")?;
        if generation <= 0 {
            return Err(Error::invalid("generation must be positive"));
        }

        let operation = operation_id.to_owned();
        let binding = binding_id.to_owned();
        let scope = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let scope = resolve_scope(&tx, &operation, &binding, generation)?;
                reserve_scope(&tx, &scope)?;
                tx.commit()?;
                Ok(scope)
            })
            .await?;

        let data_dir = self.data_dir.clone();
        let file_scope = scope.clone();
        let evidence = self
            .file_io(move |_| prepare_private_credential(&data_dir, &file_scope))
            .await?;

        let operation = operation_id.to_owned();
        let binding = binding_id.to_owned();
        let final_evidence = evidence.clone();
        let committed = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = resolve_scope(&tx, &operation, &binding, generation)?;
                verify_file_evidence(&current, &final_evidence)?;
                let changed = register_scope(&tx, &current, &final_evidence)?;
                tx.commit()?;
                Ok(changed)
            })
            .await?;
        if committed {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }

        Ok(project(&scope, evidence, true))
    }

    /// Recheck registration and the exact private file after the supervisor
    /// writes its per-scope resolver map and immediately before helper start.
    /// This is read-only and fails closed if the Operation, descriptor, Store
    /// hashes, or file changed since preparation.
    pub(crate) async fn check_module_binding_credential_ready(
        &self,
        operation_id: &str,
        binding_id: &str,
        generation: i64,
    ) -> Result<ProvisionedModuleCredential> {
        validate_scope_text(operation_id, "operation_id")?;
        validate_scope_text(binding_id, "binding_id")?;
        if generation <= 0 {
            return Err(Error::invalid("generation must be positive"));
        }

        let operation = operation_id.to_owned();
        let binding = binding_id.to_owned();
        let scope = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let scope = resolve_scope(&tx, &operation, &binding, generation)?;
                require_ready(&scope)?;
                tx.commit()?;
                Ok(scope)
            })
            .await?;

        let data_dir = self.data_dir.clone();
        let file_scope = scope.clone();
        let evidence = self
            .file_io(move |_| read_private_credential(&data_dir, &file_scope))
            .await?;

        let operation = operation_id.to_owned();
        let binding = binding_id.to_owned();
        let check_evidence = evidence.clone();
        let current = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = resolve_scope(&tx, &operation, &binding, generation)?;
                require_ready(&current)?;
                verify_file_evidence(&current, &check_evidence)?;
                tx.commit()?;
                Ok(current)
            })
            .await?;

        Ok(project(&current, evidence, true))
    }
}

fn validate_scope_text(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!(
            "{field} is outside the bounded identifier shape"
        )));
    }
    Ok(())
}

fn resolve_scope(
    db: &Connection,
    operation_id: &str,
    binding_id: &str,
    generation: i64,
) -> Result<ProvisionScope> {
    let operation = super::operations::get_operation(db, operation_id)?;
    let method = operation["method"].as_str().unwrap_or_default();
    let state = operation["state"].as_str().unwrap_or_default();
    let settled_open_anchor = method == "agent.open" && state == "settled";
    if !MODULE_OPERATIONS.contains(&method)
        || (!PENDING_OPERATION_STATES.contains(&state) && !settled_open_anchor)
    {
        return Err(Error::new(
            "MODULE_CREDENTIAL_OPERATION_NOT_PENDING",
            "credential provisioning requires pending work or a retained settled agent.open anchor",
        ));
    }
    if operation["binding_id"].as_str() != Some(binding_id)
        || operation["binding_generation"].as_i64() != Some(generation)
    {
        return Err(Error::new(
            "MODULE_CREDENTIAL_SCOPE_MISMATCH",
            "Operation does not belong to the requested binding generation",
        ));
    }

    let binding = super::operations::get_binding(db, binding_id, generation)?;
    if !binding["released_at_ms"].is_null() {
        return Err(Error::new(
            "BINDING_CLOSED",
            "cannot provision a credential for a released binding",
        ));
    }
    if settled_open_anchor
        && !(binding["native_root_id"]
            .as_str()
            .is_some_and(|value| !value.trim().is_empty())
            && binding["native_scope_key"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()))
    {
        return Err(Error::new(
            "MODULE_CREDENTIAL_OPERATION_NOT_PENDING",
            "settled agent.open is not an active retained native-session anchor",
        ));
    }
    let selector = binding["observation"]
        .get("module_contract_selector")
        .ok_or_else(|| {
            Error::new(
                "MODULE_DESCRIPTOR_NOT_SELECTED",
                "binding has no retained trusted module descriptor",
            )
        })?;
    let artifact_id = binding["module_artifact_id"].as_str().ok_or_else(|| {
        Error::new(
            "MODULE_ROUTE_CORRUPT",
            "binding artifact identity is missing",
        )
    })?;
    let retained =
        super::module_handshake::retained_contract_identity(db, artifact_id, Some(selector))?
            .ok_or_else(|| {
                Error::new(
                    "MODULE_DESCRIPTOR_NOT_SELECTED",
                    "binding has no retained trusted module descriptor",
                )
            })?;
    let descriptor = super::module_handshake::retained_descriptor(db, &retained)?;
    let credential_ref = descriptor.launch.credential_ref.clone().ok_or_else(|| {
        Error::new(
            "MODULE_CREDENTIAL_REF_REQUIRED",
            "trusted module descriptor has no protected credential reference",
        )
    })?;

    let identity = json!({
        "schema_version": 1,
        "binding_id": binding_id,
        "binding_generation": generation,
        "module_contract_selector": selector.clone(),
        "module_id": retained.module_id.clone(),
        "artifact": retained.artifact.clone(),
        "protocol": retained.protocol,
        "descriptor_revision": retained.descriptor_revision,
        "credential_ref": credential_ref.clone(),
    });
    let scope_digest = model::digest(model::canonical(&identity)?.as_bytes());
    let module_client_id = format!("module:{scope_digest}");
    let mut scope = ProvisionScope {
        operation_id: operation_id.to_owned(),
        binding_id: binding_id.to_owned(),
        generation,
        identity,
        module_id: retained.module_id,
        artifact: retained.artifact,
        descriptor_revision: retained.descriptor_revision,
        protocol: retained.protocol,
        module_client_id,
        credential_ref,
        scope_digest,
        expected_token_hash: None,
        expected_file_sha256: None,
    };
    inspect_registration(db, &binding, &mut scope)?;
    Ok(scope)
}

fn inspect_registration(
    db: &Connection,
    binding: &Value,
    scope: &mut ProvisionScope,
) -> Result<()> {
    let reservation = binding["observation"].get(PROVISION_KEY);
    let binding_client = binding["observation"]["module_client_id"].as_str();
    let registration = super::meta(db, &format!("client:{}", scope.module_client_id))?;

    match (reservation, registration) {
        (None, None) => {
            if binding_client.is_some() {
                return Err(scope_conflict());
            }
        }
        (Some(reservation), None) => {
            if !exact_keys(reservation, &["identity", "state"])
                || reservation["identity"] != scope.identity
                || reservation["state"] != "preparing"
                || binding_client.is_some()
            {
                return Err(scope_conflict());
            }
        }
        (Some(reservation), Some(registration)) => {
            if !exact_keys(
                reservation,
                &["identity", "state", "credential_file_sha256"],
            ) || reservation["identity"] != scope.identity
                || reservation["state"] != "ready"
                || binding_client != Some(scope.module_client_id.as_str())
            {
                return Err(scope_conflict());
            }
            let file_sha256 = reservation["credential_file_sha256"]
                .as_str()
                .filter(|value| valid_sha256(value))
                .ok_or_else(scope_conflict)?;
            if !exact_registration(&registration, scope, file_sha256) {
                return Err(scope_conflict());
            }
            let token_hash = registration["token_hash"]
                .as_str()
                .filter(|value| valid_sha256(value))
                .ok_or_else(scope_conflict)?;
            scope.expected_token_hash = Some(token_hash.to_owned());
            scope.expected_file_sha256 = Some(file_sha256.to_owned());
        }
        (None, Some(_)) => return Err(scope_conflict()),
    }
    Ok(())
}

fn exact_registration(registration: &Value, scope: &ProvisionScope, file_sha256: &str) -> bool {
    exact_keys(
        registration,
        &[
            "role",
            "token_hash",
            "disabled",
            "binding_id",
            "binding_generation",
            "module_credential_identity",
            "credential_file_sha256",
        ],
    ) && registration["role"] == "module"
        && registration["disabled"].as_bool() == Some(false)
        && registration["binding_id"].as_str() == Some(scope.binding_id.as_str())
        && registration["binding_generation"].as_i64() == Some(scope.generation)
        && registration["module_credential_identity"] == scope.identity
        && registration["credential_file_sha256"].as_str() == Some(file_sha256)
        && registration["token_hash"]
            .as_str()
            .is_some_and(valid_sha256)
}

fn reserve_scope(tx: &Transaction<'_>, scope: &ProvisionScope) -> Result<()> {
    let binding = super::operations::get_binding(tx, &scope.binding_id, scope.generation)?;
    if binding["observation"].get(PROVISION_KEY).is_some() {
        return Ok(());
    }
    if binding["observation"].get("module_client_id").is_some() {
        return Err(scope_conflict());
    }
    let reservation = json!({"identity":scope.identity.clone(),"state":"preparing"});
    let changed = tx.execute(
        "UPDATE bindings SET state_json=json_set(state_json,'$.module_credential_provision',json(?3)) WHERE binding_id=?1 AND generation=?2 AND released_at_ms IS NULL",
        params![
            scope.binding_id,
            scope.generation,
            model::canonical(&reservation)?
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "binding changed before module credential reservation",
        ));
    }
    Ok(())
}

fn register_scope(
    tx: &Transaction<'_>,
    scope: &ProvisionScope,
    evidence: &FileEvidence,
) -> Result<bool> {
    let binding = super::operations::get_binding(tx, &scope.binding_id, scope.generation)?;
    verify_file_evidence(scope, evidence)?;
    if let Some(expected) = &scope.expected_token_hash
        && expected != &evidence.token_hash
    {
        return Err(scope_conflict());
    }
    if let Some(expected) = &scope.expected_file_sha256
        && expected != &evidence.file_sha256
    {
        return Err(scope_conflict());
    }

    let registration_key = format!("client:{}", scope.module_client_id);
    match super::meta(tx, &registration_key)? {
        Some(existing) => {
            if !exact_registration(&existing, scope, &evidence.file_sha256)
                || existing["token_hash"].as_str() != Some(evidence.token_hash.as_str())
                || binding["observation"]["module_client_id"].as_str()
                    != Some(scope.module_client_id.as_str())
                || !is_ready_reservation(&binding, scope, &evidence.file_sha256)
            {
                return Err(scope_conflict());
            }
            Ok(false)
        }
        None => {
            if scope.expected_token_hash.is_some()
                || binding["observation"].get("module_client_id").is_some()
                || !is_preparing_reservation(&binding, scope)
            {
                return Err(scope_conflict());
            }
            let runtime_scope = super::runtime::register(
                tx,
                &json!({
                    "binding_id": scope.binding_id.clone(),
                    "binding_generation": scope.generation,
                }),
                &scope.module_client_id,
            )?;
            let mut registration = json!({
                "role":"module",
                "token_hash":evidence.token_hash.clone(),
                "disabled":false,
                "module_credential_identity":scope.identity.clone(),
                "credential_file_sha256":evidence.file_sha256.clone(),
            });
            if let Some(fields) = runtime_scope.as_object() {
                for (key, value) in fields {
                    registration[key] = value.clone();
                }
            }
            super::set_meta(tx, &registration_key, &registration)?;
            let reservation = json!({
                "identity":scope.identity.clone(),
                "state":"ready",
                "credential_file_sha256":evidence.file_sha256.clone(),
            });
            let changed = tx.execute(
                "UPDATE bindings SET state_json=json_set(state_json,'$.module_credential_provision',json(?3)) WHERE binding_id=?1 AND generation=?2 AND released_at_ms IS NULL",
                params![
                    scope.binding_id,
                    scope.generation,
                    model::canonical(&reservation)?
                ],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "binding changed during module credential registration",
                ));
            }
            Ok(true)
        }
    }
}

fn is_preparing_reservation(binding: &Value, scope: &ProvisionScope) -> bool {
    let Some(reservation) = binding["observation"].get(PROVISION_KEY) else {
        return false;
    };
    exact_keys(reservation, &["identity", "state"])
        && reservation["identity"] == scope.identity
        && reservation["state"] == "preparing"
}

fn is_ready_reservation(binding: &Value, scope: &ProvisionScope, file_sha256: &str) -> bool {
    let Some(reservation) = binding["observation"].get(PROVISION_KEY) else {
        return false;
    };
    exact_keys(
        reservation,
        &["identity", "state", "credential_file_sha256"],
    ) && reservation["identity"] == scope.identity
        && reservation["state"] == "ready"
        && reservation["credential_file_sha256"].as_str() == Some(file_sha256)
}

fn require_ready(scope: &ProvisionScope) -> Result<()> {
    if scope.expected_token_hash.is_none() || scope.expected_file_sha256.is_none() {
        return Err(Error::new(
            "MODULE_CREDENTIAL_NOT_READY",
            "binding credential has not completed Store registration",
        ));
    }
    Ok(())
}

fn prepare_private_credential(data_dir: &Path, scope: &ProvisionScope) -> Result<FileEvidence> {
    let data_root = fs::canonicalize(data_dir)?;
    let credential_root = data_root.join("module-credentials");
    ensure_private_directory(&credential_root, &data_root)?;
    let scope_dir = credential_root.join(&scope.scope_digest);
    ensure_private_directory(&scope_dir, &data_root)?;
    let path = scope_dir.join("credential.json");

    if !path.try_exists()? {
        if scope.expected_token_hash.is_some() || scope.expected_file_sha256.is_some() {
            return Err(Error::new(
                "MODULE_CREDENTIAL_FILE_MISSING",
                "registered module credential file is missing; implicit rotation is forbidden",
            ));
        }
        let credential = Credential {
            client_id: scope.module_client_id.clone(),
            token: format!("{}{}", model::new_id(), model::new_id()),
        };
        let bytes = serde_json::to_vec(&credential)?;
        if let Err(error) = platform::write_private_new(&path, &bytes)
            && !path.try_exists()?
        {
            return Err(error);
        }
    }
    let evidence = read_credential_at(&path, scope)?;
    platform::private_permissions(&evidence.credential_file, false)?;
    Ok(evidence)
}

fn read_private_credential(data_dir: &Path, scope: &ProvisionScope) -> Result<FileEvidence> {
    let data_root = fs::canonicalize(data_dir)?;
    let path = data_root
        .join("module-credentials")
        .join(&scope.scope_digest)
        .join("credential.json");
    read_credential_at(&path, scope)
}

fn read_credential_at(path: &Path, scope: &ProvisionScope) -> Result<FileEvidence> {
    if !path.is_absolute() {
        return Err(Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "private credential path is not absolute",
        ));
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "private credential must be a regular file without a symlink leaf",
        ));
    }
    if metadata.len() > MAX_CREDENTIAL_FILE_BYTES {
        return Err(Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "private credential file exceeds its bounded size",
        ));
    }
    let canonical = fs::canonicalize(path)?;
    if canonical.as_path() != path {
        return Err(Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "private credential path resolves outside its reserved location",
        ));
    }
    let file = File::open(&canonical)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_CREDENTIAL_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
        return Err(Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "private credential file exceeds its bounded size",
        ));
    }
    let credential: Credential = serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "private credential file is malformed",
        )
    })?;
    if credential.client_id != scope.module_client_id
        || credential.token.len() < 32
        || credential.token.len() > 512
        || !credential
            .token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "private credential does not match its reserved client identity",
        ));
    }
    let token_hash = model::digest(credential.token.as_bytes());
    let file_sha256 = model::digest(&bytes);
    if scope
        .expected_token_hash
        .as_deref()
        .is_some_and(|expected| expected != token_hash)
        || scope
            .expected_file_sha256
            .as_deref()
            .is_some_and(|expected| expected != file_sha256)
    {
        return Err(scope_conflict());
    }
    Ok(FileEvidence {
        credential_file: canonical,
        token_hash,
        file_sha256,
    })
}

fn ensure_private_directory(path: &Path, data_root: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(Error::new(
                    "MODULE_CREDENTIAL_PATH_INVALID",
                    "private credential directory must be a real directory",
                ));
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        },
        Err(error) => return Err(error.into()),
    }
    let canonical = fs::canonicalize(path)?;
    if canonical.as_path() != path || !canonical.starts_with(data_root) {
        return Err(Error::new(
            "MODULE_CREDENTIAL_PATH_INVALID",
            "private credential directory resolves outside the Store data root",
        ));
    }
    platform::private_permissions(&canonical, true)?;
    Ok(())
}

fn verify_file_evidence(scope: &ProvisionScope, evidence: &FileEvidence) -> Result<()> {
    if !valid_sha256(&evidence.token_hash) || !valid_sha256(&evidence.file_sha256) {
        return Err(Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "private credential digest is malformed",
        ));
    }
    if scope
        .expected_token_hash
        .as_deref()
        .is_some_and(|expected| expected != evidence.token_hash)
        || scope
            .expected_file_sha256
            .as_deref()
            .is_some_and(|expected| expected != evidence.file_sha256)
    {
        return Err(scope_conflict());
    }
    Ok(())
}

fn project(
    scope: &ProvisionScope,
    evidence: FileEvidence,
    ready: bool,
) -> ProvisionedModuleCredential {
    ProvisionedModuleCredential {
        operation_id: scope.operation_id.clone(),
        binding_id: scope.binding_id.clone(),
        generation: scope.generation,
        module_id: scope.module_id.clone(),
        artifact_id: scope.artifact.artifact_id.as_str().to_owned(),
        artifact_version: scope.artifact.version.as_str().to_owned(),
        build_id: scope.artifact.build_id.clone(),
        descriptor_revision: scope.descriptor_revision,
        protocol: scope.protocol,
        module_client_id: scope.module_client_id.clone(),
        credential_ref: scope.credential_ref.clone(),
        credential_file: evidence.credential_file,
        credential_file_sha256: evidence.file_sha256,
        ready,
    }
}

fn exact_keys(value: &Value, expected: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
    })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn scope_conflict() -> Error {
    Error::new(
        "MODULE_CREDENTIAL_SCOPE_CONFLICT",
        "stored module credential differs from the retained binding scope",
    )
}
