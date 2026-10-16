//! Store-owned trust snapshot and strict module.hello negotiation.
//!
//! Registration is an internal host call. The caller must first verify the
//! installed descriptor and its artifact at the local trust boundary. Module
//! credentials can submit only a claim; they can never add or replace a
//! registered descriptor. Negotiated capabilities are descriptive only and do
//! not alter Store method authorization.

use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use swarm_contracts::module_catalog::{
    ArtifactIdentity, ArtifactVersion, CapabilityId, ModuleCatalog, ModuleDescriptor,
    ModuleDescriptorInstallRequest, ModuleId, PreInputOpenContract, ProtocolVersion,
    SchemaDescriptor, WorkspaceOptionContract,
};
use swarm_contracts::module_command::{capability_satisfies, classify_runtime_command};
use swarm_contracts::module_contract::{
    MODULE_PROTOCOL_V1, ModuleContractClaim, native_mcp_command_schema,
};
use swarm_contracts::runtime_catalog::ModuleDescriptorEvidence;
use swarm_supervisor::control::{
    CompleteInstalledDescriptorRequest, DescriptorRegistrationReceipt,
    InstalledDescriptorCompletionOutcome, InstalledDescriptorCompletionReceipt,
    InstalledDescriptorOperationOutcome, InstalledDescriptorOperationState,
    InstalledDescriptorRequestPage, InstalledDescriptorRequestRecord,
    InstalledDescriptorRequestState,
};

const REGISTRY_KEY: &str = "module_catalog:trusted_descriptors:v1";
const REGISTRY_SCHEMA_VERSION: u16 = 1;
const MAX_DESCRIPTORS: usize = 256;
const MAX_REGISTRY_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_DESCRIPTOR_BYTES: usize = 128 * 1024;
const MAX_ROUTE_SELECTION_OWNERS: usize = 4096;
const MAX_ROUTE_SELECTIONS_PER_OWNER: usize = 256;
const MAX_PAGE_ITEMS: usize = 8;
const MAX_PAGE_BYTES: usize = 512 * 1024;

const LEGACY_SUPERVISOR_CAPABILITIES: &[&str] = &["module.descriptor.register"];
const PREVIOUS_SUPERVISOR_CAPABILITIES: &[&str] = &[
    "module.descriptor.register",
    "module.supervisor.admission",
    "module.supervisor.demand.page",
    "module.supervisor.scope.readback",
    "module.supervisor.credential.ensure",
    "module.supervisor.credential.ready",
    "module.supervisor.recovery.reconcile",
    "module.supervisor.observation.record",
    "module.supervisor.health.record",
];
const CURRENT_PRE_INSTALL_SUPERVISOR_CAPABILITIES: &[&str] = &[
    "module.descriptor.register",
    "module.supervisor.admission",
    "module.supervisor.demand.page",
    "module.supervisor.scope.readback",
    "module.supervisor.credential.ensure",
    "module.supervisor.credential.ready",
    "module.supervisor.recovery.reconcile",
    "module.supervisor.observation.record",
    "module.supervisor.health.record",
    "module.supervisor.health.read",
];
const SUPERVISOR_CAPABILITIES: &[&str] = &[
    "module.descriptor.register",
    "module.supervisor.admission",
    "module.supervisor.demand.page",
    "module.supervisor.scope.readback",
    "module.supervisor.credential.ensure",
    "module.supervisor.credential.ready",
    "module.supervisor.recovery.reconcile",
    "module.supervisor.observation.record",
    "module.supervisor.health.record",
    "module.supervisor.health.read",
    "module.supervisor.install.requests",
    "module.supervisor.install.complete",
];

/// The currently implemented generic module contract. A descriptor may
/// advertise a wider compatible range, but this Store negotiates only 1.0.
/// Bump this constant only with a host implementation of the new contract.
const HOST_PROTOCOL: ProtocolVersion = MODULE_PROTOCOL_V1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    schema_version: u16,
    revision: u64,
    descriptors: Vec<RegisteredDescriptor>,
    /// Stable Manager identity -> route alias -> selection. A Manager's
    /// selection must never change another Manager's future bindings.
    selections: BTreeMap<String, BTreeMap<String, RouteSelection>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            schema_version: REGISTRY_SCHEMA_VERSION,
            revision: 0,
            descriptors: Vec::new(),
            selections: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisteredDescriptor {
    registered_revision: u64,
    descriptor: ModuleDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteSelection {
    schema_version: u16,
    module_id: ModuleId,
    artifact: ArtifactIdentity,
    registered_revision: u64,
    selected_revision: u64,
}

#[derive(Debug, Clone)]
pub(super) struct NewBindingDescriptorContract {
    pub selector: Value,
    pub workspace_option: Option<WorkspaceOptionContract>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RetainedModuleIdentity {
    pub descriptor_revision: u64,
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    pub protocol: ProtocolVersion,
    /// Trusted implementation metadata only. These names can gate adapter
    /// compatibility, but never grant Store or native-effect authority.
    pub capabilities: BTreeSet<CapabilityId>,
    pub command_schemas: BTreeSet<SchemaDescriptor>,
    pub event_schemas: BTreeSet<SchemaDescriptor>,
}

fn catalog_error(error: impl std::fmt::Display) -> Error {
    Error::new("MODULE_DESCRIPTOR_INVALID", error.to_string())
}

fn bounded_json(value: &Value, maximum: usize, what: &str) -> Result<()> {
    if model::canonical(value)?.len() > maximum {
        return Err(Error::new(
            "MODULE_CATALOG_TOO_LARGE",
            format!("{what} exceeds its size limit"),
        ));
    }
    Ok(())
}

pub(super) fn supervisor_scope_matches(client_id: &str, registration: &Value) -> bool {
    client_id == model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID
        && registration["role"] == "module_supervisor"
        && registration["internal_only"] == false
        && registration["disabled"] == false
        && registration["module_scope"] == "descriptor_catalog"
        && (registration["capabilities"] == json!(LEGACY_SUPERVISOR_CAPABILITIES)
            || registration["capabilities"] == json!(PREVIOUS_SUPERVISOR_CAPABILITIES)
            || registration["capabilities"] == json!(CURRENT_PRE_INSTALL_SUPERVISOR_CAPABILITIES)
            || registration["capabilities"] == json!(SUPERVISOR_CAPABILITIES))
        && registration["token_hash"].as_str().is_some_and(|hash| {
            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        && registration
            .as_object()
            .is_some_and(|fields| fields.len() == 6)
}

fn runtime_supervisor_scope_matches(client_id: &str, registration: &Value) -> bool {
    supervisor_scope_matches(client_id, registration)
        && registration["capabilities"] == json!(SUPERVISOR_CAPABILITIES)
}

pub(super) fn require_supervisor_scope(db: &Connection, principal: &Principal) -> Result<()> {
    let registration = super::meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "supervisor credential is not registered"))?;
    if principal.role != Role::ModuleSupervisor
        || registration["disabled"] == true
        || !runtime_supervisor_scope_matches(&principal.client_id, &registration)
    {
        return Err(Error::new(
            "FORBIDDEN",
            "local supervisor credential has no descriptor registration scope",
        ));
    }
    Ok(())
}

fn validate_registry(registry: &Registry) -> Result<()> {
    if registry.schema_version != REGISTRY_SCHEMA_VERSION
        || registry.descriptors.len() > MAX_DESCRIPTORS
        || registry.descriptors.iter().any(|entry| {
            entry.registered_revision == 0 || entry.registered_revision > registry.revision
        })
    {
        return Err(Error::new(
            "MODULE_CATALOG_CORRUPT",
            "trusted module catalog metadata has an invalid schema or revision",
        ));
    }
    let descriptors = registry
        .descriptors
        .iter()
        .map(|entry| entry.descriptor.clone())
        .collect::<Vec<_>>();
    ModuleCatalog::from_descriptors(descriptors).map_err(catalog_error)?;
    if registry.selections.len() > MAX_ROUTE_SELECTION_OWNERS {
        return Err(Error::new(
            "MODULE_CATALOG_CORRUPT",
            "trusted module route selection owner limit is exceeded",
        ));
    }
    for (owner_id, routes) in &registry.selections {
        if owner_id.trim().is_empty() || routes.len() > MAX_ROUTE_SELECTIONS_PER_OWNER {
            return Err(Error::new(
                "MODULE_CATALOG_CORRUPT",
                "trusted module route selection owner or count is invalid",
            ));
        }
        for (alias, selection) in routes {
            if alias.is_empty()
                || alias.len() > 128
                || alias.chars().any(char::is_control)
                || selection.schema_version != 1
                || selection.selected_revision == 0
                || selection.selected_revision < selection.registered_revision
                || selection.selected_revision > registry.revision
                || !registry.descriptors.iter().any(|entry| {
                    entry.registered_revision == selection.registered_revision
                        && entry.descriptor.module_id == selection.module_id
                        && entry.descriptor.artifact == selection.artifact
                })
            {
                return Err(Error::new(
                    "MODULE_CATALOG_CORRUPT",
                    "trusted module route selection is invalid",
                ));
            }
        }
    }
    let encoded = serde_json::to_value(registry)?;
    bounded_json(&encoded, MAX_REGISTRY_BYTES, "trusted module catalog")
}

fn load_registry(db: &Connection) -> Result<Registry> {
    let Some(value) = super::meta(db, REGISTRY_KEY)? else {
        return Ok(Registry::default());
    };
    let registry: Registry = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "MODULE_CATALOG_CORRUPT",
            "trusted module catalog is malformed",
        )
    })?;
    validate_registry(&registry)?;
    Ok(registry)
}

fn save_registry(tx: &Transaction<'_>, registry: &Registry) -> Result<()> {
    validate_registry(registry)?;
    super::set_meta(tx, REGISTRY_KEY, &serde_json::to_value(registry)?)
}

const MAX_PENDING_DESCRIPTOR_INSTALLS: i64 = 1;

/// Reserve one authenticated Operator handoff as the existing durable Operation.
/// This transaction never reads the descriptor or touches the filesystem.
pub(super) fn reserve_descriptor_install(
    tx: &Transaction<'_>,
    principal: &Principal,
    params: &Value,
    operation_id: &str,
) -> Result<(Value, bool)> {
    if principal.role != Role::Operator {
        return Err(Error::new(
            "FORBIDDEN",
            "only the local Operator may request descriptor installation",
        ));
    }
    super::require_local_operator(tx, &principal.client_id)?;
    let request: ModuleDescriptorInstallRequest = serde_json::from_value(params.clone())
        .map_err(|_| Error::invalid("installed descriptor request is malformed"))?;
    let descriptor_path = request.descriptor_file.as_os_str().to_string_lossy();
    if request.client_request_id.is_empty()
        || request.client_request_id.len() > 128
        || request
            .client_request_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || !request.descriptor_file.is_absolute()
        || descriptor_path.len() > 1024
        || descriptor_path.chars().any(char::is_control)
    {
        return Err(Error::invalid(
            "descriptor install request needs an absolute path and request ID",
        ));
    }
    let pending: i64 = tx.query_row(
        "SELECT COUNT(*) FROM operations WHERE method=?1 AND state='queued' AND operation_id != ?2",
        params!["module.descriptor.install", operation_id],
        |row| row.get(0),
    )?;
    if pending >= MAX_PENDING_DESCRIPTOR_INSTALLS {
        return Err(Error::new(
            "MODULE_INSTALL_IN_PROGRESS",
            "the previous explicit descriptor install Operation is still pending",
        ));
    }
    Ok((
        json!({"operation_id":operation_id,"state":"queued","status":"queued"}),
        true,
    ))
}

pub(super) fn has_pending_descriptor_install(db: &Connection) -> Result<bool> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method=?1 AND state='queued')",
        ["module.descriptor.install"],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

/// Return only queued requests for the live child, or the latest successful
/// explicit path for each currently registered artifact when the host rebuilds
/// a child bootstrap. Store reads durable Operations only; it performs no I/O.
pub(super) fn descriptor_install_requests(
    db: &Connection,
    include_installed: bool,
) -> Result<InstalledDescriptorRequestPage> {
    let mut requests = Vec::new();
    if include_installed {
        let registry = load_registry(db)?;
        for entry in registry.descriptors {
            let identity = &entry.descriptor;
            let registered_revision = i64::try_from(entry.registered_revision).map_err(|_| {
                Error::invalid("registered descriptor revision exceeds SQLite integer range")
            })?;
            let retained: Option<(String, String)> = db
                .query_row(
                    "SELECT operation_id,original_request_json FROM operations \
                     WHERE method=?1 AND state='settled' \
                       AND CASE WHEN json_valid(result_json) THEN json_extract(result_json,'$.outcome') ELSE '' END='installed' \
                       AND CASE WHEN json_valid(result_json) THEN json_extract(result_json,'$.module_id') ELSE '' END=?2 \
                       AND CASE WHEN json_valid(result_json) THEN json_extract(result_json,'$.artifact_id') ELSE '' END=?3 \
                       AND CASE WHEN json_valid(result_json) THEN json_extract(result_json,'$.version') ELSE '' END=?4 \
                       AND CASE WHEN json_valid(result_json) THEN json_extract(result_json,'$.registered_revision') ELSE 0 END=?5 \
                     ORDER BY created_at_ms DESC,operation_id DESC LIMIT 1",
                    params![
                        "module.descriptor.install",
                        identity.module_id.to_string(),
                        identity.artifact.artifact_id.to_string(),
                        identity.artifact.version.to_string(),
                        registered_revision,
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((operation_id, original)) = retained {
                let request = parse_descriptor_install_request(&original)?;
                requests.push(InstalledDescriptorRequestRecord {
                    operation_id,
                    descriptor_file: request.descriptor_file,
                    state: InstalledDescriptorRequestState::Installed,
                });
            }
        }
    }
    let mut statement = db.prepare(
        "SELECT operation_id,original_request_json FROM operations \
         WHERE method=?1 AND state='queued' ORDER BY created_at_ms,operation_id LIMIT 2",
    )?;
    let pending = statement
        .query_map(["module.descriptor.install"], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if pending.len() > MAX_PENDING_DESCRIPTOR_INSTALLS as usize {
        return Err(Error::new(
            "MODULE_INSTALL_REQUEST_LIMIT",
            "more than one pending descriptor install Operation was retained",
        ));
    }
    for (operation_id, original) in pending {
        let request = parse_descriptor_install_request(&original)?;
        requests.push(InstalledDescriptorRequestRecord {
            operation_id,
            descriptor_file: request.descriptor_file,
            state: InstalledDescriptorRequestState::Queued,
        });
    }
    let page = InstalledDescriptorRequestPage { requests };
    let encoded = serde_json::to_vec(&page)?;
    if encoded.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "MODULE_INSTALL_REQUEST_LIMIT",
            "descriptor install request page exceeds its bounded size",
        ));
    }
    Ok(page)
}

fn parse_descriptor_install_request(raw: &str) -> Result<ModuleDescriptorInstallRequest> {
    let request: ModuleDescriptorInstallRequest = serde_json::from_str(raw).map_err(|_| {
        Error::new(
            "MODULE_INSTALL_OPERATION_DAMAGED",
            "install request is invalid",
        )
    })?;
    let descriptor_path = request.descriptor_file.as_os_str().to_string_lossy();
    if request.client_request_id.is_empty()
        || request.client_request_id.len() > 128
        || request
            .client_request_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || !request.descriptor_file.is_absolute()
        || descriptor_path.len() > 1024
        || descriptor_path.chars().any(char::is_control)
    {
        return Err(Error::new(
            "MODULE_INSTALL_OPERATION_DAMAGED",
            "install request does not retain an absolute descriptor path",
        ));
    }
    Ok(request)
}

pub(super) fn complete_descriptor_install(
    tx: &Transaction<'_>,
    request: CompleteInstalledDescriptorRequest,
    now: i64,
) -> Result<InstalledDescriptorCompletionReceipt> {
    if request.operation_id.is_empty()
        || request.operation_id.len() > 128
        || request
            .operation_id
            .bytes()
            .any(|byte| byte.is_ascii_control())
    {
        return Err(Error::invalid("descriptor install Operation ID is invalid"));
    }
    let (method, state, original, previous_result): (String, String, String, Option<String>) = tx
        .query_row(
            "SELECT method,state,original_request_json,result_json FROM operations WHERE operation_id=?1",
            [&request.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?
        .ok_or_else(|| Error::new("NOT_FOUND", "descriptor install Operation was not found"))?;
    if method != "module.descriptor.install" {
        return Err(Error::new(
            "FORBIDDEN",
            "supervisor completion is scoped to descriptor install Operations",
        ));
    }
    let _installed_request = parse_descriptor_install_request(&original)?;
    let (next_state, result, receipt) = match request.outcome {
        InstalledDescriptorCompletionOutcome::Rejected => {
            if request.registration.is_some() {
                return Err(Error::invalid(
                    "rejected install cannot include a registration receipt",
                ));
            }
            let error_code = request
                .error_code
                .as_deref()
                .ok_or_else(|| Error::invalid("rejected install needs a bounded error code"))?;
            let error_code = safe_descriptor_install_error_code(error_code)?;
            let result = json!({
                "operation_id":request.operation_id,
                "state":"rejected",
                "outcome":"rejected",
                "error_code":error_code,
            });
            let receipt = InstalledDescriptorCompletionReceipt {
                operation_id: request.operation_id.clone(),
                state: InstalledDescriptorOperationState::Rejected,
                outcome: InstalledDescriptorOperationOutcome::Rejected,
                module_id: None,
                artifact: None,
                catalog_revision: None,
                registered: None,
                unchanged: None,
                error_code: Some(error_code),
            };
            ("rejected", result, receipt)
        }
        InstalledDescriptorCompletionOutcome::Installed => {
            if request.error_code.is_some() {
                return Err(Error::invalid(
                    "installed descriptor cannot include an error code",
                ));
            }
            let registration = request.registration.ok_or_else(|| {
                Error::invalid(
                    "installed descriptor needs the committed Store registration receipt",
                )
            })?;
            validate_install_registration(tx, &registration)?;
            let result = json!({
                "operation_id":request.operation_id,
                "state":"settled",
                "outcome":"installed",
                "module_id":registration.module_id,
                "artifact_id":registration.artifact.artifact_id,
                "version":registration.artifact.version,
                "artifact":registration.artifact,
                "registered":registration.registered,
                "unchanged":registration.unchanged,
                "catalog_revision":registration.catalog_revision,
                "registered_revision":registration.registered_revision,
            });
            let receipt = InstalledDescriptorCompletionReceipt {
                operation_id: request.operation_id.clone(),
                state: InstalledDescriptorOperationState::Settled,
                outcome: InstalledDescriptorOperationOutcome::Installed,
                module_id: Some(registration.module_id),
                artifact: Some(registration.artifact),
                catalog_revision: Some(registration.catalog_revision),
                registered: Some(registration.registered),
                unchanged: Some(registration.unchanged),
                error_code: None,
            };
            ("settled", result, receipt)
        }
    };
    if state != "queued" {
        let existing: Value = previous_result
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .ok_or_else(|| {
                Error::new(
                    "MODULE_INSTALL_COMPLETION_CONFLICT",
                    "Operation is already terminal",
                )
            })?;
        if model::canonical(&existing)? == model::canonical(&result)? {
            return Ok(receipt);
        }
        return Err(Error::new(
            "MODULE_INSTALL_COMPLETION_CONFLICT",
            "descriptor install Operation already has a different terminal receipt",
        ));
    }
    let changed = tx.execute(
        "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?4 \
         WHERE operation_id=?1 AND method='module.descriptor.install' AND state='queued'",
        params![
            request.operation_id,
            next_state,
            model::canonical(&result)?,
            now
        ],
    )?;
    if changed != 1 {
        return Err(Error::new(
            "MODULE_INSTALL_COMPLETION_CONFLICT",
            "descriptor install Operation changed before completion",
        ));
    }
    Ok(receipt)
}

fn validate_install_registration(
    tx: &Transaction<'_>,
    receipt: &DescriptorRegistrationReceipt,
) -> Result<()> {
    if receipt.registered == receipt.unchanged
        || receipt.catalog_revision == 0
        || receipt.registered_revision == 0
        || receipt.registered_revision > receipt.catalog_revision
        || (receipt.registered && receipt.registered_revision != receipt.catalog_revision)
    {
        return Err(Error::new(
            "MODULE_INSTALL_REGISTRATION_INVALID",
            "Store registration receipt has inconsistent durable revisions",
        ));
    }
    let registry = load_registry(tx)?;
    if receipt.catalog_revision > registry.revision
        || !registry.descriptors.iter().any(|entry| {
            entry.descriptor.module_id == receipt.module_id
                && entry.descriptor.artifact == receipt.artifact
                && entry.registered_revision == receipt.registered_revision
        })
    {
        return Err(Error::new(
            "MODULE_INSTALL_REGISTRATION_INVALID",
            "Store registration receipt does not match its current trusted catalog",
        ));
    }
    Ok(())
}

fn safe_descriptor_install_error_code(code: &str) -> Result<String> {
    if code.is_empty()
        || code.len() > 128
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(Error::invalid("descriptor install error code is invalid"));
    }
    Ok(code.to_owned())
}
/// Internal host-only registration. This does not inspect or launch an
/// executable; the trusted local installer/supervisor caller owns that proof.
/// Existing artifact identities are immutable, so live bindings can retain
/// their exact descriptor while newer identities are registered.
pub(super) fn register_trusted_descriptor(
    tx: &Transaction<'_>,
    descriptor: ModuleDescriptor,
) -> Result<Value> {
    descriptor.validate().map_err(catalog_error)?;
    let descriptor_value = serde_json::to_value(&descriptor)?;
    bounded_json(&descriptor_value, MAX_DESCRIPTOR_BYTES, "module descriptor")?;
    let mut registry = load_registry(tx)?;
    if let Some(existing) = registry.descriptors.iter().find(|entry| {
        entry.descriptor.module_id == descriptor.module_id
            && entry.descriptor.artifact == descriptor.artifact
    }) {
        if model::canonical(&serde_json::to_value(&existing.descriptor)?)?
            != model::canonical(&serde_json::to_value(&descriptor)?)?
        {
            return Err(Error::new(
                "MODULE_DESCRIPTOR_IMMUTABLE",
                "an exact registered artifact identity cannot be replaced",
            ));
        }
        return Ok(json!({
            "registered":false,
            "unchanged":true,
            "catalog_revision":registry.revision,
            "module_id":descriptor.module_id,
            "artifact":descriptor.artifact,
            "registered_revision":existing.registered_revision,
        }));
    }
    if registry.descriptors.len() >= MAX_DESCRIPTORS {
        return Err(Error::new(
            "MODULE_CATALOG_FULL",
            "trusted module descriptor limit reached",
        ));
    }
    let next_revision = registry
        .revision
        .checked_add(1)
        .ok_or_else(|| Error::new("MODULE_CATALOG_FULL", "catalog revision is exhausted"))?;
    registry.revision = next_revision;
    registry.descriptors.push(RegisteredDescriptor {
        registered_revision: next_revision,
        descriptor: descriptor.clone(),
    });
    save_registry(tx, &registry)?;
    Ok(json!({
        "registered":true,
        "unchanged":false,
        "catalog_revision":registry.revision,
        "module_id":descriptor.module_id,
        "artifact":descriptor.artifact,
        "registered_revision":next_revision,
    }))
}

/// Snapshot this Manager's selected exact descriptor into the binding's
/// durable state at admission. Later selections affect only that Manager's
/// new bindings; already admitted bindings retain their exact descriptor.
pub(super) fn selection_for_new_binding(
    db: &Connection,
    owner_manager_id: &str,
    route_alias: &str,
    route_runtime: &str,
    configured_artifact_id: &str,
) -> Result<Option<Value>> {
    Ok(descriptor_for_new_binding(
        db,
        owner_manager_id,
        route_alias,
        route_runtime,
        configured_artifact_id,
    )?
    .map(|contract| contract.selector))
}

/// Resolve the Manager's exact selected descriptor for a future binding.
/// Workspace launch admission consumes only this trusted descriptor metadata;
/// the descriptor never grants Store or native-effect authority.
pub(super) fn descriptor_for_new_binding(
    db: &Connection,
    owner_manager_id: &str,
    route_alias: &str,
    route_runtime: &str,
    configured_artifact_id: &str,
) -> Result<Option<NewBindingDescriptorContract>> {
    let registry = load_registry(db)?;
    let selection = registry
        .selections
        .get(owner_manager_id)
        .and_then(|routes| routes.get(route_alias));
    if crate::config::is_selectorless_builtin_route(route_runtime, configured_artifact_id) {
        return if selection.is_some() {
            Err(Error::new(
                "MODULE_ROUTE_STALE",
                "built-in route cannot use a module descriptor selection",
            ))
        } else {
            Ok(None)
        };
    }
    let selection = selection.ok_or_else(|| {
        Error::new(
            "MODULE_CONTRACT_REQUIRED",
            "new module binding requires the Manager's exact trusted descriptor selection",
        )
    })?;
    if selection.artifact.artifact_id.as_str() != configured_artifact_id {
        return Err(Error::new(
            "MODULE_ROUTE_STALE",
            "configured route artifact differs from its selected module descriptor",
        ));
    }
    let descriptor = registry
        .descriptors
        .iter()
        .find(|entry| {
            entry.registered_revision == selection.registered_revision
                && entry.descriptor.module_id == selection.module_id
                && entry.descriptor.artifact == selection.artifact
        })
        .ok_or_else(|| {
            Error::new(
                "MODULE_DESCRIPTOR_MISSING",
                "selected module descriptor is unavailable for a new binding",
            )
        })?;
    let contract = &descriptor.descriptor;
    if contract
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == "task.dispatch")
        && (!contract
            .command_schemas
            .contains(&swarm_contracts::module_contract::task_prompt_schema())
            || !contract
                .command_schemas
                .contains(&swarm_contracts::module_contract::task_dispatch_context_schema())
            || !contract
                .event_schemas
                .contains(&swarm_contracts::module_contract::task_dispatch_admission_schema()))
    {
        return Err(Error::new(
            "TASK_PROMPT_CONTRACT_REQUIRED",
            "new dispatch-capable bindings require the exact TaskPrompt and admission schemas",
        ));
    }
    Ok(Some(NewBindingDescriptorContract {
        selector: serde_json::to_value(selection)?,
        workspace_option: descriptor.descriptor.workspace_option.clone(),
    }))
}

/// Manager-scoped mutation: select an already trusted exact descriptor for the
/// caller's future bindings on one configured route. This is not registration,
/// does not rewrite an active binding, and grants no descriptor capability.
pub(super) fn select_route(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: &Value,
    config: &Config,
    operation_id: &str,
) -> Result<Value> {
    if !matches!(principal.role, Role::Manager | Role::Operator) {
        return Err(Error::new(
            "FORBIDDEN",
            "authenticated Manager or Operator identity required",
        ));
    }
    if principal.client_id.trim().is_empty() {
        return Err(Error::new(
            "FORBIDDEN",
            "authenticated owner identity is invalid",
        ));
    }
    let route_alias = model::text(request, "route_alias")?;
    let module_id =
        ModuleId::new(model::text(request, "module_id")?.to_owned()).map_err(catalog_error)?;
    let artifact_id = swarm_contracts::module_catalog::ArtifactId::new(
        model::text(request, "artifact_id")?.to_owned(),
    )
    .map_err(catalog_error)?;
    let version =
        ArtifactVersion::new(model::text(request, "version")?.to_owned()).map_err(catalog_error)?;
    let expected_revision = request["expected_catalog_revision"]
        .as_u64()
        .ok_or_else(|| Error::invalid("expected_catalog_revision must be nonnegative"))?;
    let mut registry = load_registry(tx)?;
    if registry.revision != expected_revision {
        return Err(Error::new(
            "STALE_MODULE_CATALOG",
            "module catalog changed; read module.catalog.get and retry selection",
        ));
    }
    let route = config.route(route_alias)?;
    if route.module_artifact_id != artifact_id.as_str() {
        return Err(Error::new(
            "MODULE_ROUTE_ARTIFACT_MISMATCH",
            "selected artifact does not match the configured route artifact",
        ));
    }
    // `module_id` is the descriptor's opaque stable identity. `runtime` is
    // the configured native harness label consumed by the adapter; the exact
    // configured artifact ID below is their trusted route correlation.
    let registered = registry
        .descriptors
        .iter()
        .find(|entry| {
            entry.descriptor.module_id == module_id
                && entry.descriptor.artifact.artifact_id == artifact_id
                && entry.descriptor.artifact.version == version
        })
        .ok_or_else(|| {
            Error::new(
                "MODULE_DESCRIPTOR_NOT_FOUND",
                "exact trusted artifact is not registered",
            )
        })?;
    if !registered.descriptor.enabled {
        return Err(Error::new(
            "MODULE_DISABLED",
            "disabled descriptor cannot be selected for new bindings",
        ));
    }
    if !registered.descriptor.protocol.contains(HOST_PROTOCOL) {
        return Err(Error::new(
            "MODULE_PROTOCOL_INCOMPATIBLE",
            "descriptor does not support the host module protocol",
        ));
    }
    let registered_revision = registered.registered_revision;
    let next_revision = registry
        .revision
        .checked_add(1)
        .ok_or_else(|| Error::new("MODULE_CATALOG_FULL", "catalog revision is exhausted"))?;
    if !registry.selections.contains_key(&principal.client_id)
        && registry.selections.len() >= MAX_ROUTE_SELECTION_OWNERS
    {
        return Err(Error::new(
            "MODULE_ROUTE_SELECTIONS_FULL",
            "module route selection owner limit reached",
        ));
    }
    let mut selection = RouteSelection {
        schema_version: 1,
        module_id: module_id.clone(),
        artifact: registered.descriptor.artifact.clone(),
        registered_revision,
        selected_revision: next_revision,
    };
    let owner_selections = registry
        .selections
        .entry(principal.client_id.clone())
        .or_default();
    if owner_selections.len() >= MAX_ROUTE_SELECTIONS_PER_OWNER
        && !owner_selections.contains_key(route_alias)
    {
        return Err(Error::new(
            "MODULE_ROUTE_SELECTIONS_FULL",
            "caller route selection limit reached",
        ));
    }
    if let Some(previous) = owner_selections.get(route_alias)
        && previous.module_id == selection.module_id
        && previous.artifact == selection.artifact
        && previous.registered_revision == selection.registered_revision
    {
        return Ok(json!({
            "operation_id":operation_id,
            "route_alias":route_alias,
            "selection":previous,
            "catalog_revision":registry.revision,
            "unchanged":true,
            "selection_scope":"caller_future_bindings_only",
            "affects":"new_bindings_only",
        }));
    }
    registry.revision = next_revision;
    selection.selected_revision = next_revision;
    owner_selections.insert(route_alias.to_owned(), selection.clone());
    save_registry(tx, &registry)?;
    Ok(json!({
        "operation_id":operation_id,
        "route_alias":route_alias,
        "selection":selection,
        "catalog_revision":registry.revision,
        "unchanged":false,
        "selection_scope":"caller_future_bindings_only",
        "affects":"new_bindings_only",
    }))
}

fn public_descriptor(entry: &RegisteredDescriptor) -> Value {
    let descriptor = &entry.descriptor;
    // Deliberately omit launch executable, argv, environment, working dir and
    // protected references. Catalogue discovery is metadata-only.
    json!({
        "registered_revision":entry.registered_revision,
        "module_id":descriptor.module_id,
        "artifact":descriptor.artifact,
        "protocol":descriptor.protocol,
        "capabilities":descriptor.capabilities,
        "config_schema":descriptor.config_schema,
        "workspace_option":descriptor.workspace_option,
        "pre_input_open":descriptor.pre_input_open,
        "command_schemas":descriptor.command_schemas,
        "event_schemas":descriptor.event_schemas,
        "lifecycle":descriptor.lifecycle,
        "activation":descriptor.activation,
        "enabled":descriptor.enabled,
        "restart":descriptor.restart,
    })
}

/// Read only the exact registered descriptor/selection metadata needed by the
/// runtime catalog. Registration proves a trusted contract record, not that a
/// binary is currently installed or running.
pub(super) fn runtime_catalog_module_evidence(
    principal: &Principal,
    routes: &[crate::config::Route],
    db: &Connection,
) -> Result<(u64, BTreeMap<String, ModuleDescriptorEvidence>)> {
    if !matches!(principal.role, Role::Manager | Role::Operator) {
        return Err(Error::new("FORBIDDEN", "runtime catalog is Manager-only"));
    }
    if principal.role == Role::Operator {
        super::require_local_operator(db, &principal.client_id)?;
    }
    let registry = load_registry(db)?;
    let selections = registry
        .selections
        .get(&principal.client_id)
        .cloned()
        .unwrap_or_default();
    let mut result = BTreeMap::new();
    for route in routes {
        let selection = selections.get(&route.alias);
        let builtin =
            crate::config::is_selectorless_builtin_route(&route.runtime, &route.module_artifact_id);
        let registered_for_route = registry.descriptors.iter().find(|entry| {
            entry.descriptor.artifact.artifact_id.as_str() == route.module_artifact_id
        });
        let evidence = if let Some(selection) = selection {
            let selected = registry.descriptors.iter().find(|entry| {
                entry.registered_revision == selection.registered_revision
                    && entry.descriptor.module_id == selection.module_id
                    && entry.descriptor.artifact == selection.artifact
            });
            let selected =
                if builtin || selection.artifact.artifact_id.as_str() != route.module_artifact_id {
                    None
                } else {
                    selected
                };
            match selected {
                None => ModuleDescriptorEvidence {
                    status: "stale_selection".to_owned(),
                    module_id: Some(selection.module_id.to_string()),
                    artifact_id: Some(selection.artifact.artifact_id.to_string()),
                    registered_revision: Some(selection.registered_revision),
                    selected_revision: Some(selection.selected_revision),
                    contract_capabilities: Vec::new(),
                    gaps: vec![
                        "manager_selection_does_not_match_current_configured_route".to_owned(),
                    ],
                },
                Some(selected) => {
                    let descriptor = &selected.descriptor;
                    ModuleDescriptorEvidence {
                        status: "registered_and_selected".to_owned(),
                        module_id: Some(descriptor.module_id.to_string()),
                        artifact_id: Some(descriptor.artifact.artifact_id.to_string()),
                        registered_revision: Some(selection.registered_revision),
                        selected_revision: Some(selection.selected_revision),
                        contract_capabilities: descriptor
                            .capabilities
                            .iter()
                            .map(ToString::to_string)
                            .collect(),
                        gaps: vec![
                            "descriptor_registration_does_not_prove_binary_installation_or_native_observation".to_owned(),
                            "descriptor_capabilities_are_declared_contract_metadata_not_native_observations".to_owned(),
                        ],
                    }
                }
            }
        } else if builtin {
            ModuleDescriptorEvidence {
                status: "built_in_route".to_owned(),
                module_id: None,
                artifact_id: None,
                registered_revision: None,
                selected_revision: None,
                contract_capabilities: Vec::new(),
                gaps: Vec::new(),
            }
        } else if let Some(entry) = registered_for_route {
            ModuleDescriptorEvidence {
                status: "registered_but_not_selected".to_owned(),
                module_id: Some(entry.descriptor.module_id.to_string()),
                artifact_id: Some(entry.descriptor.artifact.artifact_id.to_string()),
                registered_revision: Some(entry.registered_revision),
                selected_revision: None,
                contract_capabilities: entry
                    .descriptor
                    .capabilities
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                gaps: vec![
                    "manager_has_not_selected_this_descriptor_for_future_bindings".to_owned(),
                    "descriptor_capabilities_are_declared_contract_metadata_not_native_observations".to_owned(),
                ],
            }
        } else {
            ModuleDescriptorEvidence {
                status: "descriptor_unavailable".to_owned(),
                module_id: None,
                artifact_id: None,
                registered_revision: None,
                selected_revision: None,
                contract_capabilities: Vec::new(),
                gaps: vec![
                    "no_exact_trusted_descriptor_is_retained_for_configured_route".to_owned(),
                ],
            }
        };
        result.insert(route.alias.clone(), evidence);
    }
    Ok((registry.revision, result))
}

pub(super) fn read_catalog(
    principal: &Principal,
    request: &Value,
    db: &Connection,
) -> Result<Value> {
    if !matches!(principal.role, Role::Manager | Role::Operator) {
        return Err(Error::new("FORBIDDEN", "module catalog is Manager-only"));
    }
    model::fields(request, &["after", "limit"])?;
    let after = match request.get("after") {
        None => 0,
        Some(value) => usize::try_from(
            value
                .as_u64()
                .ok_or_else(|| Error::invalid("after must be a nonnegative offset"))?,
        )
        .map_err(|_| Error::invalid("after is too large"))?,
    };
    let limit = match request.get("limit") {
        None => 8,
        Some(value) => value
            .as_u64()
            .filter(|value| (1..=MAX_PAGE_ITEMS as u64).contains(value))
            .ok_or_else(|| Error::invalid("limit must be 1..8"))? as usize,
    };
    let registry = load_registry(db)?;
    let descriptor_count = registry.descriptors.len();
    let mut page_end = after.saturating_add(limit).min(descriptor_count);
    let selections = registry
        .selections
        .get(&principal.client_id)
        .cloned()
        .unwrap_or_default();
    let result = loop {
        let descriptors = if after < page_end {
            registry.descriptors[after..page_end]
                .iter()
                .map(public_descriptor)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let result = json!({
            "schema_version":registry.schema_version,
            "catalog_revision":registry.revision,
            "host_protocol":{"major":HOST_PROTOCOL.major,"minor":HOST_PROTOCOL.minor},
            "descriptor_count":descriptor_count,
            "after":after,
            "descriptors":descriptors,
            "next_after":if page_end < descriptor_count { json!(page_end) } else { Value::Null },
            "route_selections":selections,
            "launch_details":"redacted",
        });
        let page_bytes = model::canonical(&result)?.len();
        if page_bytes <= MAX_PAGE_BYTES {
            if page_end == after && after < descriptor_count {
                return Err(Error::new(
                    "MODULE_CATALOG_TOO_LARGE",
                    "module catalog page cannot fit its next descriptor",
                ));
            }
            break result;
        }
        if page_end > after {
            page_end -= 1;
            continue;
        }
        return Err(Error::new(
            "MODULE_CATALOG_TOO_LARGE",
            "module catalog page exceeds its size limit",
        ));
    };
    Ok(result)
}

/// Exact Store-side compare. It reads no module-supplied launch metadata and
/// never turns descriptor capabilities into authorization rights.
pub(super) fn negotiate_hello(
    db: &Connection,
    binding_artifact_id: &str,
    binding_selector: Option<&Value>,
    claim_value: Option<&Value>,
) -> Result<Value> {
    let Some(selector_value) = binding_selector else {
        if claim_value.is_some_and(|value| !value.is_null()) {
            return Err(Error::new(
                "UNEXPECTED_MODULE_CONTRACT",
                "a legacy binding has no trusted descriptor selector",
            ));
        }
        return Ok(json!({
            "status":"legacy_unverified",
            "reason":"binding_has_no_retained_module_descriptor",
        }));
    };
    let retained = retained_contract_identity(db, binding_artifact_id, Some(selector_value))?
        .ok_or_else(|| {
            Error::new(
                "MODULE_DESCRIPTOR_MISSING",
                "binding has no trusted module descriptor",
            )
        })?;
    let claim_value = claim_value
        .filter(|value| !value.is_null())
        .ok_or_else(|| {
            Error::new(
                "MODULE_CONTRACT_REQUIRED",
                "versioned module binding requires a contract claim",
            )
        })?;
    bounded_json(claim_value, 64 * 1024, "module contract claim")?;
    let claim: ModuleContractClaim = serde_json::from_value(claim_value.clone()).map_err(|_| {
        Error::new(
            "MODULE_CONTRACT_INVALID",
            "module contract claim is malformed",
        )
    })?;
    claim.validate().map_err(|_| {
        Error::new(
            "MODULE_CONTRACT_INVALID",
            "module contract arrays must be sorted, unique, and use schema version 1",
        )
    })?;
    let descriptor = retained_descriptor(db, &retained)?;
    let expected_capabilities = retained.capabilities.iter().cloned().collect::<Vec<_>>();
    let expected_commands = descriptor
        .command_schemas
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let expected_events = descriptor.event_schemas.iter().cloned().collect::<Vec<_>>();
    if retained.module_id != claim.module_id
        || retained.artifact != claim.artifact
        || claim.protocol != retained.protocol
        || claim.capabilities != expected_capabilities
        || claim.config_schema != descriptor.config_schema
        || claim.pre_input_open != descriptor.pre_input_open
        || claim.command_schemas != expected_commands
        || claim.event_schemas != expected_events
    {
        return Err(Error::new(
            "MODULE_CONTRACT_MISMATCH",
            "module claim differs from the trusted descriptor or host protocol",
        ));
    }
    Ok(json!({
        "status":"negotiated",
        "source":"store_registered_descriptor",
        "descriptor_revision":retained.descriptor_revision,
        "module_id":retained.module_id,
        "artifact":retained.artifact,
        "protocol":claim.protocol,
        "capabilities":expected_capabilities,
        "config_schema":descriptor.config_schema,
        "pre_input_open":descriptor.pre_input_open,
        "command_schemas":expected_commands,
        "event_schemas":expected_events,
        "effects_authorized_by_descriptor":false,
    }))
}

/// Resolve the exact immutable descriptor snapshot retained by an admitted
/// binding. Runtime receipt validation uses this instead of trusting hello's
/// reply or a module-supplied identity.
pub(super) fn retained_contract_identity(
    db: &Connection,
    binding_artifact_id: &str,
    selector_value: Option<&Value>,
) -> Result<Option<RetainedModuleIdentity>> {
    let Some(selector_value) = selector_value else {
        return Ok(None);
    };
    let selector: RouteSelection =
        serde_json::from_value(selector_value.clone()).map_err(|_| {
            Error::new(
                "MODULE_ROUTE_CORRUPT",
                "binding module selector is malformed",
            )
        })?;
    let registry = load_registry(db)?;
    if selector.schema_version != 1
        || selector.registered_revision == 0
        || selector.selected_revision < selector.registered_revision
        || selector.selected_revision > registry.revision
        || selector.artifact.artifact_id.as_str() != binding_artifact_id
    {
        return Err(Error::new(
            "MODULE_ROUTE_CORRUPT",
            "binding module selector differs from its immutable artifact route",
        ));
    }
    let entry = registry
        .descriptors
        .iter()
        .find(|entry| {
            entry.registered_revision == selector.registered_revision
                && entry.descriptor.module_id == selector.module_id
                && entry.descriptor.artifact == selector.artifact
        })
        .ok_or_else(|| {
            Error::new(
                "MODULE_DESCRIPTOR_MISSING",
                "binding's trusted module descriptor is unavailable",
            )
        })?;
    if !entry.descriptor.enabled {
        return Err(Error::new(
            "MODULE_DISABLED",
            "binding's trusted module descriptor is disabled",
        ));
    }
    if !entry.descriptor.protocol.contains(HOST_PROTOCOL) {
        return Err(Error::new(
            "MODULE_PROTOCOL_INCOMPATIBLE",
            "binding's trusted module descriptor no longer supports the host protocol",
        ));
    }
    Ok(Some(RetainedModuleIdentity {
        descriptor_revision: entry.registered_revision,
        module_id: entry.descriptor.module_id.clone(),
        artifact: entry.descriptor.artifact.clone(),
        protocol: HOST_PROTOCOL,
        capabilities: entry.descriptor.capabilities.clone(),
        command_schemas: entry.descriptor.command_schemas.clone(),
        event_schemas: entry.descriptor.event_schemas.clone(),
    }))
}

/// Reject a native runtime command that is outside the exact descriptor
/// retained by this binding. Capabilities are compatibility metadata only;
/// the existing Store authorization and reservation checks remain authoritative.
/// Only exact built-in route pairs retain selector-less command compatibility.
pub(super) fn require_selected_native_command(
    db: &Connection,
    binding_id: &str,
    (binding_runtime, binding_artifact_id): (&str, &str),
    selector: Option<&Value>,
    method: &str,
    input: &Value,
) -> Result<()> {
    let selector = selector.filter(|value| !value.is_null());
    let selectorless_builtin =
        crate::config::is_selectorless_builtin_route(binding_runtime, binding_artifact_id);
    if selectorless_builtin && selector.is_some() {
        return Err(Error::new(
            "MODULE_ROUTE_STALE",
            "built-in route cannot retain a module descriptor selection",
        ));
    }
    if !selectorless_builtin && selector.is_none() {
        return Err(Error::new(
            "MODULE_CONTRACT_REQUIRED",
            "module runtime command requires the binding's exact trusted descriptor",
        ));
    }
    match selected_native_command_supported(db, binding_artifact_id, selector, method, input)? {
        None | Some(true) => Ok(()),
        Some(false) => Err(Error::new(
            "MODULE_COMMAND_UNSUPPORTED",
            format!("binding {binding_id} does not support runtime method {method}"),
        )),
    }
}

/// `None` means the binding has no retained descriptor and must keep legacy
/// route behavior. `Some(true/false)` is an exact compatibility answer for a
/// selected module descriptor.
fn command_control_artifact_supported(artifact_id: &str, version: &str) -> bool {
    artifact_id == "eliot-command.acp-rust.1" && matches!(version, "1" | "2")
}

pub(super) fn selected_native_command_supported(
    db: &Connection,
    binding_artifact_id: &str,
    selector: Option<&Value>,
    method: &str,
    input: &Value,
) -> Result<Option<bool>> {
    // OpenCode's loop-step input is a vendor command with its own exact
    // capability/schema pair. Do not route it through generic agent.send
    // classification, where "steer" means an expected-turn operation.
    if method == "native.opencode.loop_step" {
        let Some(retained) = retained_contract_identity(db, binding_artifact_id, selector)? else {
            return Ok(Some(false));
        };
        let exact_opencode = retained.module_id.as_str() == "eliot.opencode.v2"
            && retained.artifact.artifact_id.as_str() == "eliot-opencode-v2.rust-http.1"
            && retained.artifact.version.as_str() == "0.5.0";
        let capability_supported = retained
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "native.opencode.loop_step");
        let schema_supported = retained.command_schemas.iter().any(|schema| {
            schema.schema_id == "swarm.opencode_loop_step_command"
                && schema.version == "1"
                && schema.sha256.is_none()
        });
        return Ok(Some(
            exact_opencode && capability_supported && schema_supported,
        ));
    }
    // Unknown OpenCode-native extensions are never enabled by their prefix or
    // by a broad native capability. Each future method needs an explicit map.
    if method.starts_with("native.opencode.") {
        return Ok(Some(false));
    }

    let command = classify_runtime_command(method, input)
        .map_err(|error| Error::new(error.code(), error.message()))?;
    let native_mcp = command.is_native_mcp();
    let command_control = matches!(
        command,
        swarm_contracts::module_command::RuntimeCommandKind::CommandCancelTurn
            | swarm_contracts::module_command::RuntimeCommandKind::CommandCloseSession
    );
    let is_shutdown = matches!(
        command,
        swarm_contracts::module_command::RuntimeCommandKind::AgentShutdown
    );
    let Some(retained) = retained_contract_identity(db, binding_artifact_id, selector)? else {
        // Native MCP phase Operations are meaningful only under the exact
        // retained descriptor that authorizes their command schema. Preserve
        // legacy behavior only for known pre-existing agent.* commands.
        return Ok(if native_mcp || command_control || is_shutdown {
            Some(false)
        } else {
            None
        });
    };
    let required = command.capability();
    let capability_supported = retained
        .capabilities
        .iter()
        .any(|capability| capability_satisfies(capability.as_str(), required));
    let schema_supported = !native_mcp
        || retained
            .command_schemas
            .contains(&native_mcp_command_schema());
    let command_control_supported = !command_control
        || (command_control_artifact_supported(
            retained.artifact.artifact_id.as_str(),
            retained.artifact.version.as_str(),
        ) && retained.command_schemas.iter().any(|schema| {
            schema.schema_id == "swarm.runtime_command"
                && schema.version == "1"
                && schema.sha256.is_none()
        }));
    let is_opencode_artifact = retained.module_id.as_str() == "eliot.opencode.v2"
        && retained.artifact.artifact_id.as_str() == "eliot-opencode-v2.rust-http.1";
    let opencode_reply_schema_supported = !is_opencode_artifact
        || (matches!(
            retained.artifact.version.as_str(),
            "0.5.0" | "0.6.0" | "0.7.0"
        ) && retained.command_schemas.iter().any(|schema| {
            schema.schema_id == "swarm.opencode_reply_command"
                && schema.version == "1"
                && schema.sha256.is_none()
        }));
    Ok(Some(
        capability_supported
            && schema_supported
            && command_control_supported
            && opencode_reply_schema_supported,
    ))
}

pub(super) fn retained_descriptor(
    db: &Connection,
    identity: &RetainedModuleIdentity,
) -> Result<ModuleDescriptor> {
    let registry = load_registry(db)?;
    registry
        .descriptors
        .iter()
        .find(|entry| {
            entry.registered_revision == identity.descriptor_revision
                && entry.descriptor.module_id == identity.module_id
                && entry.descriptor.artifact == identity.artifact
        })
        .map(|entry| entry.descriptor.clone())
        .ok_or_else(|| {
            Error::new(
                "MODULE_DESCRIPTOR_MISSING",
                "binding's trusted module descriptor is unavailable",
            )
        })
}

/// Resolve pre-input semantics only from this binding's retained trusted
/// descriptor; a module's hello claim or route payload is not authoritative.
pub(super) fn retained_pre_input_open(
    db: &Connection,
    binding_artifact_id: &str,
    selector: Option<&Value>,
) -> Result<Option<PreInputOpenContract>> {
    let Some(identity) = retained_contract_identity(db, binding_artifact_id, selector)? else {
        return Ok(None);
    };
    Ok(retained_descriptor(db, &identity)?.pre_input_open)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Route;
    use swarm_contracts::module_catalog::{
        ActivationPolicy, ArtifactId, LaunchSpec, LifecycleOwnership, ModuleId, ProtectedRef,
        RestartPolicy, Sha256Digest,
    };

    #[test]
    fn command_control_accepts_acp_v1_and_v2_retained_descriptors() {
        assert!(command_control_artifact_supported(
            "eliot-command.acp-rust.1",
            "1"
        ));
        assert!(command_control_artifact_supported(
            "eliot-command.acp-rust.1",
            "2"
        ));
        assert!(!command_control_artifact_supported(
            "eliot-command.acp-rust.1",
            "3"
        ));
        assert!(!command_control_artifact_supported("other-artifact", "2"));
    }

    fn descriptor() -> ModuleDescriptor {
        ModuleDescriptor {
            schema_version: 1,
            module_id: ModuleId::new("example.adapter").unwrap(),
            artifact: ArtifactIdentity {
                artifact_id: ArtifactId::new("example-artifact").unwrap(),
                version: ArtifactVersion::new("2.1.0").unwrap(),
                build_id: Some("build-7".to_owned()),
            },
            launch: LaunchSpec {
                executable: if cfg!(windows) {
                    std::path::PathBuf::from(r"C:\modules\example.exe")
                } else {
                    std::path::PathBuf::from("/modules/example")
                },
                argv: Vec::new(),
                environment: Vec::new(),
                credential_ref: Some(ProtectedRef::new("credential:example").unwrap()),
                working_directory: None,
                inherited_environment_allowlist: Default::default(),
                executable_sha256: Some(Sha256Digest::new("a".repeat(64)).unwrap()),
            },
            config_schema: None,
            workspace_option: None,
            pre_input_open: None,
            command_schemas: Default::default(),
            event_schemas: Default::default(),
            protocol: swarm_contracts::module_catalog::ProtocolRange::exact(HOST_PROTOCOL),
            capabilities: Default::default(),
            lifecycle: LifecycleOwnership::OwnedService,
            activation: ActivationPolicy::OnDemand,
            enabled: true,
            restart: RestartPolicy::default(),
        }
    }

    #[test]
    fn descriptor_install_request_is_local_operator_owned_and_single_flight() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);\
             CREATE TABLE operations(operation_id TEXT NOT NULL,method TEXT NOT NULL,state TEXT NOT NULL);",
        )
        .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        super::super::set_meta(
            &tx,
            super::super::LOCAL_OPERATOR_CLIENT_ID_KEY,
            &json!("local-operator"),
        )
        .unwrap();
        let path = if cfg!(windows) {
            std::path::PathBuf::from(r"C:\modules\example\module-descriptor.json")
        } else {
            std::path::PathBuf::from("/modules/example/module-descriptor.json")
        };
        let params = json!({
            "client_request_id":"descriptor-install-1",
            "descriptor_file":path,
        });
        let manager = Principal {
            link_id: "manager-link".to_owned(),
            client_id: "manager".to_owned(),
            role: Role::Manager,
        };
        assert_eq!(
            reserve_descriptor_install(&tx, &manager, &params, "op-manager")
                .unwrap_err()
                .code,
            "FORBIDDEN"
        );
        let operator = Principal {
            link_id: "operator-link".to_owned(),
            client_id: "local-operator".to_owned(),
            role: Role::Operator,
        };
        // Production Store::call pre-inserts the current Operation as queued
        // before reserve runs; the single-flight gate must exclude that self row.
        tx.execute(
            "INSERT INTO operations(operation_id,method,state) VALUES('op-install-1','module.descriptor.install','queued')",
            [],
        )
        .unwrap();
        let (receipt, queued) =
            reserve_descriptor_install(&tx, &operator, &params, "op-install-1").unwrap();
        assert!(queued);
        assert_eq!(receipt["state"], "queued");
        // Self replay with the same Operation ID stays accepted.
        let (replay, replay_queued) =
            reserve_descriptor_install(&tx, &operator, &params, "op-install-1").unwrap();
        assert!(replay_queued);
        assert_eq!(replay["operation_id"], "op-install-1");
        tx.execute(
            "INSERT INTO operations(operation_id,method,state) VALUES('op-other-pending','module.descriptor.install','queued')",
            [],
        )
        .unwrap();
        let other_params = json!({
            "client_request_id":"descriptor-install-2",
            "descriptor_file":params["descriptor_file"].clone(),
        });
        assert_eq!(
            reserve_descriptor_install(&tx, &operator, &other_params, "op-install-2")
                .unwrap_err()
                .code,
            "MODULE_INSTALL_IN_PROGRESS"
        );
    }

    #[test]
    fn completed_descriptor_install_is_idempotent_and_restores_its_explicit_path() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);\
             CREATE TABLE operations(\
                 operation_id TEXT PRIMARY KEY,method TEXT NOT NULL,state TEXT NOT NULL,\
                 original_request_json TEXT NOT NULL,result_json TEXT,created_at_ms INTEGER NOT NULL,\
                 updated_at_ms INTEGER NOT NULL,settled_at_ms INTEGER\
             );",
        )
        .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let descriptor = descriptor();
        let registration: DescriptorRegistrationReceipt =
            serde_json::from_value(register_trusted_descriptor(&tx, descriptor.clone()).unwrap())
                .unwrap();
        let descriptor_file = if cfg!(windows) {
            std::path::PathBuf::from(r"C:\modules\example\module-descriptor.json")
        } else {
            std::path::PathBuf::from("/modules/example/module-descriptor.json")
        };
        let original = json!({
            "client_request_id":"descriptor-install-2",
            "descriptor_file":descriptor_file.clone(),
        });
        tx.execute(
            "INSERT INTO operations(\
                operation_id,method,state,original_request_json,result_json,created_at_ms,updated_at_ms\
             ) VALUES(?1,'module.descriptor.install','queued',?2,?3,1,1)",
            params![
                "op-install-2",
                model::canonical(&original).unwrap(),
                model::canonical(&json!({"operation_id":"op-install-2","state":"queued","status":"queued"})).unwrap(),
            ],
        )
        .unwrap();
        let completion = CompleteInstalledDescriptorRequest {
            operation_id: "op-install-2".to_owned(),
            outcome: InstalledDescriptorCompletionOutcome::Installed,
            registration: Some(registration),
            error_code: None,
        };
        let first = complete_descriptor_install(&tx, completion.clone(), 10).unwrap();
        let retry = complete_descriptor_install(&tx, completion, 11).unwrap();
        assert_eq!(first.state, InstalledDescriptorOperationState::Settled);
        assert_eq!(
            first.outcome,
            InstalledDescriptorOperationOutcome::Installed
        );
        assert_eq!(first.operation_id, retry.operation_id);
        let page = descriptor_install_requests(&tx, true).unwrap();
        assert_eq!(page.requests.len(), 1);
        assert_eq!(
            page.requests[0].state,
            InstalledDescriptorRequestState::Installed
        );
        assert_eq!(page.requests[0].descriptor_file, descriptor_file);
        assert!(
            descriptor_install_requests(&tx, false)
                .unwrap()
                .requests
                .is_empty()
        );
    }
    fn native_mcp_schema_admission(schema_version: &str) -> Option<bool> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let mut native_descriptor = descriptor();
        native_descriptor
            .capabilities
            .insert(CapabilityId::new("native.mcp.observe").unwrap());
        let mut schema = native_mcp_command_schema();
        schema.version = schema_version.to_owned();
        native_descriptor.command_schemas.insert(schema);
        register_trusted_descriptor(&tx, native_descriptor.clone()).unwrap();
        let registry = load_registry(&tx).unwrap();
        let entry = &registry.descriptors[0];
        let selector = serde_json::to_value(RouteSelection {
            schema_version: 1,
            module_id: native_descriptor.module_id.clone(),
            artifact: native_descriptor.artifact.clone(),
            registered_revision: entry.registered_revision,
            selected_revision: entry.registered_revision,
        })
        .unwrap();
        selected_native_command_supported(
            &tx,
            native_descriptor.artifact.artifact_id.as_str(),
            Some(&selector),
            "native.mcp.observe",
            &json!({}),
        )
        .unwrap()
    }

    #[test]
    fn native_mcp_descriptor_selection_requires_shared_schema_v2() {
        let schema = native_mcp_command_schema();
        assert_eq!(schema.schema_id, "swarm.native_mcp_command");
        assert_eq!(schema.version, "2");
        assert_eq!(native_mcp_schema_admission("1"), Some(false));
        assert_eq!(native_mcp_schema_admission("2"), Some(true));
    }

    #[test]
    fn exact_registered_claim_negotiates_but_never_grants_effect_rights() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let registered = descriptor();
        register_trusted_descriptor(&tx, registered.clone()).unwrap();
        let registry = load_registry(&tx).unwrap();
        let entry = &registry.descriptors[0];
        let selector = serde_json::to_value(RouteSelection {
            schema_version: 1,
            module_id: registered.module_id.clone(),
            artifact: registered.artifact.clone(),
            registered_revision: entry.registered_revision,
            selected_revision: entry.registered_revision,
        })
        .unwrap();
        let retained = retained_contract_identity(&tx, "example-artifact", Some(&selector))
            .unwrap()
            .unwrap();
        assert_eq!(retained.module_id, registered.module_id);
        assert_eq!(retained.artifact, registered.artifact);
        assert_eq!(retained.protocol, HOST_PROTOCOL);
        assert!(retained.capabilities.is_empty());
        let claim = json!({
            "schema_version":1,
            "module_id":registered.module_id,
            "artifact":registered.artifact,
            "protocol":HOST_PROTOCOL,
            "capabilities":[],
            "config_schema":null,
            "command_schemas":[],
            "event_schemas":[],
        });
        let result =
            negotiate_hello(&tx, "example-artifact", Some(&selector), Some(&claim)).unwrap();
        assert_eq!(result["status"], "negotiated");
        assert_eq!(result["source"], "store_registered_descriptor");
        assert_eq!(result["effects_authorized_by_descriptor"], false);
        assert!(result.get("launch").is_none());
        assert!(
            !model::canonical(&result)
                .unwrap()
                .contains("credential:example")
        );
    }

    #[test]
    fn mismatch_and_unversioned_self_claim_fail_closed_without_replacing_legacy() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let legacy = negotiate_hello(&tx, "old-artifact", None, None).unwrap();
        assert_eq!(legacy["status"], "legacy_unverified");
        let forged = json!({"schema_version":1});
        assert_eq!(
            negotiate_hello(&tx, "old-artifact", None, Some(&forged))
                .unwrap_err()
                .code,
            "UNEXPECTED_MODULE_CONTRACT"
        );
        register_trusted_descriptor(&tx, descriptor()).unwrap();
        let registry = load_registry(&tx).unwrap();
        let item = &registry.descriptors[0];
        let selector = serde_json::to_value(RouteSelection {
            schema_version: 1,
            module_id: item.descriptor.module_id.clone(),
            artifact: item.descriptor.artifact.clone(),
            registered_revision: item.registered_revision,
            selected_revision: item.registered_revision,
        })
        .unwrap();
        let registered_descriptor = &item.descriptor;
        let mut mismatched = json!({
            "schema_version":1,
            "module_id":registered_descriptor.module_id,
            "artifact":registered_descriptor.artifact,
            "protocol":HOST_PROTOCOL,
            "capabilities":["native.forged"],
            "config_schema":null,
            "command_schemas":[],
            "event_schemas":[],
        });
        assert_eq!(
            negotiate_hello(&tx, "example-artifact", Some(&selector), Some(&mismatched))
                .unwrap_err()
                .code,
            "MODULE_CONTRACT_MISMATCH"
        );
        mismatched["protocol"] = json!({"major":9,"minor":0});
        assert_eq!(
            negotiate_hello(&tx, "example-artifact", Some(&selector), Some(&mismatched))
                .unwrap_err()
                .code,
            "MODULE_CONTRACT_MISMATCH"
        );
        assert!(
            register_trusted_descriptor(&tx, {
                let mut replacement = descriptor();
                replacement.launch.executable = if cfg!(windows) {
                    std::path::PathBuf::from(r"C:\other\replacement.exe")
                } else {
                    std::path::PathBuf::from("/other/replacement")
                };
                replacement
            })
            .is_err()
        );
    }

    #[test]
    fn ordinary_managers_select_only_their_own_future_bindings() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let first = descriptor();
        let mut second = descriptor();
        second.artifact.version = ArtifactVersion::new("3.0.0").unwrap();
        register_trusted_descriptor(&tx, first.clone()).unwrap();
        register_trusted_descriptor(&tx, second.clone()).unwrap();

        let config = Config {
            routes: vec![Route {
                admission_policy: Default::default(),
                workspace_option: None,
                alias: "default".to_owned(),
                runtime: first.module_id.to_string(),
                module_artifact_id: first.artifact.artifact_id.to_string(),
                enabled: true,
                native_options: json!({}),
                owned_service: None,
            }],
            ..Config::default()
        };
        let manager_one = Principal {
            link_id: "link-one".to_owned(),
            client_id: "manager one".to_owned(),
            role: Role::Manager,
        };
        let manager_two = Principal {
            link_id: "link-two".to_owned(),
            client_id: "manager-two".to_owned(),
            role: Role::Manager,
        };
        let select = |version: &str, revision| {
            json!({
                "route_alias":"default",
                "module_id":first.module_id,
                "artifact_id":first.artifact.artifact_id,
                "version":version,
                "expected_catalog_revision":revision,
            })
        };

        let mut disabled_config = config.clone();
        disabled_config.routes[0].enabled = false;
        assert_eq!(
            select_route(
                &tx,
                &manager_one,
                &select("2.1.0", 2),
                &disabled_config,
                "op-disabled",
            )
            .unwrap_err()
            .code,
            "ROUTE_DISABLED"
        );
        assert_eq!(load_registry(&tx).unwrap().revision, 2);
        let one_result =
            select_route(&tx, &manager_one, &select("2.1.0", 2), &config, "op-one").unwrap();
        assert_eq!(one_result["selection_scope"], "caller_future_bindings_only");
        let one_binding = selection_for_new_binding(
            &tx,
            &manager_one.client_id,
            "default",
            first.module_id.as_str(),
            first.artifact.artifact_id.as_str(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(one_binding["artifact"]["version"], "2.1.0");
        assert_eq!(
            selection_for_new_binding(
                &tx,
                &manager_two.client_id,
                "default",
                first.module_id.as_str(),
                first.artifact.artifact_id.as_str(),
            )
            .unwrap_err()
            .code,
            "MODULE_CONTRACT_REQUIRED"
        );

        select_route(&tx, &manager_two, &select("3.0.0", 3), &config, "op-two").unwrap();
        let one_after_two_selects = selection_for_new_binding(
            &tx,
            &manager_one.client_id,
            "default",
            first.module_id.as_str(),
            first.artifact.artifact_id.as_str(),
        )
        .unwrap()
        .unwrap();
        let two_binding = selection_for_new_binding(
            &tx,
            &manager_two.client_id,
            "default",
            first.module_id.as_str(),
            first.artifact.artifact_id.as_str(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(one_after_two_selects["artifact"]["version"], "2.1.0");
        assert_eq!(two_binding["artifact"]["version"], "3.0.0");
        assert!(
            read_catalog(&manager_one, &json!({}), &tx).unwrap()["route_selections"]["default"]
                .get("artifact")
                .is_some()
        );
        assert_eq!(
            read_catalog(&manager_one, &json!({}), &tx).unwrap()["route_selections"]["default"]["artifact"]
                ["version"],
            "2.1.0"
        );
    }

    #[test]
    fn read_catalog_shrinks_large_descriptor_pages_without_gaps() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let expected = (0..8)
            .map(|index| format!("example.adapter.{index}"))
            .collect::<Vec<_>>();

        for index in 0..8 {
            let mut candidate = descriptor();
            candidate.module_id = ModuleId::new(format!("example.adapter.{index}")).unwrap();
            candidate.artifact.artifact_id =
                ArtifactId::new(format!("example-artifact-{index}")).unwrap();
            candidate.command_schemas = (0..300)
                .map(|schema_index| SchemaDescriptor {
                    schema_id: format!("schema.{}.{}", "x".repeat(110), schema_index),
                    version: format!("1.{}", "0".repeat(126)),
                    sha256: None,
                })
                .collect();
            candidate.validate().unwrap();
            let encoded = serde_json::to_value(&candidate).unwrap();
            assert!(model::canonical(&encoded).unwrap().len() <= MAX_DESCRIPTOR_BYTES);
            register_trusted_descriptor(&tx, candidate).unwrap();
        }

        let registry = load_registry(&tx).unwrap();
        let projected_descriptor_bytes = registry
            .descriptors
            .iter()
            .map(|entry| model::canonical(&public_descriptor(entry)).unwrap().len())
            .sum::<usize>();
        assert!(projected_descriptor_bytes > MAX_PAGE_BYTES);

        let manager = Principal {
            link_id: "catalog-link".to_owned(),
            client_id: "catalog-manager".to_owned(),
            role: Role::Manager,
        };
        let mut after = 0usize;
        let mut observed = Vec::new();
        loop {
            let page = read_catalog(&manager, &json!({"after":after}), &tx).unwrap();
            assert!(model::canonical(&page).unwrap().len() <= MAX_PAGE_BYTES);
            assert_eq!(page["after"].as_u64(), Some(u64::try_from(after).unwrap()));
            let descriptors = page["descriptors"].as_array().unwrap();
            if after == 0 {
                assert!(descriptors.len() < MAX_PAGE_ITEMS);
            }
            observed.extend(
                descriptors
                    .iter()
                    .map(|descriptor| descriptor["module_id"].as_str().unwrap().to_owned()),
            );
            match page["next_after"].as_u64() {
                Some(next_after) => {
                    let next_after = usize::try_from(next_after).unwrap();
                    assert_eq!(next_after, after + descriptors.len());
                    assert!(next_after > after);
                    after = next_after;
                }
                None => break,
            }
        }
        assert_eq!(observed, expected);
    }

    #[test]
    fn new_module_work_requires_selection_independently_of_artifact_ids() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        for (runtime, artifact) in [
            ("module", "future-artifact-not-in-any-current-list"),
            ("module", crate::config::OPENCODE_RUST_ARTIFACT_ID),
            ("module", crate::runtime::zed::ARTIFACT_ID),
        ] {
            assert_eq!(
                descriptor_for_new_binding(&db, "manager", "route", runtime, artifact)
                    .unwrap_err()
                    .code,
                "MODULE_CONTRACT_REQUIRED"
            );
            for selector in [None, Some(&Value::Null)] {
                assert_eq!(
                    require_selected_native_command(
                        &db,
                        "binding",
                        (runtime, artifact),
                        selector,
                        "agent.send",
                        &json!({"text":"new input"}),
                    )
                    .unwrap_err()
                    .code,
                    "MODULE_CONTRACT_REQUIRED"
                );
            }
            // Historical decoding does not authorize either new admission.
            assert!(
                retained_contract_identity(&db, artifact, None)
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM meta", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn future_module_artifact_uses_exact_manager_selection_and_shared_admission() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let trusted = descriptor();
        register_trusted_descriptor(&tx, trusted.clone()).unwrap();
        let route = Route {
            alias: "future".into(),
            runtime: "module".into(),
            module_artifact_id: trusted.artifact.artifact_id.to_string(),
            enabled: true,
            native_options: json!({}),
            workspace_option: None,
            admission_policy: None,
            owned_service: None,
        };
        let config = Config {
            routes: vec![route.clone()],
            ..Config::default()
        };
        let manager = Principal {
            link_id: "link".into(),
            client_id: "manager".into(),
            role: Role::Manager,
        };
        select_route(
            &tx,
            &manager,
            &json!({
                "route_alias":"future", "module_id":trusted.module_id,
                "artifact_id":trusted.artifact.artifact_id, "version":trusted.artifact.version,
                "expected_catalog_revision":1,
            }),
            &config,
            "select-future",
        )
        .unwrap();
        let selected = selection_for_new_binding(
            &tx,
            &manager.client_id,
            &route.alias,
            &route.runtime,
            &route.module_artifact_id,
        )
        .unwrap()
        .unwrap();
        super::super::task_prompt::require_new_binding(&route, Some(&selected)).unwrap();
        let retained = retained_contract_identity(&tx, &route.module_artifact_id, Some(&selected))
            .unwrap()
            .unwrap();
        assert_eq!(retained.artifact, trusted.artifact);
        assert_eq!(
            descriptor_for_new_binding(
                &tx,
                &manager.client_id,
                &route.alias,
                "module",
                "other-artifact"
            )
            .unwrap_err()
            .code,
            "MODULE_ROUTE_STALE"
        );
        assert_eq!(
            descriptor_for_new_binding(
                &tx,
                "unselected-manager",
                &route.alias,
                "module",
                &route.module_artifact_id
            )
            .unwrap_err()
            .code,
            "MODULE_CONTRACT_REQUIRED"
        );
    }

    #[test]
    fn corrupt_selector_revision_fails_before_new_binding_admission() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        register_trusted_descriptor(&tx, descriptor()).unwrap();
        let mut newer = descriptor();
        newer.artifact.version = ArtifactVersion::new("3.0.0").unwrap();
        register_trusted_descriptor(&tx, newer.clone()).unwrap();
        let mut registry = load_registry(&tx).unwrap();
        registry.selections.insert(
            "manager one".to_owned(),
            BTreeMap::from([(
                "default".to_owned(),
                RouteSelection {
                    schema_version: 1,
                    module_id: newer.module_id.clone(),
                    artifact: newer.artifact.clone(),
                    registered_revision: 2,
                    selected_revision: 1,
                },
            )]),
        );
        super::super::set_meta(&tx, REGISTRY_KEY, &serde_json::to_value(registry).unwrap())
            .unwrap();
        assert_eq!(
            selection_for_new_binding(
                &tx,
                "manager one",
                "default",
                newer.module_id.as_str(),
                newer.artifact.artifact_id.as_str(),
            )
            .unwrap_err()
            .code,
            "MODULE_CATALOG_CORRUPT"
        );
    }

    #[test]
    fn runtime_catalog_module_evidence_is_current_owner_scoped_and_stale_on_route_change() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
            .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        let mut trusted = descriptor();
        trusted
            .capabilities
            .insert(CapabilityId::new("module.fixture.observe").unwrap());
        register_trusted_descriptor(&tx, trusted.clone()).unwrap();
        let mut registry = load_registry(&tx).unwrap();
        let registered_revision = registry.descriptors[0].registered_revision;
        registry.selections.insert(
            "catalog-owner".to_owned(),
            BTreeMap::from([(
                "catalog-route".to_owned(),
                RouteSelection {
                    schema_version: 1,
                    module_id: trusted.module_id.clone(),
                    artifact: trusted.artifact.clone(),
                    registered_revision,
                    selected_revision: registry.revision,
                },
            )]),
        );
        save_registry(&tx, &registry).unwrap();
        tx.commit().unwrap();

        let route = Route {
            alias: "catalog-route".to_owned(),
            runtime: "module".to_owned(),
            module_artifact_id: "example-artifact".to_owned(),
            enabled: true,
            native_options: json!({}),
            workspace_option: None,
            owned_service: None,
            admission_policy: None,
        };
        let owner = Principal {
            link_id: "catalog-owner-link".to_owned(),
            client_id: "catalog-owner".to_owned(),
            role: Role::Manager,
        };
        let (revision, current) =
            runtime_catalog_module_evidence(&owner, std::slice::from_ref(&route), &db).unwrap();
        assert_eq!(revision, 1);
        assert_eq!(current["catalog-route"].status, "registered_and_selected");
        assert_eq!(
            current["catalog-route"].contract_capabilities,
            vec!["module.fixture.observe"]
        );

        let other_manager = Principal {
            client_id: "other-manager".to_owned(),
            ..owner.clone()
        };
        let (_, other) =
            runtime_catalog_module_evidence(&other_manager, std::slice::from_ref(&route), &db)
                .unwrap();
        assert_eq!(other["catalog-route"].status, "registered_but_not_selected");

        let mut changed_route = route.clone();
        changed_route.module_artifact_id = "changed-artifact".to_owned();
        let (_, stale) = runtime_catalog_module_evidence(&owner, &[changed_route], &db).unwrap();
        assert_eq!(stale["catalog-route"].status, "stale_selection");
        assert!(stale["catalog-route"].contract_capabilities.is_empty());

        let mut unknown_route = route;
        unknown_route.alias = "unknown-route".to_owned();
        unknown_route.module_artifact_id = "unregistered-artifact".to_owned();
        let (_, unknown) = runtime_catalog_module_evidence(&owner, &[unknown_route], &db).unwrap();
        assert_eq!(unknown["unknown-route"].status, "descriptor_unavailable");
    }

    #[test]
    fn supervisor_registration_scope_is_one_exact_positive_allowlist() {
        let registration = json!({
            "role":"module_supervisor",
            "token_hash":"a".repeat(64),
            "disabled":false,
            "internal_only":false,
            "module_scope":"descriptor_catalog",
            "capabilities":["module.descriptor.register"],
        });
        assert!(supervisor_scope_matches(
            model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID,
            &registration
        ));
        assert!(!supervisor_scope_matches("ordinary-manager", &registration));

        let mut previous_current = registration.clone();
        previous_current["capabilities"] = json!(CURRENT_PRE_INSTALL_SUPERVISOR_CAPABILITIES);
        assert!(supervisor_scope_matches(
            model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID,
            &previous_current
        ));
        let mut current = registration.clone();
        current["capabilities"] = json!(SUPERVISOR_CAPABILITIES);
        assert!(supervisor_scope_matches(
            model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID,
            &current
        ));
        current["capabilities"] = json!([
            "module.descriptor.register",
            "module.supervisor.admission",
            "module.supervisor.demand.page",
            "module.supervisor.scope.readback",
            "module.supervisor.credential.ensure",
            "module.supervisor.credential.ready",
            "module.supervisor.recovery.reconcile",
            "module.supervisor.observation.record",
            "module.supervisor.health.record",
            "module.supervisor.health.read",
            "module.supervisor.install.requests",
            "module.supervisor.install.complete",
            "module.route.select",
        ]);
        assert!(!supervisor_scope_matches(
            model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID,
            &current
        ));
        let mut internal = registration.clone();
        internal["internal_only"] = json!(true);
        assert!(!supervisor_scope_matches(
            model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID,
            &internal
        ));
    }
}
