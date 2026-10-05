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
use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use swarm_contracts::module_catalog::{
    ArtifactIdentity, ArtifactVersion, CapabilityId, ModuleCatalog, ModuleDescriptor, ModuleId,
    PreInputOpenContract, ProtocolVersion, SchemaDescriptor, WorkspaceOptionContract,
};
use swarm_contracts::module_contract::{MODULE_PROTOCOL_V1, ModuleContractClaim};

const REGISTRY_KEY: &str = "module_catalog:trusted_descriptors:v1";
const REGISTRY_SCHEMA_VERSION: u16 = 1;
const MAX_DESCRIPTORS: usize = 256;
const MAX_REGISTRY_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_DESCRIPTOR_BYTES: usize = 128 * 1024;
const MAX_ROUTE_SELECTION_OWNERS: usize = 4096;
const MAX_ROUTE_SELECTIONS_PER_OWNER: usize = 256;
const MAX_PAGE_ITEMS: usize = 8;
const MAX_PAGE_BYTES: usize = 512 * 1024;

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
        && registration["capabilities"] == json!(["module.descriptor.register"])
        && registration["token_hash"].as_str().is_some_and(|hash| {
            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        && registration
            .as_object()
            .is_some_and(|fields| fields.len() == 6)
}

pub(super) fn require_supervisor_scope(db: &Connection, principal: &Principal) -> Result<()> {
    let registration = super::meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "supervisor credential is not registered"))?;
    if principal.role != Role::ModuleSupervisor
        || registration["disabled"] == true
        || !supervisor_scope_matches(&principal.client_id, &registration)
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
    _route_runtime: &str,
    configured_artifact_id: &str,
) -> Result<Option<NewBindingDescriptorContract>> {
    let registry = load_registry(db)?;
    let Some(selection) = registry
        .selections
        .get(owner_manager_id)
        .and_then(|routes| routes.get(route_alias))
    else {
        return Ok(None);
    };
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
    let end = after.saturating_add(limit).min(registry.descriptors.len());
    let descriptors = if after < end {
        registry.descriptors[after..end]
            .iter()
            .map(public_descriptor)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let selections = registry
        .selections
        .get(&principal.client_id)
        .cloned()
        .unwrap_or_default();
    let result = json!({
        "schema_version":registry.schema_version,
        "catalog_revision":registry.revision,
        "host_protocol":{"major":HOST_PROTOCOL.major,"minor":HOST_PROTOCOL.minor},
        "descriptor_count":registry.descriptors.len(),
        "after":after,
        "descriptors":descriptors,
        "next_after":if end < registry.descriptors.len() { json!(end) } else { Value::Null },
        "route_selections":selections,
        "launch_details":"redacted",
    });
    bounded_json(&result, MAX_PAGE_BYTES, "module catalog page")?;
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
/// A missing selector preserves the legacy route contract.
pub(super) fn require_selected_native_command(
    db: &Connection,
    binding_id: &str,
    binding_artifact_id: &str,
    selector: Option<&Value>,
    method: &str,
    input: &Value,
) -> Result<()> {
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
pub(super) fn selected_native_command_supported(
    db: &Connection,
    binding_artifact_id: &str,
    selector: Option<&Value>,
    method: &str,
    input: &Value,
) -> Result<Option<bool>> {
    let Some(required) = native_command_capability(method, input) else {
        return Ok(Some(true));
    };
    let Some(retained) = retained_contract_identity(db, binding_artifact_id, selector)? else {
        return Ok(None);
    };
    Ok(Some(retained.capabilities.iter().any(|capability| {
        capability_satisfies(capability.as_str(), required)
    })))
}

fn native_command_capability(method: &str, input: &Value) -> Option<&'static str> {
    Some(match method {
        "agent.open" => "agent.open",
        "task.dispatch" => "task.dispatch",
        "agent.send" => match input.get("delivery").and_then(Value::as_str) {
            Some("next_turn") => "agent.send/next_turn",
            Some("steer") => "agent.send/steer",
            _ => "agent.send",
        },
        "agent.reply" => "agent.reply",
        "agent.configure" => "agent.configure",
        "agent.goal" => "agent.goal",
        "agent.background" => "agent.background",
        "agent.refresh" => "agent.refresh",
        "agent.reconcile" => "agent.reconcile",
        "agent.result" => "agent.result",
        "agent.recover" => "agent.recover",
        _ => return None,
    })
}

fn capability_satisfies(advertised: &str, required: &str) -> bool {
    advertised == required || (required.starts_with("agent.send/") && advertised == "agent.send")
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
        let descriptor = &item.descriptor;
        let mut mismatched = json!({
            "schema_version":1,
            "module_id":descriptor.module_id,
            "artifact":descriptor.artifact,
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
        assert!(
            selection_for_new_binding(
                &tx,
                &manager_two.client_id,
                "default",
                first.module_id.as_str(),
                first.artifact.artifact_id.as_str(),
            )
            .unwrap()
            .is_none()
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

        let mut expanded = registration.clone();
        expanded["capabilities"] = json!(["module.descriptor.register", "module.route.select"]);
        assert!(!supervisor_scope_matches(
            model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID,
            &expanded
        ));
        let mut internal = registration.clone();
        internal["internal_only"] = json!(true);
        assert!(!supervisor_scope_matches(
            model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID,
            &internal
        ));
    }
}
