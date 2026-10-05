//! Authenticated Store handlers for the shared bus's read and admission seam.
//!
//! These handlers run against the existing Store connection and mutation
//! transaction. The bus worker never receives SQL or an arbitrary `run_as`.

use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Principal},
};
use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use swarm_contracts::DeclaredServiceScope;

#[path = "bus_service.rs"]
mod managed_service;
pub(crate) use managed_service::{
    ManagedBusOwnerState, ManagedBusServiceDemand, health_projection as managed_health_projection,
    managed_demands as managed_service_demands, record_health as record_managed_service_health,
    record_owner_readback as record_managed_service_owner_readback,
    record_start as record_managed_service_start,
    reset_for_host_start as reset_managed_service_health_for_host_start,
};

const MAX_ID_BYTES: usize = 128;
const MAX_SELECTOR_BYTES: usize = 256;
const MAX_PAGE: usize = 32;
const BUS_METHODS: [&str; 2] = ["bus.events.page", "bus.consumer.admit"];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScriptRunConsumerBinding {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    consumer_client_id: String,
    scope_digest: String,
    method_scope: Vec<String>,
    created_at_ms: i64,
    #[serde(default)]
    managed_service: bool,
    #[serde(default)]
    service_generation: u64,
    #[serde(default)]
    worker_config_sha256: Option<String>,
}

impl ScriptRunConsumerBinding {
    pub(crate) fn owner_manager_id(&self) -> &str {
        &self.owner_manager_id
    }

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn automation_id(&self) -> &str {
        &self.automation_id
    }

    pub(crate) fn scope_digest(&self) -> &str {
        &self.scope_digest
    }

    pub(crate) fn managed_service(&self) -> bool {
        self.managed_service
    }

    pub(crate) fn service_scope(&self) -> Result<DeclaredServiceScope> {
        if !self.managed_service {
            return Err(Error::new(
                "BUS_SERVICE_NOT_MANAGED",
                "consumer has no managed service scope",
            ));
        }
        DeclaredServiceScope::new(
            swarm_contracts::DeclaredServicePurpose::BusConsumer,
            self.consumer_client_id.clone(),
            self.service_generation,
        )
    }

    pub(crate) fn worker_config_sha256(&self) -> Option<&str> {
        self.worker_config_sha256.as_deref()
    }

    fn require_method(&self, method: &str) -> Result<()> {
        if self
            .method_scope
            .iter()
            .any(|allowed| allowed.as_str() == method)
        {
            Ok(())
        } else {
            Err(Error::new(
                "FORBIDDEN",
                "method is outside the authenticated bus consumer scope",
            ))
        }
    }

    fn validate(&self, client_id: &str) -> Result<()> {
        if self.schema_version != 1
            || self.owner_manager_id.is_empty()
            || self.project_id.is_empty()
            || self.automation_id.is_empty()
            || self.consumer_client_id != client_id
            || !client_id.starts_with("bus-script-")
            || self.scope_digest.len() != 64
            || !self
                .scope_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.method_scope.iter().map(String::as_str).ne(BUS_METHODS)
            || self.created_at_ms < 0
            || (self.managed_service
                && (self.service_generation == 0
                    || self.worker_config_sha256.as_deref().is_none_or(|value| {
                        value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
                    })))
            || (!self.managed_service
                && (self.service_generation != 0 || self.worker_config_sha256.is_some()))
        {
            return Err(Error::new(
                "BUS_CONSUMER_REGISTRATION_CORRUPT",
                "scoped ScriptRun consumer registration is invalid",
            ));
        }
        Ok(())
    }
}

/// Reload the actual authenticated Module registration, including its narrow
/// method scope. This is not a Manager/Operator delegation and is never
/// reconstructed as another Principal.
pub(crate) fn module_consumer_binding(
    db: &Connection,
    principal: &Principal,
) -> Result<ScriptRunConsumerBinding> {
    if principal.role != crate::model::Role::Module {
        return Err(Error::new(
            "FORBIDDEN",
            "bus consumer calls require their registered scoped Module credential",
        ));
    }
    let record = crate::store::meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "bus consumer is no longer registered"))?;
    if record["role"] != "module" || record["disabled"] == true {
        return Err(Error::new(
            "UNAUTHORIZED",
            "scoped bus consumer is disabled or has another role",
        ));
    }
    let token_hash = record["token_hash"].as_str().unwrap_or_default();
    if token_hash.len() != 64 || !token_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::new(
            "BUS_CONSUMER_REGISTRATION_CORRUPT",
            "scoped Module registration has no valid credential hash",
        ));
    }
    let binding: ScriptRunConsumerBinding = serde_json::from_value(
        record
            .get("bus_consumer")
            .cloned()
            .ok_or_else(|| Error::new("FORBIDDEN", "Module has no bus consumer scope"))?,
    )
    .map_err(|_| {
        Error::new(
            "BUS_CONSUMER_REGISTRATION_CORRUPT",
            "scoped ScriptRun consumer registration fields are invalid",
        )
    })?;
    binding.validate(&principal.client_id)?;
    Ok(binding)
}

pub(crate) fn validate_mutation(method: &str, params: &Value) -> Result<()> {
    if !matches!(
        method,
        "bus.consumer.register" | "bus.consumer.revoke" | "bus.consumer.admit"
    ) {
        return Err(Error::new("METHOD_NOT_FOUND", method));
    }
    let allowed = match method {
        "bus.consumer.register" => &[
            "client_request_id",
            "project_id",
            "automation_id",
            "consumer_client_id",
            "token_hash",
            "managed_service",
            "worker_config_sha256",
        ][..],
        "bus.consumer.revoke" => &[
            "client_request_id",
            "project_id",
            "automation_id",
            "consumer_client_id",
        ][..],
        _ => &[
            "client_request_id",
            "project_id",
            "consumer_id",
            "automation_revision",
            "expected_cursor",
            "through_observation_id",
            "occurrences",
        ][..],
    };
    model::fields(params, allowed)?;
    let request_id = model::text(params, "client_request_id")?;
    if request_id.len() > MAX_ID_BYTES
        || request_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(
            "client_request_id must be 1..=128 bytes without whitespace",
        ));
    }
    let scope_field = if method == "bus.consumer.admit" {
        "consumer_id"
    } else {
        "automation_id"
    };
    for field in ["project_id", scope_field] {
        bounded_name(model::text(params, field)?, MAX_ID_BYTES, field)?;
    }
    if method == "bus.consumer.register" || method == "bus.consumer.revoke" {
        let consumer_client_id = model::text(params, "consumer_client_id")?;
        if !consumer_client_id.starts_with("bus-script-") {
            return Err(Error::invalid(
                "consumer_client_id must use the bus-script- namespace",
            ));
        }
        bounded_name(consumer_client_id, MAX_ID_BYTES, "consumer_client_id")?;
        if method == "bus.consumer.register" {
            let token_hash = model::text(params, "token_hash")?;
            if token_hash.len() != 64 || !token_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(Error::invalid("token_hash must be a SHA-256 hex digest"));
            }
            let managed_service = match params.get("managed_service") {
                None => false,
                Some(Value::Bool(value)) => *value,
                Some(_) => return Err(Error::invalid("managed_service must be boolean")),
            };
            if managed_service {
                let file_hash = model::text(params, "worker_config_sha256")?;
                if file_hash.len() != 64 || !file_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err(Error::invalid(
                        "managed worker_config_sha256 must be a SHA-256 hex digest",
                    ));
                }
            } else if params.get("worker_config_sha256").is_some() {
                return Err(Error::invalid(
                    "worker_config_sha256 requires managed_service=true",
                ));
            }
        }
        return Ok(());
    }
    model::positive(params, "automation_revision")?;
    let expected_cursor = nonnegative(params, "expected_cursor")?;
    let through = model::positive(params, "through_observation_id")?;
    if through <= expected_cursor {
        return Err(Error::invalid(
            "through_observation_id must be beyond expected_cursor",
        ));
    }
    let occurrences = params
        .get("occurrences")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("occurrences must be an array"))?;
    if occurrences.len() > MAX_PAGE {
        return Err(Error::invalid("occurrences exceeds the 32-item bound"));
    }
    let mut previous_observation_id = expected_cursor;
    for occurrence in occurrences {
        model::fields(
            occurrence,
            &[
                "observation_id",
                "source_id",
                "event_kind",
                "status",
                "action",
            ],
        )?;
        let observation_id = model::positive(occurrence, "observation_id")?;
        if observation_id <= previous_observation_id || observation_id > through {
            return Err(Error::invalid(
                "occurrences must be strictly ordered within the admitted page cut",
            ));
        }
        previous_observation_id = observation_id;
        for field in ["source_id", "event_kind"] {
            bounded_name(model::text(occurrence, field)?, MAX_SELECTOR_BYTES, field)?;
        }
        match occurrence.get("status") {
            Some(Value::Null) => {}
            Some(Value::String(value))
                if matches!(
                    value.as_str(),
                    "applied"
                        | "completed"
                        | "failed"
                        | "incomplete"
                        | "cancelled"
                        | "rejected"
                        | "sent"
                        | "answered"
                        | "invalidated"
                        | "unknown"
                ) => {}
            _ => return Err(Error::invalid("occurrence status is unsupported")),
        }
        let action = occurrence
            .get("action")
            .filter(|value| value.is_object())
            .ok_or_else(|| Error::invalid("occurrence action must be an object"))?;
        model::fields(action, &["kind", "script_id"])?;
        if model::text(action, "kind")? != "script_run" {
            return Err(Error::new(
                "AUTOMATION_CAPABILITY_GAP",
                "the kernel bus consumer currently admits only the configured ScriptRun action",
            ));
        }
        let script_id = model::text(action, "script_id")?;
        if script_id.len() > 64
            || !script_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            return Err(Error::invalid("action.script_id is invalid"));
        }
    }
    Ok(())
}

pub(crate) fn read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    params: &Value,
    config: &Config,
) -> Result<Value> {
    if method != "bus.events.page" {
        return Err(Error::new("METHOD_NOT_FOUND", method));
    }
    if principal.role == crate::model::Role::Module {
        module_consumer_binding(db, principal)?.require_method(method)?;
    }
    model::fields(
        params,
        &["project_id", "consumer_id", "after_observation_id", "limit"],
    )?;
    let project_id = bounded_name(
        model::text(params, "project_id")?,
        MAX_ID_BYTES,
        "project_id",
    )?;
    let consumer_id = bounded_name(
        model::text(params, "consumer_id")?,
        MAX_ID_BYTES,
        "consumer_id",
    )?;
    let after = params
        .get("after_observation_id")
        .map(|value| {
            value
                .as_i64()
                .ok_or_else(|| Error::invalid("after_observation_id must be a nonnegative integer"))
        })
        .transpose()?;
    let limit = match params.get("limit") {
        None => 20,
        Some(value) => value
            .as_u64()
            .and_then(|number| usize::try_from(number).ok())
            .ok_or_else(|| Error::invalid("limit must be an integer in 1..=32"))?,
    };
    if after.is_some_and(|value| value < 0) || !(1..=MAX_PAGE).contains(&limit) {
        return Err(Error::invalid(
            "after_observation_id must be nonnegative and limit must be 1..=32",
        ));
    }
    crate::store::automation_dispatch::bus_kernel::events_page(
        db,
        principal,
        project_id,
        consumer_id,
        after,
        limit,
        config,
    )
}

pub(crate) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    params: &Value,
    operation_id: &str,
    config: &Config,
    now_ms: i64,
) -> Result<(Value, bool)> {
    validate_mutation(method, params)?;
    match method {
        "bus.consumer.register" | "bus.consumer.revoke"
            if principal.role != crate::model::Role::Manager =>
        {
            return Err(Error::new(
                "FORBIDDEN",
                "bus consumer lifecycle requires its authenticated Manager owner",
            ));
        }
        "bus.consumer.admit"
            if !matches!(
                principal.role,
                crate::model::Role::Manager | crate::model::Role::Module
            ) =>
        {
            return Err(Error::new(
                "FORBIDDEN",
                "bus consumer admission requires its Manager owner or scoped Module credential",
            ));
        }
        _ => {}
    }
    if principal.role == crate::model::Role::Module {
        module_consumer_binding(tx, principal)?.require_method(method)?;
    }
    if method == "bus.consumer.register" {
        return register(tx, principal, params, operation_id, now_ms);
    }
    if method == "bus.consumer.revoke" {
        return revoke(tx, principal, params, operation_id);
    }
    let occurrences = params["occurrences"]
        .as_array()
        .ok_or_else(|| Error::invalid("occurrences must be an array"))?;
    let value = crate::store::automation_dispatch::bus_kernel::admit_script_run_page(
        tx,
        principal,
        model::text(params, "project_id")?,
        model::text(params, "consumer_id")?,
        model::positive(params, "automation_revision")?,
        nonnegative(params, "expected_cursor")?,
        model::positive(params, "through_observation_id")?,
        occurrences,
        config,
        now_ms,
    )?;
    let mut receipt = value;
    receipt["operation_id"] = json!(operation_id);
    Ok((receipt, false))
}

fn register(
    tx: &Transaction<'_>,
    principal: &Principal,
    params: &Value,
    operation_id: &str,
    now_ms: i64,
) -> Result<(Value, bool)> {
    crate::automation::authorization::require_registered_manager(tx, &principal.client_id)?;
    let project_id = model::text(params, "project_id")?;
    let automation_id = model::text(params, "automation_id")?;
    let entry =
        crate::automation::config::load_entry(tx, &principal.client_id, project_id, automation_id)?
            .ok_or_else(|| Error::new("NOT_FOUND", "selected automation was not found"))?;
    crate::automation::config::validate_entry(&entry)?;
    if entry.owner_manager_id != principal.client_id || !entry.script_run_ready() {
        return Err(Error::new(
            "FORBIDDEN",
            "bus consumer must be scoped to the Manager's own enabled ScriptRun entry",
        ));
    }
    let consumer_client_id = model::text(params, "consumer_client_id")?;
    let client_key = format!("client:{consumer_client_id}");
    if crate::store::meta(tx, &client_key)?.is_some() {
        return Err(Error::conflict(
            "consumer credential identity already exists; no implicit rotation",
        ));
    }
    let scope_digest =
        crate::store::automation_dispatch::bus_kernel::script_run_consumer_scope_digest(&entry)?;
    let managed_service = params
        .get("managed_service")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let service_generation = if managed_service {
        next_service_generation(tx, principal, project_id, automation_id)?
    } else {
        0
    };
    let binding = ScriptRunConsumerBinding {
        schema_version: 1,
        owner_manager_id: principal.client_id.clone(),
        project_id: project_id.to_owned(),
        automation_id: automation_id.to_owned(),
        consumer_client_id: consumer_client_id.to_owned(),
        scope_digest,
        method_scope: BUS_METHODS
            .iter()
            .map(|method| (*method).to_owned())
            .collect(),
        created_at_ms: now_ms,
        managed_service,
        service_generation,
        worker_config_sha256: if managed_service {
            Some(model::text(params, "worker_config_sha256")?.to_ascii_lowercase())
        } else {
            None
        },
    };
    binding.validate(consumer_client_id)?;
    let registration = json!({
        "role":"module",
        "token_hash":model::text(params, "token_hash")?.to_ascii_lowercase(),
        "disabled":false,
        "bus_consumer":binding,
        "created_by":principal.client_id,
        "created_operation_id":operation_id,
        "created_at_ms":now_ms,
    });
    crate::store::set_meta(tx, &client_key, &registration)?;
    Ok((
        json!({
            "operation_id":operation_id,
            "registered":true,
            "consumer_client_id":consumer_client_id,
            "project_id":project_id,
            "automation_id":automation_id,
            "method_scope":BUS_METHODS,
            "credential_retained":"sha256_only",
            "managed_service":managed_service,
            "service_generation":if managed_service {json!(service_generation)} else {Value::Null},
        }),
        false,
    ))
}

fn next_service_generation(
    tx: &Transaction<'_>,
    principal: &Principal,
    project_id: &str,
    automation_id: &str,
) -> Result<u64> {
    let identity = model::canonical(&json!({
        "owner_manager_id":principal.client_id,
        "project_id":project_id,
        "automation_id":automation_id,
    }))?;
    let key = format!(
        "bus:service-generation:v1:{}",
        model::digest(identity.as_bytes())
    );
    let current = match crate::store::meta(tx, &key)? {
        None => 0,
        Some(value) => value.as_u64().ok_or_else(|| {
            Error::new(
                "BUS_SERVICE_GENERATION_CORRUPT",
                "persisted managed service generation is invalid",
            )
        })?,
    };
    let next = current.checked_add(1).ok_or_else(|| {
        Error::new(
            "BUS_SERVICE_GENERATION_EXHAUSTED",
            "managed bus service generation exhausted",
        )
    })?;
    crate::store::set_meta(tx, &key, &json!(next))?;
    Ok(next)
}

fn revoke(
    tx: &Transaction<'_>,
    principal: &Principal,
    params: &Value,
    operation_id: &str,
) -> Result<(Value, bool)> {
    crate::automation::authorization::require_registered_manager(tx, &principal.client_id)?;
    let consumer_client_id = model::text(params, "consumer_client_id")?;
    let client_key = format!("client:{consumer_client_id}");
    let mut registration = crate::store::meta(tx, &client_key)?
        .ok_or_else(|| Error::new("NOT_FOUND", "scoped consumer was not found"))?;
    let binding: ScriptRunConsumerBinding = serde_json::from_value(
        registration
            .get("bus_consumer")
            .cloned()
            .ok_or_else(|| Error::new("FORBIDDEN", "client is not a scoped bus consumer"))?,
    )
    .map_err(|_| {
        Error::new(
            "BUS_CONSUMER_REGISTRATION_CORRUPT",
            "scoped ScriptRun consumer registration fields are invalid",
        )
    })?;
    binding.validate(consumer_client_id)?;
    if binding.owner_manager_id != principal.client_id
        || binding.project_id != model::text(params, "project_id")?
        || binding.automation_id != model::text(params, "automation_id")?
    {
        return Err(Error::new(
            "FORBIDDEN",
            "only the registered Manager owner may revoke this consumer",
        ));
    }
    registration["disabled"] = json!(true);
    crate::store::set_meta(tx, &client_key, &registration)?;
    Ok((
        json!({
            "operation_id":operation_id,
            "revoked":true,
            "consumer_client_id":consumer_client_id,
        }),
        false,
    ))
}

fn nonnegative(params: &Value, name: &str) -> Result<i64> {
    let value = params
        .get(name)
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::invalid(format!("{name} must be a nonnegative integer")))?;
    if value < 0 {
        return Err(Error::invalid(format!("{name} must be nonnegative")));
    }
    Ok(value)
}

fn bounded_name<'a>(value: &'a str, max_bytes: usize, field: &str) -> Result<&'a str> {
    if value.is_empty()
        || value.len() > max_bytes
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
    {
        return Err(Error::invalid(format!("{field} is invalid or too long")));
    }
    Ok(value)
}
