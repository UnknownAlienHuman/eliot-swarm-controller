//! Manager-owned, metadata-only diagnostic policy.
//!
//! Policies live in the existing meta table and are keyed by one stable
//! Manager plus an optional exact Task/Attempt, Operation, binding, or module
//! selector. A selector is useful only after the Store proves that the
//! authenticated Manager owns the retained caller/route; Task context is
//! inherited from a selected Operation or binding when that context exists.
//! Recorder file selection, retention, and expiry remain owned by the
//! swarm-observer.

use super::{meta, operations, set_meta, tasks};
use crate::{
    automation::authorization,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const POLICY_PREFIX: &str = "diagnostic:logging:v1:";
const MAX_POLICIES: i64 = 64;
const MAX_POLICY_BYTES: usize = 8 * 1024;
const MAX_ID_BYTES: usize = 128;
const MAX_TTL_SECONDS: u64 = 86_400;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Level {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl Level {
    fn telemetry(&self) -> swarm_telemetry::FilterLevel {
        match self {
            Self::Off => swarm_telemetry::FilterLevel::Off,
            Self::Error => swarm_telemetry::FilterLevel::Error,
            Self::Warn => swarm_telemetry::FilterLevel::Warn,
            Self::Info => swarm_telemetry::FilterLevel::Info,
            Self::Debug => swarm_telemetry::FilterLevel::Debug,
            Self::Trace => swarm_telemetry::FilterLevel::Trace,
        }
    }
}

/// The Manager identity is the implicit client selector. Optional Task
/// fields are an inherited ownership context, while operation/binding/module
/// fields are the actual diagnostic selector.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Scope {
    manager_id: String,
    task_id: Option<String>,
    task_revision: Option<i64>,
    attempt_id: Option<String>,
    operation_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<u64>,
    module_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredPolicy {
    schema_version: u8,
    manager_id: String,
    scope: Scope,
    level: Level,
    content: String,
    revision: u64,
    updated_at_ms: i64,
    #[serde(default)]
    expires_at_ms: Option<i64>,
}

#[derive(Clone, Debug)]
struct TaskFacts {
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    binding_id: Option<String>,
    binding_generation: Option<u64>,
}

#[derive(Clone, Debug)]
struct OwnedBinding {
    task: Option<TaskFacts>,
}

pub(super) fn get(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    let scope = resolve_scope(db, principal, value)?;
    let key = policy_key(&scope)?;
    let policy = meta(db, &key)?
        .map(|value| parse_stored(&value))
        .transpose()?;
    if let Some(policy) = policy.as_ref()
        && (policy.manager_id != principal.client_id || policy.scope != scope)
    {
        return Err(Error::new(
            "LOGGING_POLICY_SCOPE_MISMATCH",
            "retained diagnostic policy does not match this Manager scope",
        ));
    }
    let now_ms = model::now_ms()?;
    let active = policy.as_ref().is_some_and(|policy| {
        policy
            .expires_at_ms
            .map_or(true, |expires| expires > now_ms)
    });
    Ok(json!({
        "scope": public_scope(&scope),
        "scope_kind": scope_kind(&scope),
        "configured": policy.is_some(),
        "active": active,
        "policy": policy
            .as_ref()
            .map(|policy| public_policy(policy, now_ms))
            .unwrap_or_else(|| json!({
                "level":"info",
                "content":"metadata",
                "revision":0,
                "expires_at_ms":null,
                "active":true,
                "source":"default"
            })),
        "durable_source":"store_meta",
        "consumer":"swarm_telemetry_producer_before_line_observer",
        "recorder_file_policy":"swarm_observer_live_config",
        "redaction":"unsupported",
    }))
}

pub(super) fn set(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now_ms: i64,
) -> Result<Value> {
    let scope = resolve_scope(tx, principal, value)?;
    let level = parse_level(model::text(value, "level")?)?;
    let content = model::text(value, "content")?;
    if content != "metadata" {
        return Err(Error::new(
            "LOGGING_CONTENT_UNSUPPORTED",
            "only metadata diagnostic content is currently supported; redacted text and native frames require an installed redactor",
        ));
    }
    let expires_at_ms = ttl_expiry(value, now_ms)?;
    let key = policy_key(&scope)?;
    let previous = meta(tx, &key)?
        .map(|value| parse_stored(&value))
        .transpose()?;
    if let Some(previous) = previous.as_ref()
        && (previous.manager_id != principal.client_id || previous.scope != scope)
    {
        return Err(Error::new(
            "LOGGING_POLICY_SCOPE_MISMATCH",
            "retained diagnostic policy does not match this Manager scope",
        ));
    }
    let revision = previous
        .as_ref()
        .map(|policy| {
            policy.revision.checked_add(1).ok_or_else(|| {
                Error::new(
                    "LOGGING_POLICY_REVISION_EXHAUSTED",
                    "diagnostic policy revision is exhausted",
                )
            })
        })
        .transpose()?
        .unwrap_or(1);
    let policy = StoredPolicy {
        schema_version: 1,
        manager_id: principal.client_id.clone(),
        scope: scope.clone(),
        level,
        content: content.to_owned(),
        revision,
        updated_at_ms: now_ms,
        expires_at_ms,
    };
    let encoded = model::canonical(&serde_json::to_value(&policy)?)?;
    if encoded.len() > MAX_POLICY_BYTES {
        return Err(Error::new(
            "LOGGING_POLICY_TOO_LARGE",
            "diagnostic policy exceeds its bounded metadata envelope",
        ));
    }
    let stored_value: Value = serde_json::from_str(&encoded)?;
    set_meta(tx, &key, &stored_value)?;
    Ok(json!({
        "operation_id":operation_id,
        "scope":public_scope(&scope),
        "scope_kind":scope_kind(&scope),
        "policy":public_policy(&policy, now_ms),
        "durable":true,
        "apply":"after_commit",
        "consumer":"swarm_telemetry_producer_before_line_observer",
        "recorder_file_policy":"swarm_observer_live_config",
    }))
}

/// Load validated active policies after Store startup or a committed update.
/// The query is bounded; malformed retained rows fail reload rather than
/// silently widening the diagnostic surface. Expired policies stay durable
/// for readback but are omitted from the live Producer snapshot.
pub(super) fn load_filters(db: &Connection) -> Result<Vec<swarm_telemetry::ScopedFilter>> {
    let prefix = format!("{POLICY_PREFIX}%");
    let mut statement =
        db.prepare("SELECT value_json FROM meta WHERE key LIKE ?1 ORDER BY key LIMIT ?2")?;
    let rows = statement
        .query_map(params![prefix, MAX_POLICIES + 1], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > MAX_POLICIES as usize {
        return Err(Error::new(
            "LOGGING_POLICY_LIMIT",
            "retained diagnostic policies exceed the bounded runtime limit",
        ));
    }
    let now_ms = model::now_ms()?;
    let mut filters = Vec::with_capacity(rows.len());
    for raw in rows {
        let value: Value = serde_json::from_str(&raw)?;
        let policy = parse_stored(&value)?;
        if policy
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
        {
            continue;
        }
        let scope = swarm_telemetry::FilterScope::new(
            &policy.manager_id,
            policy.scope.task_id.as_deref(),
            policy.scope.attempt_id.as_deref(),
            policy.scope.operation_id.as_deref(),
            policy.scope.binding_id.as_deref(),
            policy.scope.binding_generation,
            policy.scope.module_id.as_deref(),
        )
        .ok_or_else(|| {
            Error::new(
                "LOGGING_POLICY_SCOPE_INVALID",
                "retained diagnostic policy has invalid metadata identity",
            )
        })?;
        filters.push(swarm_telemetry::ScopedFilter {
            scope,
            level: policy.level.telemetry(),
        });
    }
    Ok(filters)
}

pub(super) fn producer_projection(
    producer: &swarm_telemetry::Producer,
    configured: &Value,
) -> Value {
    json!({
        "applied":true,
        "configured":configured["configured"],
        "active":configured["active"],
        "scoped_filter_count":producer.scoped_filter_count(),
        "sink":format!("{:?}", producer.stats().sink_state).to_lowercase(),
        "diagnostic_only":true,
    })
}

fn resolve_scope(db: &Connection, principal: &Principal, value: &Value) -> Result<Scope> {
    if principal.role != Role::Manager {
        return Err(Error::new(
            "FORBIDDEN",
            "logging control requires an ordinary Manager identity",
        ));
    }
    authorization::require_registered_manager(db, &principal.client_id)?;
    if let Some(client_id) = optional_text(value, "client_id")?
        && client_id != principal.client_id
    {
        return Err(Error::new(
            "FORBIDDEN",
            "logging control cannot select another client identity",
        ));
    }

    let requested_task_id = optional_text(value, "task_id")?;
    let requested_task_revision = optional_positive_i64(value, "task_revision")?;
    let requested_attempt_id = optional_text(value, "attempt_id")?;
    let task_selector_present = requested_task_id.is_some()
        || requested_task_revision.is_some()
        || requested_attempt_id.is_some();
    if task_selector_present
        && (requested_task_id.is_none()
            || requested_task_revision.is_none()
            || requested_attempt_id.is_none())
    {
        return Err(Error::new(
            "LOGGING_SCOPE_INCOMPLETE",
            "Task logging scope requires task_id, task_revision, and attempt_id together",
        ));
    }
    let requested_operation_id = optional_text(value, "operation_id")?;
    let requested_binding_id = optional_text(value, "binding_id")?;
    let requested_binding_generation = optional_positive_u64(value, "binding_generation")?;
    if requested_binding_id.is_some() != requested_binding_generation.is_some() {
        return Err(Error::new(
            "LOGGING_SCOPE_INCOMPLETE",
            "binding logging scope requires binding_id and binding_generation together",
        ));
    }
    let requested_module_id = optional_text(value, "module_id")?;
    for (field, selected) in [
        ("task_id", requested_task_id.as_deref()),
        ("attempt_id", requested_attempt_id.as_deref()),
        ("operation_id", requested_operation_id.as_deref()),
        ("binding_id", requested_binding_id.as_deref()),
        ("module_id", requested_module_id.as_deref()),
    ] {
        if let Some(selected) = selected {
            valid_selector(selected, field)?;
        }
    }

    let mut task = if task_selector_present {
        Some(current_task_scope(
            db,
            principal,
            requested_task_id.as_deref().unwrap_or_default(),
            requested_task_revision.unwrap_or_default(),
            requested_attempt_id.as_deref().unwrap_or_default(),
        )?)
    } else {
        None
    };
    let mut binding_id = requested_binding_id;
    let mut binding_generation = requested_binding_generation;

    if let Some(operation_id) = requested_operation_id.as_deref() {
        let operation = operations::get_operation(db, operation_id)?;
        if !operation_owned_by(db, principal, operation_id, &operation)? {
            return Err(Error::new(
                "FORBIDDEN",
                "logging Operation is not owned by this Manager's caller or retained route",
            ));
        }
        let operation_task_id = optional_object_text(&operation, "task_id")?;
        let operation_attempt_id = optional_object_text(&operation, "attempt_id")?;
        if operation_task_id.is_some() != operation_attempt_id.is_some() {
            return Err(Error::new(
                "LOGGING_SCOPE_CORRUPT",
                "selected Operation has incomplete Task/Attempt identity",
            ));
        }
        if let (Some(task_id), Some(attempt_id)) = (
            operation_task_id.as_deref(),
            operation_attempt_id.as_deref(),
        ) {
            let revision = tasks::get_task(db, task_id)?["revision"]
                .as_i64()
                .ok_or_else(|| {
                    Error::new(
                        "LOGGING_SCOPE_CORRUPT",
                        "Operation Task revision is invalid",
                    )
                })?;
            let operation_task = current_task_scope(db, principal, task_id, revision, attempt_id)?;
            if let Some(requested) = task.as_ref()
                && (requested.task_id != operation_task.task_id
                    || requested.task_revision != operation_task.task_revision
                    || requested.attempt_id != operation_task.attempt_id)
            {
                return Err(Error::new(
                    "LOGGING_SCOPE_MISMATCH",
                    "logging Operation does not inherit the requested current Task/Attempt",
                ));
            }
            task = Some(operation_task);
        } else if task.is_some() {
            return Err(Error::new(
                "LOGGING_SCOPE_MISMATCH",
                "taskless logging Operation cannot inherit a supplied Task/Attempt",
            ));
        }
        let operation_binding_id = optional_object_text(&operation, "binding_id")?;
        let operation_generation = operation["binding_generation"].as_i64();
        if operation_binding_id.is_some() != operation_generation.is_some()
            || operation_generation.is_some_and(|generation| generation <= 0)
        {
            return Err(Error::new(
                "LOGGING_SCOPE_CORRUPT",
                "selected Operation has incomplete binding identity",
            ));
        }
        if let (Some(operation_binding_id), Some(operation_generation)) =
            (operation_binding_id.as_deref(), operation_generation)
        {
            let generation = u64::try_from(operation_generation).map_err(|_| {
                Error::new(
                    "LOGGING_SCOPE_CORRUPT",
                    "Operation binding generation is invalid",
                )
            })?;
            if binding_id.is_some() || binding_generation.is_some() {
                if binding_id.as_deref() != Some(operation_binding_id)
                    || binding_generation != Some(generation)
                {
                    return Err(Error::new(
                        "LOGGING_SCOPE_MISMATCH",
                        "logging binding selector differs from the selected Operation",
                    ));
                }
            } else {
                binding_id = Some(operation_binding_id.to_owned());
                binding_generation = Some(generation);
            }
        }
    }

    let module_binding_proof = requested_module_id.as_deref().is_some()
        && binding_id.is_none()
        && requested_operation_id.is_none();
    if let Some(binding_id) = binding_id.as_deref() {
        let generation = binding_generation.ok_or_else(|| {
            Error::new(
                "LOGGING_SCOPE_INCOMPLETE",
                "binding logging scope requires a positive generation",
            )
        })?;
        let owned = owned_binding(
            db,
            principal,
            binding_id,
            generation,
            requested_module_id.as_deref(),
        )?;
        if task.is_none() && !module_binding_proof {
            task = owned.task;
        }
    } else if let Some(module_id) = requested_module_id.as_deref() {
        if requested_operation_id.is_some() {
            return Err(Error::new(
                "LOGGING_MODULE_UNPROVEN",
                "module logging scope requires the selected Operation's retained binding",
            ));
        }
        if let Some(task) = task.as_ref() {
            let binding_id = task.binding_id.as_deref().ok_or_else(|| {
                Error::new(
                    "LOGGING_MODULE_UNPROVEN",
                    "Task module logging scope requires its retained binding",
                )
            })?;
            let generation = task.binding_generation.ok_or_else(|| {
                Error::new(
                    "LOGGING_MODULE_UNPROVEN",
                    "Task module logging scope requires its retained binding generation",
                )
            })?;
            let owned = owned_binding(db, principal, binding_id, generation, Some(module_id))?;
            let inherited = owned.task.as_ref().ok_or_else(|| {
                Error::new(
                    "LOGGING_MODULE_UNPROVEN",
                    "Task module logging scope has no retained current Task route",
                )
            })?;
            if inherited.task_id != task.task_id
                || inherited.task_revision != task.task_revision
                || inherited.attempt_id != task.attempt_id
            {
                return Err(Error::new(
                    "LOGGING_SCOPE_MISMATCH",
                    "module logging selector is outside the selected current Task route",
                ));
            }
        } else {
            let _ = owned_module_binding(db, principal, module_id)?;
        }
    }

    if let Some(task) = task.as_ref() {
        if let (Some(binding_id), Some(binding_generation)) =
            (binding_id.as_deref(), binding_generation)
        {
            if task.binding_id.as_deref() != Some(binding_id)
                || task.binding_generation != Some(binding_generation)
            {
                return Err(Error::new(
                    "LOGGING_SCOPE_MISMATCH",
                    "logging binding selector differs from the current Task Attempt",
                ));
            }
        }
    }

    Ok(Scope {
        manager_id: principal.client_id.clone(),
        task_id: task.as_ref().map(|facts| facts.task_id.clone()),
        task_revision: task.as_ref().map(|facts| facts.task_revision),
        attempt_id: task.as_ref().map(|facts| facts.attempt_id.clone()),
        operation_id: requested_operation_id,
        binding_id,
        binding_generation,
        module_id: requested_module_id,
    })
}

fn current_task_scope(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<TaskFacts> {
    let task = tasks::get_task(db, task_id)?;
    if task["revision"].as_i64() != Some(task_revision)
        || task["current_attempt_id"].as_str() != Some(attempt_id)
        || task["state"] != "open"
    {
        return Err(Error::new(
            "STALE_REVISION",
            "logging scope must name the current open Task revision and Attempt",
        ));
    }
    let attempt = tasks::get_attempt(db, attempt_id)?;
    if attempt["task_id"].as_str() != Some(task_id)
        || attempt["task_revision"].as_i64() != Some(task_revision)
        || attempt["owner_id"].as_str() != Some(principal.client_id.as_str())
        || !attempt["released_at_ms"].is_null()
    {
        return Err(Error::new(
            "FORBIDDEN",
            "logging Task scope is limited to the current Attempt owner",
        ));
    }
    let binding_id = optional_object_text(&attempt, "binding_id")?;
    let binding_generation = attempt["binding_generation"].as_i64();
    if binding_id.is_some() != binding_generation.is_some()
        || binding_generation.is_some_and(|generation| generation <= 0)
    {
        return Err(Error::new(
            "LOGGING_SCOPE_CORRUPT",
            "current Attempt binding identity is incomplete",
        ));
    }
    Ok(TaskFacts {
        task_id: task_id.to_owned(),
        task_revision,
        attempt_id: attempt_id.to_owned(),
        binding_id,
        binding_generation: binding_generation
            .and_then(|generation| u64::try_from(generation).ok()),
    })
}

fn operation_owned_by(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
    operation: &Value,
) -> Result<bool> {
    if operation["caller_id"].as_str() == Some(principal.client_id.as_str()) {
        return Ok(true);
    }
    if let Some(link) = authorization::operation_link(db, operation_id)? {
        return Ok(link.effective_manager_id == principal.client_id);
    }
    if let Some(link) = super::automation_work_dispatch::operation_link(db, operation_id)? {
        return Ok(link.effective_manager_id == principal.client_id);
    }
    if let Some(link) = super::automation_repair::operation_link(db, operation_id)? {
        return Ok(link.effective_manager_id == principal.client_id);
    }
    Ok(false)
}

fn owned_binding(
    db: &Connection,
    principal: &Principal,
    binding_id: &str,
    generation: u64,
    module_id: Option<&str>,
) -> Result<OwnedBinding> {
    let generation_i64 = i64::try_from(generation)
        .map_err(|_| Error::invalid("binding_generation is outside the supported range"))?;
    let binding = operations::get_binding(db, binding_id, generation_i64)?;
    if !binding["released_at_ms"].is_null() {
        return Err(Error::new(
            "LOGGING_SCOPE_STALE",
            "logging binding scope must name an active selected route",
        ));
    }
    if let Some(module_id) = module_id {
        let selector = binding["observation"].get("module_contract_selector");
        let retained = super::module_handshake::retained_contract_identity(
            db,
            binding["module_artifact_id"].as_str().unwrap_or_default(),
            selector,
        )?
        .ok_or_else(|| {
            Error::new(
                "LOGGING_MODULE_UNPROVEN",
                "module identity is not the exact trusted descriptor retained by this binding",
            )
        })?;
        if retained.module_id.as_str() != module_id {
            return Err(Error::new(
                "LOGGING_MODULE_UNPROVEN",
                "module identity is not the exact trusted descriptor retained by this binding",
            ));
        }
    }
    let mut statement = db.prepare(
        "SELECT operation_id FROM operations
         WHERE binding_id=?1 AND binding_generation=?2
         ORDER BY created_at_ms DESC, operation_id DESC LIMIT 16",
    )?;
    let operation_ids = statement
        .query_map(params![binding_id, generation_i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for operation_id in operation_ids {
        let operation = operations::get_operation(db, &operation_id)?;
        let task_id = optional_object_text(&operation, "task_id")?;
        let attempt_id = optional_object_text(&operation, "attempt_id")?;
        if task_id.is_some() != attempt_id.is_some() {
            return Err(Error::new(
                "LOGGING_SCOPE_CORRUPT",
                "binding Operation has incomplete Task/Attempt identity",
            ));
        }
        let task = if let (Some(task_id), Some(attempt_id)) =
            (task_id.as_deref(), attempt_id.as_deref())
        {
            let revision = tasks::get_task(db, task_id)["revision"]
                .as_i64()
                .ok_or_else(|| {
                    Error::new("LOGGING_SCOPE_CORRUPT", "binding Task revision is invalid")
                })?;
            Some(current_task_scope(
                db, principal, task_id, revision, attempt_id,
            )?)
        } else {
            None
        };
        if operation_owned_by(db, principal, &operation_id, &operation)? || task.is_some() {
            return Ok(OwnedBinding { task });
        }
    }
    Err(Error::new(
        "FORBIDDEN",
        "selected binding route is not owned by this Manager",
    ))
}

fn owned_module_binding(db: &Connection, principal: &Principal, module_id: &str) -> Result<()> {
    let mut statement = db.prepare(
        "SELECT binding_id,generation FROM bindings
         WHERE released_at_ms IS NULL
           AND json_extract(state_json,'$.module_contract_selector.module_id')=?1
         ORDER BY created_at_ms DESC, binding_id DESC LIMIT 16",
    )?;
    let rows = statement
        .query_map([module_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (binding_id, generation) in rows {
        if let Ok(generation) = u64::try_from(generation)
            && owned_binding(db, principal, &binding_id, generation, Some(module_id)).is_ok()
        {
            return Ok(());
        }
    }
    Err(Error::new(
        "LOGGING_MODULE_UNPROVEN",
        "module scope requires a selected active route owned by this Manager",
    ))
}

fn parse_stored(value: &Value) -> Result<StoredPolicy> {
    let policy: StoredPolicy = serde_json::from_value(value.clone()).map_err(|_| {
        Error::new(
            "LOGGING_POLICY_CORRUPT",
            "retained diagnostic policy has an unsupported shape",
        )
    })?;
    if policy.schema_version != 1
        || policy.manager_id != policy.scope.manager_id
        || policy.revision == 0
        || policy.updated_at_ms <= 0
        || policy.content != "metadata"
    {
        return Err(Error::new(
            "LOGGING_POLICY_CORRUPT",
            "retained diagnostic policy identity or content is invalid",
        ));
    }
    valid_selector(&policy.manager_id, "manager_id")?;
    for (name, value) in [
        ("task_id", policy.scope.task_id.as_deref()),
        ("attempt_id", policy.scope.attempt_id.as_deref()),
        ("operation_id", policy.scope.operation_id.as_deref()),
        ("binding_id", policy.scope.binding_id.as_deref()),
        ("module_id", policy.scope.module_id.as_deref()),
    ] {
        if let Some(value) = value {
            valid_selector(value, name)?;
        }
    }
    if policy.scope.task_id.is_some() != policy.scope.attempt_id.is_some()
        || policy.scope.task_id.is_some() != policy.scope.task_revision.is_some()
        || policy
            .scope
            .task_revision
            .is_some_and(|revision| revision <= 0)
    {
        return Err(Error::new(
            "LOGGING_POLICY_CORRUPT",
            "retained diagnostic Task identity is incomplete",
        ));
    }
    if policy.scope.binding_id.is_some() != policy.scope.binding_generation.is_some()
        || policy
            .scope
            .binding_generation
            .is_some_and(|generation| generation == 0)
    {
        return Err(Error::new(
            "LOGGING_POLICY_CORRUPT",
            "retained diagnostic binding identity is incomplete",
        ));
    }
    if policy
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms <= 0)
    {
        return Err(Error::new(
            "LOGGING_POLICY_CORRUPT",
            "retained diagnostic expiry is invalid",
        ));
    }
    Ok(policy)
}

fn parse_level(value: &str) -> Result<Level> {
    match value {
        "off" => Ok(Level::Off),
        "error" => Ok(Level::Error),
        "warn" => Ok(Level::Warn),
        "info" => Ok(Level::Info),
        "debug" => Ok(Level::Debug),
        "trace" => Ok(Level::Trace),
        _ => Err(Error::invalid(
            "level must be one of off, error, warn, info, debug, trace",
        )),
    }
}

fn policy_key(scope: &Scope) -> Result<String> {
    let identity = json!({
        "manager_id":&scope.manager_id,
        "task_id":&scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":&scope.attempt_id,
        "operation_id":&scope.operation_id,
        "binding_id":&scope.binding_id,
        "binding_generation":scope.binding_generation,
        "module_id":&scope.module_id,
    });
    Ok(format!(
        "{POLICY_PREFIX}{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn public_scope(scope: &Scope) -> Value {
    json!({
        "manager_id":&scope.manager_id,
        "client_id":&scope.manager_id,
        "task_id":&scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":&scope.attempt_id,
        "operation_id":&scope.operation_id,
        "binding_id":&scope.binding_id,
        "binding_generation":scope.binding_generation,
        "module_id":&scope.module_id,
    })
}

fn scope_kind(scope: &Scope) -> &'static str {
    if scope.operation_id.is_some() {
        "operation"
    } else if scope.module_id.is_some() {
        "module"
    } else if scope.task_id.is_some() {
        "task"
    } else if scope.binding_id.is_some() {
        "binding"
    } else {
        "client"
    }
}

fn public_policy(policy: &StoredPolicy, now_ms: i64) -> Value {
    json!({
        "level":&policy.level,
        "content":&policy.content,
        "revision":policy.revision,
        "updated_at_ms":policy.updated_at_ms,
        "expires_at_ms":policy.expires_at_ms,
        "active":policy.expires_at_ms.map_or(true, |expires| expires > now_ms),
        "source":"store_meta",
    })
}

fn ttl_expiry(value: &Value, now_ms: i64) -> Result<Option<i64>> {
    let Some(seconds) = optional_positive_u64(value, "ttl_seconds")? else {
        return Ok(None);
    };
    if seconds > MAX_TTL_SECONDS {
        return Err(Error::invalid(format!(
            "ttl_seconds must be between 1 and {MAX_TTL_SECONDS}"
        )));
    }
    let millis = seconds
        .checked_mul(1_000)
        .and_then(|millis| i64::try_from(millis).ok())
        .ok_or_else(|| Error::invalid("ttl_seconds is outside the supported range"))?;
    now_ms
        .checked_add(millis)
        .map(Some)
        .ok_or_else(|| Error::invalid("ttl_seconds exceeds the timestamp range"))
}

fn optional_text(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => Ok(Some(model::text(value, field)?.to_owned())),
    }
}

fn optional_positive_i64(value: &Value, field: &str) -> Result<Option<i64>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => Ok(Some(model::positive(value, field)?)),
    }
}

fn optional_positive_u64(value: &Value, field: &str) -> Result<Option<u64>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => Ok(Some(
            u64::try_from(model::positive(value, field)?)
                .map_err(|_| Error::invalid(format!("{field} is outside the supported range")))?,
        )),
    }
}

fn optional_object_text(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if !text.trim().is_empty() => Ok(Some(text.clone())),
        Some(_) => Err(Error::new(
            "LOGGING_SCOPE_CORRUPT",
            format!("retained {field} is malformed"),
        )),
    }
}

fn valid_selector(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Error::invalid(format!(
            "{field} is outside the metadata identity vocabulary"
        )));
    }
    Ok(())
}
