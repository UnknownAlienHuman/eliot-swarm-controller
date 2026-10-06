//! Durable demand and safe health readback for explicitly managed bus workers.
//!
//! This code reads only existing consumer registrations and ScriptRun entries.
//! It does not create a Principal, mint a bearer, move the bus cursor, or
//! replay an admitted action.

use crate::{
    automation::{authorization, config as automation_config},
    error::{Error, Result},
    store::{meta, set_meta},
};
use rusqlite::{Connection, Transaction};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use swarm_contracts::DeclaredServiceScope;

const MAX_MANAGED_CONSUMERS: usize = 128;
const HEALTH_VERSION: u64 = 1;
const START_WINDOW_MS: i64 = 60_000;
const MAX_STARTS_PER_WINDOW: usize = 5;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ManagedBusServiceDemand {
    pub scope: DeclaredServiceScope,
    pub owner_manager_id: String,
    pub project_id: String,
    pub automation_id: String,
    pub consumer_client_id: String,
    pub scope_digest: String,
    /// Store-only evidence used by the host to validate its private config.
    /// The token hash is never returned to IPC or Manager status.
    pub credential_token_sha256: String,
    pub worker_config_sha256: String,
    pub owner_state: ManagedBusOwnerState,
    pub owner_receipt_sha256: Option<String>,
    pub state: DemandState,
}

/// Durable distinction between a never-started registration, a start whose
/// process outcome is not yet known, and a host-verified owner readback.
/// This state is intentionally independent of the Manager-facing health text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManagedBusOwnerState {
    NeverStarted,
    LaunchUncertain,
    Live,
    Departed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DemandState {
    Ready,
    Disabled,
    OwnerInactive,
    ScriptRunInactive,
    ScopeChanged,
}

pub(crate) fn managed_demands(db: &Connection) -> Result<Vec<ManagedBusServiceDemand>> {
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta \
         WHERE key LIKE 'client:bus-script-%' ORDER BY key LIMIT ?1",
    )?;
    let rows = statement
        .query_map([MAX_MANAGED_CONSUMERS as i64 + 1], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > MAX_MANAGED_CONSUMERS {
        return Err(Error::new(
            "BUS_SERVICE_CAPACITY",
            "managed bus consumer count exceeds the host bound",
        ));
    }

    let mut demands = Vec::new();
    for (key, encoded) in rows {
        let Some(client_id) = key.strip_prefix("client:") else {
            continue;
        };
        let Ok(registration) = serde_json::from_str::<Value>(&encoded) else {
            continue;
        };
        if registration["role"] != "module" {
            continue;
        }
        let Some(binding_value) = registration.get("bus_consumer") else {
            continue;
        };
        let Ok(binding) =
            serde_json::from_value::<super::ScriptRunConsumerBinding>(binding_value.clone())
        else {
            continue;
        };
        if binding.validate(client_id).is_err() {
            continue;
        }
        if !binding.managed_service() {
            continue;
        }
        let Ok(scope) = binding.service_scope() else {
            continue;
        };
        let token_hash = registration["token_hash"].as_str().unwrap_or_default();
        if !is_sha256(token_hash) {
            continue;
        }
        let worker_config_sha256 = binding
            .worker_config_sha256()
            .filter(|value| is_sha256(value))
            .map(str::to_ascii_lowercase);
        let Some(worker_config_sha256) = worker_config_sha256 else {
            continue;
        };
        let Some(disabled) = registration.get("disabled").and_then(Value::as_bool) else {
            continue;
        };
        let health = registration.get("managed_bus_service_health");
        let owner_state = parse_owner_state(health);
        let owner_receipt_sha256 = health
            .and_then(|value| value.get("owner_receipt_sha256"))
            .and_then(Value::as_str)
            .filter(|value| is_sha256(value))
            .map(str::to_ascii_lowercase);
        let owner_state = if health
            .and_then(|value| value.get("owner_receipt_sha256"))
            .is_some_and(|value| !value.is_null() && owner_receipt_sha256.is_none())
            || (owner_receipt_sha256.is_some()
                && !matches!(
                    owner_state,
                    ManagedBusOwnerState::Live | ManagedBusOwnerState::Unknown
                ))
            || (matches!(owner_state, ManagedBusOwnerState::Live) && owner_receipt_sha256.is_none())
        {
            ManagedBusOwnerState::Unknown
        } else {
            owner_state
        };
        let state = if disabled {
            DemandState::Disabled
        } else {
            match authorization::require_registered_manager(db, binding.owner_manager_id()) {
                Err(error) if matches!(error.code.as_str(), "FORBIDDEN" | "INVALID_PARAMS") => {
                    DemandState::OwnerInactive
                }
                Err(error) => return Err(error),
                Ok(()) => match automation_config::load_entry(
                    db,
                    binding.owner_manager_id(),
                    binding.project_id(),
                    binding.automation_id(),
                ) {
                    Err(error) if is_automation_record_invalid(&error.code) => {
                        DemandState::ScriptRunInactive
                    }
                    Err(error) => return Err(error),
                    Ok(Some(entry))
                        if entry.owner_manager_id == binding.owner_manager_id()
                            && entry.script_run_ready() =>
                    {
                        let digest = crate::store::automation_dispatch::bus_kernel::
                            script_run_consumer_scope_digest(&entry)?;
                        if digest == binding.scope_digest() {
                            DemandState::Ready
                        } else {
                            DemandState::ScopeChanged
                        }
                    }
                    Ok(_) => DemandState::ScriptRunInactive,
                },
            }
        };
        demands.push(ManagedBusServiceDemand {
            scope,
            owner_manager_id: binding.owner_manager_id().to_owned(),
            project_id: binding.project_id().to_owned(),
            automation_id: binding.automation_id().to_owned(),
            consumer_client_id: client_id.to_owned(),
            scope_digest: binding.scope_digest().to_owned(),
            credential_token_sha256: token_hash.to_ascii_lowercase(),
            worker_config_sha256,
            owner_state,
            owner_receipt_sha256,
            state,
        });
    }
    Ok(demands)
}

pub(crate) fn record_health(
    tx: &Transaction<'_>,
    scope: &DeclaredServiceScope,
    state: &str,
    consecutive_failures: u32,
    error_code: Option<&str>,
    retry_in_ms: Option<u64>,
    now_ms: i64,
) -> Result<()> {
    scope.validate()?;
    if !matches!(
        state,
        "dormant" | "starting" | "running" | "stopping" | "retry_wait" | "isolated" | "unknown"
    ) || consecutive_failures > 32
        || retry_in_ms.is_some_and(|delay| delay > 300_000)
        || error_code.is_some_and(|code| !safe_error_code(code))
        || (matches!(state, "retry_wait" | "isolated" | "unknown") && error_code.is_none())
    {
        return Err(Error::new(
            "BUS_SERVICE_HEALTH_INVALID",
            "managed bus service health receipt is invalid",
        ));
    }
    let key = format!("client:{}", scope.service_id);
    let mut registration = meta(tx, &key)?
        .ok_or_else(|| Error::new("BUS_SERVICE_SCOPE_STALE", "managed bus scope was removed"))?;
    let binding: super::ScriptRunConsumerBinding =
        serde_json::from_value(registration.get("bus_consumer").cloned().ok_or_else(|| {
            Error::new("BUS_SERVICE_SCOPE_STALE", "managed bus scope was removed")
        })?)
        .map_err(|_| Error::new("BUS_CONSUMER_REGISTRATION_CORRUPT", "invalid bus scope"))?;
    binding.validate(&scope.service_id)?;
    if !binding.managed_service() || &binding.service_scope()? != scope {
        return Err(Error::new(
            "BUS_SERVICE_SCOPE_STALE",
            "consumer is not enrolled for managed service lifecycle",
        ));
    }
    let start_attempts_ms = registration
        .get("managed_bus_service_health")
        .and_then(|health| health.get("start_attempts_ms"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let previous_health = registration.get("managed_bus_service_health");
    let owner_state = owner_state_json(previous_health);
    let owner_receipt_sha256 = previous_health
        .and_then(|health| health.get("owner_receipt_sha256"))
        .cloned()
        .unwrap_or(Value::Null);
    registration["managed_bus_service_health"] = json!({
        "schema_version":HEALTH_VERSION,
        "state":state,
        "consecutive_failures":consecutive_failures,
        "error_code":error_code,
        "retry_after_ms":retry_in_ms.map(|delay| now_ms.saturating_add(i64::try_from(delay).unwrap_or(i64::MAX))),
        "updated_at_ms":now_ms,
        "start_attempts_ms":start_attempts_ms,
        "owner_state":owner_state,
        "owner_receipt_sha256":owner_receipt_sha256,
    });
    set_meta(tx, &key, &registration)
}

/// Persist the start budget before process creation. The Store writer
/// serializes callers and retains the rolling window across host restarts.
pub(crate) fn record_start(
    tx: &Transaction<'_>,
    scope: &DeclaredServiceScope,
    now_ms: i64,
) -> Result<()> {
    scope.validate()?;
    if now_ms < 0 {
        return Err(Error::new(
            "BUS_SERVICE_CLOCK_INVALID",
            "managed service start time is invalid",
        ));
    }
    let key = format!("client:{}", scope.service_id);
    let mut registration = meta(tx, &key)?
        .ok_or_else(|| Error::new("BUS_SERVICE_SCOPE_STALE", "managed bus scope was removed"))?;
    let binding: super::ScriptRunConsumerBinding =
        serde_json::from_value(registration.get("bus_consumer").cloned().ok_or_else(|| {
            Error::new("BUS_SERVICE_SCOPE_STALE", "managed bus scope was removed")
        })?)
        .map_err(|_| Error::new("BUS_CONSUMER_REGISTRATION_CORRUPT", "invalid bus scope"))?;
    binding.validate(&scope.service_id)?;
    if !binding.managed_service() || &binding.service_scope()? != scope {
        return Err(Error::new(
            "BUS_SERVICE_SCOPE_STALE",
            "consumer is not enrolled for this managed service generation",
        ));
    }
    if registration["disabled"] == true {
        return Err(Error::new(
            "BUS_SERVICE_SCOPE_INACTIVE",
            "managed bus consumer is disabled",
        ));
    }
    match authorization::require_registered_manager(tx, binding.owner_manager_id()) {
        Ok(()) => {}
        Err(error) if error.code == "FORBIDDEN" => {
            return Err(Error::new(
                "BUS_SERVICE_SCOPE_INACTIVE",
                "managed bus owner is inactive",
            ));
        }
        Err(error) => return Err(error),
    }
    let entry = automation_config::load_entry(
        tx,
        binding.owner_manager_id(),
        binding.project_id(),
        binding.automation_id(),
    )?
    .ok_or_else(|| {
        Error::new(
            "BUS_SERVICE_SCOPE_INACTIVE",
            "selected ScriptRun entry is unavailable",
        )
    })?;
    if entry.owner_manager_id != binding.owner_manager_id() || !entry.script_run_ready() {
        return Err(Error::new(
            "BUS_SERVICE_SCOPE_INACTIVE",
            "selected ScriptRun entry is inactive",
        ));
    }
    let digest =
        crate::store::automation_dispatch::bus_kernel::script_run_consumer_scope_digest(&entry)?;
    if digest != binding.scope_digest() {
        return Err(Error::new(
            "BUS_SERVICE_SCOPE_INACTIVE",
            "selected ScriptRun scope changed",
        ));
    }

    let mut health = registration
        .get("managed_bus_service_health")
        .cloned()
        .unwrap_or_else(|| json!({"schema_version":HEALTH_VERSION}));
    let owner_state = parse_owner_state(Some(&health));
    if !matches!(
        owner_state,
        ManagedBusOwnerState::NeverStarted | ManagedBusOwnerState::Departed
    ) {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_UNKNOWN",
            "managed service owner has not been proven departed",
        ));
    }
    let mut starts = health
        .get("start_attempts_ms")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if health
        .get("start_attempts_ms")
        .is_some_and(|value| !value.is_array())
    {
        return Err(Error::new(
            "BUS_SERVICE_HEALTH_CORRUPT",
            "managed service start history is invalid",
        ));
    }
    let mut previous = None;
    let mut retained = Vec::with_capacity(MAX_STARTS_PER_WINDOW);
    for value in starts.drain(..) {
        let timestamp = value
            .as_i64()
            .filter(|value| *value >= 0 && *value <= now_ms)
            .ok_or_else(|| {
                Error::new(
                    "BUS_SERVICE_HEALTH_CORRUPT",
                    "managed service start history is invalid",
                )
            })?;
        if previous.is_some_and(|previous| timestamp < previous) {
            return Err(Error::new(
                "BUS_SERVICE_HEALTH_CORRUPT",
                "managed service start history is unordered",
            ));
        }
        previous = Some(timestamp);
        if now_ms.saturating_sub(timestamp) < START_WINDOW_MS {
            retained.push(json!(timestamp));
        }
    }
    if retained.len() >= MAX_STARTS_PER_WINDOW {
        return Err(Error::new(
            "BUS_SERVICE_START_LIMIT",
            "managed service reached its persisted start limit",
        ));
    }
    retained.push(json!(now_ms));
    health["start_attempts_ms"] = json!(retained);
    health["owner_state"] = json!("launch_uncertain");
    health["owner_receipt_sha256"] = Value::Null;
    health["updated_at_ms"] = json!(now_ms);
    if health["schema_version"].as_u64().is_none() {
        health["schema_version"] = json!(HEALTH_VERSION);
    }
    registration["managed_bus_service_health"] = health;
    set_meta(tx, &key, &registration)
}

/// A prior host's running receipt is never treated as current liveness after
/// restart. The service actor must re-read the exact owner and process family.
pub(crate) fn reset_for_host_start(tx: &Transaction<'_>, now_ms: i64) -> Result<()> {
    let mut statement = tx.prepare(
        "SELECT key,value_json FROM meta \
         WHERE key LIKE 'client:bus-script-%' ORDER BY key LIMIT ?1",
    )?;
    let rows = statement
        .query_map([MAX_MANAGED_CONSUMERS as i64 + 1], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    if rows.len() > MAX_MANAGED_CONSUMERS {
        // This optional subsystem must not block the core host from starting.
        // The read-only health projection reports the shared capacity error;
        // no extra demand is started until the bounded set is repaired.
        return Ok(());
    }
    for (key, encoded) in rows {
        let Some(client_id) = key.strip_prefix("client:") else {
            continue;
        };
        let Ok(mut registration) = serde_json::from_str::<Value>(&encoded) else {
            continue;
        };
        let Some(binding_value) = registration.get("bus_consumer") else {
            continue;
        };
        let Ok(binding) =
            serde_json::from_value::<super::ScriptRunConsumerBinding>(binding_value.clone())
        else {
            continue;
        };
        if !binding.managed_service() {
            continue;
        }
        if binding.validate(client_id).is_err() {
            continue;
        }
        let previous_health = registration.get("managed_bus_service_health");
        let starts = previous_health
            .and_then(|health| health.get("start_attempts_ms"))
            .cloned()
            .unwrap_or_else(|| json!([]));
        let owner_state = owner_state_json(previous_health);
        let owner_receipt_sha256 = previous_health
            .and_then(|health| health.get("owner_receipt_sha256"))
            .cloned()
            .unwrap_or(Value::Null);
        let error_code = previous_health
            .and_then(|health| health.get("error_code"))
            .and_then(Value::as_str)
            .filter(|code| safe_error_code(code))
            .unwrap_or("BUS_SERVICE_HOST_RESTART_RECONCILING");
        registration["managed_bus_service_health"] = json!({
            "schema_version":HEALTH_VERSION,
            "state":"unknown",
            "consecutive_failures":0,
            "error_code":error_code,
            "retry_after_ms":Value::Null,
            "updated_at_ms":now_ms,
            "start_attempts_ms":starts,
            "owner_state":owner_state,
            "owner_receipt_sha256":owner_receipt_sha256,
        });
        set_meta(tx, &key, &registration)?;
    }
    Ok(())
}

/// Persist a host-only process-family readback. The Store does not interpret
/// this as cursor progress or permission to replay a bus action.
pub(crate) fn record_owner_readback(
    tx: &Transaction<'_>,
    scope: &DeclaredServiceScope,
    state: ManagedBusOwnerState,
    receipt_sha256: Option<&str>,
    now_ms: i64,
) -> Result<()> {
    scope.validate()?;
    if now_ms < 0
        || receipt_sha256.is_some_and(|digest| !is_sha256(digest))
        || (matches!(state, ManagedBusOwnerState::Live) && receipt_sha256.is_none())
        || (matches!(
            state,
            ManagedBusOwnerState::Departed | ManagedBusOwnerState::NeverStarted
        ) && receipt_sha256.is_some())
    {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_READBACK_INVALID",
            "managed service owner readback is invalid",
        ));
    }
    let key = format!("client:{}", scope.service_id);
    let mut registration = meta(tx, &key)?
        .ok_or_else(|| Error::new("BUS_SERVICE_SCOPE_STALE", "managed bus scope was removed"))?;
    let binding: super::ScriptRunConsumerBinding =
        serde_json::from_value(registration.get("bus_consumer").cloned().ok_or_else(|| {
            Error::new("BUS_SERVICE_SCOPE_STALE", "managed bus scope was removed")
        })?)
        .map_err(|_| Error::new("BUS_CONSUMER_REGISTRATION_CORRUPT", "invalid bus scope"))?;
    binding.validate(&scope.service_id)?;
    if !binding.managed_service() || &binding.service_scope()? != scope {
        return Err(Error::new(
            "BUS_SERVICE_SCOPE_STALE",
            "consumer is not enrolled for this managed service generation",
        ));
    }
    let mut health = registration
        .get("managed_bus_service_health")
        .cloned()
        .unwrap_or_else(|| json!({"schema_version":HEALTH_VERSION}));
    if !health.is_object() {
        return Err(Error::new(
            "BUS_SERVICE_HEALTH_CORRUPT",
            "managed service health record is invalid",
        ));
    }
    let previous = parse_owner_state(Some(&health));
    let previous_digest = health
        .get("owner_receipt_sha256")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let transition_allowed = match state {
        ManagedBusOwnerState::Live => matches!(
            previous,
            ManagedBusOwnerState::LaunchUncertain
                | ManagedBusOwnerState::Live
                | ManagedBusOwnerState::Unknown
        ),
        ManagedBusOwnerState::Departed => {
            matches!(
                previous,
                ManagedBusOwnerState::LaunchUncertain
                    | ManagedBusOwnerState::Live
                    | ManagedBusOwnerState::Unknown
            ) || (matches!(previous, ManagedBusOwnerState::Departed) && receipt_sha256.is_none())
        }
        ManagedBusOwnerState::Unknown => !matches!(
            previous,
            ManagedBusOwnerState::NeverStarted | ManagedBusOwnerState::Departed
        ),
        ManagedBusOwnerState::NeverStarted | ManagedBusOwnerState::LaunchUncertain => false,
    };
    if !transition_allowed
        || (matches!(previous, ManagedBusOwnerState::Live)
            && matches!(state, ManagedBusOwnerState::Live)
            && previous_digest.as_deref() != receipt_sha256)
    {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_READBACK_STALE",
            "owner readback conflicts with the retained service incarnation",
        ));
    }
    health["schema_version"] = json!(HEALTH_VERSION);
    health["owner_state"] = json!(state);
    health["owner_receipt_sha256"] = receipt_sha256
        .map(|digest| json!(digest.to_ascii_lowercase()))
        .unwrap_or(Value::Null);
    health["updated_at_ms"] = json!(now_ms);
    registration["managed_bus_service_health"] = health;
    set_meta(tx, &key, &registration)
}

fn parse_owner_state(health: Option<&Value>) -> ManagedBusOwnerState {
    match health
        .and_then(|value| value.get("owner_state"))
        .and_then(Value::as_str)
    {
        Some("never_started") => ManagedBusOwnerState::NeverStarted,
        Some("launch_uncertain") => ManagedBusOwnerState::LaunchUncertain,
        Some("live") => ManagedBusOwnerState::Live,
        Some("departed") => ManagedBusOwnerState::Departed,
        Some("unknown") => ManagedBusOwnerState::Unknown,
        Some(_) => ManagedBusOwnerState::Unknown,
        None => {
            let has_attempts = health
                .and_then(|value| value.get("start_attempts_ms"))
                .and_then(Value::as_array)
                .is_some_and(|attempts| !attempts.is_empty());
            if has_attempts {
                ManagedBusOwnerState::Unknown
            } else {
                ManagedBusOwnerState::NeverStarted
            }
        }
    }
}

fn owner_state_json(health: Option<&Value>) -> Value {
    match parse_owner_state(health) {
        ManagedBusOwnerState::NeverStarted => json!("never_started"),
        ManagedBusOwnerState::LaunchUncertain => json!("launch_uncertain"),
        ManagedBusOwnerState::Live => json!("live"),
        ManagedBusOwnerState::Departed => json!("departed"),
        ManagedBusOwnerState::Unknown => json!("unknown"),
    }
}

/// Manager-readable host.status projection. It never exposes Manager IDs,
/// consumer IDs, file paths, process identities, credentials, cursors, or
/// event data; the opaque service key is a one-way digest prefix.
pub(crate) fn health_projection(db: &Connection) -> Result<Value> {
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta \
         WHERE key LIKE 'client:bus-script-%' ORDER BY key LIMIT ?1",
    )?;
    let rows = statement
        .query_map([MAX_MANAGED_CONSUMERS as i64 + 1], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut output = Vec::new();
    let capacity_exceeded = rows.len() > MAX_MANAGED_CONSUMERS;
    if capacity_exceeded {
        return Ok(json!([unknown_health(
            "managed_bus_capacity",
            "BUS_SERVICE_CAPACITY"
        )]));
    }
    for (key, encoded) in rows {
        let client_id = key.strip_prefix("client:").unwrap_or(&key);
        let digest = format!("{:x}", Sha256::digest(client_id.as_bytes()));
        let service_key = format!("sha256:{}", &digest[..16]);
        let Some(registration) = serde_json::from_str::<Value>(&encoded).ok() else {
            output.push(unknown_health(
                &service_key,
                "BUS_CONSUMER_REGISTRATION_CORRUPT",
            ));
            continue;
        };
        if registration["role"] != "module" {
            output.push(unknown_health(
                &service_key,
                "BUS_CONSUMER_REGISTRATION_CORRUPT",
            ));
            continue;
        }
        let Some(binding_value) = registration.get("bus_consumer") else {
            output.push(unknown_health(
                &service_key,
                "BUS_CONSUMER_REGISTRATION_CORRUPT",
            ));
            continue;
        };
        let Ok(binding) =
            serde_json::from_value::<super::ScriptRunConsumerBinding>(binding_value.clone())
        else {
            output.push(unknown_health(
                &service_key,
                "BUS_CONSUMER_REGISTRATION_CORRUPT",
            ));
            continue;
        };
        if !binding.managed_service() {
            continue;
        }
        if binding.validate(client_id).is_err() || binding.service_scope().is_err() {
            output.push(unknown_health(
                &service_key,
                "BUS_CONSUMER_REGISTRATION_CORRUPT",
            ));
            continue;
        }
        if !registration["token_hash"].as_str().is_some_and(is_sha256)
            || registration
                .get("disabled")
                .and_then(Value::as_bool)
                .is_none()
        {
            output.push(unknown_health(
                &service_key,
                "BUS_CONSUMER_REGISTRATION_CORRUPT",
            ));
            continue;
        }
        let Some(health) = registration.get("managed_bus_service_health") else {
            output.push(unknown_health(&service_key, "BUS_SERVICE_HEALTH_MISSING"));
            continue;
        };
        let state = health["state"].as_str();
        let owner_state_field_present = health.get("owner_state").is_some();
        let owner_state = health.get("owner_state").and_then(Value::as_str);
        let owner_receipt_sha256 = health.get("owner_receipt_sha256").and_then(Value::as_str);
        let failures = health["consecutive_failures"].as_u64();
        let updated_at_ms = health["updated_at_ms"].as_i64();
        let retry_after_ms = health["retry_after_ms"].as_i64();
        let error_code = health["error_code"]
            .as_str()
            .filter(|value| safe_error_code(value));
        if health["schema_version"] != HEALTH_VERSION
            || !(owner_state.is_none()
                || matches!(
                    owner_state,
                    Some("never_started" | "launch_uncertain" | "live" | "departed" | "unknown")
                ))
            || (owner_state_field_present && owner_state.is_none())
            || !(health
                .get("owner_receipt_sha256")
                .is_none_or(Value::is_null)
                || owner_receipt_sha256.is_some_and(is_sha256))
            || (owner_state == Some("live")
                && owner_receipt_sha256.is_none_or(|value| !is_sha256(value)))
            || (owner_state.is_none() && owner_receipt_sha256.is_some())
            || (owner_receipt_sha256.is_some()
                && matches!(
                    owner_state,
                    Some("never_started" | "launch_uncertain" | "departed")
                ))
            || !matches!(
                state,
                Some(
                    "dormant"
                        | "starting"
                        | "running"
                        | "stopping"
                        | "retry_wait"
                        | "isolated"
                        | "unknown"
                )
            )
            || failures.is_none_or(|value| value > 32)
            || updated_at_ms.is_none_or(|value| value < 0)
            || !(health["error_code"].is_null() || error_code.is_some())
            || !(health["retry_after_ms"].is_null() || retry_after_ms.is_some())
            || retry_after_ms.is_some_and(|retry| {
                updated_at_ms.is_none_or(|updated| {
                    retry < updated || retry.saturating_sub(updated) > 300_000
                })
            })
            || (matches!(state, Some("retry_wait" | "isolated" | "unknown"))
                && error_code.is_none())
        {
            output.push(unknown_health(&service_key, "BUS_SERVICE_HEALTH_CORRUPT"));
            continue;
        }
        output.push(json!({
            "service_key":service_key,
            "state":health["state"],
            "owner_state":owner_state_json(Some(health)),
            "consecutive_failures":health["consecutive_failures"],
            "last_error_code":health["error_code"],
            "retry_after_ms":health["retry_after_ms"],
            "updated_at_ms":health["updated_at_ms"],
        }));
    }
    Ok(json!(output))
}

fn unknown_health(service_key: &str, error_code: &str) -> Value {
    json!({
        "service_key":service_key,
        "state":"unknown",
        "consecutive_failures":0,
        "last_error_code":error_code,
        "retry_after_ms":Value::Null,
        "updated_at_ms":0,
    })
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_automation_record_invalid(code: &str) -> bool {
    matches!(
        code,
        "AUTOMATION_RECORD_CORRUPT" | "AUTOMATION_RECORD_INVALID" | "AUTOMATION_RECORD_VERSION"
    )
}

fn safe_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}
