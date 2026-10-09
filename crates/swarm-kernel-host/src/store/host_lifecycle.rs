//! Host lifecycle receipts survive the IPC endpoint and the manager's chat.
use super::{meta, set_meta};
use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const CURRENT: &str = "host:lifecycle:v1";
const LAST_EXIT: &str = "host:last-exit:v1";
const LATEST_FAILURE: &str = "host:latest-failure:v1";
const OPTIONAL_WORKER_HEALTH: &str = "host:optional-workers:v1";
const SUPERVISORS: &[&str] = &[
    "module-supervisor",
    "managed-bus-supervisor",
    "legacy-workers",
    "checks",
    "scripts",
    "opencode",
    "zed",
    "scheduler",
    "automation-scheduler",
    "automation",
    "launcher",
    "native-mcp",
    "native-mcp-tools",
    "forge",
];
const OPTIONAL_WORKERS: &[&str] = &[
    "module-supervisor",
    "managed-bus-supervisor",
    "checks",
    "scripts",
    "opencode",
    "zed",
    "scheduler",
    "automation-scheduler",
    "automation",
    "launcher",
    "native-mcp",
    "native-mcp-tools",
    "forge",
];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lifecycle {
    schema_version: u8,
    host_epoch: i64,
    state: State,
    started_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum State {
    Starting,
    Running,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum FailureCategory {
    StartupFailure,
    SupervisorStopped,
    SupervisorFailed,
    RuntimeFailure,
}

impl FailureCategory {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::StartupFailure => "startup_failure",
            Self::SupervisorStopped => "supervisor_stopped",
            Self::SupervisorFailed => "supervisor_failed",
            Self::RuntimeFailure => "runtime_failure",
        }
    }

    fn valid_for(self, error_code: Option<&str>, has_supervisor: bool) -> bool {
        match self {
            Self::StartupFailure | Self::RuntimeFailure => {
                !has_supervisor
                    && !matches!(error_code, Some("SUPERVISOR_STOPPED" | "SUPERVISOR_FAILED"))
            }
            Self::SupervisorStopped => error_code == Some("SUPERVISOR_STOPPED"),
            Self::SupervisorFailed => has_supervisor || error_code == Some("SUPERVISOR_FAILED"),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Exit {
    schema_version: u8,
    host_epoch: Option<i64>,
    observed_at_ms: i64,
    error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    secondary_codes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failed_supervisor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure_category: Option<FailureCategory>,
    manager_action_required: bool,
    retry_authorized: bool,
}

/// One closed serialization authority for the paired host terminal events.
/// Detailed error codes remain only in the retained Exit receipt.
#[derive(Debug, Serialize)]
struct HostTerminalFact {
    schema_version: u8,
    phase: &'static str,
    status: &'static str,
    occurrence_id: String,
    host_epoch: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_supervisor: Option<String>,
}

impl HostTerminalFact {
    fn from_exit(receipt: &Exit, host_epoch: i64) -> Result<Self> {
        let status = if receipt.error_code.is_some() {
            "failed"
        } else {
            "completed"
        };
        let failed_supervisor = receipt.failed_supervisor.clone();
        let category_valid = receipt.failure_category.is_none_or(|category| {
            category.valid_for(receipt.error_code.as_deref(), failed_supervisor.is_some())
        });
        let supervisor_valid = failed_supervisor.as_deref().is_none_or(is_known_supervisor);
        if host_epoch <= 0
            || !category_valid
            || !supervisor_valid
            || (status == "failed") != receipt.failure_category.is_some()
        {
            return Err(Error::new(
                "SYSTEM_EVENT_INVALID",
                "host terminal fact identity or category is invalid",
            ));
        }
        Ok(Self {
            schema_version: 1,
            phase: "host_terminal_exit_observed",
            status,
            occurrence_id: format!("host-terminal-exit:{host_epoch}"),
            host_epoch,
            failure_category: receipt.failure_category.map(FailureCategory::as_str),
            failed_supervisor,
        })
    }

    fn payload_json(&self) -> Result<String> {
        model::canonical(&serde_json::to_value(self)?)
    }

    fn is_failure(&self) -> bool {
        self.status == "failed"
    }
}

pub(super) fn is_known_supervisor(name: &str) -> bool {
    SUPERVISORS.contains(&name)
}

fn epoch(db: &Connection) -> Result<i64> {
    meta(db, "host_epoch")?
        .and_then(|v| v.as_i64())
        .filter(|epoch| *epoch > 0)
        .ok_or_else(|| Error::new("HOST_LIFECYCLE_INVALID", "host epoch is missing"))
}

fn load(db: &Connection) -> Result<Option<Lifecycle>> {
    meta(db, CURRENT)?
        .map(|value| {
            let record: Lifecycle = serde_json::from_value(value).map_err(|_| {
                Error::new(
                    "HOST_LIFECYCLE_INVALID",
                    "host lifecycle receipt is invalid",
                )
            })?;
            if record.schema_version != 1
                || record.host_epoch <= 0
                || record.started_at_ms < 0
                || record.updated_at_ms < record.started_at_ms
            {
                return Err(Error::new(
                    "HOST_LIFECYCLE_INVALID",
                    "host lifecycle receipt is invalid",
                ));
            }
            Ok(record)
        })
        .transpose()
}

fn retain_exit(
    tx: &Transaction<'_>,
    receipt: &Exit,
    interrupted_epochs: Option<(i64, i64)>,
) -> Result<()> {
    let value = json!(receipt);
    set_meta(tx, LAST_EXIT, &value)?;
    if receipt.manager_action_required {
        set_meta(tx, LATEST_FAILURE, &value)?;
    }
    let (observation, key) = if let Some((previous_epoch, current_epoch)) = interrupted_epochs {
        if receipt.host_epoch != Some(previous_epoch)
            || receipt.error_code.as_deref() != Some("HOST_INTERRUPTED")
            || current_epoch <= previous_epoch
        {
            return Err(Error::new(
                "HOST_LIFECYCLE_INVALID",
                "interrupted host epoch pair is invalid",
            ));
        }
        let occurrence_id = format!("host-interruption:{previous_epoch}:{current_epoch}");
        let mut observation = value.clone();
        observation["phase"] = json!("host_interruption_observed");
        observation["occurrence_id"] = json!(occurrence_id);
        observation["previous_host_epoch"] = json!(previous_epoch);
        observation["current_host_epoch"] = json!(current_epoch);
        (
            observation,
            format!("interrupted:{previous_epoch}:{current_epoch}"),
        )
    } else if let Some(host_epoch) = receipt.host_epoch {
        let fact = HostTerminalFact::from_exit(receipt, host_epoch)?;
        let payload_json = fact.payload_json()?;
        if fact.is_failure() {
            tx.execute(
                "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:host-lifecycle',?1,NULL,'host.failed',?2,?3)",
                params![
                    format!("failed:{host_epoch}"),
                    &payload_json,
                    receipt.observed_at_ms
                ],
            )?;
        }
        tx.execute(
            "INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms) VALUES('controller:host-lifecycle',?1,'host.exit',?2,?3)",
            params![
                format!("terminal:{host_epoch}"),
                &payload_json,
                receipt.observed_at_ms
            ],
        )?;
        return Ok(());
    } else {
        (
            value.clone(),
            format!(
                "{}:{}",
                receipt.host_epoch.unwrap_or(0),
                receipt.observed_at_ms
            ),
        )
    };
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms) VALUES('controller:host-lifecycle',?1,'host.exit',?2,?3)",
        params![key, model::canonical(&observation)?, receipt.observed_at_ms],
    )?;
    if let Some((previous_epoch, current_epoch)) = interrupted_epochs {
        let occurrence_id = format!("host-interruption:{previous_epoch}:{current_epoch}");
        super::insert_safe_system_event(
            tx,
            "controller:host-lifecycle",
            &format!("interruption:{previous_epoch}:{current_epoch}"),
            None,
            "host.interrupted",
            "host_interruption_observed",
            "unknown",
            Some(&occurrence_id),
            Some((previous_epoch, current_epoch)),
            Some("HOST_INTERRUPTED"),
            receipt.observed_at_ms,
        )?;
    }
    Ok(())
}

/// Called after acquiring the exclusive data-root lock and starting the Store.
/// A previous active marker proves interruption, but does not identify its cause.
pub(super) fn start(tx: &Transaction<'_>, now: i64) -> Result<()> {
    let current_epoch = epoch(tx)?;
    match load(tx) {
        Ok(Some(previous)) if matches!(previous.state, State::Starting | State::Running) => {
            retain_exit(
                tx,
                &Exit {
                    schema_version: 1,
                    host_epoch: Some(previous.host_epoch),
                    observed_at_ms: now,
                    error_code: Some("HOST_INTERRUPTED".into()),
                    secondary_codes: Vec::new(),
                    failed_supervisor: None,
                    failure_category: None,
                    manager_action_required: true,
                    retry_authorized: false,
                },
                Some((previous.host_epoch, current_epoch)),
            )?;
        }
        Err(error) if error.code != "STORE_ERROR" && error.code != "STORE_CLOSED" => {
            // Keep the original malformed fact for inspection; recovery must
            // not make an optional lifecycle receipt prevent host startup.
            let original: Option<String> = tx
                .query_row(
                    "SELECT value_json FROM meta WHERE key=?1",
                    [CURRENT],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(original) = original {
                set_meta(
                    tx,
                    "host:invalid-lifecycle:v1",
                    &json!({"raw_value_json": original}),
                )?;
            }
            retain_exit(
                tx,
                &Exit {
                    schema_version: 1,
                    host_epoch: None,
                    observed_at_ms: now,
                    error_code: Some("HOST_LIFECYCLE_INVALID".into()),
                    secondary_codes: Vec::new(),
                    failed_supervisor: None,
                    failure_category: None,
                    manager_action_required: true,
                    retry_authorized: false,
                },
                None,
            )?;
        }
        Err(error) => return Err(error),
        _ => {}
    }
    set_meta(
        tx,
        CURRENT,
        &json!(Lifecycle {
            schema_version: 1,
            host_epoch: current_epoch,
            state: State::Starting,
            started_at_ms: now,
            updated_at_ms: now
        }),
    )?;
    // Previous host tasks may have been interrupted before their shutdown
    // receipt. Never expose their last `running` state as current readiness;
    // retain only bounded degraded state plus failure history until the new
    // actor reports its own state, so a restart does not erase the last typed
    // failure.
    let health = preserve_optional_failure_health(tx, now)?;
    set_meta(tx, OPTIONAL_WORKER_HEALTH, &health)
}

fn preserve_optional_failure_health(db: &Connection, now: i64) -> Result<Value> {
    let mut retained = serde_json::Map::new();
    let Some(value) = meta(db, OPTIONAL_WORKER_HEALTH)? else {
        return Ok(json!({"schema_version":1,"workers":retained}));
    };
    let Some(workers) = value.get("workers").and_then(Value::as_object) else {
        return Ok(json!({"schema_version":1,"workers":retained}));
    };
    if value["schema_version"] != 1 || workers.len() > OPTIONAL_WORKERS.len() {
        return Ok(json!({"schema_version":1,"workers":retained}));
    }
    for (name, receipt) in workers {
        let state = receipt["state"].as_str();
        let code = receipt["last_error_code"].as_str();
        let failures = receipt["consecutive_failures"].as_u64();
        let updated_at_ms = receipt["updated_at_ms"].as_i64();
        let retry_after_ms = receipt["retry_after_ms"].as_i64();
        let degraded = matches!(state, Some("retry_wait" | "isolated"));
        let valid_code = code.is_some_and(valid_optional_worker_code);
        let valid_retry = retry_after_ms.is_some_and(|retry| {
            updated_at_ms
                .is_some_and(|updated| retry >= updated && retry.saturating_sub(updated) <= 60_000)
        });
        let historical_failure = historical_failure(receipt);
        let child_health = receipt
            .get("child")
            .filter(|value| !value.is_null())
            .and_then(bounded_child_health);
        let valid_degraded = degraded
            && failures.is_some_and(|value| value <= 32)
            && updated_at_ms.is_some_and(|value| value >= 0)
            && valid_code
            && valid_retry;
        if !OPTIONAL_WORKERS.contains(&name.as_str())
            || (!valid_degraded && historical_failure.is_none() && child_health.is_none())
        {
            continue;
        }
        let retained_state = if valid_degraded {
            state.unwrap_or("retry_wait")
        } else {
            "dormant"
        };
        let retained_failures = if valid_degraded {
            failures.unwrap_or(0)
        } else {
            0
        };
        let retained_code = valid_degraded.then_some(code.unwrap_or_default());
        let retained_retry = valid_degraded.then_some(retry_after_ms.unwrap_or_default());
        let retained_updated_at = if valid_degraded {
            updated_at_ms.unwrap_or(now)
        } else {
            now
        };
        let retained_child = child_health.and_then(|value| restart_child_health(value, state));
        let restart_count = bounded_restart_count(receipt).unwrap_or(0);
        retained.insert(
            name.clone(),
            json!({
                "state":retained_state,
                "consecutive_failures":retained_failures,
                "last_error_code":retained_code,
                "retry_after_ms":retained_retry,
                "updated_at_ms":retained_updated_at,
                "last_failure":historical_failure,
                "restart_count":restart_count,
                "child":retained_child,
            }),
        );
    }
    Ok(json!({"schema_version":1,"workers":retained}))
}

fn valid_optional_worker_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn bounded_last_failure(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "code" | "observed_at_ms" | "host_epoch"))
    {
        return None;
    }
    let code = object
        .get("code")
        .and_then(Value::as_str)
        .filter(|code| valid_optional_worker_code(code))?;
    let observed_at_ms = object
        .get("observed_at_ms")
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)?;
    let host_epoch = match object.get("host_epoch") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_i64().filter(|epoch| *epoch > 0)?),
    };
    Some(json!({
        "code":code,
        "observed_at_ms":observed_at_ms,
        "host_epoch":host_epoch,
    }))
}

fn historical_failure(receipt: &Value) -> Option<Value> {
    if let Some(value) = receipt.get("last_failure").and_then(bounded_last_failure) {
        return Some(value);
    }
    if !matches!(receipt["state"].as_str(), Some("retry_wait" | "isolated")) {
        return None;
    }
    let code = receipt["last_error_code"]
        .as_str()
        .filter(|code| valid_optional_worker_code(code))?;
    let observed_at_ms = receipt["updated_at_ms"]
        .as_i64()
        .filter(|value| *value >= 0)?;
    Some(json!({
        "code":code,
        "observed_at_ms":observed_at_ms,
        "host_epoch":Value::Null,
    }))
}

fn bounded_restart_count(receipt: &Value) -> Option<u64> {
    receipt
        .get("restart_count")
        .and_then(Value::as_u64)
        .filter(|value| *value <= 32)
}

fn bounded_child_health(value: &Value) -> Option<Value> {
    let child: swarm_supervisor::control::SupervisorChildHealth =
        serde_json::from_value(value.clone()).ok()?;
    child.validate().ok()?;
    serde_json::to_value(child).ok()
}

fn restart_child_health(value: Value, state: Option<&str>) -> Option<Value> {
    let mut child: swarm_supervisor::control::SupervisorChildHealth =
        serde_json::from_value(value).ok()?;
    if state == Some("running")
        && matches!(
            child.stop,
            swarm_supervisor::control::SupervisorChildStopState::Running
        )
    {
        child.stop = swarm_supervisor::control::SupervisorChildStopState::Uncertain;
    }
    child.validate().ok()?;
    serde_json::to_value(child).ok()
}

pub(super) fn ready(tx: &Transaction<'_>, now: i64) -> Result<()> {
    let mut current = load(tx)?
        .ok_or_else(|| Error::new("HOST_LIFECYCLE_INVALID", "host startup receipt is missing"))?;
    if current.host_epoch != epoch(tx)? || current.state != State::Starting {
        return Err(Error::new(
            "HOST_LIFECYCLE_INVALID",
            "host startup receipt is not current",
        ));
    }
    current.state = State::Running;
    current.updated_at_ms = now;
    set_meta(tx, CURRENT, &json!(current))
}

/// Retain the latest bounded state for the thirteen optional workers. The
/// Store status reader exposes this through the existing `host.status` path.
/// Error details, process output, and route/native payloads are never stored.
#[expect(
    clippy::too_many_arguments,
    reason = "the existing optional-worker receipt boundary keeps transaction, identity, state, failure metadata, child receipt, and timestamp as separate validated fields"
)]
pub(super) fn update_optional_worker_with_child(
    tx: &Transaction<'_>,
    name: &str,
    state: &str,
    consecutive_failures: u32,
    error_code: Option<&str>,
    retry_in_ms: Option<u64>,
    child_receipt: Option<&Value>,
    now: i64,
) -> Result<()> {
    if !OPTIONAL_WORKERS.contains(&name)
        || !matches!(state, "dormant" | "running" | "retry_wait" | "isolated")
        || consecutive_failures > 32
        || retry_in_ms.is_some_and(|delay| delay > 60_000)
        || error_code.is_some_and(|code| {
            code.is_empty()
                || code.len() > 64
                || !code
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        })
        || (state == "retry_wait" && (error_code.is_none() || retry_in_ms.is_none()))
        || (state == "isolated" && (error_code.is_none() || retry_in_ms.is_none()))
    {
        return Err(Error::new(
            "HOST_LIFECYCLE_INVALID",
            "optional worker health receipt is invalid",
        ));
    }
    let mut health = optional_worker_health(tx)?;
    let workers = health
        .get_mut("workers")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| {
            Error::new(
                "HOST_LIFECYCLE_INVALID",
                "optional worker health map is invalid",
            )
        })?;
    let previous = workers.get(name).cloned();
    let previous_state = previous
        .as_ref()
        .and_then(|value| value["state"].as_str())
        .map(str::to_owned);
    let previous_failure = previous.as_ref().and_then(historical_failure);
    let mut restart_count = previous
        .as_ref()
        .and_then(bounded_restart_count)
        .unwrap_or(0);
    if state == "running"
        && previous_failure.is_some()
        && previous_state.as_deref() != Some("running")
    {
        restart_count = restart_count.saturating_add(1).min(32);
    }
    let last_failure = if let Some(code) = error_code {
        let host_epoch = meta(tx, "host_epoch")?
            .and_then(|value| value.as_i64())
            .filter(|value| *value > 0);
        Some(json!({
            "code":code,
            "observed_at_ms":now,
            "host_epoch":host_epoch,
        }))
    } else {
        previous_failure
    };
    let child_receipt = match child_receipt {
        Some(value) => Some(bounded_child_health(value).ok_or_else(|| {
            Error::new(
                "HOST_LIFECYCLE_INVALID",
                "optional worker child receipt is invalid",
            )
        })?),
        None => previous
            .as_ref()
            .and_then(|value| value.get("child"))
            .and_then(bounded_child_health),
    };
    let retry_after_ms =
        retry_in_ms.map(|delay| now.saturating_add(i64::try_from(delay).unwrap_or(i64::MAX)));
    workers.insert(
        name.to_owned(),
        json!({
            "state":state,
            "consecutive_failures":consecutive_failures,
            "last_error_code":error_code,
            "retry_after_ms":retry_after_ms,
            "updated_at_ms":now,
            "last_failure":last_failure,
            "restart_count":restart_count,
            "child":child_receipt,
        }),
    );
    set_meta(tx, OPTIONAL_WORKER_HEALTH, &health)
}

/// Read only the existing module-supervisor child receipt used to prevent a
/// replacement while a prior process is still live or its departure remains
/// uncertain. No new lifecycle record is created by this helper.
pub(super) fn module_supervisor_health_readback(
    db: &Connection,
) -> Result<swarm_supervisor::control::SupervisorChildHealthReadback> {
    let health = optional_worker_health(db)?;
    let receipt = health
        .get("workers")
        .and_then(Value::as_object)
        .and_then(|workers| workers.get("module-supervisor"));
    let state = receipt
        .and_then(|value| value.get("state"))
        .and_then(Value::as_str)
        .unwrap_or("dormant")
        .to_owned();
    let error_code = receipt
        .and_then(|value| value.get("last_error_code"))
        .map(|value| {
            if value.is_null() {
                Ok(None)
            } else {
                value
                    .as_str()
                    .map(|code| Some(code.to_owned()))
                    .ok_or_else(|| {
                        Error::new(
                            "HOST_LIFECYCLE_INVALID",
                            "module supervisor health error code is invalid",
                        )
                    })
            }
        })
        .transpose()?
        .flatten();
    let child = receipt
        .and_then(|value| value.get("child"))
        .filter(|value| !value.is_null())
        .map(
            |value| -> Result<swarm_supervisor::control::SupervisorChildHealth> {
                let child: swarm_supervisor::control::SupervisorChildHealth =
                    serde_json::from_value(value.clone()).map_err(|_| {
                        Error::new(
                            "HOST_LIFECYCLE_INVALID",
                            "module supervisor child health receipt is invalid",
                        )
                    })?;
                child.validate().map_err(|_| {
                    Error::new(
                        "HOST_LIFECYCLE_INVALID",
                        "module supervisor child health receipt failed validation",
                    )
                })?;
                Ok(child)
            },
        )
        .transpose()?;
    let readback = swarm_supervisor::control::SupervisorChildHealthReadback {
        state,
        error_code,
        child,
    };
    readback.validate().map_err(|_| {
        Error::new(
            "HOST_LIFECYCLE_INVALID",
            "module supervisor health readback is invalid",
        )
    })?;
    Ok(readback)
}

fn optional_worker_health(db: &Connection) -> Result<Value> {
    let Some(value) = meta(db, OPTIONAL_WORKER_HEALTH)? else {
        return Ok(json!({"schema_version":1,"workers":{}}));
    };
    let workers = value
        .get("workers")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            Error::new(
                "HOST_LIFECYCLE_INVALID",
                "optional worker health map is invalid",
            )
        })?;
    if value["schema_version"] != 1 || workers.len() > OPTIONAL_WORKERS.len() {
        return Err(Error::new(
            "HOST_LIFECYCLE_INVALID",
            "optional worker health version or capacity is invalid",
        ));
    }
    for (name, receipt) in workers {
        let state = receipt["state"].as_str().unwrap_or_default();
        let failures = receipt["consecutive_failures"].as_u64();
        let updated_at_ms = receipt["updated_at_ms"].as_i64();
        let retry_after_ms = receipt["retry_after_ms"].as_i64();
        let code = receipt["last_error_code"].as_str();
        if !OPTIONAL_WORKERS.contains(&name.as_str())
            || !matches!(state, "dormant" | "running" | "retry_wait" | "isolated")
            || failures.is_none_or(|value| value > 32)
            || updated_at_ms.is_none_or(|value| value < 0)
            || retry_after_ms.is_some_and(|value| {
                let updated_at_ms = updated_at_ms.unwrap_or(0);
                value < updated_at_ms || value.saturating_sub(updated_at_ms) > 60_000
            })
            || code.is_some_and(|value| !valid_optional_worker_code(value))
            || (matches!(state, "retry_wait" | "isolated")
                && (code.is_none() || retry_after_ms.is_none()))
            || receipt["restart_count"]
                .as_u64()
                .is_some_and(|value| value > 32)
            || (receipt.get("restart_count").is_some()
                && receipt["restart_count"].as_u64().is_none())
            || (receipt
                .get("last_failure")
                .is_some_and(|value| !value.is_null() && bounded_last_failure(value).is_none()))
            || receipt
                .get("child")
                .is_some_and(|value| !value.is_null() && bounded_child_health(value).is_none())
        {
            return Err(Error::new(
                "HOST_LIFECYCLE_INVALID",
                "optional worker health entry is invalid",
            ));
        }
    }
    Ok(value)
}

pub(super) fn finish(
    tx: &Transaction<'_>,
    error_code: Option<&str>,
    secondary_codes: &[String],
    failed_supervisor: Option<&str>,
    now: i64,
) -> Result<()> {
    let mut current = load(tx)?
        .ok_or_else(|| Error::new("HOST_LIFECYCLE_INVALID", "host startup receipt is missing"))?;
    if current.host_epoch != epoch(tx)?
        || !matches!(current.state, State::Starting | State::Running)
    {
        return Err(Error::new(
            "HOST_LIFECYCLE_INVALID",
            "host exit receipt is not current",
        ));
    }
    if !valid_secondary_codes(secondary_codes)
        || (error_code.is_none() && !secondary_codes.is_empty())
        || error_code.is_some_and(|primary| secondary_codes.iter().any(|code| code == primary))
    {
        return Err(Error::new(
            "HOST_LIFECYCLE_INVALID",
            "host exit secondary codes are invalid",
        ));
    }
    // Codes are controller identifiers. Do not persist error messages, paths,
    // process output, connection strings, request bodies or credentials here.
    let error_code = error_code.map(|code| {
        if valid_error_code(code) {
            code.to_owned()
        } else {
            "HOST_FAILED".to_owned()
        }
    });
    let failed_supervisor = match failed_supervisor {
        Some(name) if SUPERVISORS.contains(&name) && error_code.is_some() => Some(name.to_owned()),
        Some(_) => {
            return Err(Error::new(
                "HOST_LIFECYCLE_INVALID",
                "host supervisor receipt is invalid",
            ));
        }
        None => None,
    };
    let failure_category = error_code.as_deref().map(|code| {
        if code == "SUPERVISOR_STOPPED" {
            FailureCategory::SupervisorStopped
        } else if failed_supervisor.is_some() || code == "SUPERVISOR_FAILED" {
            FailureCategory::SupervisorFailed
        } else if current.state == State::Starting {
            FailureCategory::StartupFailure
        } else {
            FailureCategory::RuntimeFailure
        }
    });
    if failure_category.is_some_and(|category| {
        !category.valid_for(error_code.as_deref(), failed_supervisor.is_some())
    }) {
        return Err(Error::new(
            "HOST_LIFECYCLE_INVALID",
            "host failure category is invalid",
        ));
    }
    current.state = if error_code.is_some() {
        State::Failed
    } else {
        State::Stopped
    };
    current.updated_at_ms = now;
    retain_exit(
        tx,
        &Exit {
            schema_version: 1,
            host_epoch: Some(current.host_epoch),
            observed_at_ms: now,
            manager_action_required: error_code.is_some(),
            error_code,
            secondary_codes: secondary_codes.to_vec(),
            failed_supervisor,
            failure_category,
            retry_authorized: false,
        },
        None,
    )?;
    set_meta(tx, CURRENT, &json!(current))
}

fn exit_receipt(db: &Connection, key: &str) -> Result<Option<Value>> {
    meta(db, key)?
        .map(|value| {
            let receipt: Exit = serde_json::from_value(value).map_err(|_| {
                Error::new("HOST_LIFECYCLE_INVALID", "host exit receipt is invalid")
            })?;
            if receipt.schema_version != 1
                || receipt.observed_at_ms < 0
                || receipt.retry_authorized
                || receipt.host_epoch.is_some_and(|epoch| epoch <= 0)
                || receipt
                    .failed_supervisor
                    .as_deref()
                    .is_some_and(|name| !is_known_supervisor(name))
                || (receipt.failed_supervisor.is_some() && receipt.error_code.is_none())
                || (receipt.failure_category.is_some() && receipt.error_code.is_none())
                || receipt.failure_category.is_some_and(|category| {
                    !category.valid_for(
                        receipt.error_code.as_deref(),
                        receipt.failed_supervisor.is_some(),
                    )
                })
                || (receipt.failed_supervisor.is_some() && receipt.failure_category.is_none())
                || receipt
                    .error_code
                    .as_ref()
                    .is_some_and(|code| !valid_error_code(code))
                || !valid_secondary_codes(&receipt.secondary_codes)
                || (receipt.error_code.is_none() && !receipt.secondary_codes.is_empty())
                || receipt.error_code.as_ref().is_some_and(|primary| {
                    receipt.secondary_codes.iter().any(|code| code == primary)
                })
                || receipt.manager_action_required != receipt.error_code.is_some()
            {
                return Err(Error::new(
                    "HOST_LIFECYCLE_INVALID",
                    "host exit receipt is invalid",
                ));
            }
            Ok(json!(receipt))
        })
        .transpose()
}

fn valid_error_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_secondary_codes(codes: &[String]) -> bool {
    codes.len() <= 2
        && codes.iter().all(|code| valid_error_code(code))
        && (codes.len() < 2 || codes[0] != codes[1])
}

fn status_value(value: Result<Option<Value>>) -> Result<Value> {
    match value {
        Ok(value) => Ok(value.unwrap_or(Value::Null)),
        Err(error)
            if matches!(
                error.code.as_str(),
                "HOST_LIFECYCLE_INVALID" | "INVALID_PARAMS"
            ) =>
        {
            Ok(
                json!({"status":"invalid","error_code":"HOST_LIFECYCLE_INVALID","manager_action_required":true,"retry_authorized":false}),
            )
        }
        Err(error) => Err(error),
    }
}

pub(super) fn status(db: &Connection) -> Result<Value> {
    let current = status_value(load(db).map(|value| value.map(|record| json!(record))))?;
    let last_exit = status_value(exit_receipt(db, LAST_EXIT))?;
    let latest_failure = status_value(exit_receipt(db, LATEST_FAILURE))?;
    let optional_workers = optional_worker_health(db)?;
    Ok(json!({
        "current":current,
        "last_exit":last_exit,
        "latest_failure":latest_failure,
        "optional_workers":optional_workers["workers"],
        "failure_history":"retained; a later graceful exit does not acknowledge or erase an earlier failure",
        "required_readback":"operation.get before retrying admitted work"
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_current_is_archived_raw_and_replaced_without_exposure() {
        let mut db = Connection::open_in_memory().expect("open in-memory database");
        db.execute_batch(
            "CREATE TABLE meta(key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
             CREATE TABLE observations(
                 source_stream_id TEXT NOT NULL,
                 source_event_key TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 payload_json TEXT NOT NULL,
                 recorded_at_ms INTEGER NOT NULL
             );",
        )
        .expect("create lifecycle tables");

        let secret_marker = "host-lifecycle-secret-marker";
        let raw = format!(
            "{{\"schema_version\":1,\"state\":\"running\",\"diagnostic\":\"{secret_marker}\""
        );
        set_meta(&db, "host_epoch", &json!(7)).expect("set current epoch");
        db.execute(
            "INSERT INTO meta(key, value_json) VALUES(?1, ?2)",
            params![CURRENT, raw],
        )
        .expect("insert malformed lifecycle fact");

        let tx = db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .expect("begin recovery transaction");
        start(&tx, 1_000).expect("recover malformed optional lifecycle fact");
        tx.commit().expect("commit recovery transaction");

        let current: Lifecycle = serde_json::from_value(
            meta(&db, CURRENT)
                .expect("read current lifecycle")
                .expect("new current lifecycle exists"),
        )
        .expect("new current lifecycle is valid");
        assert_eq!(current.host_epoch, 7);
        assert!(matches!(current.state, State::Starting));

        let archived = meta(&db, "host:invalid-lifecycle:v1")
            .expect("read malformed fact archive")
            .expect("malformed fact was archived");
        assert_eq!(archived["raw_value_json"].as_str(), Some(raw.as_str()));

        let last_exit = meta(&db, LAST_EXIT)
            .expect("read lifecycle diagnosis")
            .expect("lifecycle diagnosis exists");
        assert_eq!(
            last_exit["error_code"].as_str(),
            Some("HOST_LIFECYCLE_INVALID")
        );

        let public_status = status(&db).expect("public lifecycle status remains readable");
        let public_status = serde_json::to_string(&public_status).expect("serialize status");
        assert!(!public_status.contains(secret_marker));
        assert!(public_status.len() < 1_024);
    }
}
