//! Host-only, status-only delivery for module supervisor failures.
//!
//! This file is an additive candidate for `src/store`. The callback accepts a
//! serializable supervisor DTO without adding a Store RPC or a Cargo dependency
//! on the supervisor crate. The Store reparses it into this closed schema,
//! validates it against the binding's admission-time selector, and appends it
//! to the existing observations journal. It never changes an Operation.

use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use swarm_telemetry::{Code, Kind, Phase, Record, Severity};

impl super::Store {
    /// Retain a bounded host-only status transition from the dedicated module
    /// supervisor. This has no RPC or bearer credential path and cannot
    /// settle or requeue an Operation.
    pub(crate) async fn record_module_supervisor_observation<T: Serialize>(
        &self,
        observation: T,
    ) -> Result<()> {
        let observation = parse(observation)?;
        let committed = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let committed = record(&tx, &observation, model::now_ms()?)?;
                tx.commit()?;
                Ok(committed)
            })
            .await?;
        if committed.inserted {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
            if let Some(diagnostic) = committed.diagnostic {
                // Diagnostics are explicitly lossy. Store commit failures
                // still return above, and recorder loss cannot revise a
                // durable observation or its operation authority.
                let _ = self.telemetry.emit(diagnostic);
            }
        }
        Ok(())
    }
}

struct ObservationCommit {
    inserted: bool,
    diagnostic: Option<Record>,
}

const MAX_OBSERVATION_IDS: usize = 256;
const MAX_UNKNOWN_OPERATION_COUNT: u64 = 1_000_000;
const MAX_RECORD_BYTES: usize = 64 * 1024;
const STREAM_PREFIX: &str = "controller:module-supervisor";
const EVENT_KIND: &str = "module.supervisor_observation";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ModuleSupervisorObservation {
    schema_version: u16,
    actor_instance_id: String,
    event_id: String,
    sequence: u64,
    module_id: String,
    artifact_id: String,
    artifact_version: String,
    build_id: Option<String>,
    scope: ServiceScope,
    boot_id: Option<String>,
    phase: ModuleSupervisorPhase,
    effect_certainty: ModuleEffectCertainty,
    stage: Option<ModuleFailureStage>,
    error_code: Option<String>,
    unknown_operation_ids: Vec<String>,
    unknown_operation_count: usize,
    unknown_operation_ids_truncated: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceScope {
    binding_id: String,
    generation: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ModuleSupervisorPhase {
    WaitingForDemand,
    WaitingForKernel,
    Starting,
    Ready,
    RestartBackoff,
    Exited,
    OwnerRetained,
    IdentityUnknown,
    Completed,
    Isolated,
}

impl ModuleSupervisorPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForDemand => "waiting_for_demand",
            Self::WaitingForKernel => "waiting_for_kernel",
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::RestartBackoff => "restart_backoff",
            Self::Exited => "exited",
            Self::OwnerRetained => "owner_retained",
            Self::IdentityUnknown => "identity_unknown",
            Self::Completed => "completed",
            Self::Isolated => "isolated",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ModuleEffectCertainty {
    Unknown,
    NotStarted,
}

impl ModuleEffectCertainty {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::NotStarted => "not_started",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ModuleFailureStage {
    ResolveRefs,
    ValidateLaunch,
    Spawn,
    Worker,
    Owner,
    Store,
    Journal,
}

impl ModuleFailureStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::ResolveRefs => "resolve_refs",
            Self::ValidateLaunch => "validate_launch",
            Self::Spawn => "spawn",
            Self::Worker => "worker",
            Self::Owner => "owner",
            Self::Store => "store",
            Self::Journal => "journal",
        }
    }

    fn proves_pre_spawn(self) -> bool {
        matches!(self, Self::ResolveRefs | Self::ValidateLaunch)
    }
}

/// Parse a typed host DTO through the exact serialized contract. The
/// serializable input is consumed before any await, so callers need no extra
/// Store credential and the root crate need not depend on `swarm-supervisor`.
pub(super) fn parse<T: Serialize>(value: T) -> Result<ModuleSupervisorObservation> {
    let value = serde_json::to_value(value)
        .map_err(|_| invalid("module supervisor observation cannot be serialized"))?;
    let Some(object) = value.as_object() else {
        return Err(invalid("module supervisor observation must be an object"));
    };
    for field in ["build_id", "boot_id", "stage", "error_code"] {
        if !object.contains_key(field) {
            return Err(invalid(
                "module supervisor observation omitted a required nullable field",
            ));
        }
    }
    let observation: ModuleSupervisorObservation = serde_json::from_value(value)
        .map_err(|_| invalid("module supervisor observation has an unsupported shape"))?;
    validate_header(&observation)?;
    Ok(observation)
}

/// Append an immutable transition and update the binding's safe latest
/// readback in one Store transaction. Exact callback retries return an
/// `ObservationCommit` with `inserted: false` and no diagnostic candidate.
pub(super) fn record(
    tx: &Transaction<'_>,
    observation: &ModuleSupervisorObservation,
    now_ms: i64,
) -> Result<ObservationCommit> {
    validate_header(observation)?;
    let generation = i64::try_from(observation.scope.generation)
        .map_err(|_| invalid("module supervisor generation exceeds the Store range"))?;
    let binding = super::operations::get_binding(tx, &observation.scope.binding_id, generation)?;
    validate_binding_identity(&binding, observation)?;

    let source_stream_id = format!(
        "{STREAM_PREFIX}:{}:{generation}",
        observation.scope.binding_id
    );
    let payload = payload(observation, generation);
    let payload_json = model::canonical(&payload)?;
    if payload_json.len() > MAX_RECORD_BYTES {
        return Err(invalid(
            "module supervisor observation exceeds its record bound",
        ));
    }

    let existing: Option<(String, Option<String>, Option<i64>, String)> = tx
        .query_row(
            "SELECT payload_json,binding_id,binding_generation,kind FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2",
            params![source_stream_id, observation.event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    if let Some((old_payload, old_binding, old_generation, old_kind)) = existing {
        if old_payload == payload_json
            && old_binding.as_deref() == Some(observation.scope.binding_id.as_str())
            && old_generation == Some(generation)
            && old_kind == EVENT_KIND
        {
            return Ok(ObservationCommit {
                inserted: false,
                diagnostic: None,
            });
        }
        return Err(Error::new(
            "MODULE_OBSERVATION_CONFLICT",
            "module supervisor event identity was reused with different retained facts",
        ));
    }

    let current = &binding["observation"]["module_supervisor"];
    if current["actor_instance_id"].as_str() == Some(observation.actor_instance_id.as_str())
        && current["sequence"]
            .as_u64()
            .is_some_and(|sequence| sequence >= observation.sequence)
    {
        return Err(Error::new(
            "MODULE_OBSERVATION_STALE",
            "module supervisor callback sequence is older than the retained binding readback",
        ));
    }
    let diagnostic = diagnostic_for_transition(current, observation);
    validate_operation_ids(tx, observation, generation)?;

    tx.execute(
        "INSERT INTO observations(\
             source_stream_id,source_event_key,binding_id,binding_generation,\
             operation_id,kind,payload_json,recorded_at_ms\
         ) VALUES(?1,?2,?3,?4,NULL,?5,?6,?7)",
        params![
            source_stream_id,
            observation.event_id,
            observation.scope.binding_id,
            generation,
            EVENT_KIND,
            payload_json,
            now_ms,
        ],
    )?;

    let mut latest = payload;
    latest["recorded_at_ms"] = json!(now_ms);
    let updated = tx.execute(
        "UPDATE bindings SET state_json=json_set(state_json,'$.module_supervisor',json(?3)) \
         WHERE binding_id=?1 AND generation=?2",
        params![
            observation.scope.binding_id,
            generation,
            model::canonical(&latest)?,
        ],
    )?;
    if updated != 1 {
        return Err(Error::new(
            "MODULE_OBSERVATION_SCOPE_MISSING",
            "module supervisor binding disappeared during event recording",
        ));
    }
    Ok(ObservationCommit {
        inserted: true,
        diagnostic,
    })
}

/// Map only exact lifecycle transitions into the existing closed diagnostic
/// vocabulary. Status is not an event acknowledgement: it becomes visible
/// only after the enclosing Store transaction commits.
fn diagnostic_for_transition(
    previous: &Value,
    observation: &ModuleSupervisorObservation,
) -> Option<Record> {
    let boot_id = observation.boot_id.as_deref()?;
    let same_boot = previous["schema_version"] == 1
        && previous["binding_id"].as_str() == Some(observation.scope.binding_id.as_str())
        && previous["generation"].as_u64() == Some(observation.scope.generation)
        && previous["module_id"].as_str() == Some(observation.module_id.as_str())
        && previous["artifact_id"].as_str() == Some(observation.artifact_id.as_str())
        && previous["artifact_version"].as_str() == Some(observation.artifact_version.as_str())
        && previous["boot_id"].as_str() == Some(boot_id);
    let previous_phase = if same_boot {
        previous["phase"].as_str()
    } else {
        None
    };
    if same_boot && previous_phase.is_none() {
        return None;
    }

    let (severity, kind, phase, code) = match observation.phase {
        ModuleSupervisorPhase::Ready if !same_boot || previous_phase == Some("starting") => (
            Severity::Info,
            Kind::ModuleStarted,
            Phase::ModuleStart,
            None,
        ),
        ModuleSupervisorPhase::Exited
        | ModuleSupervisorPhase::RestartBackoff
        | ModuleSupervisorPhase::Completed
            if matches!(
                observation.effect_certainty,
                ModuleEffectCertainty::NotStarted
            ) && observation
                .stage
                .is_some_and(ModuleFailureStage::proves_pre_spawn)
                && previous_phase != Some("ready") =>
        {
            (
                Severity::Error,
                Kind::ModuleStopped,
                Phase::ModuleStart,
                Some(Code::ModuleStartFailed),
            )
        }
        ModuleSupervisorPhase::Exited | ModuleSupervisorPhase::RestartBackoff
            if same_boot && previous_phase == Some("ready") =>
        {
            (Severity::Warn, Kind::ModuleStopped, Phase::ModuleExit, None)
        }
        ModuleSupervisorPhase::Completed if same_boot && previous_phase == Some("ready") => {
            (Severity::Info, Kind::ModuleStopped, Phase::ModuleExit, None)
        }
        _ => return None,
    };
    let operation_id = (observation.unknown_operation_count == 1
        && observation.unknown_operation_ids.len() == 1
        && !observation.unknown_operation_ids_truncated)
        .then(|| observation.unknown_operation_ids[0].as_str());
    Some(
        Record::new(severity, kind, phase)
            .with_code(code)
            .with_binding_id(Some(&observation.scope.binding_id))
            .with_binding_generation(Some(observation.scope.generation))
            .with_operation_id(operation_id)
            .with_module_boot_id(Some(boot_id)),
    )
}

fn validate_header(observation: &ModuleSupervisorObservation) -> Result<()> {
    if observation.schema_version != 1
        || observation.scope.generation == 0
        || observation.sequence == 0
        || observation.sequence > i64::MAX as u64
        || observation.unknown_operation_ids.len() > MAX_OBSERVATION_IDS
        || observation.unknown_operation_count > MAX_UNKNOWN_OPERATION_COUNT as usize
    {
        return Err(invalid(
            "module supervisor observation is outside its numeric bounds",
        ));
    }
    valid_token(&observation.scope.binding_id)?;
    valid_token(&observation.actor_instance_id)?;
    valid_atom(&observation.module_id, false)?;
    valid_atom(&observation.artifact_id, false)?;
    valid_version(&observation.artifact_version)?;
    if let Some(build_id) = observation.build_id.as_deref() {
        valid_atom(build_id, true)?;
    }
    if let Some(boot_id) = observation.boot_id.as_deref() {
        valid_token(boot_id)?;
    }
    let expected_event_id = match observation.boot_id.as_deref() {
        Some(boot_id) => format!(
            "{boot_id}:{}:{}",
            observation.actor_instance_id, observation.sequence
        ),
        None => format!("{}:{}", observation.actor_instance_id, observation.sequence),
    };
    if observation.event_id != expected_event_id || observation.event_id.len() > 256 {
        return Err(invalid(
            "module supervisor event ID does not match its actor, boot, and sequence",
        ));
    }
    if let Some(error_code) = observation.error_code.as_deref()
        && (error_code.is_empty()
            || error_code.len() > 128
            || !error_code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'))
    {
        return Err(invalid(
            "module supervisor error code is outside its safe format",
        ));
    }
    if observation.stage.is_some() && observation.error_code.is_none() {
        // A stage with no bounded error code is ambiguous diagnostic text,
        // not a manager-readable failure fact.
        return Err(invalid(
            "module supervisor failure stage is missing an error code",
        ));
    }
    if matches!(
        observation.effect_certainty,
        ModuleEffectCertainty::NotStarted
    ) && (!observation
        .stage
        .is_some_and(ModuleFailureStage::proves_pre_spawn)
        || observation.error_code.is_none())
    {
        return Err(Error::new(
            "MODULE_OBSERVATION_CERTAINTY_INVALID",
            "not_started requires the helper's bounded pre-spawn stage and error code",
        ));
    }
    if observation.unknown_operation_count < observation.unknown_operation_ids.len()
        || observation.unknown_operation_ids_truncated
            != (observation.unknown_operation_count > observation.unknown_operation_ids.len())
    {
        return Err(invalid(
            "module supervisor unresolved Operation count is inconsistent",
        ));
    }
    let mut previous: Option<&str> = None;
    for operation_id in &observation.unknown_operation_ids {
        valid_token(operation_id)?;
        if previous.is_some_and(|prior| prior >= operation_id.as_str()) {
            return Err(invalid(
                "module supervisor Operation IDs must be sorted and unique",
            ));
        }
        previous = Some(operation_id);
    }
    Ok(())
}

fn validate_binding_identity(
    binding: &Value,
    observation: &ModuleSupervisorObservation,
) -> Result<()> {
    let selector = binding["observation"].get("module_contract_selector");
    let Some(selector) = selector.filter(|selector| selector.is_object()) else {
        return Err(Error::new(
            "MODULE_DESCRIPTOR_MISSING",
            "module supervisor observation has no admission-time descriptor selector",
        ));
    };
    let artifact = &selector["artifact"];
    let binding_generation = binding["generation"].as_i64();
    let selector_build_id = match artifact.get("build_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        _ => {
            return Err(Error::new(
                "MODULE_ROUTE_CORRUPT",
                "retained artifact build identity is malformed",
            ));
        }
    };
    let registered_revision = selector["registered_revision"].as_u64();
    let selected_revision = selector["selected_revision"].as_u64();
    if selector["schema_version"] != 1
        || registered_revision.is_none_or(|revision| revision == 0)
        || selected_revision.is_none_or(|revision| revision < registered_revision.unwrap_or(0))
        || binding["binding_id"].as_str() != Some(observation.scope.binding_id.as_str())
        || binding_generation != i64::try_from(observation.scope.generation).ok()
        || binding["module_artifact_id"].as_str() != Some(observation.artifact_id.as_str())
        || binding["route"]["runtime"].as_str() != Some(observation.module_id.as_str())
        || selector["module_id"].as_str() != Some(observation.module_id.as_str())
        || artifact["artifact_id"].as_str() != Some(observation.artifact_id.as_str())
        || artifact["version"].as_str() != Some(observation.artifact_version.as_str())
        || selector_build_id != observation.build_id.as_deref()
    {
        return Err(Error::new(
            "MODULE_OBSERVATION_IDENTITY_MISMATCH",
            "module supervisor observation differs from the binding's retained artifact identity",
        ));
    }
    Ok(())
}

fn validate_operation_ids(
    tx: &Transaction<'_>,
    observation: &ModuleSupervisorObservation,
    generation: i64,
) -> Result<()> {
    for operation_id in &observation.unknown_operation_ids {
        let state: Option<String> = tx
            .query_row(
                "SELECT state FROM operations WHERE operation_id=?1 AND binding_id=?2 \
                 AND binding_generation=?3",
                params![operation_id, observation.scope.binding_id, generation],
                |row| row.get(0),
            )
            .optional()?;
        let Some(state) = state else {
            return Err(Error::new(
                "MODULE_OBSERVATION_OPERATION_SCOPE",
                "listed Operation is absent or outside the exact binding generation",
            ));
        };
        if !matches!(
            state.as_str(),
            "sending" | "native_accepted" | "outcome_unknown"
        ) {
            return Err(Error::new(
                "MODULE_OBSERVATION_OPERATION_TERMINAL",
                "listed Operation in the exact binding generation is no longer pending",
            ));
        }
    }
    Ok(())
}

fn payload(observation: &ModuleSupervisorObservation, generation: i64) -> Value {
    json!({
        "schema_version": 1,
        "actor_instance_id": observation.actor_instance_id,
        "event_id": observation.event_id,
        "sequence": observation.sequence,
        "module_id": observation.module_id,
        "artifact_id": observation.artifact_id,
        "artifact_version": observation.artifact_version,
        "build_id": observation.build_id,
        "binding_id": observation.scope.binding_id,
        "generation": generation,
        "boot_id": observation.boot_id,
        "phase": observation.phase.as_str(),
        "effect_certainty": observation.effect_certainty.as_str(),
        "certainty_scope": "latest_helper_attempt_only",
        "stage": observation.stage.map(ModuleFailureStage::as_str),
        "error_code": observation.error_code,
        "unknown_operation_ids": observation.unknown_operation_ids,
        "unknown_operation_count": observation.unknown_operation_count,
        "unknown_operation_ids_truncated": observation.unknown_operation_ids_truncated,
        "retry_authorized": false,
    })
}

/// Narrow public projection for the existing `agent.state` binding read.
/// Unknown fields are never copied from database JSON into an API response.
pub(super) fn public_projection(value: &Value) -> Value {
    if value["schema_version"] != 1
        || value["sequence"]
            .as_u64()
            .is_none_or(|sequence| sequence == 0)
        || value["recorded_at_ms"]
            .as_i64()
            .is_none_or(|time| time <= 0)
        || value["generation"]
            .as_i64()
            .is_none_or(|generation| generation <= 0)
        || value["certainty_scope"] != "latest_helper_attempt_only"
        || value["retry_authorized"] != false
        || !value["event_id"].as_str().is_some_and(|id| id.len() <= 256)
        || !value["actor_instance_id"]
            .as_str()
            .is_some_and(|id| valid_token(id).is_ok())
        || !value["module_id"]
            .as_str()
            .is_some_and(|id| valid_atom(id, false).is_ok())
        || !value["artifact_id"]
            .as_str()
            .is_some_and(|id| valid_atom(id, false).is_ok())
        || !value["artifact_version"]
            .as_str()
            .is_some_and(|version| valid_version(version).is_ok())
    {
        return Value::Null;
    }
    let actor_instance_id = value["actor_instance_id"].as_str().unwrap_or_default();
    let sequence = value["sequence"].as_u64().unwrap_or_default();
    let binding_id = value["binding_id"].as_str().unwrap_or_default();
    if valid_token(binding_id).is_err() {
        return Value::Null;
    }
    let boot_id = match value.get("boot_id") {
        Some(Value::Null) => None,
        Some(Value::String(id)) if valid_token(id).is_ok() => Some(id.as_str()),
        _ => return Value::Null,
    };
    let expected_event_id = match boot_id {
        Some(boot_id) => format!("{boot_id}:{actor_instance_id}:{sequence}"),
        None => format!("{actor_instance_id}:{sequence}"),
    };
    if value["event_id"].as_str() != Some(expected_event_id.as_str()) {
        return Value::Null;
    }
    match value.get("build_id") {
        Some(Value::Null) => {}
        Some(Value::String(id)) if valid_atom(id, true).is_ok() => {}
        _ => return Value::Null,
    }
    let phase = match value["phase"].as_str() {
        Some(
            "waiting_for_demand" | "waiting_for_kernel" | "starting" | "ready" | "restart_backoff"
            | "exited" | "owner_retained" | "identity_unknown" | "completed" | "isolated",
        ) => value["phase"].as_str().unwrap_or_default(),
        _ => return Value::Null,
    };
    let certainty = match value["effect_certainty"].as_str() {
        Some("unknown" | "not_started") => value["effect_certainty"].as_str().unwrap_or_default(),
        _ => return Value::Null,
    };
    let stage = match value.get("stage") {
        Some(Value::Null) => None,
        Some(Value::String(stage))
            if matches!(
                stage.as_str(),
                "resolve_refs"
                    | "validate_launch"
                    | "spawn"
                    | "worker"
                    | "owner"
                    | "store"
                    | "journal"
            ) =>
        {
            Some(stage.as_str())
        }
        _ => return Value::Null,
    };
    let error_code = match value.get("error_code") {
        Some(Value::Null) => None,
        Some(Value::String(code))
            if !code.is_empty()
                && code.len() <= 128
                && code.bytes().all(|byte| {
                    byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                }) =>
        {
            Some(code.as_str())
        }
        _ => return Value::Null,
    };
    if stage.is_some() && error_code.is_none()
        || certainty == "not_started"
            && !stage.is_some_and(|stage| matches!(stage, "resolve_refs" | "validate_launch"))
    {
        return Value::Null;
    }
    let Some(ids) = value["unknown_operation_ids"].as_array() else {
        return Value::Null;
    };
    if ids.len() > MAX_OBSERVATION_IDS {
        return Value::Null;
    }
    let mut previous: Option<&str> = None;
    for id in ids {
        let Some(id) = id.as_str().filter(|id| valid_token(id).is_ok()) else {
            return Value::Null;
        };
        if previous.is_some_and(|prior| prior >= id) {
            return Value::Null;
        }
        previous = Some(id);
    }
    let count = value["unknown_operation_count"].as_u64();
    let truncated = value["unknown_operation_ids_truncated"].as_bool();
    if count.is_none_or(|count| count < ids.len() as u64 || count > MAX_UNKNOWN_OPERATION_COUNT)
        || truncated.is_none_or(|truncated| truncated != (count.unwrap_or(0) > ids.len() as u64))
    {
        return Value::Null;
    }
    let build_id = value.get("build_id").and_then(Value::as_str);
    json!({
        "schema_version":1,
        "actor_instance_id":actor_instance_id,
        "event_id":value["event_id"],
        "sequence":value["sequence"],
        "module_id":value["module_id"],
        "artifact_id":value["artifact_id"],
        "artifact_version":value["artifact_version"],
        "build_id":build_id,
        "binding_id":binding_id,
        "generation":value["generation"],
        "boot_id":boot_id,
        "phase":phase,
        "effect_certainty":certainty,
        "certainty_scope":"latest_helper_attempt_only",
        "stage":stage,
        "error_code":error_code,
        "unknown_operation_ids":ids,
        "unknown_operation_count":count,
        "unknown_operation_ids_truncated":truncated,
        "retry_authorized":false,
        "recorded_at_ms":value["recorded_at_ms"],
    })
}

/// Manager-only presentation through the existing `report.attention`
/// projection. The suggested read is `agent.state`; this status never
/// authorizes an Operation retry or claims native absence.
pub(super) fn manager_attention_item(
    value: &Value,
    scope_key: &str,
    binding_id: &str,
    generation: i64,
    now_ms: i64,
    stale_after_ms: i64,
) -> Option<Value> {
    let observation = public_projection(value);
    if observation.is_null()
        || observation["binding_id"].as_str() != Some(binding_id)
        || observation["generation"].as_i64() != Some(generation)
    {
        return None;
    }
    let phase = observation["phase"].as_str()?;
    let observed_at_ms = observation["recorded_at_ms"].as_i64()?;
    let stale = now_ms.saturating_sub(observed_at_ms) > stale_after_ms;
    let error_code = observation["error_code"].as_str();
    let stage = observation["stage"].as_str();
    let unresolved = observation["unknown_operation_count"].as_u64().unwrap_or(0) > 0;
    let degraded_phase = matches!(
        phase,
        "waiting_for_kernel"
            | "restart_backoff"
            | "exited"
            | "owner_retained"
            | "identity_unknown"
            | "isolated"
    );
    let failed_start = phase == "starting" && (error_code.is_some() || stage.is_some());
    if !(unresolved || degraded_phase || failed_start) {
        return None;
    }
    let address = json!({
        "binding_id":binding_id,
        "generation":generation,
        "module_id":observation["module_id"],
        "artifact_id":observation["artifact_id"],
        "artifact_version":observation["artifact_version"],
        "event_id":observation["event_id"],
        "phase":phase,
        "effect_certainty":observation["effect_certainty"],
        "certainty_scope":"latest_helper_attempt_only",
        "stage":observation["stage"],
        "error_code":observation["error_code"],
        "unknown_operation_ids":observation["unknown_operation_ids"],
        "unknown_operation_count":observation["unknown_operation_count"],
        "unknown_operation_ids_truncated":observation["unknown_operation_ids_truncated"],
        "retry_authorized":false,
        "next_step":"read this binding and its listed unresolved Operations; if IDs are truncated, enumerate this generation's Operations; do not replay unresolved native work",
    });
    Some(json!({
        "kind":"module_supervisor_failure",
        "scope_key":scope_key,
        "binding_id":binding_id,
        "generation":generation,
        "address":address,
        "source":{"kind":"module_supervisor","observed_at_ms":observed_at_ms,"stale":stale},
        "suggested_action":{"method":"agent.state","binding_id":binding_id,"generation":generation},
        "manager_actionable":true,
    }))
}

fn valid_token(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(invalid("module supervisor token is invalid"));
    }
    Ok(())
}

fn valid_atom(value: &str, build_id: bool) -> Result<()> {
    let extra = if build_id {
        b"+".as_slice()
    } else {
        b"".as_slice()
    };
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || b"._:-".contains(&byte) || extra.contains(&byte)
        })
    {
        return Err(invalid("module supervisor artifact identity is invalid"));
    }
    Ok(())
}

fn valid_version(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".+_-".contains(&byte))
    {
        return Err(invalid("module supervisor artifact version is invalid"));
    }
    Ok(())
}

fn invalid(message: &'static str) -> Error {
    Error::new("MODULE_OBSERVATION_INVALID", message)
}
