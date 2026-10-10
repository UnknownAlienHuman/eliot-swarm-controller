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
use swarm_telemetry::{Code, Component, Kind, Phase, Record, Severity};

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

pub(super) struct ObservationCommit {
    inserted: bool,
    diagnostic: Option<Record>,
}

const MAX_OBSERVATION_IDS: usize = 256;
const MAX_UNKNOWN_OPERATION_COUNT: u64 = 1_000_000;
const MAX_RECORD_BYTES: usize = 64 * 1024;
const STREAM_PREFIX: &str = "controller:module-supervisor";
const EVENT_KIND: &str = "module.supervisor_observation";
const READY_EVENT_KIND: &str = "module.ready";
const START_FAILURE_EVENT_KIND: &str = "module.start_failed";
const FAMILY_EXIT_EVENT_KIND: &str = "module.family_exited";
const RECOVERY_BLOCKED_EVENT_KIND: &str = "module.recovery_blocked";
const IDENTITY_UNKNOWN_EVENT_KIND: &str = "module.identity_unknown";
const OWNER_RETAINED_EVENT_KIND: &str = "module.owner_retained";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleTransition {
    Ready,
    CertifiedNotStarted,
    FamilyExited,
}

impl LifecycleTransition {
    fn event_kind(self) -> &'static str {
        match self {
            Self::Ready => READY_EVENT_KIND,
            Self::CertifiedNotStarted => START_FAILURE_EVENT_KIND,
            Self::FamilyExited => FAMILY_EXIT_EVENT_KIND,
        }
    }

    fn occurrence_phase(self) -> &'static str {
        match self {
            Self::Ready => "module_ready",
            Self::CertifiedNotStarted => "module_start_failure_not_started",
            Self::FamilyExited => "module_family_exit",
        }
    }

    fn proof(self) -> &'static str {
        match self {
            Self::Ready => "authenticated_module_hello",
            Self::CertifiedNotStarted => "certified_pre_spawn_failure",
            Self::FamilyExited => "proven_owner_family_departure",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NonterminalTransition {
    RecoveryBlocked,
    IdentityUnknown,
    OwnerRetained,
}

impl NonterminalTransition {
    fn event_kind(self) -> &'static str {
        match self {
            Self::RecoveryBlocked => RECOVERY_BLOCKED_EVENT_KIND,
            Self::IdentityUnknown => IDENTITY_UNKNOWN_EVENT_KIND,
            Self::OwnerRetained => OWNER_RETAINED_EVENT_KIND,
        }
    }

    fn proof(self) -> &'static str {
        match self {
            Self::RecoveryBlocked => "scoped_recovery_block",
            Self::IdentityUnknown => "process_identity_unproven",
            Self::OwnerRetained => "owner_family_retained",
        }
    }
}

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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModuleLifecycleTrigger {
    schema_version: u16,
    event_kind: String,
    occurrence_phase: String,
    occurrence_id: String,
    proof: String,
    module_id: String,
    binding_id: String,
    generation: i64,
    operation_id: Option<String>,
    boot_id: String,
    event_id: String,
    actor_instance_id: String,
    sequence: u64,
    phase: String,
    effect_certainty: String,
    stage: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModuleNonterminalTrigger {
    schema_version: u16,
    event_kind: String,
    occurrence_phase: String,
    occurrence_id: String,
    proof: String,
    module_id: String,
    binding_id: String,
    generation: i64,
    operation_id: Option<String>,
    boot_id: Option<String>,
    event_id: String,
    actor_instance_id: String,
    sequence: u64,
    phase: String,
    effect_certainty: String,
    stage: Option<String>,
    error_code: Option<String>,
    unknown_operation_count: usize,
    unknown_operation_ids_truncated: bool,
}

pub(super) struct VerifiedModuleLifecycleEvent {
    pub(super) binding_id: String,
    pub(super) generation: i64,
    occurrence_phase: String,
    occurrence_id: String,
    error_code: Option<String>,
}

type LifecycleOriginalRow = (
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    i64,
    i64,
);

type NonterminalOriginalRow = (
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    i64,
    i64,
);

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ModuleSupervisorPhase {
    WaitingForDemand,
    WaitingForKernel,
    Starting,
    Ready,
    RestartBackoff,
    Exited,
    ExitedProven,
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
            Self::ExitedProven => "exited_proven",
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

/// The journal's closed, flat status projection predates the producer DTO's
/// nested scope. Decode that retained format explicitly without changing any
/// historical payload or accepting arbitrary projection metadata.
fn parse_retained_observation(raw: &str) -> Result<ModuleSupervisorObservation> {
    let mut value: Value = serde_json::from_str(raw)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid("retained module observation must be an object"))?;
    if object.remove("certainty_scope") != Some(json!("latest_helper_attempt_only"))
        || object.remove("retry_authorized") != Some(json!(false))
    {
        return Err(invalid(
            "retained module observation has invalid authority metadata",
        ));
    }
    let binding_id = object
        .remove("binding_id")
        .ok_or_else(|| invalid("retained module observation omitted binding identity"))?;
    let generation = object
        .remove("generation")
        .ok_or_else(|| invalid("retained module observation omitted generation"))?;
    if object
        .insert(
            "scope".to_owned(),
            json!({"binding_id":binding_id,"generation":generation}),
        )
        .is_some()
    {
        return Err(invalid(
            "retained module observation contains conflicting scope",
        ));
    }
    parse(value)
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
    let transition = lifecycle_transition(current, observation);
    let nonterminal = nonterminal_transition(observation);
    let operation_link = validate_operation_ids(tx, observation, generation)?;
    let diagnostic = diagnostic_for_transition(current, observation, operation_link.as_ref());

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

    if let Some(transition) = transition {
        record_lifecycle_trigger(
            tx,
            &source_stream_id,
            observation,
            generation,
            transition,
            operation_link.as_ref(),
            now_ms,
        )?;
    }
    if let Some(transition) = nonterminal {
        record_nonterminal_trigger(
            tx,
            &source_stream_id,
            observation,
            generation,
            transition,
            now_ms,
        )?;
    }
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

fn lifecycle_transition(
    previous: &Value,
    observation: &ModuleSupervisorObservation,
) -> Option<LifecycleTransition> {
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

    match observation.phase {
        ModuleSupervisorPhase::Ready if !same_boot || previous_phase == Some("starting") => {
            Some(LifecycleTransition::Ready)
        }
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
            Some(LifecycleTransition::CertifiedNotStarted)
        }
        ModuleSupervisorPhase::ExitedProven
            if same_boot
                && previous_phase == Some("ready")
                && matches!(observation.effect_certainty, ModuleEffectCertainty::Unknown)
                && observation.stage.is_none() =>
        {
            Some(LifecycleTransition::FamilyExited)
        }
        ModuleSupervisorPhase::Completed
            if same_boot
                && previous_phase == Some("ready")
                && matches!(observation.effect_certainty, ModuleEffectCertainty::Unknown)
                && observation.stage.is_none() =>
        {
            Some(LifecycleTransition::FamilyExited)
        }
        _ => None,
    }
}

/// Select exact nonterminal supervisor facts only. These events describe
/// blocked recovery or uncertain identity/ownership; none claims process
/// departure, native failure, or Task completion.
fn nonterminal_transition(
    observation: &ModuleSupervisorObservation,
) -> Option<NonterminalTransition> {
    match observation.phase {
        ModuleSupervisorPhase::WaitingForKernel | ModuleSupervisorPhase::Isolated => {
            Some(NonterminalTransition::RecoveryBlocked)
        }
        ModuleSupervisorPhase::Starting if observation.error_code.is_some() => {
            Some(NonterminalTransition::RecoveryBlocked)
        }
        ModuleSupervisorPhase::IdentityUnknown => Some(NonterminalTransition::IdentityUnknown),
        ModuleSupervisorPhase::OwnerRetained => Some(NonterminalTransition::OwnerRetained),
        _ => None,
    }
}

fn record_nonterminal_trigger(
    tx: &Transaction<'_>,
    source_stream_id: &str,
    observation: &ModuleSupervisorObservation,
    generation: i64,
    transition: NonterminalTransition,
    now_ms: i64,
) -> Result<()> {
    let event_kind = transition.event_kind();
    let source_event_key = format!("nonterminal:{event_kind}:{}", observation.event_id);
    let occurrence_id = nonterminal_occurrence_id(
        &observation.scope.binding_id,
        generation,
        observation.boot_id.as_deref(),
        &observation.event_id,
        event_kind,
    )?;
    let operation_id = (observation.unknown_operation_count == 1
        && observation.unknown_operation_ids.len() == 1
        && !observation.unknown_operation_ids_truncated)
        .then(|| observation.unknown_operation_ids[0].as_str());
    let payload = json!({
        "schema_version":1,
        "event_kind":event_kind,
        "occurrence_phase":"module_nonterminal_attention",
        "occurrence_id":occurrence_id,
        "proof":transition.proof(),
        "module_id":observation.module_id,
        "binding_id":observation.scope.binding_id,
        "generation":generation,
        "operation_id":operation_id,
        "boot_id":observation.boot_id,
        "event_id":observation.event_id,
        "actor_instance_id":observation.actor_instance_id,
        "sequence":observation.sequence,
        "phase":observation.phase.as_str(),
        "effect_certainty":observation.effect_certainty.as_str(),
        "stage":observation.stage.map(ModuleFailureStage::as_str),
        "error_code":observation.error_code,
        "unknown_operation_count":observation.unknown_operation_count,
        "unknown_operation_ids_truncated":observation.unknown_operation_ids_truncated,
    });
    let payload_json = model::canonical(&payload)?;
    if payload_json.len() > 4096 {
        return Err(invalid(
            "module nonterminal trigger exceeds its record bound",
        ));
    }
    let duplicate: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations \
         WHERE source_stream_id=?1 AND source_event_key=?2)",
        params![source_stream_id, source_event_key],
        |row| row.get(0),
    )?;
    if duplicate {
        return Err(Error::new(
            "MODULE_NONTERMINAL_TRIGGER_CONFLICT",
            "nonterminal trigger already exists without its matching supervisor callback",
        ));
    }
    tx.execute(
        "INSERT INTO observations(\
             source_stream_id,source_event_key,binding_id,binding_generation,\
             operation_id,kind,payload_json,recorded_at_ms\
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            source_stream_id,
            source_event_key,
            observation.scope.binding_id,
            generation,
            operation_id,
            event_kind,
            payload_json,
            now_ms,
        ],
    )?;
    Ok(())
}

fn nonterminal_occurrence_id(
    binding_id: &str,
    generation: i64,
    boot_id: Option<&str>,
    event_id: &str,
    event_kind: &str,
) -> Result<String> {
    Ok(model::digest(
        model::canonical(&json!({
            "binding_id":binding_id,
            "generation":generation,
            "boot_id":boot_id,
            "event_id":event_id,
            "event_kind":event_kind,
        }))?
        .as_bytes(),
    ))
}

/// Map only exact lifecycle transitions into the existing closed diagnostic
/// vocabulary. Status is not an event acknowledgement: it becomes visible
/// only after the enclosing Store transaction commits.
fn diagnostic_for_transition(
    previous: &Value,
    observation: &ModuleSupervisorObservation,
    operation_link: Option<&ValidatedOperationLink>,
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
    let diagnostic_text = diagnostic_text_for_transition(observation);
    let mut record = Record::new(severity, kind, phase)
        .with_component(Some(Component::ModuleSupervisor))
        .with_code(code)
        .with_client_id(operation_link.and_then(|link| link.owner_id.as_deref()))
        .with_binding_id(Some(&observation.scope.binding_id))
        .with_binding_generation(Some(observation.scope.generation))
        .with_operation_id(operation_link.map(|link| link.operation_id.as_str()))
        .with_module_boot_id(Some(boot_id))
        .with_event_id(Some(&observation.event_id))
        .with_module_id(Some(&observation.module_id))
        .with_artifact_id(Some(&observation.artifact_id))
        .with_artifact_version(Some(&observation.artifact_version))
        .with_build_id(observation.build_id.as_deref());
    if let Some(link) = operation_link {
        record = record
            .with_task_id(link.task_id.as_deref())
            .with_attempt_id(link.attempt_id.as_deref());
    }
    if let Some(text) = diagnostic_text.as_deref() {
        record = record.with_text(Some(text));
    }
    Some(record)
}

/// Render only closed supervisor status fields. The helper never includes
/// native stdout/stderr, prompts, request headers, credentials, or Store JSON.
fn diagnostic_text_for_transition(observation: &ModuleSupervisorObservation) -> Option<String> {
    if !matches!(
        observation.phase,
        ModuleSupervisorPhase::Exited
            | ModuleSupervisorPhase::ExitedProven
            | ModuleSupervisorPhase::RestartBackoff
    ) && observation.error_code.is_none()
        && observation.stage.is_none()
    {
        return None;
    }
    Some(format!(
        "module supervisor status: phase={} effect_certainty={} stage={} error_code={} unresolved_operations={}",
        observation.phase.as_str(),
        observation.effect_certainty.as_str(),
        observation
            .stage
            .map(ModuleFailureStage::as_str)
            .unwrap_or("none"),
        observation.error_code.as_deref().unwrap_or("none"),
        observation.unknown_operation_count,
    ))
}

fn record_lifecycle_trigger(
    tx: &Transaction<'_>,
    source_stream_id: &str,
    observation: &ModuleSupervisorObservation,
    generation: i64,
    transition: LifecycleTransition,
    operation_link: Option<&ValidatedOperationLink>,
    now_ms: i64,
) -> Result<()> {
    let Some(boot_id) = observation.boot_id.as_deref() else {
        return Ok(());
    };
    let event_kind = transition.event_kind();
    let source_event_key = format!("lifecycle:{event_kind}:{}", observation.event_id);
    let occurrence_id = lifecycle_occurrence_id(
        &observation.scope.binding_id,
        generation,
        boot_id,
        &observation.event_id,
        event_kind,
    )?;
    let operation_id = operation_link.map(|link| link.operation_id.as_str());
    let payload = json!({
        "schema_version":if transition == LifecycleTransition::Ready { 2 } else { 1 },
        "event_kind":event_kind,
        "occurrence_phase":transition.occurrence_phase(),
        "occurrence_id":occurrence_id,
        "proof":transition.proof(),
        "module_id":observation.module_id,
        "binding_id":observation.scope.binding_id,
        "generation":generation,
        "operation_id":operation_id,
        "boot_id":boot_id,
        "event_id":observation.event_id,
        "actor_instance_id":observation.actor_instance_id,
        "sequence":observation.sequence,
        "phase":observation.phase.as_str(),
        "effect_certainty":observation.effect_certainty.as_str(),
        "stage":observation.stage.map(ModuleFailureStage::as_str),
    });
    let payload_json = model::canonical(&payload)?;
    if payload_json.len() > 4096 {
        return Err(invalid("module lifecycle trigger exceeds its record bound"));
    }
    let duplicate: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations \
         WHERE source_stream_id=?1 AND source_event_key=?2)",
        params![source_stream_id, source_event_key],
        |row| row.get(0),
    )?;
    if duplicate {
        return Err(Error::new(
            "MODULE_LIFECYCLE_TRIGGER_CONFLICT",
            "lifecycle trigger already exists without its matching supervisor callback",
        ));
    }
    tx.execute(
        "INSERT INTO observations(\
             source_stream_id,source_event_key,binding_id,binding_generation,\
             operation_id,kind,payload_json,recorded_at_ms\
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            source_stream_id,
            source_event_key,
            observation.scope.binding_id,
            generation,
            operation_id,
            event_kind,
            payload_json,
            now_ms,
        ],
    )?;
    Ok(())
}

fn lifecycle_occurrence_id(
    binding_id: &str,
    generation: i64,
    boot_id: &str,
    event_id: &str,
    event_kind: &str,
) -> Result<String> {
    Ok(model::digest(
        model::canonical(&json!({
            "binding_id":binding_id,
            "generation":generation,
            "boot_id":boot_id,
            "event_id":event_id,
            "event_kind":event_kind,
        }))?
        .as_bytes(),
    ))
}

pub(super) fn is_lifecycle_event_source_kind(source_id: &str, event_kind: &str) -> bool {
    source_id
        .strip_prefix(STREAM_PREFIX)
        .is_some_and(|tail| tail.starts_with(':'))
        && matches!(
            event_kind,
            READY_EVENT_KIND
                | START_FAILURE_EVENT_KIND
                | FAMILY_EXIT_EVENT_KIND
                | RECOVERY_BLOCKED_EVENT_KIND
                | IDENTITY_UNKNOWN_EVENT_KIND
                | OWNER_RETAINED_EVENT_KIND
        )
}

pub(super) fn verified_lifecycle_event(
    db: &rusqlite::Connection,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<Option<VerifiedModuleLifecycleEvent>> {
    if !is_lifecycle_event_source_kind(&event.source_id, &event.event_kind) {
        return Ok(None);
    }
    if is_nonterminal_event_kind(&event.event_kind) {
        return verified_nonterminal_event(db, event);
    }
    type TriggerRow = (
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
        i64,
        i64,
    );
    let row: Option<TriggerRow> = db
        .query_row(
            "SELECT CASE WHEN source_event_key IS NOT NULL \
                         AND length(CAST(source_event_key AS BLOB))<=512 \
                         THEN source_event_key END, binding_id,binding_generation,operation_id, \
                    CASE WHEN length(CAST(payload_json AS BLOB))<=4096 \
                         THEN payload_json END, recorded_at_ms, \
                    length(CAST(payload_json AS BLOB)) \
             FROM observations WHERE observation_id=?1 AND source_stream_id=?2 AND kind=?3",
            params![event.observation_id, event.source_id, event.event_kind],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((source_event_key, binding_id, generation, operation_id, raw, recorded_at_ms, bytes)) =
        row
    else {
        return Ok(None);
    };
    let (Some(source_event_key), Some(binding_id), Some(generation), operation_id, Some(raw)) =
        (source_event_key, binding_id, generation, operation_id, raw)
    else {
        return Ok(None);
    };
    if event.recorded_at_ms != recorded_at_ms
        || generation <= 0
        || !(0..=4096).contains(&bytes)
        || valid_token(&binding_id).is_err()
        || event.source_id != format!("{STREAM_PREFIX}:{binding_id}:{generation}")
    {
        return Ok(None);
    }
    let value: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    const TRIGGER_FIELDS: &[&str] = &[
        "schema_version",
        "event_kind",
        "occurrence_phase",
        "occurrence_id",
        "proof",
        "module_id",
        "binding_id",
        "generation",
        "operation_id",
        "boot_id",
        "event_id",
        "actor_instance_id",
        "sequence",
        "phase",
        "effect_certainty",
        "stage",
    ];
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if object.len() != TRIGGER_FIELDS.len()
        || TRIGGER_FIELDS
            .iter()
            .any(|field| !object.contains_key(*field))
    {
        return Ok(None);
    }
    let fact: ModuleLifecycleTrigger = match serde_json::from_value(value) {
        Ok(fact) => fact,
        Err(_) => return Ok(None),
    };
    if !matches!(fact.schema_version, 1 | 2)
        || (fact.schema_version == 2 && event.event_kind != READY_EVENT_KIND)
        || fact.event_kind != event.event_kind
        || fact.binding_id != binding_id
        || fact.generation != generation
        || fact.operation_id.as_deref() != operation_id.as_deref()
        || fact
            .operation_id
            .as_deref()
            .is_some_and(|id| valid_token(id).is_err())
        || fact.occurrence_phase != occurrence_phase_for_kind(&event.event_kind).unwrap_or_default()
        || fact.proof != proof_for_kind(&event.event_kind).unwrap_or_default()
        || source_event_key != format!("lifecycle:{}:{}", event.event_kind, fact.event_id)
        || fact.event_id.is_empty()
        || fact.event_id.len() > 256
        || fact.boot_id.is_empty()
        || fact.boot_id.len() > 128
        || fact.sequence == 0
        || valid_token(&fact.actor_instance_id).is_err()
        || valid_atom(&fact.module_id, false).is_err()
        || fact.occurrence_id
            != lifecycle_occurrence_id(
                &fact.binding_id,
                fact.generation,
                &fact.boot_id,
                &fact.event_id,
                &fact.event_kind,
            )?
    {
        return Ok(None);
    }
    let original: Option<LifecycleOriginalRow> = db
        .query_row(
            "SELECT source_event_key,binding_id,binding_generation,operation_id, \
                    CASE WHEN length(CAST(payload_json AS BLOB))<=?5 THEN payload_json END, \
                    recorded_at_ms,length(CAST(payload_json AS BLOB)) \
             FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind=?3 \
               AND observation_id<?4",
            params![
                event.source_id,
                fact.event_id,
                EVENT_KIND,
                event.observation_id,
                MAX_RECORD_BYTES as i64,
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        Some(original_key),
        Some(original_binding_id),
        Some(original_generation),
        None,
        Some(original_raw),
        original_recorded_at_ms,
        original_bytes,
    )) = original
    else {
        return Ok(None);
    };
    if original_key != fact.event_id
        || original_binding_id != binding_id
        || original_generation != generation
        || original_recorded_at_ms != recorded_at_ms
        || !(0..=MAX_RECORD_BYTES as i64).contains(&original_bytes)
    {
        return Ok(None);
    }
    let observation = match parse_retained_observation(&original_raw) {
        Ok(observation) => observation,
        Err(_) => return Ok(None),
    };
    if validate_header(&observation).is_err()
        || observation.event_id != fact.event_id
        || observation.scope.binding_id != binding_id
        || observation.scope.generation != generation as u64
        || observation.boot_id.as_deref() != Some(fact.boot_id.as_str())
        || observation.module_id != fact.module_id
        || observation.actor_instance_id != fact.actor_instance_id
        || observation.sequence != fact.sequence
        || observation.phase.as_str() != fact.phase
        || observation.effect_certainty.as_str() != fact.effect_certainty
        || observation.stage.map(ModuleFailureStage::as_str) != fact.stage.as_deref()
        || !lifecycle_operation_link_matches(db, &fact, &observation)?
        || !lifecycle_fact_matches_observation(&event.event_kind, &observation)
    {
        return Ok(None);
    }
    let binding = match super::operations::get_binding(db, &binding_id, generation) {
        Ok(binding) => binding,
        Err(_) => return Ok(None),
    };
    if validate_binding_identity(&binding, &observation).is_err() {
        return Ok(None);
    }
    Ok(Some(VerifiedModuleLifecycleEvent {
        binding_id,
        generation,
        occurrence_phase: fact.occurrence_phase,
        occurrence_id: fact.occurrence_id,
        error_code: None,
    }))
}

fn lifecycle_operation_link_matches(
    db: &rusqlite::Connection,
    fact: &ModuleLifecycleTrigger,
    observation: &ModuleSupervisorObservation,
) -> Result<bool> {
    let single_id = (observation.unknown_operation_count == 1
        && observation.unknown_operation_ids.len() == 1
        && !observation.unknown_operation_ids_truncated)
        .then(|| observation.unknown_operation_ids[0].as_str());
    // Retained v1 triggers linked the sole sampled ID regardless of its later
    // state. Preserve that historical contract; v2 Ready links only the exact
    // Operation that was still unresolved at commit time.
    if fact.schema_version == 1 {
        return Ok(fact.operation_id.as_deref() == single_id);
    }
    if fact.operation_id.is_some() && fact.operation_id.as_deref() != single_id {
        return Ok(false);
    }
    for id in &observation.unknown_operation_ids {
        let state: Option<String> = db
            .query_row(
                "SELECT state FROM operations WHERE operation_id=?1 \
                 AND binding_id=?2 AND binding_generation=?3",
                params![id, fact.binding_id, fact.generation],
                |row| row.get(0),
            )
            .optional()?;
        let Some(state) = state else {
            return Ok(false);
        };
        let terminal = matches!(state.as_str(), "settled" | "rejected" | "cancelled");
        if !terminal
            && !matches!(
                state.as_str(),
                "sending" | "native_accepted" | "outcome_unknown"
            )
        {
            return Ok(false);
        }
        // A historical active link remains valid after settlement. An absent
        // sole-ID link requires terminal evidence, never an arbitrary omission.
        if single_id == Some(id.as_str()) && fact.operation_id.is_none() && !terminal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn is_nonterminal_event_kind(event_kind: &str) -> bool {
    matches!(
        event_kind,
        RECOVERY_BLOCKED_EVENT_KIND | IDENTITY_UNKNOWN_EVENT_KIND | OWNER_RETAINED_EVENT_KIND
    )
}

fn verified_nonterminal_event(
    db: &rusqlite::Connection,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<Option<VerifiedModuleLifecycleEvent>> {
    type TriggerRow = (
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
        i64,
        i64,
    );
    let row: Option<TriggerRow> = db
        .query_row(
            "SELECT CASE WHEN source_event_key IS NOT NULL \
                         AND length(CAST(source_event_key AS BLOB))<=512 \
                         THEN source_event_key END, binding_id,binding_generation,operation_id, \
                    CASE WHEN length(CAST(payload_json AS BLOB))<=4096 \
                         THEN payload_json END, recorded_at_ms, \
                    length(CAST(payload_json AS BLOB)) \
             FROM observations WHERE observation_id=?1 AND source_stream_id=?2 AND kind=?3",
            params![event.observation_id, event.source_id, event.event_kind],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((source_event_key, binding_id, generation, operation_id, raw, recorded_at_ms, bytes)) =
        row
    else {
        return Ok(None);
    };
    let (Some(source_event_key), Some(binding_id), Some(generation), operation_id, Some(raw)) =
        (source_event_key, binding_id, generation, operation_id, raw)
    else {
        return Ok(None);
    };
    if event.recorded_at_ms != recorded_at_ms
        || generation <= 0
        || !(0..=4096).contains(&bytes)
        || valid_token(&binding_id).is_err()
        || event.source_id != format!("{STREAM_PREFIX}:{binding_id}:{generation}")
    {
        return Ok(None);
    }
    let value: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    const TRIGGER_FIELDS: &[&str] = &[
        "schema_version",
        "event_kind",
        "occurrence_phase",
        "occurrence_id",
        "proof",
        "module_id",
        "binding_id",
        "generation",
        "operation_id",
        "boot_id",
        "event_id",
        "actor_instance_id",
        "sequence",
        "phase",
        "effect_certainty",
        "stage",
        "error_code",
        "unknown_operation_count",
        "unknown_operation_ids_truncated",
    ];
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if object.len() != TRIGGER_FIELDS.len()
        || TRIGGER_FIELDS
            .iter()
            .any(|field| !object.contains_key(*field))
    {
        return Ok(None);
    }
    let fact: ModuleNonterminalTrigger = match serde_json::from_value(value) {
        Ok(fact) => fact,
        Err(_) => return Ok(None),
    };
    let expected_proof = match event.event_kind.as_str() {
        RECOVERY_BLOCKED_EVENT_KIND => NonterminalTransition::RecoveryBlocked.proof(),
        IDENTITY_UNKNOWN_EVENT_KIND => NonterminalTransition::IdentityUnknown.proof(),
        OWNER_RETAINED_EVENT_KIND => NonterminalTransition::OwnerRetained.proof(),
        _ => return Ok(None),
    };
    if fact.schema_version != 1
        || fact.event_kind != event.event_kind
        || fact.binding_id != binding_id
        || fact.generation != generation
        || fact.operation_id.as_deref() != operation_id.as_deref()
        || fact
            .operation_id
            .as_deref()
            .is_some_and(|id| valid_token(id).is_err())
        || fact.occurrence_phase != "module_nonterminal_attention"
        || fact.proof != expected_proof
        || source_event_key != format!("nonterminal:{}:{}", event.event_kind, fact.event_id)
        || fact.event_id.is_empty()
        || fact.event_id.len() > 256
        || fact
            .boot_id
            .as_deref()
            .is_some_and(|id| valid_token(id).is_err())
        || fact.sequence == 0
        || valid_token(&fact.actor_instance_id).is_err()
        || valid_atom(&fact.module_id, false).is_err()
        || fact.error_code.as_deref().is_some_and(|code| {
            code.is_empty()
                || code.len() > 128
                || !code
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        })
        || fact.unknown_operation_count > MAX_UNKNOWN_OPERATION_COUNT as usize
        || fact.occurrence_id
            != nonterminal_occurrence_id(
                &fact.binding_id,
                fact.generation,
                fact.boot_id.as_deref(),
                &fact.event_id,
                &fact.event_kind,
            )?
    {
        return Ok(None);
    }
    let original: Option<NonterminalOriginalRow> = db
        .query_row(
            "SELECT source_event_key,binding_id,binding_generation,operation_id, \
                    CASE WHEN length(CAST(payload_json AS BLOB))<=?5 THEN payload_json END, \
                    recorded_at_ms,length(CAST(payload_json AS BLOB)) \
             FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind=?3 \
               AND observation_id<?4",
            params![
                event.source_id,
                fact.event_id,
                EVENT_KIND,
                event.observation_id,
                MAX_RECORD_BYTES as i64,
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        Some(original_key),
        Some(original_binding_id),
        Some(original_generation),
        None,
        Some(original_raw),
        original_recorded_at_ms,
        original_bytes,
    )) = original
    else {
        return Ok(None);
    };
    if original_key != fact.event_id
        || original_binding_id != binding_id
        || original_generation != generation
        || original_recorded_at_ms != recorded_at_ms
        || !(0..=MAX_RECORD_BYTES as i64).contains(&original_bytes)
    {
        return Ok(None);
    }
    let observation = match parse_retained_observation(&original_raw) {
        Ok(observation) => observation,
        Err(_) => return Ok(None),
    };
    let expected_operation_id = (observation.unknown_operation_count == 1
        && observation.unknown_operation_ids.len() == 1
        && !observation.unknown_operation_ids_truncated)
        .then(|| observation.unknown_operation_ids[0].as_str());
    if validate_header(&observation).is_err()
        || observation.event_id != fact.event_id
        || observation.scope.binding_id != binding_id
        || observation.scope.generation != generation as u64
        || observation.boot_id != fact.boot_id
        || observation.module_id != fact.module_id
        || observation.actor_instance_id != fact.actor_instance_id
        || observation.sequence != fact.sequence
        || observation.phase.as_str() != fact.phase
        || observation.effect_certainty.as_str() != fact.effect_certainty
        || observation.stage.map(ModuleFailureStage::as_str) != fact.stage.as_deref()
        || observation.error_code != fact.error_code
        || expected_operation_id != fact.operation_id.as_deref()
        || observation.unknown_operation_count != fact.unknown_operation_count
        || observation.unknown_operation_ids_truncated != fact.unknown_operation_ids_truncated
        || !nonterminal_fact_matches_observation(&event.event_kind, &observation)
    {
        return Ok(None);
    }
    let binding = match super::operations::get_binding(db, &binding_id, generation) {
        Ok(binding) => binding,
        Err(_) => return Ok(None),
    };
    if validate_binding_identity(&binding, &observation).is_err() {
        return Ok(None);
    }
    Ok(Some(VerifiedModuleLifecycleEvent {
        binding_id,
        generation,
        occurrence_phase: fact.occurrence_phase,
        occurrence_id: fact.occurrence_id,
        error_code: fact.error_code,
    }))
}

pub(super) fn lifecycle_event_projection(
    db: &rusqlite::Connection,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<crate::automation::intake::SafeEventProjection> {
    let Some(fact) = verified_lifecycle_event(db, event)? else {
        return Ok(Default::default());
    };
    Ok(crate::automation::intake::SafeEventProjection {
        occurrence_phase: Some(fact.occurrence_phase),
        occurrence_id: Some(fact.occurrence_id),
        error_code: fact.error_code,
        ..Default::default()
    })
}

fn occurrence_phase_for_kind(event_kind: &str) -> Option<&'static str> {
    match event_kind {
        READY_EVENT_KIND => Some(LifecycleTransition::Ready.occurrence_phase()),
        START_FAILURE_EVENT_KIND => {
            Some(LifecycleTransition::CertifiedNotStarted.occurrence_phase())
        }
        FAMILY_EXIT_EVENT_KIND => Some(LifecycleTransition::FamilyExited.occurrence_phase()),
        _ => None,
    }
}

fn proof_for_kind(event_kind: &str) -> Option<&'static str> {
    match event_kind {
        READY_EVENT_KIND => Some(LifecycleTransition::Ready.proof()),
        START_FAILURE_EVENT_KIND => Some(LifecycleTransition::CertifiedNotStarted.proof()),
        FAMILY_EXIT_EVENT_KIND => Some(LifecycleTransition::FamilyExited.proof()),
        _ => None,
    }
}

fn lifecycle_fact_matches_observation(
    event_kind: &str,
    observation: &ModuleSupervisorObservation,
) -> bool {
    match event_kind {
        READY_EVENT_KIND => matches!(observation.phase, ModuleSupervisorPhase::Ready),
        START_FAILURE_EVENT_KIND => {
            matches!(
                observation.phase,
                ModuleSupervisorPhase::Exited
                    | ModuleSupervisorPhase::RestartBackoff
                    | ModuleSupervisorPhase::Completed
            ) && matches!(
                observation.effect_certainty,
                ModuleEffectCertainty::NotStarted
            ) && observation
                .stage
                .is_some_and(ModuleFailureStage::proves_pre_spawn)
        }
        FAMILY_EXIT_EVENT_KIND => {
            matches!(
                observation.phase,
                ModuleSupervisorPhase::ExitedProven | ModuleSupervisorPhase::Completed
            ) && matches!(observation.effect_certainty, ModuleEffectCertainty::Unknown)
                && observation.stage.is_none()
        }
        _ => false,
    }
}

fn nonterminal_fact_matches_observation(
    event_kind: &str,
    observation: &ModuleSupervisorObservation,
) -> bool {
    match event_kind {
        RECOVERY_BLOCKED_EVENT_KIND => {
            matches!(
                observation.phase,
                ModuleSupervisorPhase::WaitingForKernel | ModuleSupervisorPhase::Isolated
            ) || (matches!(observation.phase, ModuleSupervisorPhase::Starting)
                && observation.error_code.is_some())
        }
        IDENTITY_UNKNOWN_EVENT_KIND => {
            matches!(observation.phase, ModuleSupervisorPhase::IdentityUnknown)
        }
        OWNER_RETAINED_EVENT_KIND => {
            matches!(observation.phase, ModuleSupervisorPhase::OwnerRetained)
        }
        _ => false,
    }
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
    // The route runtime names the native harness; the retained descriptor
    // selector identifies the exact module, artifact and version for this binding.
    if selector["schema_version"] != 1
        || registered_revision.is_none_or(|revision| revision == 0)
        || selected_revision.is_none_or(|revision| revision < registered_revision.unwrap_or(0))
        || binding["binding_id"].as_str() != Some(observation.scope.binding_id.as_str())
        || binding_generation != i64::try_from(observation.scope.generation).ok()
        || binding["module_artifact_id"].as_str() != Some(observation.artifact_id.as_str())
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

#[derive(Debug)]
struct ValidatedOperationLink {
    operation_id: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    owner_id: Option<String>,
}

fn validate_operation_ids(
    tx: &Transaction<'_>,
    observation: &ModuleSupervisorObservation,
    generation: i64,
) -> Result<Option<ValidatedOperationLink>> {
    type OperationValidationRow = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );

    let has_single_exact_operation = observation.unknown_operation_count == 1
        && observation.unknown_operation_ids.len() == 1
        && !observation.unknown_operation_ids_truncated;
    let mut exact_link = None;
    for operation_id in &observation.unknown_operation_ids {
        let row: Option<OperationValidationRow> = tx
            .query_row(
                "SELECT o.state,
                        o.task_id,
                        o.attempt_id,
                        a.task_id,
                        a.owner_id
                   FROM operations o
                   LEFT JOIN attempts a
                     ON a.attempt_id=o.attempt_id
                    AND a.binding_id=o.binding_id
                    AND a.binding_generation=o.binding_generation
                  WHERE o.operation_id=?1
                    AND o.binding_id=?2
                    AND o.binding_generation=?3",
                params![operation_id, observation.scope.binding_id, generation],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((state, operation_task_id, attempt_id, attempt_task_id, owner_id)) = row else {
            return Err(Error::new(
                "MODULE_OBSERVATION_OPERATION_SCOPE",
                "listed Operation is absent or outside the exact binding generation",
            ));
        };
        if !matches!(
            state.as_str(),
            "sending" | "native_accepted" | "outcome_unknown"
        ) {
            if matches!(observation.phase, ModuleSupervisorPhase::Ready)
                && matches!(state.as_str(), "settled" | "rejected" | "cancelled")
            {
                // The immutable hello snapshot may wait behind delivery while
                // an exact scoped Operation settles. Keep the sampled ID but
                // do not attach terminal work as an active lifecycle cause.
                continue;
            }
            return Err(Error::new(
                "MODULE_OBSERVATION_OPERATION_TERMINAL",
                "listed Operation in the exact binding generation is no longer pending",
            ));
        }
        if has_single_exact_operation {
            let (task_id, attempt_id) = match (
                operation_task_id.as_deref(),
                attempt_id.as_deref(),
                attempt_task_id.as_deref(),
            ) {
                (Some(task_id), Some(attempt_id), Some(attempt_task_id))
                    if task_id == attempt_task_id =>
                {
                    (Some(task_id.to_owned()), Some(attempt_id.to_owned()))
                }
                (None, Some(attempt_id), Some(attempt_task_id)) => (
                    Some(attempt_task_id.to_owned()),
                    Some(attempt_id.to_owned()),
                ),
                (Some(task_id), None, _) | (Some(task_id), Some(_), None) => {
                    (Some(task_id.to_owned()), None)
                }
                (Some(_), Some(_), Some(_)) | (None, _, _) => (None, None),
            };
            exact_link = Some(ValidatedOperationLink {
                operation_id: operation_id.clone(),
                task_id,
                attempt_id,
                owner_id,
            });
        }
    }
    Ok(exact_link)
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
            | "exited" | "exited_proven" | "owner_retained" | "identity_unknown" | "completed"
            | "isolated",
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
            | "exited_proven"
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
        "build_id":observation["build_id"],
        "event_id":observation["event_id"],
        "boot_id":observation["boot_id"],
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

#[cfg(test)]
mod journal_regression_tests {
    use super::*;
    use rusqlite::{Connection, TransactionBehavior, params};
    use serde_json::Value;
    use swarm_supervisor::{
        ModuleEffectCertainty as ProducerCertainty, ModuleFailureStage as ProducerStage,
        ModuleSupervisorObservation as ProducerObservation, ModuleSupervisorPhase as ProducerPhase,
        ServiceScope,
    };

    const BINDING_ID: &str = "binding-fixture";
    const GENERATION: u64 = 7;
    const MODULE_ID: &str = "module-fixture";
    const ARTIFACT_ID: &str = "artifact-fixture";
    const ARTIFACT_VERSION: &str = "1.2.3";
    const BUILD_ID: &str = "build-fixture";
    const ACTOR_INSTANCE_ID: &str = "actor-fixture";
    const BOOT_ID: &str = "boot-fixture";

    fn fixture_db() -> Connection {
        let db = Connection::open_in_memory().expect("open in-memory Store database");
        db.execute_batch("PRAGMA foreign_keys=ON;")
            .expect("enable Store foreign keys");
        db.execute_batch(include_str!("../../migrations/001_core.sql"))
            .expect("install the real core Store schema");

        let binding_state = json!({
            "module_contract_selector": {
                "schema_version": 1,
                "registered_revision": 3,
                "selected_revision": 3,
                "module_id": MODULE_ID,
                "artifact": {
                    "artifact_id": ARTIFACT_ID,
                    "version": ARTIFACT_VERSION,
                    "build_id": BUILD_ID
                }
            }
        });
        db.execute(
            "INSERT INTO bindings(
                 binding_id,generation,lane_id,module_instance_id,module_artifact_id,
                 state,native_scope_key,native_root_id,route_json,state_json,created_at_ms
             ) VALUES(?1,?2,'lane-fixture','instance-fixture',?3,'ready',NULL,NULL,'{}',?4,1)",
            params![
                BINDING_ID,
                GENERATION as i64,
                ARTIFACT_ID,
                model::canonical(&binding_state).expect("encode binding selector"),
            ],
        )
        .expect("insert exact fixture binding");
        db
    }

    fn producer_observation(
        sequence: u64,
        phase: ProducerPhase,
        error_code: Option<&str>,
    ) -> ProducerObservation {
        ProducerObservation {
            schema_version: 1,
            actor_instance_id: ACTOR_INSTANCE_ID.to_owned(),
            event_id: format!("{BOOT_ID}:{ACTOR_INSTANCE_ID}:{sequence}"),
            sequence,
            module_id: MODULE_ID.to_owned(),
            artifact_id: ARTIFACT_ID.to_owned(),
            artifact_version: ARTIFACT_VERSION.to_owned(),
            build_id: Some(BUILD_ID.to_owned()),
            scope: ServiceScope {
                binding_id: BINDING_ID.to_owned(),
                generation: GENERATION,
            },
            boot_id: Some(BOOT_ID.to_owned()),
            phase,
            effect_certainty: ProducerCertainty::Unknown,
            stage: error_code.map(|_| ProducerStage::Worker),
            error_code: error_code.map(str::to_owned),
            unknown_operation_ids: Vec::new(),
            unknown_operation_count: 0,
            unknown_operation_ids_truncated: false,
        }
    }

    fn record_in_transaction(
        db: &mut Connection,
        observation: &ProducerObservation,
        now_ms: i64,
    ) -> Result<ObservationCommit> {
        let parsed = parse(observation)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let committed = record(&tx, &parsed, now_ms)?;
        tx.commit()?;
        Ok(committed)
    }

    fn expect_record_error(
        db: &mut Connection,
        observation: &ProducerObservation,
        now_ms: i64,
    ) -> Error {
        match record_in_transaction(db, observation, now_ms) {
            Err(error) => error,
            Ok(_) => panic!("mismatched or conflicting observation was accepted"),
        }
    }

    fn insert_operation(db: &Connection, id: &str, state: &str, scoped: bool) {
        let terminal = matches!(state, "settled" | "rejected" | "cancelled");
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,
                 original_request_json,effective_request_json,binding_id,binding_generation,
                 state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms)
             VALUES(?1,'caller-fixture',?1,'agent.send','{}','{}',?2,?3,?4,?5,1,?6,1,1)",
            params![
                id,
                scoped.then_some(BINDING_ID),
                scoped.then_some(GENERATION as i64),
                state,
                terminal.then_some("{}"),
                terminal.then_some(2_i64),
            ],
        )
        .unwrap();
    }

    fn retained_event(db: &Connection, kind: &str) -> crate::automation::intake::ObservedEvent {
        db.query_row(
            "SELECT observation_id,source_stream_id,kind,operation_id,recorded_at_ms
             FROM observations WHERE kind=?1 ORDER BY observation_id DESC LIMIT 1",
            [kind],
            |row| {
                Ok(crate::automation::intake::ObservedEvent {
                    observation_id: row.get(0)?,
                    source_id: row.get(1)?,
                    event_kind: row.get(2)?,
                    operation_id: row.get(3)?,
                    recorded_at_ms: row.get(4)?,
                })
            },
        )
        .unwrap()
    }

    #[test]
    fn ready_preserves_terminal_ids_without_linking_terminal_work() {
        for state in ["settled", "rejected", "cancelled"] {
            let mut db = fixture_db();
            insert_operation(&db, "operation-fixture", state, true);
            let mut ready = producer_observation(1, ProducerPhase::Ready, None);
            ready.unknown_operation_ids = vec!["operation-fixture".to_owned()];
            ready.unknown_operation_count = 1;
            assert!(
                record_in_transaction(&mut db, &ready, 100)
                    .unwrap()
                    .inserted
            );
            let event = retained_event(&db, READY_EVENT_KIND);
            assert!(event.operation_id.is_none());
            assert!(verified_lifecycle_event(&db, &event).unwrap().is_some());
            let raw: String = db
                .query_row(
                    "SELECT payload_json FROM observations WHERE kind=?1",
                    [EVENT_KIND],
                    |row| row.get(0),
                )
                .unwrap();
            let original = parse_retained_observation(&raw).unwrap();
            assert_eq!(original.unknown_operation_ids, ready.unknown_operation_ids);
            assert_eq!(original.unknown_operation_count, 1);

            // A terminal row remains invalid for ordinary sampled phases.
            let mut starting = ready;
            starting.phase = ProducerPhase::Starting;
            starting.sequence = 2;
            starting.event_id = format!("{BOOT_ID}:{ACTOR_INSTANCE_ID}:2");
            assert_eq!(
                expect_record_error(&mut db, &starting, 200).code,
                "MODULE_OBSERVATION_OPERATION_TERMINAL"
            );
        }
    }

    #[test]
    fn ready_pending_link_survives_settlement_and_v1_facts_remain_readable() {
        let mut db = fixture_db();
        insert_operation(&db, "operation-fixture", "sending", true);
        let mut ready = producer_observation(1, ProducerPhase::Ready, None);
        ready.unknown_operation_ids = vec!["operation-fixture".to_owned()];
        ready.unknown_operation_count = 1;
        record_in_transaction(&mut db, &ready, 100).unwrap();
        let event = retained_event(&db, READY_EVENT_KIND);
        assert_eq!(event.operation_id.as_deref(), Some("operation-fixture"));
        assert!(verified_lifecycle_event(&db, &event).unwrap().is_some());
        db.execute(
            "UPDATE operations SET state='settled',settled_at_ms=200,result_json='{}'
                    WHERE operation_id='operation-fixture'",
            [],
        )
        .unwrap();
        assert!(verified_lifecycle_event(&db, &event).unwrap().is_some());
        db.execute(
            "UPDATE observations SET payload_json=json_set(payload_json,'$.schema_version',1)
                    WHERE observation_id=?1",
            [event.observation_id],
        )
        .unwrap();
        assert!(verified_lifecycle_event(&db, &event).unwrap().is_some());
    }

    #[test]
    fn ready_cannot_hide_unscoped_or_still_pending_ids() {
        let mut db = fixture_db();
        insert_operation(&db, "foreign-operation", "settled", false);
        let mut ready = producer_observation(1, ProducerPhase::Ready, None);
        ready.unknown_operation_ids = vec!["foreign-operation".to_owned()];
        ready.unknown_operation_count = 1;
        assert_eq!(
            expect_record_error(&mut db, &ready, 100).code,
            "MODULE_OBSERVATION_OPERATION_SCOPE"
        );
        insert_operation(&db, "operation-fixture", "sending", true);
        ready.unknown_operation_ids = vec!["operation-fixture".to_owned()];
        record_in_transaction(&mut db, &ready, 100).unwrap();
        let mut event = retained_event(&db, READY_EVENT_KIND);
        db.execute(
            "UPDATE observations SET operation_id=NULL,
                    payload_json=json_set(payload_json,'$.operation_id',NULL)
                    WHERE observation_id=?1",
            [event.observation_id],
        )
        .unwrap();
        event.operation_id = None;
        assert!(verified_lifecycle_event(&db, &event).unwrap().is_none());
    }

    #[test]
    fn ready_occurrence_is_idempotent_and_owner_retained_remains_latest() {
        let mut db = fixture_db();
        let starting = producer_observation(1, ProducerPhase::Starting, None);
        let ready = producer_observation(2, ProducerPhase::Ready, None);
        let owner_retained = producer_observation(
            3,
            ProducerPhase::OwnerRetained,
            Some("MODULE_WORKER_EXITED"),
        );

        assert!(
            record_in_transaction(&mut db, &starting, 100)
                .unwrap()
                .inserted
        );
        assert!(
            record_in_transaction(&mut db, &ready, 200)
                .unwrap()
                .inserted
        );
        assert!(
            !record_in_transaction(&mut db, &ready, 201)
                .unwrap()
                .inserted,
            "an identical Ready callback is an idempotent replay"
        );

        let mut changed_ready = ready.clone();
        changed_ready.error_code = Some("MODULE_WORKER_EXITED".to_owned());
        changed_ready.stage = Some(ProducerStage::Worker);
        let conflict = expect_record_error(&mut db, &changed_ready, 202);
        assert_eq!(conflict.code, "MODULE_OBSERVATION_CONFLICT");

        assert!(
            record_in_transaction(&mut db, &owner_retained, 300)
                .unwrap()
                .inserted
        );

        let ready_event_id = ready.event_id.as_str();
        let ready_raw: String = db
            .query_row(
                "SELECT payload_json FROM observations
                 WHERE source_stream_id=?1 AND source_event_key=?2 AND kind=?3",
                params![
                    format!("{STREAM_PREFIX}:{BINDING_ID}:{GENERATION}"),
                    ready_event_id,
                    EVENT_KIND,
                ],
                |row| row.get(0),
            )
            .expect("read serialized Ready producer DTO");
        let ready_payload: Value =
            serde_json::from_str(&ready_raw).expect("decode retained Ready DTO");
        assert_eq!(ready_payload["event_id"], ready_event_id);
        assert_eq!(ready_payload["sequence"], 2);
        assert_eq!(ready_payload["phase"], "ready");
        assert_eq!(ready_payload["binding_id"], BINDING_ID);
        assert_eq!(ready_payload["generation"], GENERATION);
        assert_eq!(ready_payload["module_id"], MODULE_ID);
        assert_eq!(ready_payload["artifact_id"], ARTIFACT_ID);
        assert_eq!(ready_payload["artifact_version"], ARTIFACT_VERSION);
        assert_eq!(ready_payload["build_id"], BUILD_ID);
        assert_eq!(ready_payload["boot_id"], BOOT_ID);

        let mut statement = db
            .prepare(
                "SELECT kind,payload_json,length(CAST(payload_json AS BLOB))
                 FROM observations
                 WHERE binding_id=?1 AND binding_generation=?2
                   AND kind IN (?3,?4)
                 ORDER BY observation_id",
            )
            .expect("prepare lifecycle journal query");
        let events = statement
            .query_map(
                params![
                    BINDING_ID,
                    GENERATION as i64,
                    READY_EVENT_KIND,
                    OWNER_RETAINED_EVENT_KIND
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .expect("query lifecycle journal")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect lifecycle events");
        assert_eq!(events.len(), 2, "Ready is written once before retention");
        assert_eq!(events[0].0, READY_EVENT_KIND);
        assert_eq!(events[1].0, OWNER_RETAINED_EVENT_KIND);

        let owner_payload: Value =
            serde_json::from_str(&events[1].1).expect("decode bounded retained occurrence");
        assert_eq!(owner_payload["error_code"], "MODULE_WORKER_EXITED");
        assert_eq!(owner_payload["phase"], "owner_retained");
        assert!(owner_payload["occurrence_id"].as_str().is_some());
        assert!((1..=4096).contains(&events[1].2));

        let latest: String = db
            .query_row(
                "SELECT json_extract(state_json,'$.module_supervisor.phase')
                 FROM bindings WHERE binding_id=?1 AND generation=?2",
                params![BINDING_ID, GENERATION as i64],
                |row| row.get(0),
            )
            .expect("read latest module supervisor phase");
        assert_eq!(latest, "owner_retained");
        assert_ne!(latest, "ready");
        for kind in [READY_EVENT_KIND, OWNER_RETAINED_EVENT_KIND] {
            assert!(
                verified_lifecycle_event(&db, &retained_event(&db, kind))
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn exact_binding_artifact_and_generation_are_required() {
        let mut db = fixture_db();
        let base = producer_observation(1, ProducerPhase::Starting, None);

        let mut wrong_binding = base.clone();
        wrong_binding.scope.binding_id = "other-binding".to_owned();
        let error = expect_record_error(&mut db, &wrong_binding, 100);
        assert_eq!(error.code, "NOT_FOUND");

        let mut wrong_generation = base.clone();
        wrong_generation.scope.generation += 1;
        let error = expect_record_error(&mut db, &wrong_generation, 101);
        assert_eq!(error.code, "NOT_FOUND");

        let mut wrong_artifact = base.clone();
        wrong_artifact.artifact_id = "other-artifact".to_owned();
        let error = expect_record_error(&mut db, &wrong_artifact, 102);
        assert_eq!(error.code, "MODULE_OBSERVATION_IDENTITY_MISMATCH");

        let mut wrong_version = base.clone();
        wrong_version.artifact_version = "9.9.9".to_owned();
        let error = expect_record_error(&mut db, &wrong_version, 103);
        assert_eq!(error.code, "MODULE_OBSERVATION_IDENTITY_MISMATCH");

        let mut wrong_build = base;
        wrong_build.build_id = Some("other-build".to_owned());
        let error = expect_record_error(&mut db, &wrong_build, 104);
        assert_eq!(error.code, "MODULE_OBSERVATION_IDENTITY_MISMATCH");
    }
}
