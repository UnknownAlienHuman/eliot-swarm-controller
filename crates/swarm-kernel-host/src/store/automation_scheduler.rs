//! Narrow authenticated Store projection for the independent scheduler worker.
//!
//! The Module registration is host-issued and stores only the parsed-token
//! digest. Its `DeclaredServiceScope` is an identity/generation fence, not an
//! authorization grant; this file supplies the exact closed method scope.

use super::{meta, set_meta};
use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use swarm_contracts::{DeclaredServicePurpose, DeclaredServiceScope};

pub(super) const SERVICE_ID: &str = "automation-scheduler-v1";
const REGISTRATION_FIELD: &str = "automation_scheduler";
const GENERATION_KEY: &str = "automation_scheduler:v1:generation";
const OWNER_KEY: &str = "automation_scheduler:v1:owner";
const METHOD_SCOPE: [&str; 2] = ["automation.scheduler.page", "automation.scheduler.admit"];
const MAX_SOURCE_PAGE: usize = 32;
const MAX_CHECK_RECOVERY_ERROR_MESSAGE_CHARS: usize = 512;
const CRON_DUE_PREFIX: &str = "automation:v1:cron:due:";
const GOAL_DUE_PREFIX: &str = "goals:v1:due:";
const SCHEDULE_REGISTRY_KEY: &str = "schedule_registry:v1";
const SCHEDULER_QUARANTINE_PREFIX: &str = "automation:v1:quarantine:scheduler:";
type ScheduleCursor = (String, Option<i64>, Option<i64>, Option<i64>);

#[derive(Debug, Clone)]
struct DueSourceEvidence {
    kind: &'static str,
    code: String,
    evidence: super::automation_reconcile::QuarantineEvidence,
    source_key: Option<String>,
    source_raw: Option<String>,
}

#[derive(Debug, Clone)]
struct DueIndexSnapshot {
    next_due_at_ms: Option<i64>,
    due_count: usize,
    rows: Vec<(String, String)>,
    damaged_subjects: Vec<DueSourceEvidence>,
}

struct ScheduleSourceSnapshot {
    due_count: usize,
    next_due_at_ms: Option<i64>,
    cursor_digest: String,
    schedule_cursors: Vec<ScheduleCursor>,
    damaged_subjects: Vec<DueSourceEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerRegistration {
    schema_version: u32,
    scope: DeclaredServiceScope,
    method_scope: Vec<String>,
    config_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerOwner {
    schema_version: u32,
    scope: DeclaredServiceScope,
    launch_id: String,
    phase: SchedulerOwnerPhase,
    group_identity: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SchedulerOwnerPhase {
    Launching,
    Active,
    Departed,
}

#[derive(Debug, Clone)]
struct DueProjection {
    scope: DeclaredServiceScope,
    observed_at_ms: i64,
    next_due_at_ms: Option<i64>,
    schedule_due_count: u32,
    schedule_digest: String,
    cron_due_at_ms: Option<i64>,
    cron_due_count: u32,
    cron_digest: String,
    goal_due_at_ms: Option<i64>,
    goal_due_count: u32,
    goal_digest: String,
    snapshot_sha256: String,
    schedule_next_due: Option<i64>,
    damaged_subjects: Vec<DueSourceEvidence>,
    schedule_cursors: Vec<ScheduleCursor>,
    cron_rows: Vec<(String, String)>,
    goal_rows: Vec<(String, String)>,
}

impl DueProjection {
    fn value(&self) -> Value {
        json!({
            "schema_version":1,
            "scope":self.scope,
            "observed_at_ms":self.observed_at_ms,
            "next_due_at_ms":self.next_due_at_ms,
            "sources":[
                {"kind":"interval_schedule","next_due_at_ms":self.schedule_next_due_at_ms(),"due_count":self.schedule_due_count,"cursor_digest":self.schedule_digest},
                {"kind":"manager_calendar","next_due_at_ms":self.cron_due_at_ms,"due_count":self.cron_due_count,"cursor_digest":self.cron_digest},
                {"kind":"goal_reminder","next_due_at_ms":self.goal_due_at_ms,"due_count":self.goal_due_count,"cursor_digest":self.goal_digest},
            ],
            "snapshot_sha256":self.snapshot_sha256,
        })
    }

    fn schedule_next_due_at_ms(&self) -> Option<i64> {
        // The interval/config digest includes each schedule's selected next
        // time. Keep the source-level wake as the earliest such value in the
        // projection, retained below by the digest reader.
        self.schedule_next_due
    }

    fn is_due(&self) -> bool {
        self.next_due_at_ms
            .is_some_and(|due| due <= self.observed_at_ms)
    }

    fn due_sources(&self) -> [bool; 3] {
        [
            self.schedule_due_count > 0 && !self.has_damage("interval_schedule"),
            self.cron_due_count > 0,
            self.goal_due_count > 0,
        ]
    }

    fn has_damage(&self, kind: &str) -> bool {
        self.damaged_subjects
            .iter()
            .any(|damage| damage.kind == kind)
    }

    fn cursor_advanced(&self, after: &Self, kind: &str) -> bool {
        match kind {
            "interval_schedule" => {
                schedule_cursor_advanced(&self.schedule_cursors, &after.schedule_cursors)
            }
            "manager_calendar" => source_row_removed(&self.cron_rows, &after.cron_rows),
            "goal_reminder" => source_row_removed(&self.goal_rows, &after.goal_rows),
            _ => false,
        }
    }

    fn source_outcome(
        &self,
        after: &Self,
        kind: &'static str,
        invoked: bool,
        additional_damage: &[DueSourceEvidence],
        additional_pending: &[DueSourceEvidence],
    ) -> DueSourceOutcome {
        let damaged_subjects = self
            .damaged_subjects
            .iter()
            .chain(additional_damage)
            .filter(|damage| damage.kind == kind)
            .cloned()
            .collect::<Vec<_>>();
        let pending_subjects = additional_pending
            .iter()
            .filter(|pending| pending.kind == kind)
            .cloned()
            .collect::<Vec<_>>();
        let cursor_advanced = self.cursor_advanced(after, kind);
        let disposition =
            DueSourceDisposition::observe(invoked, cursor_advanced, !damaged_subjects.is_empty());
        DueSourceOutcome {
            kind,
            disposition,
            cursor_advanced,
            damaged_subjects,
            pending_subjects,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DueSourceDisposition {
    Progressed,
    Idle,
    Degraded,
}

impl DueSourceDisposition {
    fn observe(invoked: bool, cursor_advanced: bool, degraded: bool) -> Self {
        if degraded {
            Self::Degraded
        } else if invoked && cursor_advanced {
            Self::Progressed
        } else {
            Self::Idle
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Progressed => "progressed",
            Self::Idle => "idle",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Debug, Clone)]
struct DueSourceOutcome {
    kind: &'static str,
    disposition: DueSourceDisposition,
    cursor_advanced: bool,
    damaged_subjects: Vec<DueSourceEvidence>,
    pending_subjects: Vec<DueSourceEvidence>,
}

impl DueSourceOutcome {
    fn value(&self) -> Value {
        let damaged_subjects = self
            .damaged_subjects
            .iter()
            .map(due_source_evidence_value)
            .collect::<Vec<_>>();
        let pending_subjects = self
            .pending_subjects
            .iter()
            .map(due_source_evidence_value)
            .collect::<Vec<_>>();
        json!({
            "kind":self.kind,
            "disposition":self.disposition.as_str(),
            "cursor_advanced":self.cursor_advanced,
            "damaged_subjects":damaged_subjects,
            "pending_subjects":pending_subjects,
        })
    }
}

fn due_source_evidence_value(subject: &DueSourceEvidence) -> Value {
    json!({
        "code":subject.code,
        "subject_identity":subject.evidence.subject_identity,
        "source_pointer":subject.evidence.source_pointer,
        "source_digest":subject.evidence.source_digest,
    })
}

fn aggregate_source_disposition(outcomes: &[DueSourceOutcome]) -> DueSourceDisposition {
    if outcomes
        .iter()
        .any(|outcome| outcome.disposition == DueSourceDisposition::Degraded)
    {
        DueSourceDisposition::Degraded
    } else if outcomes
        .iter()
        .any(|outcome| outcome.disposition == DueSourceDisposition::Progressed)
    {
        DueSourceDisposition::Progressed
    } else {
        DueSourceDisposition::Idle
    }
}

fn aggregate_scheduler_disposition(
    source_outcomes: &[DueSourceOutcome],
    check_recovery_outcome: &Value,
) -> DueSourceDisposition {
    if check_recovery_outcome["disposition"] == "degraded" {
        DueSourceDisposition::Degraded
    } else {
        aggregate_source_disposition(source_outcomes)
    }
}

fn check_recovery_failure(error: Error) -> Result<Value> {
    // Only closed malformed/mismatched receipt codes are degradable here.
    // Custody uncertainty and infrastructure errors remain fatal.
    let disposition = match error.code.as_str() {
        "CHECK_LAUNCH_RECEIPT_INVALID"
        | "CHECK_LAUNCH_RECEIPT_CONFLICT"
        | "CHECK_LAUNCH_UNKNOWN_RECEIPT_INVALID"
        | "CHECK_LAUNCH_DEPARTURE_INVALID"
            if error.secondary_codes.is_empty() =>
        {
            super::automation_reconcile::DomainErrorDisposition::Degraded {
                code: error.code.clone(),
            }
        }
        _ => super::automation_reconcile::DomainErrorDisposition::Fatal(error.clone()),
    };

    let code = match disposition {
        super::automation_reconcile::DomainErrorDisposition::Degraded { code } => code,
        super::automation_reconcile::DomainErrorDisposition::Fatal(error) => return Err(error),
    };
    let mut message_chars = error.message.chars();
    let message = message_chars
        .by_ref()
        .take(MAX_CHECK_RECOVERY_ERROR_MESSAGE_CHARS)
        .collect::<String>();
    let message_truncated = message_chars.next().is_some();
    let evidence = json!({
        "code":code,
        "message":message,
        "message_truncated":message_truncated,
        "secondary_codes":error.secondary_codes,
    });
    let source_digest = digest_json(&evidence)?;
    Ok(json!({
        "invoked":true,
        "disposition":"degraded",
        "pending_error":{
            "code":evidence["code"],
            "message":evidence["message"],
            "message_truncated":evidence["message_truncated"],
            "secondary_codes":evidence["secondary_codes"],
            "boundary":"check_run/recovery",
            "error_digest":source_digest,
        },
    }))
}

fn schedule_cursor_advanced(before: &[ScheduleCursor], after: &[ScheduleCursor]) -> bool {
    fn advanced(before: Option<i64>, after: Option<i64>) -> bool {
        match (before, after) {
            (None, Some(_)) => true,
            (Some(before), Some(after)) => after > before,
            _ => false,
        }
    }

    after.iter().any(
        |(schedule_id, after_observed, after_considered, after_admitted)| {
            let Some((_, observed, considered, admitted)) = before
                .iter()
                .find(|(before_id, _, _, _)| before_id == schedule_id)
            else {
                return after_observed.is_some()
                    || after_considered.is_some()
                    || after_admitted.is_some();
            };
            advanced(*observed, *after_observed)
                || advanced(*considered, *after_considered)
                || advanced(*admitted, *after_admitted)
        },
    )
}

fn source_row_removed(before: &[(String, String)], after: &[(String, String)]) -> bool {
    before
        .iter()
        .any(|(key, _)| after.iter().all(|(after_key, _)| after_key != key))
}

/// The Store owns the one local scheduler Module credential. A retained
/// identity is reused exactly across restarts; absent or mismatched private
/// config is a visible hold, never a reason to mint a new generation.
pub(crate) fn provision(
    tx: &Transaction<'_>,
    credential: &crate::model::Credential,
    retained_scope: Option<&DeclaredServiceScope>,
    config_sha256: &str,
) -> Result<DeclaredServiceScope> {
    if credential.client_id != SERVICE_ID
        || credential.token.len() < 32
        || !is_sha256(config_sha256)
    {
        return Err(Error::new(
            "AUTOMATION_SERVICE_IDENTITY_INVALID",
            "scheduler credential has the wrong reserved identity",
        ));
    }
    let previous_value = meta(tx, GENERATION_KEY)?;
    let existing_client = meta(tx, &format!("client:{SERVICE_ID}"))?;
    if previous_value.is_some() != existing_client.is_some() {
        return Err(Error::new(
            "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
            "scheduler generation and Module registration must be retained together",
        ));
    }
    let previous = previous_value
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
                    "scheduler generation is invalid",
                )
            })
        })
        .transpose()?;
    let client_key = format!("client:{SERVICE_ID}");
    if let Some(existing) = existing_client {
        if existing["role"] != "module" || existing["disabled"] == true {
            return Err(Error::new(
                "AUTOMATION_SERVICE_REGISTRATION_CONFLICT",
                "reserved scheduler Module identity is occupied or disabled",
            ));
        }
        let registration: SchedulerRegistration =
            serde_json::from_value(existing.get(REGISTRATION_FIELD).cloned().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_SERVICE_REGISTRATION_CONFLICT",
                    "reserved scheduler Module has no owned scheduler registration",
                )
            })?)
            .map_err(|_| {
                Error::new(
                    "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
                    "reserved scheduler Module registration is malformed",
                )
            })?;
        registration.scope.validate().map_err(|_| {
            Error::new(
                "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
                "reserved scheduler Module scope is invalid",
            )
        })?;
        if registration.schema_version != 1
            || registration.scope.purpose != DeclaredServicePurpose::AutomationScheduler
            || registration.scope.service_id != SERVICE_ID
            || registration.scope.generation == 0
            || Some(registration.scope.generation) != previous
            || registration
                .method_scope
                .iter()
                .map(String::as_str)
                .ne(METHOD_SCOPE)
            || !is_sha256(&registration.config_sha256)
            || existing["token_hash"]
                .as_str()
                .is_none_or(|hash| !is_sha256(hash))
        {
            return Err(Error::new(
                "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
                "reserved scheduler Module registration has changed shape",
            ));
        }
        if retained_scope != Some(&registration.scope)
            || existing["token_hash"] != model::digest(credential.token.as_bytes())
            || registration.config_sha256 != config_sha256
        {
            return Err(Error::new(
                "AUTOMATION_SERVICE_RECOVERY_REQUIRED",
                "retained scheduler identity cannot be replaced without its exact private config",
            ));
        }
        return Ok(registration.scope);
    }
    if previous.is_some() || retained_scope.is_some() {
        return Err(Error::new(
            "AUTOMATION_SERVICE_RECOVERY_REQUIRED",
            "retained scheduler identity is missing its matching registration",
        ));
    }
    let generation = 1;
    let scope = DeclaredServiceScope::new(
        DeclaredServicePurpose::AutomationScheduler,
        SERVICE_ID,
        generation,
    )
    .map_err(|_| {
        Error::new(
            "AUTOMATION_SERVICE_IDENTITY_INVALID",
            "scheduler service scope is invalid",
        )
    })?;
    let registration = SchedulerRegistration {
        schema_version: 1,
        scope: scope.clone(),
        method_scope: METHOD_SCOPE
            .iter()
            .map(|method| (*method).to_owned())
            .collect(),
        config_sha256: config_sha256.to_owned(),
    };
    set_meta(tx, GENERATION_KEY, &json!(generation))?;
    set_meta(
        tx,
        &client_key,
        &json!({
            "role":"module",
            "token_hash":model::digest(credential.token.as_bytes()),
            "disabled":false,
            "automation_scheduler":registration,
        }),
    )?;
    Ok(scope)
}

pub(crate) fn current_scope(db: &Connection) -> Result<DeclaredServiceScope> {
    let record = meta(db, &format!("client:{SERVICE_ID}"))?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_SERVICE_NOT_REGISTERED",
            "scheduler Module is absent",
        )
    })?;
    if record["role"] != "module" || record["disabled"] == true {
        return Err(Error::new(
            "AUTOMATION_SERVICE_NOT_REGISTERED",
            "scheduler Module is disabled or has another role",
        ));
    }
    let registration: SchedulerRegistration =
        serde_json::from_value(record.get(REGISTRATION_FIELD).cloned().ok_or_else(|| {
            Error::new(
                "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
                "scheduler scope is absent",
            )
        })?)
        .map_err(|_| {
            Error::new(
                "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
                "scheduler scope is malformed",
            )
        })?;
    if registration.schema_version != 1
        || registration.scope.purpose != DeclaredServicePurpose::AutomationScheduler
        || registration.scope.service_id != SERVICE_ID
        || registration.scope.generation == 0
        || meta(db, GENERATION_KEY)?.as_ref().and_then(Value::as_u64)
            != Some(registration.scope.generation)
        || registration
            .method_scope
            .iter()
            .map(String::as_str)
            .ne(METHOD_SCOPE)
        || !is_sha256(&registration.config_sha256)
        || record["token_hash"]
            .as_str()
            .is_none_or(|hash| !is_sha256(hash))
    {
        return Err(Error::new(
            "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
            "scheduler scope has an unsupported shape",
        ));
    }
    registration.scope.validate().map_err(|_| {
        Error::new(
            "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
            "scheduler scope identity is invalid",
        )
    })?;
    Ok(registration.scope)
}

fn owner_record(db: &Connection) -> Result<Option<SchedulerOwner>> {
    meta(db, OWNER_KEY)?
        .map(|value| {
            let owner: SchedulerOwner = serde_json::from_value(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_SERVICE_OWNER_CORRUPT",
                    "scheduler process owner receipt is malformed",
                )
            })?;
            owner.scope.validate().map_err(|_| {
                Error::new(
                    "AUTOMATION_SERVICE_OWNER_CORRUPT",
                    "scheduler process owner scope is invalid",
                )
            })?;
            if owner.schema_version != 1
                || owner.scope.purpose != DeclaredServicePurpose::AutomationScheduler
                || owner.scope.service_id != SERVICE_ID
                || owner.launch_id.is_empty()
                || owner.launch_id.len() > 128
                || !owner
                    .launch_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
                || (owner.phase == SchedulerOwnerPhase::Launching && owner.group_identity.is_some())
                || (owner.phase == SchedulerOwnerPhase::Active && owner.group_identity.is_none())
            {
                return Err(Error::new(
                    "AUTOMATION_SERVICE_OWNER_CORRUPT",
                    "scheduler process owner receipt has an unsupported shape",
                ));
            }
            Ok(owner)
        })
        .transpose()
}

fn save_owner(tx: &Transaction<'_>, owner: &SchedulerOwner) -> Result<()> {
    set_meta(tx, OWNER_KEY, &serde_json::to_value(owner)?)
}

/// Reserve the one Store-owned worker launch. A retained active process may
/// be replaced only after the exact service family is proven empty. An
/// interrupted `launching` receipt has no child identity and therefore holds
/// visibly rather than guessing whether spawn occurred.
pub(crate) fn begin_owner(
    tx: &Transaction<'_>,
    scope: &DeclaredServiceScope,
    launch_id: &str,
    owner_token: &str,
) -> Result<()> {
    if current_scope(tx)? != *scope {
        return Err(Error::new(
            "AUTOMATION_SERVICE_SCOPE_STALE",
            "scheduler launch scope is no longer current and enabled",
        ));
    }
    if launch_id.is_empty()
        || launch_id.len() > 128
        || !launch_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
    {
        return Err(Error::invalid("scheduler launch identity is invalid"));
    }
    if let Some(owner) = owner_record(tx)? {
        if owner.scope != *scope {
            return Err(Error::new(
                "AUTOMATION_SERVICE_OWNER_SCOPE_STALE",
                "retained scheduler process belongs to another scope",
            ));
        }
        match owner.phase {
            SchedulerOwnerPhase::Launching => {
                return Err(Error::new(
                    "AUTOMATION_SERVICE_OWNER_UNKNOWN",
                    "previous scheduler launch has no process departure proof",
                ));
            }
            SchedulerOwnerPhase::Active => {
                let identity = owner.group_identity.as_ref().ok_or_else(|| {
                    Error::new(
                        "AUTOMATION_SERVICE_OWNER_CORRUPT",
                        "active scheduler owner has no process identity",
                    )
                })?;
                if !swarm_automation::service_owner_family_empty(identity, owner_token).map_err(
                    |_| {
                        Error::new(
                            "AUTOMATION_SERVICE_OWNER_PROOF_UNAVAILABLE",
                            "prior scheduler process family cannot be inspected safely",
                        )
                    },
                )? {
                    return Err(Error::new(
                        "AUTOMATION_SERVICE_OWNER_RETAINED",
                        "previous scheduler process family is still live",
                    ));
                }
            }
            SchedulerOwnerPhase::Departed => {}
        }
    }
    save_owner(
        tx,
        &SchedulerOwner {
            schema_version: 1,
            scope: scope.clone(),
            launch_id: launch_id.to_owned(),
            phase: SchedulerOwnerPhase::Launching,
            group_identity: None,
        },
    )
}

pub(crate) fn activate_owner(
    tx: &Transaction<'_>,
    scope: &DeclaredServiceScope,
    launch_id: &str,
    identity: Value,
    owner_token: &str,
) -> Result<()> {
    let mut owner = owner_record(tx)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_SERVICE_OWNER_STALE",
            "scheduler launch reservation is missing",
        )
    })?;
    if current_scope(tx)? != *scope
        || owner.scope != *scope
        || owner.launch_id != launch_id
        || owner.phase != SchedulerOwnerPhase::Launching
        || swarm_automation::service_owner_family_empty(&identity, owner_token).map_err(|_| {
            Error::new(
                "AUTOMATION_SERVICE_OWNER_PROOF_UNAVAILABLE",
                "scheduler process family cannot be inspected safely",
            )
        })?
    {
        return Err(Error::new(
            "AUTOMATION_SERVICE_OWNER_STALE",
            "scheduler process identity does not match its current launch reservation",
        ));
    }
    owner.phase = SchedulerOwnerPhase::Active;
    owner.group_identity = Some(identity);
    save_owner(tx, &owner)
}

pub(crate) fn abandon_unspawned_owner(
    tx: &Transaction<'_>,
    scope: &DeclaredServiceScope,
    launch_id: &str,
) -> Result<()> {
    let mut owner = owner_record(tx)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_SERVICE_OWNER_STALE",
            "scheduler launch reservation is missing",
        )
    })?;
    if owner.scope != *scope
        || owner.launch_id != launch_id
        || owner.phase != SchedulerOwnerPhase::Launching
    {
        return Err(Error::new(
            "AUTOMATION_SERVICE_OWNER_STALE",
            "scheduler launch reservation changed before spawn failure was recorded",
        ));
    }
    if owner.group_identity.is_some() {
        return Err(Error::new(
            "AUTOMATION_SERVICE_OWNER_STALE",
            "unspawned scheduler receipt unexpectedly has a process identity",
        ));
    }
    owner.phase = SchedulerOwnerPhase::Departed;
    save_owner(tx, &owner)
}

pub(crate) fn abandon_exited_owner(
    tx: &Transaction<'_>,
    scope: &DeclaredServiceScope,
    launch_id: &str,
    identity: &Value,
    owner_token: &str,
) -> Result<()> {
    let mut owner = owner_record(tx)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_SERVICE_OWNER_STALE",
            "scheduler launch reservation is missing",
        )
    })?;
    if owner.scope != *scope
        || owner.launch_id != launch_id
        || owner.phase != SchedulerOwnerPhase::Launching
        || owner.group_identity.is_some()
        || !swarm_automation::service_owner_family_empty(identity, owner_token).map_err(|_| {
            Error::new(
                "AUTOMATION_SERVICE_OWNER_PROOF_UNAVAILABLE",
                "scheduler process family cannot be inspected safely",
            )
        })?
    {
        return Err(Error::new(
            "AUTOMATION_SERVICE_OWNER_RETAINED",
            "scheduler process family has no exact departure proof",
        ));
    }
    owner.phase = SchedulerOwnerPhase::Departed;
    save_owner(tx, &owner)
}

pub(crate) fn finish_owner(
    tx: &Transaction<'_>,
    scope: &DeclaredServiceScope,
    launch_id: &str,
    identity: &Value,
    owner_token: &str,
) -> Result<()> {
    let mut owner = owner_record(tx)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_SERVICE_OWNER_STALE",
            "scheduler process owner receipt is missing",
        )
    })?;
    if owner.scope != *scope
        || owner.launch_id != launch_id
        || owner.phase != SchedulerOwnerPhase::Active
        || owner.group_identity.as_ref() != Some(identity)
        || !swarm_automation::service_owner_family_empty(identity, owner_token).map_err(|_| {
            Error::new(
                "AUTOMATION_SERVICE_OWNER_PROOF_UNAVAILABLE",
                "scheduler process family cannot be inspected safely",
            )
        })?
    {
        return Err(Error::new(
            "AUTOMATION_SERVICE_OWNER_RETAINED",
            "scheduler process family has no exact departure proof",
        ));
    }
    owner.phase = SchedulerOwnerPhase::Departed;
    save_owner(tx, &owner)
}

/// Authenticated host/worker entry point. Each call revalidates the exact
/// Store-issued Module registration; admission then invokes only the
/// existing durable domain reconcilers and returns a fresh page for readback.
pub(crate) async fn call(
    store: &super::Store,
    principal: crate::model::Principal,
    method: &str,
    params: Value,
) -> Result<Value> {
    let owner_token = store
        .automation_scheduler_owner_token
        .as_ref()
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_SERVICE_NOT_CONFIGURED",
                "the independent scheduler service is not configured",
            )
        })?
        .clone();
    if method == METHOD_SCOPE[0] {
        let config = store.config.clone();
        return store
            .run(move |db| {
                let principal = super::current_principal(db, principal)?;
                read(db, &principal, &params, &config, &owner_token)
            })
            .await;
    }
    if method != METHOD_SCOPE[1] {
        return Err(Error::new("METHOD_NOT_FOUND", method));
    }
    let config = store.config.clone();
    let decision = store
        .run(move |db| {
            let principal = super::current_principal(db, principal)?;
            prepare_admit(db, &principal, &params, &config, &owner_token)
        })
        .await?;
    let cut = match decision {
        Ok(cut) => cut,
        Err(receipt) => return Ok(receipt),
    };
    let due = cut.due_sources();
    // Goal reminders are independent of CheckRun recovery and schedule
    // admission, so reconcile them before touching either source.
    let mut goal_next_due_at_ms = None;
    if due[2] {
        goal_next_due_at_ms = store.reconcile_goals_once(cut.observed_at_ms).await?;
    }
    let mut schedule_pending = Vec::new();
    if due[0] {
        for schedule in store.schedule_configs() {
            let result = store
                .consider_scheduled(schedule.clone(), cut.observed_at_ms)
                .await;
            if let Err(error) = result {
                let schedule = schedule.clone();
                let pending = store
                    .run(move |db| {
                        let mut retained_pending = None;
                        super::reconcile_automation_domain(
                            db,
                            "interval_schedule",
                            |tx| {
                                retained_pending =
                                    Some(isolate_schedule_error(tx, &schedule, error)?);
                                Ok(Value::Null)
                            },
                            |error| {
                                super::automation_reconcile::DomainErrorDisposition::Fatal(error)
                            },
                        )?;
                        retained_pending.ok_or_else(|| {
                            Error::new(
                                "SCHEDULER_SOURCE_DISPOSITION_MISSING",
                                "schedule isolation committed without a retained pending disposition",
                            )
                        })
                    })
                    .await?;
                schedule_pending.push(pending);
            }
        }
    }
    let mut calendar_next_due_at_ms = None;
    if due[1] {
        calendar_next_due_at_ms = store
            .reconcile_automation_cron_once(MAX_SOURCE_PAGE, cut.observed_at_ms)
            .await?;
    }
    // CheckRun recovery follows due automation work. Recognized receipt defects
    // return a bounded degraded result; all other check and Store errors remain
    // fatal after the independent due sources have completed.
    let check_recovery_outcome = if due[0] || due[1] {
        match store.reconcile_checks_once().await {
            Ok(()) => json!({"invoked":true,"disposition":"completed"}),
            Err(error) => check_recovery_failure(error)?,
        }
    } else {
        json!({"invoked":false,"disposition":"not_due"})
    };
    let scope = cut.scope.clone();
    let config = store.config.clone();
    let latest = store
        .run(move |db| {
            let latest_scope = current_scope(db)?;
            if latest_scope != scope {
                return Ok(Err(json!({
                    "schema_version":1,
                    "scope":scope,
                    "disposition":"page_stale",
                    "next_due_at_ms":null,
                })));
            }
            project_due_page(db, &config, latest_scope).map(Ok)
        })
        .await?;
    let latest = match latest {
        Ok(page) => page,
        Err(receipt) => return Ok(receipt),
    };
    let source_outcomes = vec![
        cut.source_outcome(&latest, "interval_schedule", due[0], &[], &schedule_pending),
        cut.source_outcome(&latest, "manager_calendar", due[1], &[], &[]),
        cut.source_outcome(&latest, "goal_reminder", due[2], &[], &[]),
    ];
    let disposition = aggregate_scheduler_disposition(&source_outcomes, &check_recovery_outcome);
    // Keep the admission receipt accepted by the existing worker protocol;
    // source_disposition carries the truthful aggregate for this Store call.
    Ok(json!({
        "schema_version":1,
        "scope":cut.scope,
        "disposition":"reconcilers_completed",
        "source_disposition":disposition.as_str(),
        "reconcilers_invoked":{
            "interval_schedule":due[0],
            "manager_calendar":due[1],
            "goal_reminder":due[2],
            "check_recovery":due[0] || due[1],
        },
        "check_recovery":check_recovery_outcome,
        "calendar_next_due_at_ms":calendar_next_due_at_ms,
        "goal_next_due_at_ms":goal_next_due_at_ms,
        "next_due_at_ms":latest.next_due_at_ms,
        "source_outcomes":source_outcomes.iter().map(DueSourceOutcome::value).collect::<Vec<_>>(),
    }))
}

fn registration(
    db: &Connection,
    principal: &Principal,
    method: &str,
    owner_token: &str,
) -> Result<SchedulerRegistration> {
    if principal.role != Role::Module || principal.client_id != SERVICE_ID {
        return Err(Error::new(
            "FORBIDDEN",
            "scheduler calls require the host-issued automation service Module",
        ));
    }
    let client = super::meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "scheduler service is no longer registered"))?;
    if client["disabled"] == true || client["role"] != "module" {
        return Err(Error::new("UNAUTHORIZED", "scheduler service is disabled"));
    }
    let token_hash = client["token_hash"].as_str().unwrap_or_default();
    if !is_sha256(token_hash) {
        return Err(Error::new(
            "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
            "scheduler registration credential digest is invalid",
        ));
    }
    let scoped: SchedulerRegistration = serde_json::from_value(
        client
            .get(REGISTRATION_FIELD)
            .cloned()
            .ok_or_else(|| Error::new("FORBIDDEN", "Module has no scheduler service scope"))?,
    )
    .map_err(|_| {
        Error::new(
            "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
            "scheduler service scope is malformed",
        )
    })?;
    scoped.scope.validate().map_err(|_| {
        Error::new(
            "AUTOMATION_SERVICE_REGISTRATION_CORRUPT",
            "scheduler service identity is invalid",
        )
    })?;
    if scoped.schema_version != 1
        || scoped.scope.purpose != DeclaredServicePurpose::AutomationScheduler
        || scoped.scope.service_id != principal.client_id
        || scoped.scope.generation == 0
        || meta(db, GENERATION_KEY)?.as_ref().and_then(Value::as_u64)
            != Some(scoped.scope.generation)
        || scoped
            .method_scope
            .iter()
            .map(String::as_str)
            .ne(METHOD_SCOPE)
        || !is_sha256(&scoped.config_sha256)
        || !METHOD_SCOPE.contains(&method)
    {
        return Err(Error::new(
            "FORBIDDEN",
            "method is outside the current scheduler service scope",
        ));
    }
    require_active_owner(db, &scoped.scope, owner_token)?;
    Ok(scoped)
}

fn require_active_owner(
    db: &Connection,
    scope: &DeclaredServiceScope,
    owner_token: &str,
) -> Result<()> {
    let owner = owner_record(db)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_SERVICE_OWNER_PENDING",
            "host has not recorded the scheduler process identity yet",
        )
    })?;
    if owner.scope != *scope || owner.phase != SchedulerOwnerPhase::Active {
        return Err(Error::new(
            "AUTOMATION_SERVICE_OWNER_PENDING",
            "scheduler process is not active under the current Store scope",
        ));
    }
    let identity = owner.group_identity.as_ref().ok_or_else(|| {
        Error::new(
            "AUTOMATION_SERVICE_OWNER_CORRUPT",
            "active scheduler owner has no process identity",
        )
    })?;
    if swarm_automation::service_owner_family_empty(identity, owner_token).map_err(|_| {
        Error::new(
            "AUTOMATION_SERVICE_OWNER_PROOF_UNAVAILABLE",
            "scheduler process family cannot be inspected safely",
        )
    })? {
        return Err(Error::new(
            "AUTOMATION_SERVICE_OWNER_GONE",
            "scheduler process family is no longer active",
        ));
    }
    Ok(())
}

pub(crate) fn read(
    db: &Connection,
    principal: &Principal,
    params: &Value,
    config: &Config,
    owner_token: &str,
) -> Result<Value> {
    if !config.automation_scheduler.enabled {
        return Err(Error::new(
            "AUTOMATION_SCHEDULER_DISABLED",
            "the independent scheduler service is not enabled",
        ));
    }
    model::fields(params, &["scope"])?;
    let service = registration(db, principal, METHOD_SCOPE[0], owner_token)?;
    let requested: DeclaredServiceScope = serde_json::from_value(
        params
            .get("scope")
            .cloned()
            .ok_or_else(|| Error::invalid("scope is required"))?,
    )
    .map_err(|_| Error::invalid("scope is invalid"))?;
    if requested != service.scope {
        return Err(Error::new(
            "AUTOMATION_SERVICE_SCOPE_STALE",
            "page scope differs from the authenticated Store registration",
        ));
    }
    Ok(project_due_page(db, config, service.scope)?.value())
}

/// Validate an admission cut using the authenticated service registration.
/// The host then invokes the existing Store reconcilers; their domain cursors
/// and normal Operations remain the replay boundary.
fn prepare_admit(
    db: &Connection,
    principal: &Principal,
    params: &Value,
    config: &Config,
    owner_token: &str,
) -> Result<std::result::Result<DueProjection, Value>> {
    if !config.automation_scheduler.enabled {
        return Err(Error::new(
            "AUTOMATION_SCHEDULER_DISABLED",
            "the independent scheduler service is not enabled",
        ));
    }
    model::fields(
        params,
        &[
            "client_request_id",
            "scope",
            "expected_snapshot_sha256",
            "observed_at_ms",
        ],
    )?;
    let tx = Transaction::new_unchecked(db, TransactionBehavior::Immediate)?;
    let service = registration(&tx, principal, METHOD_SCOPE[1], owner_token)?;
    let requested_scope: DeclaredServiceScope = serde_json::from_value(params["scope"].clone())
        .map_err(|_| Error::invalid("scope is invalid"))?;
    let request_id = model::text(params, "client_request_id")?;
    let expected_digest = model::text(params, "expected_snapshot_sha256")?;
    let observed_at_ms = params["observed_at_ms"]
        .as_i64()
        .filter(|value| *value >= 0)
        .ok_or_else(|| Error::invalid("observed_at_ms must be a nonnegative integer"))?;
    if requested_scope != service.scope || request_id.len() > 128 || !is_sha256(expected_digest) {
        return Err(Error::new(
            "AUTOMATION_SERVICE_SCOPE_STALE",
            "scheduler admission identity or source page is invalid",
        ));
    }
    let expected_request_id = format!(
        "automation-pulse-{}",
        digest_json(&json!({
            "schema_version":1,
            "scope":requested_scope,
            "snapshot_sha256":expected_digest,
        }))?
    );
    if request_id != expected_request_id {
        return Err(Error::invalid(
            "client_request_id must bind the exact scheduler page cut",
        ));
    }
    let current = project_due_page(&tx, config, service.scope)?;
    if current.snapshot_sha256 != expected_digest
        || current.observed_at_ms < observed_at_ms
        || current.observed_at_ms.saturating_sub(observed_at_ms) > 60_000
    {
        let receipt = json!({
            "schema_version":1,
            "scope":current.scope,
            "disposition":"page_stale",
            "next_due_at_ms":current.next_due_at_ms,
        });
        tx.commit()?;
        return Ok(Err(receipt));
    }
    if !current.is_due() {
        let receipt = json!({
            "schema_version":1,
            "scope":current.scope,
            "disposition":"already_observed",
            "next_due_at_ms":current.next_due_at_ms,
        });
        tx.commit()?;
        return Ok(Err(receipt));
    }
    for damage in current
        .damaged_subjects
        .iter()
        .filter(|damage| damage.kind != "goal_reminder")
    {
        quarantine_due_source_damage(&tx, damage, current.observed_at_ms)?;
    }
    tx.commit()?;
    Ok(Ok(current))
}

fn quarantine_due_source_damage(
    tx: &Transaction<'_>,
    damage: &DueSourceEvidence,
    now_ms: i64,
) -> Result<()> {
    let quarantine_key = super::automation_reconcile::quarantine_record_key(
        SCHEDULER_QUARANTINE_PREFIX,
        &damage.evidence,
    )?;
    super::automation_reconcile::persist_quarantine(
        tx,
        &quarantine_key,
        &damage.code,
        damage.evidence.clone(),
        now_ms,
    )?;

    if damage.kind != "manager_calendar" {
        return Ok(());
    }
    let (Some(source_key), Some(source_raw)) = (&damage.source_key, &damage.source_raw) else {
        return Err(Error::new(
            "AUTOMATION_DUE_EVIDENCE_INCOMPLETE",
            "damaged manager-calendar subject has no exact source row for quarantine CAS",
        ));
    };
    let key_digest = model::digest(source_key.as_bytes());
    let expected_identity = format!("meta-key-sha256:{key_digest}");
    let expected_pointer = format!("meta/key-sha256:{key_digest}");
    let expected_digest = model::digest(source_raw.as_bytes());
    if damage.evidence.subject_identity != expected_identity
        || damage.evidence.source_pointer.as_deref() != Some(expected_pointer.as_str())
        || damage.evidence.source_digest.as_deref() != Some(expected_digest.as_str())
    {
        return Err(Error::new(
            "AUTOMATION_DUE_EVIDENCE_INVALID",
            "damaged manager-calendar evidence does not bind its exact source row",
        ));
    }
    let deleted = tx.execute(
        "DELETE FROM meta WHERE key=?1 AND value_json=?2",
        params![source_key, source_raw],
    )?;
    if deleted != 1 {
        return Err(Error::new(
            "AUTOMATION_DUE_SUBJECT_CHANGED",
            "manager-calendar source row changed before quarantine CAS",
        ));
    }
    Ok(())
}

fn isolate_schedule_error(
    tx: &Transaction<'_>,
    schedule: &crate::scheduler::ScheduleConfig,
    error: Error,
) -> Result<DueSourceEvidence> {
    let evidence = super::schedules::scheduler_source_evidence(schedule)?;
    let disposition: super::automation_reconcile::SubjectDisposition<()> =
        super::automation_reconcile::with_subject_savepoint(
            tx,
            || Err(error),
            super::schedules::classify_scheduler_error,
        )?;
    let code = match disposition {
        super::automation_reconcile::SubjectDisposition::Pending { code, .. } => code,
        super::automation_reconcile::SubjectDisposition::Applied(()) => {
            return Err(Error::new(
                "SCHEDULER_SOURCE_DISPOSITION_INVALID",
                "schedule error classifier returned an applied disposition for a failed subject",
            ));
        }
        super::automation_reconcile::SubjectDisposition::Quarantined { .. }
        | super::automation_reconcile::SubjectDisposition::Skipped { .. } => {
            return Err(Error::new(
                "SCHEDULER_SOURCE_DISPOSITION_INVALID",
                "schedule capacity must remain pending and cannot be quarantined or skipped",
            ));
        }
    };
    Ok(DueSourceEvidence {
        kind: "interval_schedule",
        code,
        evidence,
        source_key: None,
        source_raw: None,
    })
}

fn project_due_page(
    db: &Connection,
    config: &Config,
    scope: DeclaredServiceScope,
) -> Result<DueProjection> {
    let observed_at_ms = model::now_ms()?;
    let ScheduleSourceSnapshot {
        due_count: schedule_due,
        next_due_at_ms: schedule_next,
        cursor_digest: schedule_digest,
        schedule_cursors,
        damaged_subjects: schedule_damage,
    } = schedule_source_snapshot(db, config, observed_at_ms)?;
    let cron = due_index_snapshot(db, CRON_DUE_PREFIX, 20, "manager_calendar", observed_at_ms)?;
    let goal = due_index_snapshot(db, GOAL_DUE_PREFIX, 19, "goal_reminder", observed_at_ms)?;
    let mut damaged_subjects = schedule_damage;
    damaged_subjects.extend(cron.damaged_subjects.iter().cloned());
    damaged_subjects.extend(goal.damaged_subjects.iter().cloned());
    let cron_next = cron.next_due_at_ms;
    let goal_next = goal.next_due_at_ms;
    let cron_due = cron.due_count;
    let goal_due = goal.due_count;
    let cron_digest = digest_index_rows(&cron.rows)?;
    let goal_digest = digest_index_rows(&goal.rows)?;
    let next_due_at_ms = [schedule_next, cron_next, goal_next]
        .into_iter()
        .flatten()
        .min();
    let source_identity = json!({
        "schema_version":1,
        "scope":scope,
        "sources":[
            {"kind":"interval_schedule","next_due_at_ms":schedule_next,"due_count":schedule_due,"cursor_digest":schedule_digest},
            {"kind":"manager_calendar","next_due_at_ms":cron_next,"due_count":cron_due,"cursor_digest":cron_digest},
            {"kind":"goal_reminder","next_due_at_ms":goal_next,"due_count":goal_due,"cursor_digest":goal_digest},
        ],
    });
    let snapshot_sha256 = digest_json(&source_identity)?;
    Ok(DueProjection {
        scope,
        observed_at_ms,
        next_due_at_ms,
        schedule_due_count: bounded_count(schedule_due),
        schedule_digest,
        schedule_next_due: schedule_next,
        cron_due_at_ms: cron_next,
        cron_due_count: bounded_count(cron_due),
        cron_digest,
        goal_due_at_ms: goal_next,
        goal_due_count: bounded_count(goal_due),
        goal_digest,
        snapshot_sha256,
        damaged_subjects,
        schedule_cursors,
        cron_rows: cron.rows,
        goal_rows: goal.rows,
    })
}

fn schedule_source_snapshot(
    db: &Connection,
    config: &Config,
    observed_at_ms: i64,
) -> Result<ScheduleSourceSnapshot> {
    let raw_registry = raw_meta(db, SCHEDULE_REGISTRY_KEY)?;
    if let Some(raw) = raw_registry.as_deref()
        && serde_json::from_str::<Value>(raw).is_err()
    {
        return damaged_schedule_registry(config, raw, "SCHEDULE_STATE_INVALID");
    }
    let schedule_status = match super::schedules::status(db, &config.schedules, observed_at_ms) {
        Ok(status) => status,
        Err(error)
            if matches!(
                error.code.as_str(),
                "SCHEDULE_STATE_INVALID" | "SCHEDULE_STATE_VERSION"
            ) && raw_registry.is_some() =>
        {
            let code = if error.code == "SCHEDULE_STATE_VERSION" {
                "SCHEDULE_STATE_VERSION"
            } else {
                "SCHEDULE_STATE_INVALID"
            };
            return damaged_schedule_registry(
                config,
                raw_registry.as_deref().unwrap_or_default(),
                code,
            );
        }
        Err(error) => return Err(error),
    };
    let schedule_items = schedule_status["items"].as_array().ok_or_else(|| {
        Error::new(
            "SCHEDULER_SOURCE_PROJECTION_INVALID",
            "schedule status does not contain its closed item list",
        )
    })?;
    let mut schedule_cursors = Vec::with_capacity(schedule_items.len());
    for item in schedule_items {
        let schedule_id = item["schedule_id"].as_str().ok_or_else(|| {
            Error::new(
                "SCHEDULER_SOURCE_PROJECTION_INVALID",
                "schedule status item has no retained identity",
            )
        })?;
        schedule_cursors.push((
            schedule_id.to_owned(),
            item["last_observed_due_slot"].as_i64(),
            item["last_considered_slot"].as_i64(),
            item["last_admitted_slot"].as_i64(),
        ));
    }
    let due_count = schedule_items
        .iter()
        .filter(|item| {
            item["next_due_ms"]
                .as_i64()
                .is_some_and(|due| due <= observed_at_ms)
        })
        .count();
    let next_due = schedule_items
        .iter()
        .filter_map(|item| item["next_due_ms"].as_i64())
        .min();
    let cursor_digest = digest_json(&json!({
        "definitions":config.schedules,
        "state":schedule_status,
    }))?;
    Ok(ScheduleSourceSnapshot {
        due_count,
        next_due_at_ms: next_due,
        cursor_digest,
        schedule_cursors,
        damaged_subjects: Vec::new(),
    })
}

fn damaged_schedule_registry(
    config: &Config,
    raw_registry: &str,
    code: &'static str,
) -> Result<ScheduleSourceSnapshot> {
    let evidence =
        super::automation_reconcile::automation_entry_evidence(SCHEDULE_REGISTRY_KEY, raw_registry);
    let cursor_digest = digest_json(&json!({
        "definitions":config.schedules,
        "damaged_registry_sha256":evidence.source_digest,
    }))?;
    Ok(ScheduleSourceSnapshot {
        due_count: 1,
        next_due_at_ms: Some(0),
        cursor_digest,
        schedule_cursors: Vec::new(),
        damaged_subjects: vec![DueSourceEvidence {
            kind: "interval_schedule",
            code: code.to_owned(),
            evidence,
            source_key: None,
            source_raw: None,
        }],
    })
}

pub(super) fn has_demand(db: &Connection, config: &Config) -> Result<bool> {
    if !config.automation_scheduler.enabled {
        return Ok(false);
    }
    let scope = current_scope(db)?;
    Ok(project_due_page(db, config, scope)?
        .next_due_at_ms
        .is_some())
}

fn due_index_snapshot(
    db: &Connection,
    prefix: &str,
    timestamp_width: usize,
    kind: &'static str,
    observed_at_ms: i64,
) -> Result<DueIndexSnapshot> {
    let upper = format!("{prefix}~");
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT ?3",
    )?;
    let rows = statement
        .query_map(params![prefix, upper, MAX_SOURCE_PAGE as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut due_times = Vec::with_capacity(rows.len());
    let mut damaged_subjects = Vec::new();
    for (key, raw) in &rows {
        let due = match parse_due_key(key, prefix, timestamp_width) {
            Ok(due) => due,
            Err(error) if error.code == "AUTOMATION_DUE_INDEX_INVALID" => {
                damaged_subjects.push(due_index_damage(kind, key, raw));
                continue;
            }
            Err(error) => return Err(error),
        };
        let value: Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => {
                damaged_subjects.push(due_index_damage(kind, key, raw));
                continue;
            }
        };
        if kind == "manager_calendar" && !valid_cron_due_index(key, &value, due) {
            damaged_subjects.push(due_index_damage(kind, key, raw));
            continue;
        }
        due_times.push(due);
    }
    let valid_next = due_times.iter().copied().min();
    let next_due_at_ms = if damaged_subjects.is_empty() {
        valid_next
    } else {
        Some(0)
    };
    let due_count = due_times
        .iter()
        .filter(|due| **due <= observed_at_ms)
        .count()
        .saturating_add(if kind == "goal_reminder" {
            damaged_subjects.len()
        } else {
            0
        });
    Ok(DueIndexSnapshot {
        next_due_at_ms,
        due_count,
        rows,
        damaged_subjects,
    })
}

fn due_index_damage(kind: &'static str, key: &str, raw: &str) -> DueSourceEvidence {
    DueSourceEvidence {
        kind,
        code: match kind {
            "manager_calendar" => "AUTOMATION_CRON_STATE_INVALID",
            "goal_reminder" => "GOAL_RECORD_CORRUPT",
            _ => unreachable!("closed due source kind"),
        }
        .to_owned(),
        evidence: super::automation_reconcile::automation_entry_evidence(key, raw),
        source_key: Some(key.to_owned()),
        source_raw: Some(raw.to_owned()),
    }
}

fn valid_cron_due_index(key: &str, value: &Value, due_at_ms: i64) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let required = [
        "schema_version",
        "logical_id",
        "origin_manager_id",
        "current_owner_manager_id",
        "project_id",
        "automation_id",
        "generation",
        "wake_at_ms",
    ];
    if object.len() != required.len() || required.iter().any(|field| !object.contains_key(*field)) {
        return false;
    }
    let logical_id = object["logical_id"].as_str().unwrap_or_default();
    let generation = object["generation"].as_str().unwrap_or_default();
    object["schema_version"].as_u64() == Some(1)
        && is_sha256(logical_id)
        && [
            "origin_manager_id",
            "current_owner_manager_id",
            "project_id",
            "automation_id",
        ]
        .iter()
        .all(|field| {
            object[*field]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        })
        && is_sha256(generation)
        && object["wake_at_ms"].as_i64() == Some(due_at_ms)
        && key == format!("{CRON_DUE_PREFIX}{due_at_ms:020}:{logical_id}")
}

fn raw_meta(db: &Connection, key: &str) -> Result<Option<String>> {
    Ok(db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?)
}

fn parse_due_key(key: &str, prefix: &str, timestamp_width: usize) -> Result<i64> {
    let tail = key
        .strip_prefix(prefix)
        .ok_or_else(|| Error::new("AUTOMATION_DUE_INDEX_INVALID", "due index key is malformed"))?;
    let (timestamp, identity) = tail
        .split_at_checked(timestamp_width)
        .ok_or_else(|| Error::new("AUTOMATION_DUE_INDEX_INVALID", "due index key is malformed"))?;
    if timestamp.len() != timestamp_width
        || !timestamp.bytes().all(|byte| byte.is_ascii_digit())
        || identity.strip_prefix(':').is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "AUTOMATION_DUE_INDEX_INVALID",
            "due index key is malformed",
        ));
    }
    let due = timestamp.parse::<i64>().map_err(|_| {
        Error::new(
            "AUTOMATION_DUE_INDEX_INVALID",
            "due index time is malformed",
        )
    })?;
    if due < 0 {
        return Err(Error::new(
            "AUTOMATION_DUE_INDEX_INVALID",
            "due index time is negative",
        ));
    }
    Ok(due)
}

fn digest_index_rows(rows: &[(String, String)]) -> Result<String> {
    let projected = rows
        .iter()
        .map(|(key, value)| json!([key, model::digest(value.as_bytes())]))
        .collect::<Vec<_>>();
    digest_json(&json!(projected))
}

fn digest_json(value: &Value) -> Result<String> {
    Ok(model::digest(model::canonical(value)?.as_bytes()))
}

fn bounded_count(count: usize) -> u32 {
    count.min(MAX_SOURCE_PAGE) as u32
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
#[path = "automation_scheduler_fixture.rs"]
mod fixture;
