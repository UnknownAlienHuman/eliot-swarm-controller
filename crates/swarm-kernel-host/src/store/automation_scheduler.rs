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
use rusqlite::{Connection, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use swarm_contracts::{DeclaredServicePurpose, DeclaredServiceScope};

pub(super) const SERVICE_ID: &str = "automation-scheduler-v1";
const REGISTRATION_FIELD: &str = "automation_scheduler";
const GENERATION_KEY: &str = "automation_scheduler:v1:generation";
const OWNER_KEY: &str = "automation_scheduler:v1:owner";
const METHOD_SCOPE: [&str; 2] = ["automation.scheduler.page", "automation.scheduler.admit"];
const MAX_SOURCE_PAGE: usize = 32;
const CRON_DUE_PREFIX: &str = "automation:v1:cron:due:";
const GOAL_DUE_PREFIX: &str = "goals:v1:due:";

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
            self.schedule_due_count > 0,
            self.cron_due_count > 0,
            self.goal_due_count > 0,
        ]
    }
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
    if due[0] || due[1] {
        store.reconcile_checks_once().await?;
    }
    if due[0] {
        for schedule in store.schedule_configs() {
            let _ = store
                .consider_scheduled(schedule, cut.observed_at_ms)
                .await?;
        }
    }
    let mut calendar_next_due_at_ms = None;
    if due[1] {
        calendar_next_due_at_ms = store
            .reconcile_automation_cron_once(MAX_SOURCE_PAGE, cut.observed_at_ms)
            .await?;
    }
    let mut goal_next_due_at_ms = None;
    if due[2] {
        goal_next_due_at_ms = store.reconcile_goals_once(cut.observed_at_ms).await?;
    }
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
    Ok(json!({
        "schema_version":1,
        "scope":cut.scope,
        "disposition":"reconcilers_completed",
        "reconcilers_invoked":{
            "interval_schedule":due[0],
            "manager_calendar":due[1],
            "goal_reminder":due[2],
        },
        "calendar_next_due_at_ms":calendar_next_due_at_ms,
        "goal_next_due_at_ms":goal_next_due_at_ms,
        "next_due_at_ms":latest.next_due_at_ms,
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
pub(crate) fn prepare_admit(
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
    let service = registration(db, principal, METHOD_SCOPE[1], owner_token)?;
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
    let current = project_due_page(db, config, service.scope)?;
    if current.snapshot_sha256 != expected_digest
        || current.observed_at_ms < observed_at_ms
        || current.observed_at_ms.saturating_sub(observed_at_ms) > 60_000
    {
        return Ok(Err(json!({
            "schema_version":1,
            "scope":current.scope,
            "disposition":"page_stale",
            "next_due_at_ms":current.next_due_at_ms,
        })));
    }
    if !current.is_due() {
        return Ok(Err(json!({
            "schema_version":1,
            "scope":current.scope,
            "disposition":"already_observed",
            "next_due_at_ms":current.next_due_at_ms,
        })));
    }
    Ok(Ok(current))
}

fn project_due_page(
    db: &Connection,
    config: &Config,
    scope: DeclaredServiceScope,
) -> Result<DueProjection> {
    let observed_at_ms = model::now_ms()?;
    let schedule_status = super::schedules::status(db, &config.schedules, observed_at_ms)?;
    let schedule_items = schedule_status["items"]
        .as_array()
        .ok_or_else(|| Error::new("SCHEDULE_STATE_INVALID", "schedule status has no items"))?;
    let schedule_due = schedule_items
        .iter()
        .filter(|item| {
            item["next_due_ms"]
                .as_i64()
                .is_some_and(|due| due <= observed_at_ms)
        })
        .count();
    let schedule_next = schedule_items
        .iter()
        .filter_map(|item| item["next_due_ms"].as_i64())
        .min();
    let schedule_digest = digest_json(&json!({
        "definitions":config.schedules,
        "state":schedule_status,
    }))?;

    let (cron_next, cron_rows) = due_index_snapshot(db, CRON_DUE_PREFIX, 20)?;
    let (goal_next, goal_rows) = due_index_snapshot(db, GOAL_DUE_PREFIX, 19)?;
    let cron_due = due_count(&cron_rows, CRON_DUE_PREFIX, 20, observed_at_ms)?;
    let goal_due = due_count(&goal_rows, GOAL_DUE_PREFIX, 19, observed_at_ms)?;
    let cron_digest = digest_index_rows(&cron_rows)?;
    let goal_digest = digest_index_rows(&goal_rows)?;
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
) -> Result<(Option<i64>, Vec<(String, String)>)> {
    let upper = format!("{prefix}~");
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT ?3",
    )?;
    let rows = statement
        .query_map(params![prefix, upper, MAX_SOURCE_PAGE as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let first = rows
        .first()
        .map(|(key, _)| parse_due_key(key, prefix, timestamp_width))
        .transpose()?;
    if let Some(first) = first
        && first < 0
    {
        return Err(Error::new(
            "AUTOMATION_DUE_INDEX_INVALID",
            "due index contains a negative timestamp",
        ));
    }
    Ok((first, rows))
}

fn due_count(rows: &[(String, String)], prefix: &str, width: usize, now_ms: i64) -> Result<usize> {
    rows.iter()
        .map(|(key, _)| parse_due_key(key, prefix, width))
        .collect::<Result<Vec<_>>>()
        .map(|times| times.iter().filter(|due| **due <= now_ms).count())
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
