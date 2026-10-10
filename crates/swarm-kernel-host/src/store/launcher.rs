//! Bounded manager-side views over Task, Attempt, Binding, Operation,
//! capacity and attention authorities, plus digest-bound launch admission.
//! Workspace and runtime effects remain outside Store transactions.

use super::{acceptance, capacity, meta, projection, provider_conditions, tasks::task_sources};
use crate::{
    config::{Config, McpToolProfile},
    error::{Error, Result},
    launcher::{self, PageRequest},
    model::{self, Dependency, Principal, Role, TaskSpec},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::path::Path;

const DASHBOARD_PAGE_LIMIT: i64 = 5;
const MAX_INSPECT_OPERATIONS: i64 = 20;
const MAX_INSPECT_CHECKS: i64 = 20;
const MAX_INSPECT_OVERLAPS: i64 = 10;
const MAX_SCOPE_PATHS: usize = 32;
const MAX_MATCHED_PATHS: usize = 8;
const MAX_DEPENDENCIES: usize = 64;
const MAX_SCOPE_PATH_BYTES: usize = 512;
const ATTENTION_SCAN_LIMIT: i64 = 200;
const MAX_ATTEMPT_BINDING_ATTENTION_ITEMS: usize = 8;
const MAX_INSPECT_SNAPSHOT_FIELD_BYTES: usize = 12_288;
const MAX_INSPECT_SNAPSHOT_FIELD_ITEMS: usize = 64;
const MAX_LAUNCH_OPERATION_ROWS: i64 = 8;
const MAX_LAUNCH_BRIEF_BYTES: usize = 8_192;
const MAX_DECISION_CARD_OBJECTIVE_BYTES: usize = 4_096;
const MAX_DECISION_CARD_TEXT_BYTES: usize = 512;
const MAX_DECISION_CARD_LIST_ITEMS: usize = 16;
const MAX_DECISION_CARD_OVERLAP_ITEMS: usize = 8;
const MAX_LAUNCH_MANIFEST_BYTES: usize = 1_048_576;

/// Constructed only from one Store snapshot. Display detachment cannot change
/// these effect facts or their digest; clients never supply this value.
#[derive(Serialize)]
struct LaunchPlanAuthority {
    schema_version: u32,
    task: Value,
    task_spec_sha256: String,
    attempt_action: String,
    current_attempt: Value,
    candidate_scope: Value,
    workspace: Value,
    baseline: Value,
    route: Value,
    selected_route_sha256: Option<String>,
    mcp: Value,
    selected_mcp_profile_sha256: Option<String>,
    requested_plan_facts: Value,
    hard_blocks: Vec<String>,
    gaps: Vec<String>,
    preview_readiness: String,
    coverage: String,
}

struct PlannedLaunch {
    authority: LaunchPlanAuthority,
    projection: Value,
    plan_digest: String,
}

/// Count only other root claims when an already reserved root is rechecked.
/// Provider evidence and the explicit policy use the same predicate at every
/// admission boundary; read-only views do not create a reservation.
pub(super) fn route_admission(
    db: &Connection,
    route: &crate::config::Route,
    binding: Option<&Value>,
    exclude_operation_id: Option<&str>,
) -> Result<provider_conditions::RouteAdmissionProjection> {
    let exclude_binding = binding
        .map(|binding| {
            Ok::<_, Error>((
                model::text(binding, "binding_id")?,
                model::positive(binding, "generation")?,
            ))
        })
        .transpose()?;
    let root_claims = if route.admission_policy.is_some() {
        capacity::root_claims_for_route(db, route, exclude_operation_id, exclude_binding)?
    } else {
        Some(0)
    };
    provider_conditions::check_route(db, route, binding, root_claims, model::now_ms()?)
}

pub(super) fn require_route_admission(
    admission: &provider_conditions::RouteAdmissionProjection,
) -> Result<()> {
    use provider_conditions::RouteAdmissionDecision;
    match &admission.decision {
        RouteAdmissionDecision::Admit => Ok(()),
        RouteAdmissionDecision::Hold { code, .. } => {
            Err(Error::new("ROUTE_ADMISSION_HELD", code.clone()))
        }
        RouteAdmissionDecision::Unavailable { code } => {
            Err(Error::new("ROUTE_ADMISSION_UNAVAILABLE", code.clone()))
        }
    }
}

pub(super) fn current_launch_admission(
    db: &Connection,
    operation_id: &str,
    config: &Config,
) -> Result<provider_conditions::RouteAdmissionProjection> {
    let manifest = retained_launch_manifest(db, operation_id)?;
    let route = config.route(model::text(&manifest["request"], "route")?)?;
    let operation = super::operations::get_operation(db, operation_id)?;
    let binding = operation["binding_id"]
        .as_str()
        .zip(operation["binding_generation"].as_i64())
        .map(|(id, generation)| super::operations::get_binding(db, id, generation))
        .transpose()?;
    if binding
        .as_ref()
        .is_some_and(|binding| binding["route"]["alias"] != route.alias)
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch binding belongs to another route",
        ));
    }
    route_admission(db, &route, binding.as_ref(), Some(operation_id))
}

pub(super) fn retain_launch_admission(
    tx: &Transaction<'_>,
    operation_id: &str,
    admission: &provider_conditions::RouteAdmissionProjection,
    now: i64,
) -> Result<()> {
    let details = model::canonical(&serde_json::to_value(admission)?)?;
    let changed = tx.execute(
        "UPDATE operations SET effective_request_json=json_set(effective_request_json,'$.launch_manifest.current_route_admission',json(?2)), \
         result_json=json_set(result_json,'$.current_route_admission',json(?2)),updated_at_ms=?3 \
         WHERE operation_id=?1 AND method='swarm.launch' AND state='queued'",
        params![operation_id, details, now],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "launch changed before route admission was retained",
        ));
    }
    Ok(())
}

impl PlannedLaunch {
    fn effect_facts(&self) -> Result<Value> {
        let mut facts = serde_json::to_value(&self.authority)?;
        facts["plan_digest"] = json!(self.plan_digest);
        Ok(facts)
    }
}

type OpeningChildRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    String,
    String,
    Option<String>,
);

/// Authority carried by the shared launcher from admission through native
/// readback. WorkDispatch is deliberately an opaque context, never a forged
/// manager Principal or a deserializable request field.
#[derive(Debug, Clone)]
pub(crate) enum LaunchActor {
    Direct(Principal),
    OnBehalf(Box<crate::automation::work_dispatch::WorkDispatchContext>),
}

pub(crate) type WorkDispatchLaunchSlotOutcome =
    super::automation_work_dispatch::LaunchSlotResolution;
pub(crate) type LaunchSlotRetentionOutcome = super::automation_work_dispatch::LaunchSlotRetention;

impl LaunchActor {
    /// The identity recorded as the Operation caller / technical requester.
    pub(crate) fn technical_requester_id(&self) -> &str {
        match self {
            Self::Direct(principal) => &principal.client_id,
            Self::OnBehalf(context) => context.technical_requester_id(),
        }
    }

    /// The manager whose Task and Attempt scope is being acted on.
    pub(crate) fn effective_manager_id(&self) -> &str {
        match self {
            Self::Direct(principal) => &principal.client_id,
            Self::OnBehalf(context) => context.effective_manager_id(),
        }
    }

    /// Role proof used by bounded metadata projections. OnBehalf has a
    /// manager-only constructor and returns its effective role, not a
    /// Principal that can be passed to generic Store authorization.
    pub(crate) fn role(&self) -> Role {
        match self {
            Self::Direct(principal) => principal.role.clone(),
            Self::OnBehalf(_) => Role::Manager,
        }
    }

    /// A link identity exists only for an authenticated direct caller.
    pub(crate) fn link_id(&self) -> Option<&str> {
        match self {
            Self::Direct(principal) => Some(&principal.link_id),
            Self::OnBehalf(_) => None,
        }
    }

    pub(crate) fn direct_principal(&self) -> Option<&Principal> {
        match self {
            Self::Direct(principal) => Some(principal),
            Self::OnBehalf(_) => None,
        }
    }

    pub(crate) fn work_dispatch_context(
        &self,
    ) -> Option<&crate::automation::work_dispatch::WorkDispatchContext> {
        match self {
            Self::Direct(_) => None,
            Self::OnBehalf(context) => Some(context),
        }
    }

    /// Require the caller's current registration or the exact active
    /// WorkDispatch action context. This never upgrades either authority.
    pub(crate) fn require_current(&self, db: &Connection) -> Result<()> {
        match self {
            Self::Direct(principal) => {
                require_manager(db, principal)?;
                let registration = meta(db, &format!("client:{}", principal.client_id))?
                    .ok_or_else(|| {
                        Error::new("FORBIDDEN", "launch actor is no longer registered")
                    })?;
                let role: Role = serde_json::from_value(registration["role"].clone())?;
                if registration["disabled"] == true || role != principal.role {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "launch actor registration changed after admission",
                    ));
                }
                Ok(())
            }
            Self::OnBehalf(context) => context.require_current(db),
        }
    }

    /// Permit only exact unknown workspace readback under a separate
    /// successor-GM capability. This never authorizes queued work or a new
    /// native effect; direct actors retain their ordinary current check.
    fn require_workspace_readback_authority(
        &self,
        db: &Connection,
        operation_id: &str,
    ) -> Result<()> {
        match self {
            Self::Direct(_) => self.require_current(db),
            Self::OnBehalf(context) => {
                let _authority = context.require_workspace_readback_authority(db, operation_id)?;
                Ok(())
            }
        }
    }

    pub(crate) fn require_action_object(
        &self,
        db: &Connection,
        action: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: Option<&str>,
    ) -> Result<()> {
        if action != "swarm.launch" {
            return Err(Error::new(
                "FORBIDDEN",
                "launch actor cannot authorize this action",
            ));
        }
        match self {
            Self::OnBehalf(context) => {
                context.require_action_object(db, action, task_id, task_revision, attempt_id)?;
                if attempt_id.is_none()
                    && super::gm::current(db)?
                        .is_none_or(|current| current.client_id != context.effective_manager_id())
                    && !context.is_workspace_readback_authorized(db)?
                {
                    return Err(Error::new(
                        "WORK_DISPATCH_GM_REQUIRED",
                        "initial WorkDispatch launch requires the current GM",
                    ));
                }
                Ok(())
            }
            Self::Direct(principal) => {
                self.require_current(db)?;
                let task = query_task(db, task_id)?;
                if task.revision != task_revision || task.state != "open" {
                    return Err(Error::new(
                        "STALE_LAUNCH",
                        "launch Task is not current at the admitted revision",
                    ));
                }
                let current_attempt = task
                    .current_attempt_id
                    .as_deref()
                    .map(|id| get_attempt_row(db, id))
                    .transpose()?
                    .flatten()
                    .filter(|attempt| {
                        attempt.task_id == task.task_id && attempt.released_at_ms.is_none()
                    });
                if current_attempt.as_ref().map(|row| row.attempt_id.as_str()) != attempt_id {
                    return Err(Error::new(
                        "STALE_LAUNCH",
                        "launch Attempt differs from the current Task assignment",
                    ));
                }
                if principal.role == Role::Manager
                    && !crate::automation::authorization::current_manager_has_task_scope(
                        db,
                        principal,
                        &task.task_id,
                        &task.project_id,
                    )?
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "manager no longer has current rights to this Task project",
                    ));
                }
                authorize_launch_project(db, principal, current_attempt.as_ref())
            }
        }
    }

    pub(crate) fn require_claimed_launch_attempt(
        &self,
        db: &Connection,
        operation_id: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
    ) -> Result<()> {
        match self {
            Self::OnBehalf(context) => context.require_claimed_launch_attempt(
                db,
                operation_id,
                task_id,
                task_revision,
                attempt_id,
            ),
            Self::Direct(principal) => {
                let operation: Option<(String, String, Option<String>, Option<String>)> = db
                    .query_row(
                        "SELECT caller_id,method,task_id,attempt_id FROM operations WHERE operation_id=?1",
                        [operation_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()?;
                if !operation.is_some_and(|(caller, method, task, attempt)| {
                    caller == principal.client_id
                        && method == "swarm.launch"
                        && task.as_deref() == Some(task_id)
                        && attempt.as_deref() == Some(attempt_id)
                }) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "launch Operation is not bound to the exact claimed Attempt",
                    ));
                }
                self.require_action_object(
                    db,
                    "swarm.launch",
                    task_id,
                    task_revision,
                    Some(attempt_id),
                )?;
                let active_attempts: i64 = db.query_row(
                    "SELECT count(*) FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL",
                    [task_id],
                    |row| row.get(0),
                )?;
                let exact = db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
                     AND task_revision=?3 AND owner_id=?4 AND state='reserved' \
                     AND released_at_ms IS NULL AND start_operation_id IS NULL \
                     AND binding_id IS NULL AND binding_generation IS NULL)",
                    params![attempt_id, task_id, task_revision, principal.client_id],
                    |row| row.get::<_, bool>(0),
                )?;
                if active_attempts != 1 || !exact {
                    return Err(Error::new(
                        "STALE_LAUNCH",
                        "claimed launch Attempt is no longer the exact unstarted reservation",
                    ));
                }
                Ok(())
            }
        }
    }

    /// Verify the exact queued pre-ready launch opening before advancing its
    /// `agent.open` child. The ready binding validator remains a separate
    /// post-readback authority.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_opening_launch_attempt(
        &self,
        db: &Connection,
        launch_operation_id: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
        binding_id: &str,
        binding_generation: i64,
    ) -> Result<()> {
        match self {
            Self::OnBehalf(context) => context.require_opening_launch_attempt(
                db,
                launch_operation_id,
                task_id,
                task_revision,
                attempt_id,
                binding_id,
                binding_generation,
            ),
            Self::Direct(principal) => {
                if launch_operation_id.is_empty()
                    || task_id.is_empty()
                    || task_revision <= 0
                    || attempt_id.is_empty()
                    || binding_id.is_empty()
                    || binding_generation <= 0
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "opening launch subject is incomplete",
                    ));
                }
                self.require_action_object(
                    db,
                    "swarm.launch",
                    task_id,
                    task_revision,
                    Some(attempt_id),
                )?;

                type OpeningLaunchParentRow = (
                    String,
                    String,
                    String,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<i64>,
                    String,
                    String,
                );
                let parent: Option<OpeningLaunchParentRow> = db
                    .query_row(
                        "SELECT caller_id,method,state,task_id,attempt_id,binding_id,binding_generation, \
                         original_request_json,effective_request_json FROM operations WHERE operation_id=?1",
                        [launch_operation_id],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                                row.get(6)?,
                                row.get(7)?,
                                row.get(8)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((
                    caller,
                    method,
                    state,
                    operation_task,
                    operation_attempt,
                    operation_binding,
                    operation_generation,
                    original_request_json,
                    effective_request_json,
                )) = parent
                else {
                    return Err(Error::new("NOT_FOUND", "launch Operation was not found"));
                };
                if caller != principal.client_id
                    || method != "swarm.launch"
                    || state != "queued"
                    || operation_task.as_deref() != Some(task_id)
                    || operation_attempt.as_deref() != Some(attempt_id)
                    || operation_binding.as_deref() != Some(binding_id)
                    || operation_generation != Some(binding_generation)
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "launch Operation is not bound to the exact queued opening subject",
                    ));
                }

                let original_request: Value = serde_json::from_str(&original_request_json)
                    .map_err(|_| {
                        Error::new("LAUNCH_MANIFEST_CORRUPT", "launch request is invalid")
                    })?;
                let request = launcher::LaunchRequest::parse(&original_request)?;
                if request.preview.task_id != task_id
                    || request.preview.expected_task_revision != task_revision
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "launch request does not retain the exact opening Task revision",
                    ));
                }

                let effective: Value =
                    serde_json::from_str(&effective_request_json).map_err(|_| {
                        Error::new("LAUNCH_MANIFEST_CORRUPT", "launch manifest is invalid")
                    })?;
                let expected_contract = json!({
                    "effect_scope":"one_exact_launch_plan",
                    "completion_condition":"workspace_binding_dispatch_and_capability_readback",
                    "replay_policy":"same_request_id_returns_retained_launch_receipt; unknown_effects_require_readback",
                    "contract_revision":"swarm-launch-v1",
                });
                let manifest = effective.get("launch_manifest").ok_or_else(|| {
                    Error::new("LAUNCH_MANIFEST_CORRUPT", "launch manifest is missing")
                })?;
                let task = query_task(db, task_id)?;
                let attempt_action =
                    manifest["task"]["attempt_action"].as_str().ok_or_else(|| {
                        Error::new("LAUNCH_MANIFEST_CORRUPT", "Attempt action is missing")
                    })?;
                let expected_claim_operation = match attempt_action {
                    "claim_new" => json!(launch_child_request_id(launch_operation_id, "claim")),
                    "use_existing" => Value::Null,
                    _ => {
                        return Err(Error::new(
                            "LAUNCH_MANIFEST_CORRUPT",
                            "launch Attempt action is invalid",
                        ));
                    }
                };
                let attempt_start_owner =
                    manifest["attempt"]["start_owner"].as_str().ok_or_else(|| {
                        Error::new("LAUNCH_MANIFEST_CORRUPT", "Attempt start owner is missing")
                    })?;
                let expected_preview = request.preview_params();
                let runtime_route_alias = model::text(&manifest["runtime"]["route"], "alias")
                    .map_err(|_| {
                        Error::new(
                            "LAUNCH_MANIFEST_CORRUPT",
                            "launch runtime route alias is invalid",
                        )
                    })?;
                let expected_route_alias = model::text(&expected_preview, "route")?;
                let open_operation_id = manifest["binding"]["operation_id"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        Error::new("LAUNCH_MANIFEST_CORRUPT", "open child is missing")
                    })?;
                if effective["operation_contract"] != expected_contract
                    || manifest["manifest_version"] != "eliot-launch-manifest-v1"
                    || manifest["state"] != "awaiting_binding"
                    || manifest["actor"] != launch_actor_manifest(self)
                    || manifest["client_request_id"] != request.client_request_id
                    || manifest["plan_digest"] != request.plan_digest
                    || manifest["request"] != expected_preview
                    || manifest["task"]["task_id"] != task_id
                    || manifest["task"]["project_id"] != task.project_id
                    || manifest["task"]["expected_revision"] != task_revision
                    || manifest["task"]["observed_revision"] != task_revision
                    || manifest["task"]["attempt_id"] != attempt_id
                    || manifest["attempt"]["state"] != "reserved"
                    || manifest["attempt"]["action"] != attempt_action
                    || manifest["attempt"]["claim_operation_id"] != expected_claim_operation
                    || manifest["workspace"]["policy"] != expected_preview["workspace_policy"]
                    || manifest["workspace"]["lease_state"] != "held"
                    || manifest["workspace"]["dirty_state"] != "clean_verified"
                    || manifest["workspace"]["filesystem_inspected"] != true
                    || manifest["workspace"]["manifest_digest"].as_str().is_none()
                    || runtime_route_alias != expected_route_alias
                    || manifest["runtime"]["agent_profile"] != expected_preview["agent_profile"]
                    || manifest["runtime"]["requested_model"] != expected_preview["requested_model"]
                    || manifest["runtime"]["requested_effort"]
                        != expected_preview["requested_effort"]
                    || manifest["runtime"]["budget"] != expected_preview["budget"]
                    || manifest["runtime"]["stop_conditions"] != expected_preview["stop_conditions"]
                    || manifest["runtime"]["purpose"] != expected_preview["purpose"]
                    || manifest["runtime"]["state"] != "opening"
                    || manifest["runtime"]["native_effect"] != "not_observed"
                    || manifest["progress"]["binding_open"] != "queued"
                    || manifest["progress"]["task_dispatch"] != "not_started"
                    || manifest["binding"]["binding_id"] != binding_id
                    || manifest["binding"]["generation"] != binding_generation
                    || manifest["binding"]["state"] != "queued"
                {
                    return Err(Error::new(
                        "LAUNCH_MANIFEST_CORRUPT",
                        "launch manifest does not retain the exact queued opening phase",
                    ));
                }
                let lease: crate::workspace::LeaseAuthorityRef =
                    serde_json::from_value(manifest["workspace"]["lease_authority"].clone())
                        .map_err(|_| {
                            Error::new(
                                "LAUNCH_MANIFEST_CORRUPT",
                                "launch held-lease authority reference is invalid",
                            )
                        })?;
                if lease.state != "held"
                    || lease.lease_id.is_empty()
                    || lease.generation <= 0
                    || lease.registration_generation <= 0
                    || lease.operation_id != launch_operation_id
                    || lease.project_id != task.project_id
                    || lease.task_id != task_id
                    || lease.task_revision != task_revision
                    || lease.plan_digest != request.plan_digest
                    || lease.owner_client_id != principal.client_id
                    || lease.attempt_id.as_deref() != Some(attempt_id)
                    || !crate::forge::valid_object_id(&lease.baseline_commit)
                    || lease.binding_digest.is_empty()
                    || super::workspace::held_lease_for_operation(db, launch_operation_id)?.as_ref()
                        != Some(&lease)
                {
                    return Err(Error::new(
                        "WORKSPACE_LEASE_STALE",
                        "launch manifest does not retain the exact current held lease",
                    ));
                }
                let lease_view = super::workspace::get_lease_view(db, &lease)?;
                let expected_workspace_manifest_digest =
                    final_workspace_manifest_digest(&request.plan_digest, &lease, &lease_view)?;
                if model::canonical(&lease_view)?
                    != model::canonical(&manifest["workspace"]["lease"])?
                    || manifest["workspace"]["manifest_digest"].as_str()
                        != Some(expected_workspace_manifest_digest.as_str())
                {
                    return Err(Error::new(
                        "WORKSPACE_LEASE_STALE",
                        "launch manifest lease view differs from persisted lease authority",
                    ));
                }

                let active_attempts: i64 = db.query_row(
                    "SELECT count(*) FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL",
                    [task_id],
                    |row| row.get(0),
                )?;
                let exact_attempt: bool = db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
                     AND task_revision=?3 AND owner_id=?4 AND state='reserved' \
                     AND released_at_ms IS NULL AND start_operation_id IS NULL \
                     AND binding_id=?5 AND binding_generation=?6 AND start_owner=?7)",
                    params![
                        attempt_id,
                        task_id,
                        task_revision,
                        principal.client_id,
                        binding_id,
                        binding_generation,
                        attempt_start_owner,
                    ],
                    |row| row.get(0),
                )?;
                if active_attempts != 1 || !exact_attempt {
                    return Err(Error::new(
                        "STALE_LAUNCH",
                        "opening Attempt is not the sole current reserved unstarted assignment",
                    ));
                }

                let child: Option<OpeningChildRow> = db
                    .query_row(
                        "SELECT caller_id,method,client_request_id,prerequisite_operation_id,task_id, \
                         attempt_id,binding_id,binding_generation,state,original_request_json, \
                         effective_request_json,result_json FROM operations WHERE operation_id=?1",
                        [open_operation_id],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                                row.get(6)?,
                                row.get(7)?,
                                row.get(8)?,
                                row.get(9)?,
                                row.get(10)?,
                                row.get(11)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((
                    child_caller,
                    child_method,
                    child_request_id,
                    prerequisite,
                    child_task,
                    child_attempt,
                    child_binding,
                    child_generation,
                    child_state,
                    child_original_json,
                    child_effective_json,
                    child_result_json,
                )) = child
                else {
                    return Err(Error::new(
                        "LAUNCH_OPEN_MISSING",
                        "launch open child is missing",
                    ));
                };
                let expected_child_request = json!({
                    "client_request_id":launch_child_request_id(launch_operation_id, "open"),
                    "lane_id":format!("launch-{}", lease.lease_id),
                    "route":request.preview.route,
                });
                let child_original: Value =
                    serde_json::from_str(&child_original_json).map_err(|_| {
                        Error::new("LAUNCH_OPEN_CORRUPT", "open child request is invalid")
                    })?;
                if child_caller != principal.client_id
                    || child_method != "agent.open"
                    || child_request_id != launch_child_request_id(launch_operation_id, "open")
                    || prerequisite.as_deref() != Some(launch_operation_id)
                    || child_task.as_deref() != Some(task_id)
                    || child_attempt.as_deref() != Some(attempt_id)
                    || child_binding.as_deref() != Some(binding_id)
                    || child_generation != Some(binding_generation)
                    || child_state != "queued"
                    || model::canonical(&child_original)?
                        != model::canonical(&expected_child_request)?
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "agent.open child is not the exact queued child of this launch and lease",
                    ));
                }
                let child_effective: Value =
                    serde_json::from_str(&child_effective_json).map_err(|_| {
                        Error::new("LAUNCH_OPEN_CORRUPT", "open child linkage is invalid")
                    })?;
                model::fields(
                    &child_effective,
                    &[
                        "route",
                        "module_instance_id",
                        "operation_contract",
                        "workspace_lease",
                        "receipt",
                    ],
                )?;
                let child_result: Value =
                    serde_json::from_str(child_result_json.as_deref().ok_or_else(|| {
                        Error::new("LAUNCH_OPEN_CORRUPT", "queued open child has no receipt")
                    })?)
                    .map_err(|_| {
                        Error::new("LAUNCH_OPEN_CORRUPT", "open child receipt is invalid")
                    })?;
                if child_effective["operation_contract"]["parent_launch_operation_id"]
                    != launch_operation_id
                    || child_effective["workspace_lease"]["lease_id"] != lease.lease_id
                    || child_effective["workspace_lease"]["generation"] != lease.generation
                    || child_effective["workspace_lease"]["binding_digest"] != lease.binding_digest
                    || child_effective["route"]["alias"] != expected_preview["route"]
                    || child_effective["receipt"]["ok"] != true
                    || child_effective["receipt"]["value"]["operation_id"] != open_operation_id
                    || child_effective["receipt"]["value"]["binding_id"] != binding_id
                    || child_effective["receipt"]["value"]["generation"] != binding_generation
                    || child_effective["receipt"]["value"]["state"] != "queued"
                    || model::canonical(&child_result)?
                        != model::canonical(&child_effective["receipt"]["value"])?
                {
                    return Err(Error::new(
                        "LAUNCH_OPEN_CORRUPT",
                        "open child receipt does not bind this queued opening and held lease",
                    ));
                }

                let binding = super::operations::get_binding(db, binding_id, binding_generation)?;
                if binding["state"] != "opening"
                    || !binding["released_at_ms"].is_null()
                    || !binding["native_root_id"].is_null()
                    || !binding["native_scope_key"].is_null()
                    || binding["lane_id"] != format!("launch-{}", lease.lease_id)
                    || binding["module_instance_id"] != child_effective["module_instance_id"]
                    || binding["route"] != child_effective["route"]
                {
                    return Err(Error::new(
                        "BINDING_NOT_READY",
                        "binding is not the exact unreleased pre-native opening for this lease",
                    ));
                }
                Ok(())
            }
        }
    }

    // Keep the independently checked launch subject and binding tuple explicit.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_bound_launch_attempt(
        &self,
        db: &Connection,
        operation_id: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
        binding_id: &str,
        binding_generation: i64,
    ) -> Result<()> {
        match self {
            Self::OnBehalf(context) => context.require_bound_launch_attempt(
                db,
                operation_id,
                task_id,
                task_revision,
                attempt_id,
                binding_id,
                binding_generation,
            ),
            Self::Direct(principal) => {
                self.require_current(db)?;
                type BoundLaunchOperationRow = (
                    String,
                    String,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<i64>,
                );
                let operation: Option<BoundLaunchOperationRow> = db
                    .query_row(
                        "SELECT caller_id,method,task_id,attempt_id,binding_id,binding_generation \
                         FROM operations WHERE operation_id=?1",
                        [operation_id],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                            ))
                        },
                    )
                    .optional()?;
                if !operation.is_some_and(
                    |(caller, method, task, attempt, operation_binding, operation_generation)| {
                        caller == principal.client_id
                            && method == "swarm.launch"
                            && task.as_deref() == Some(task_id)
                            && attempt.as_deref() == Some(attempt_id)
                            && operation_binding.as_deref() == Some(binding_id)
                            && operation_generation == Some(binding_generation)
                    },
                ) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "launch Operation is not bound to the exact current Attempt",
                    ));
                }
                self.require_action_object(
                    db,
                    "swarm.launch",
                    task_id,
                    task_revision,
                    Some(attempt_id),
                )?;
                let active_attempts: i64 = db.query_row(
                    "SELECT count(*) FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL",
                    [task_id],
                    |row| row.get(0),
                )?;
                let exact: bool = db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
                     AND task_revision=?3 AND owner_id=?4 AND state='reserved' \
                     AND released_at_ms IS NULL AND start_operation_id IS NULL \
                     AND binding_id=?5 AND binding_generation=?6)",
                    params![
                        attempt_id,
                        task_id,
                        task_revision,
                        principal.client_id,
                        binding_id,
                        binding_generation
                    ],
                    |row| row.get(0),
                )?;
                let binding: Value =
                    super::operations::get_binding(db, binding_id, binding_generation)?;
                if active_attempts != 1
                    || !exact
                    || binding["state"] != "ready"
                    || !binding["released_at_ms"].is_null()
                {
                    return Err(Error::new(
                        "STALE_LAUNCH",
                        "binding readback is not the exact current launch Attempt binding",
                    ));
                }
                Ok(())
            }
        }
    }

    /// Revalidate only the bound, ready initial-dispatch stage. An absent child
    /// Operation means admission before the Attempt start-pointer CAS; a child
    /// means pre-effect validation of that exact queued dispatch Operation.
    // Each argument identifies one independently checked retained launch fact.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_dispatch_launch_attempt(
        &self,
        db: &Connection,
        operation_id: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
        binding_id: &str,
        binding_generation: i64,
        dispatch_operation_id: Option<&str>,
    ) -> Result<()> {
        if operation_id.is_empty()
            || task_id.is_empty()
            || task_revision <= 0
            || attempt_id.is_empty()
            || binding_id.is_empty()
            || binding_generation <= 0
            || dispatch_operation_id.is_some_and(|id| {
                id.is_empty() || id.len() > 256 || id.chars().any(char::is_control)
            })
        {
            return Err(Error::new("FORBIDDEN", "dispatch launch scope is invalid"));
        }
        if new_work_state(db)?["new_work"] != "enabled" {
            return Err(Error::new(
                "ADMISSION_DISABLED",
                "new work is disabled before launch dispatch",
            ));
        }
        let (request, manifest) = require_dispatch_launch_parent_stage(
            db,
            self,
            operation_id,
            task_id,
            task_revision,
            attempt_id,
            binding_id,
            binding_generation,
            dispatch_operation_id,
        )?;
        match self {
            Self::Direct(_) => {
                self.require_current(db)?;
                self.require_action_object(
                    db,
                    "swarm.launch",
                    task_id,
                    task_revision,
                    Some(attempt_id),
                )?;
            }
            Self::OnBehalf(context) => context.require_dispatch_launch_attempt(
                db,
                operation_id,
                task_id,
                task_revision,
                attempt_id,
                binding_id,
                binding_generation,
                dispatch_operation_id,
            )?,
        }
        require_current_dispatch_launch_chain(
            db,
            self,
            operation_id,
            task_id,
            task_revision,
            attempt_id,
            binding_id,
            binding_generation,
            dispatch_operation_id,
            &request,
            &manifest,
        )?;
        if let Some(dispatch_operation_id) = dispatch_operation_id {
            require_dispatch_child_link(
                db,
                self,
                operation_id,
                task_id,
                task_revision,
                attempt_id,
                binding_id,
                binding_generation,
                dispatch_operation_id,
                &manifest,
            )?;
        }
        Ok(())
    }

    pub(crate) fn same_authority_identity(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Direct(left), Self::Direct(right)) => {
                left.client_id == right.client_id
                    && left.role == right.role
                    && left.link_id == right.link_id
            }
            (Self::OnBehalf(left), Self::OnBehalf(right)) => {
                left.linkage_value() == right.linkage_value()
                    && left.semantic_slot_id() == right.semantic_slot_id()
            }
            _ => false,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn require_dispatch_launch_parent_stage(
    db: &Connection,
    actor: &LaunchActor,
    operation_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    binding_id: &str,
    binding_generation: i64,
    dispatch_operation_id: Option<&str>,
) -> Result<(launcher::LaunchRequest, Value)> {
    type ParentRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        String,
    );
    let row: Option<ParentRow> = db
        .query_row(
            "SELECT caller_id,method,state,task_id,attempt_id,binding_id,binding_generation,\
             original_request_json,effective_request_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .optional()?;
    let Some((
        caller,
        method,
        state,
        operation_task,
        operation_attempt,
        operation_binding,
        operation_generation,
        original_json,
        effective_json,
    )) = row
    else {
        return Err(Error::new("NOT_FOUND", "launch Operation was not found"));
    };
    if caller != actor.technical_requester_id()
        || method != "swarm.launch"
        || state != "queued"
        || operation_task.as_deref() != Some(task_id)
        || operation_attempt.as_deref() != Some(attempt_id)
        || operation_binding.as_deref() != Some(binding_id)
        || operation_generation != Some(binding_generation)
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch Operation is not bound to the exact current dispatch tuple",
        ));
    }
    let original: Value = serde_json::from_str(&original_json)
        .map_err(|_| Error::new("INVALID_LAUNCH_MANIFEST", "launch request is invalid"))?;
    let request = launcher::LaunchRequest::parse(&original)?;
    let effective: Value = serde_json::from_str(&effective_json)
        .map_err(|_| Error::new("INVALID_LAUNCH_MANIFEST", "launch manifest is invalid"))?;
    let manifest = effective
        .get("launch_manifest")
        .ok_or_else(|| Error::new("INVALID_LAUNCH_MANIFEST", "launch manifest is missing"))?
        .clone();
    let runtime_route_alias =
        model::text(&manifest["runtime"]["route"], "alias").map_err(|_| {
            Error::new(
                "LAUNCH_MANIFEST_CORRUPT",
                "launch runtime route alias is invalid",
            )
        })?;
    let expected_contract = json!({
        "effect_scope":"one_exact_launch_plan",
        "completion_condition":"workspace_binding_dispatch_and_capability_readback",
        "replay_policy":"same_request_id_returns_retained_launch_receipt; unknown_effects_require_readback",
        "contract_revision":"swarm-launch-v1",
    });
    if effective["operation_contract"] != expected_contract
        || manifest["manifest_version"] != "eliot-launch-manifest-v1"
        || manifest["actor"] != launch_actor_manifest(actor)
        || manifest["plan_digest"] != request.plan_digest
        || manifest["request"] != launch_preview_value(&request.preview)
        || request.preview.task_id != task_id
        || request.preview.expected_task_revision != task_revision
        || manifest["task"]["task_id"] != task_id
        || manifest["task"]["expected_revision"] != task_revision
        || manifest["task"]["observed_revision"] != task_revision
        || manifest["task"]["attempt_id"] != attempt_id
        || manifest["attempt"]["start_owner"] != "controller"
        || manifest["binding"]["binding_id"] != binding_id
        || manifest["binding"]["generation"] != binding_generation
        || manifest["binding"]["state"] != "ready"
        || runtime_route_alias != request.preview.route
    {
        return Err(Error::new(
            "LAUNCH_MANIFEST_CORRUPT",
            "retained launch does not match the exact dispatch actor and tuple",
        ));
    }
    match dispatch_operation_id {
        None if manifest["progress"]["task_dispatch"] != "not_started"
            || !manifest["progress"]["task_dispatch_operation_id"].is_null()
            || !manifest["progress"]["task_dispatch_packet_digest"].is_null() =>
        {
            return Err(Error::new(
                "STALE_LAUNCH",
                "launch has already left the unstarted dispatch stage",
            ));
        }
        Some(dispatch_id)
            if manifest["progress"]["task_dispatch"] != "queued"
                || manifest["progress"]["task_dispatch_operation_id"] != dispatch_id
                || manifest["progress"]["task_dispatch_packet_digest"]
                    .as_str()
                    .is_none_or(str::is_empty) =>
        {
            return Err(Error::new(
                "STALE_LAUNCH",
                "launch manifest does not retain this queued dispatch child",
            ));
        }
        _ => {}
    }
    Ok((request, manifest))
}

#[allow(clippy::too_many_arguments)]
fn require_current_dispatch_launch_chain(
    db: &Connection,
    actor: &LaunchActor,
    operation_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    binding_id: &str,
    binding_generation: i64,
    dispatch_operation_id: Option<&str>,
    request: &launcher::LaunchRequest,
    manifest: &Value,
) -> Result<()> {
    let task = super::tasks::get_task(db, task_id)?;
    if task["project_id"] != manifest["task"]["project_id"]
        || task["revision"] != task_revision
        || task["state"] != "open"
        || task["current_attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "Task is no longer current at the exact launch revision and Attempt",
        ));
    }
    let active_attempts: i64 = db.query_row(
        "SELECT count(*) FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL",
        [task_id],
        |row| row.get(0),
    )?;
    type AttemptStage = (
        String,
        i64,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
    );
    let attempt: Option<AttemptStage> = db
        .query_row(
            "SELECT task_id,task_revision,owner_id,state,start_owner,start_operation_id,binding_id,\
             binding_generation,released_at_ms FROM attempts WHERE attempt_id=?1",
            [attempt_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .optional()?;
    let exact_attempt = attempt.is_some_and(
        |(
            attempt_task,
            revision,
            owner_id,
            state,
            start_owner,
            start_operation,
            attempt_binding,
            generation,
            released,
        )| {
            attempt_task == task_id
                && revision == task_revision
                && owner_id == actor.effective_manager_id()
                && state == "reserved"
                && start_owner == "controller"
                && start_operation.as_deref() == dispatch_operation_id
                && attempt_binding.as_deref() == Some(binding_id)
                && generation == Some(binding_generation)
                && released.is_none()
        },
    );
    if active_attempts != 1 || !exact_attempt {
        return Err(Error::new(
            "STALE_LAUNCH",
            "Attempt is not the sole current bound controller assignment at this dispatch stage",
        ));
    }
    let binding = super::operations::get_binding(db, binding_id, binding_generation)?;
    if binding["state"] != "ready" || !binding["released_at_ms"].is_null() {
        return Err(Error::new(
            "BINDING_NOT_READY",
            "dispatch requires the exact ready, unreleased native binding",
        ));
    }

    let lease: crate::workspace::LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone()).map_err(|_| {
            Error::new(
                "LAUNCH_MANIFEST_CORRUPT",
                "launch held-lease authority reference is invalid",
            )
        })?;
    if lease.state != "held"
        || lease.operation_id != operation_id
        || lease.project_id != task["project_id"]
        || lease.task_id != task_id
        || lease.task_revision != task_revision
        || lease.plan_digest != request.plan_digest
        || lease.owner_client_id != actor.effective_manager_id()
        || lease.attempt_id.as_deref() != Some(attempt_id)
        || lease.generation <= 0
        || lease.registration_generation <= 0
        || lease.binding_digest.is_empty()
        || super::workspace::held_lease_for_operation(db, operation_id)?.as_ref() != Some(&lease)
    {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "dispatch parent does not retain its exact held workspace lease",
        ));
    }
    let lease_view = super::workspace::get_lease_view(db, &lease)?;
    let manifest_digest =
        final_workspace_manifest_digest(&request.plan_digest, &lease, &lease_view)?;
    if model::canonical(&lease_view)? != model::canonical(&manifest["workspace"]["lease"])?
        || manifest["workspace"]["manifest_digest"] != manifest_digest
    {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "dispatch parent workspace snapshot differs from current lease authority",
        ));
    }

    let open_operation_id = model::text(&manifest["binding"], "operation_id")?;
    type OpenRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        Option<String>,
        String,
        String,
    );
    let open: Option<OpenRow> = db
        .query_row(
            "SELECT caller_id,method,client_request_id,prerequisite_operation_id,task_id,\
             attempt_id,binding_id,binding_generation,state,result_json,original_request_json,\
             effective_request_json FROM operations WHERE operation_id=?1",
            [open_operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                ))
            },
        )
        .optional()?;
    let Some((
        caller,
        method,
        client_request_id,
        prerequisite,
        open_task,
        open_attempt,
        open_binding,
        open_generation,
        state,
        open_result_json,
        open_original_json,
        open_effective_json,
    )) = open
    else {
        return Err(Error::new(
            "LAUNCH_OPEN_MISSING",
            "launch open child was not found",
        ));
    };
    let expected_open_request_id = format!("launch:{operation_id}:open");
    if caller != actor.technical_requester_id()
        || method != "agent.open"
        || client_request_id != expected_open_request_id
        || prerequisite.as_deref() != Some(operation_id)
        || open_task.as_deref() != Some(task_id)
        || open_attempt.as_deref() != Some(attempt_id)
        || open_binding.as_deref() != Some(binding_id)
        || open_generation != Some(binding_generation)
        || state != "settled"
    {
        return Err(Error::new(
            "FORBIDDEN",
            "agent.open is not the exact settled child of this launch binding",
        ));
    }
    let open_original: Value = serde_json::from_str(&open_original_json)
        .map_err(|_| Error::new("LAUNCH_OPEN_CORRUPT", "agent.open request is invalid"))?;
    model::fields(&open_original, &["client_request_id", "lane_id", "route"])?;
    if open_original["client_request_id"] != expected_open_request_id
        || open_original["lane_id"] != format!("launch-{}", lease.lease_id)
        || open_original["route"] != request.preview.route
    {
        return Err(Error::new(
            "FORBIDDEN",
            "agent.open request differs from the exact launch route and lease",
        ));
    }
    let open_effective: Value = serde_json::from_str(&open_effective_json)
        .map_err(|_| Error::new("LAUNCH_OPEN_CORRUPT", "agent.open receipt is invalid"))?;
    if open_effective["receipt"]["ok"] != true
        || open_effective["receipt"]["value"]["operation_id"] != open_operation_id
        || open_effective["receipt"]["value"]["binding_id"] != binding_id
        || open_effective["receipt"]["value"]["generation"] != binding_generation
        || open_effective["receipt"]["value"]["state"] != "queued"
        || open_effective["operation_contract"]["parent_launch_operation_id"] != operation_id
        || open_effective["workspace_lease"]["lease_id"] != lease.lease_id
        || open_effective["workspace_lease"]["generation"] != lease.generation
        || open_effective["workspace_lease"]["binding_digest"] != lease.binding_digest
    {
        return Err(Error::new(
            "LAUNCH_OPEN_CORRUPT",
            "agent.open receipt does not prove the exact parent lease and binding",
        ));
    }
    let result_raw = open_result_json.ok_or_else(|| {
        Error::new(
            "LAUNCH_OPEN_CORRUPT",
            "settled agent.open has no result receipt",
        )
    })?;
    let result: crate::runtime::RuntimeOutcome =
        serde_json::from_str(&result_raw).map_err(|_| {
            Error::new(
                "LAUNCH_OPEN_CORRUPT",
                "settled agent.open result is not a runtime outcome",
            )
        })?;
    let expected_root = model::text(&binding, "native_root_id")?;
    let expected_scope = model::text(&binding, "native_scope_key")?;
    if result.operation_id != open_operation_id
        || !matches!(result.outcome, crate::runtime::EffectOutcome::Applied)
        || result.native_root_id.as_deref() != Some(expected_root)
        || result.native_scope_key.as_deref() != Some(expected_scope)
    {
        return Err(Error::new(
            "LAUNCH_OPEN_CORRUPT",
            "settled agent.open result differs from the exact ready native binding",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Exact retained authority tuple.
fn require_dispatch_child_link(
    db: &Connection,
    actor: &LaunchActor,
    parent_operation_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    binding_id: &str,
    binding_generation: i64,
    dispatch_operation_id: &str,
    manifest: &Value,
) -> Result<()> {
    type DispatchRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        String,
    );
    let row: Option<DispatchRow> = db
        .query_row(
            "SELECT caller_id,method,state,task_id,attempt_id,binding_id,binding_generation,\
             original_request_json,effective_request_json FROM operations WHERE operation_id=?1",
            [dispatch_operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .optional()?;
    let Some((
        caller,
        method,
        state,
        operation_task,
        operation_attempt,
        operation_binding,
        operation_generation,
        original_json,
        effective_json,
    )) = row
    else {
        return Err(Error::new(
            "STALE_LAUNCH",
            "queued dispatch child was not found",
        ));
    };
    let expected_caller = match actor {
        LaunchActor::Direct(_) => actor.technical_requester_id(),
        LaunchActor::OnBehalf(_) => actor.effective_manager_id(),
    };
    if caller != expected_caller
        || method != "task.dispatch"
        || state != "queued"
        || operation_task.as_deref() != Some(task_id)
        || operation_attempt.as_deref() != Some(attempt_id)
        || operation_binding.as_deref() != Some(binding_id)
        || operation_generation != Some(binding_generation)
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "dispatch child is not the exact queued Task, Attempt, and binding Operation",
        ));
    }
    let original: Value = serde_json::from_str(&original_json)
        .map_err(|_| Error::new("INVALID_RECEIPT", "dispatch request is invalid"))?;
    if original["attempt_id"] != attempt_id
        || original["launch_operation_id"] != parent_operation_id
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "dispatch request does not name this exact launch and Attempt",
        ));
    }
    let effective: Value = serde_json::from_str(&effective_json)
        .map_err(|_| Error::new("INVALID_RECEIPT", "dispatch packet is invalid"))?;
    let packet = &effective["launch_dispatch_packet"];
    if packet["schema_version"] != 1
        || packet["launch_operation_id"] != parent_operation_id
        || packet["plan_digest"] != manifest["plan_digest"]
        || packet["task"]["task_id"] != task_id
        || packet["task"]["revision"] != task_revision
        || packet["task"]["attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "dispatch packet does not retain the exact parent and Task tuple",
        ));
    }
    let packet_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(packet)?.as_bytes())
    );
    if manifest["progress"]["task_dispatch_packet_digest"] != packet_digest {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch manifest dispatch packet digest differs from the queued child",
        ));
    }
    let identity_digest = model::text(&packet["capability"], "identity_digest")?;
    let key = format!(
        "launcher:task_dispatch:v1:{}",
        model::digest(dispatch_operation_id.as_bytes())
    );
    let record = meta(db, &key)?
        .ok_or_else(|| Error::new("LAUNCH_DISPATCH_LINK_MISSING", "dispatch link is missing"))?;
    model::fields(
        &record,
        &[
            "schema_version",
            "kind",
            "launch_operation_id",
            "dispatch_operation_id",
            "plan_digest",
            "task_id",
            "task_revision",
            "attempt_id",
            "binding_id",
            "binding_generation",
            "packet_digest",
            "capability_identity_digest",
            "created_at_ms",
        ],
    )?;
    if record["schema_version"] != 1
        || record["kind"] != "launcher_task_dispatch"
        || record["launch_operation_id"] != parent_operation_id
        || record["dispatch_operation_id"] != dispatch_operation_id
        || record["plan_digest"] != manifest["plan_digest"]
        || record["task_id"] != task_id
        || record["task_revision"] != task_revision
        || record["attempt_id"] != attempt_id
        || record["binding_id"] != binding_id
        || record["binding_generation"] != binding_generation
        || record["packet_digest"] != packet_digest
        || record["capability_identity_digest"] != identity_digest
        || record["created_at_ms"]
            .as_i64()
            .is_none_or(|time| time <= 0)
        || manifest["progress"]["task_dispatch"] != "queued"
        || manifest["progress"]["task_dispatch_operation_id"] != dispatch_operation_id
    {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_INVALID",
            "retained dispatch link does not match the exact queued child and packet",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct TaskRow {
    task_id: String,
    project_id: String,
    revision: i64,
    state: String,
    accepted_attempt_id: Option<String>,
    accepted_operation_id: Option<String>,
    accepted_revision: Option<i64>,
    accepted_phase: Option<String>,
    accepted_candidate_ref: Option<String>,
    spec: Value,
    created_at_ms: i64,
    updated_at_ms: i64,
    current_attempt_id: Option<String>,
}

#[derive(Debug)]
struct AttemptRow {
    attempt_id: String,
    task_id: String,
    task_revision: i64,
    owner_id: String,
    start_owner: String,
    start_operation_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    state: String,
    released_at_ms: Option<i64>,
    snapshot: Value,
    producers: Value,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

struct AttemptDbRow {
    attempt_id: String,
    task_id: String,
    task_revision: i64,
    owner_id: String,
    start_owner: String,
    start_operation_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    state: String,
    released_at_ms: Option<i64>,
    snapshot: String,
    producers: String,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

struct CurrentTaskRow {
    revision: i64,
    state: String,
    accepted_attempt_id: Option<String>,
    accepted_operation_id: Option<String>,
    current_attempt_id: Option<String>,
}

struct BindingReadRow {
    state: String,
    native_scope_key: Option<String>,
    native_root_id: Option<String>,
    route_json: String,
    state_json: String,
    released_at_ms: Option<i64>,
}

fn require_manager(db: &Connection, p: &Principal) -> Result<()> {
    match p.role {
        Role::Operator => super::require_local_operator(db, &p.client_id),
        Role::Manager => Ok(()),
        _ => Err(Error::new("FORBIDDEN", "manager authority required")),
    }
}

fn require_dashboard_reader(db: &Connection, p: &Principal) -> Result<()> {
    match p.role {
        Role::Operator => super::require_local_operator(db, &p.client_id),
        Role::Manager | Role::Observer => Ok(()),
        _ => Err(Error::new("FORBIDDEN", "dashboard read authority required")),
    }
}

fn count(db: &Connection, sql: &str) -> Result<i64> {
    Ok(db.query_row(sql, [], |row| row.get(0))?)
}

fn grouped_counts(db: &Connection, sql: &str) -> Result<Value> {
    let mut statement = db.prepare(sql)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut result = Map::new();
    for (name, value) in rows {
        result.insert(name, json!(value));
    }
    Ok(Value::Object(result))
}

fn task_counts(db: &Connection) -> Result<Value> {
    let by_state = grouped_counts(db, "SELECT state,count(*) FROM tasks GROUP BY state")?;
    Ok(json!({"total": count(db,"SELECT count(*) FROM tasks")?, "by_state": by_state}))
}

fn attempt_counts(db: &Connection) -> Result<Value> {
    let current_by_state = grouped_counts(
        db,
        "SELECT state,count(*) FROM attempts WHERE released_at_ms IS NULL GROUP BY state",
    )?;
    Ok(json!({
        "current_total": count(db,"SELECT count(*) FROM attempts WHERE released_at_ms IS NULL")?,
        "current_by_state": current_by_state,
        "released_total": count(db,"SELECT count(*) FROM attempts WHERE released_at_ms IS NOT NULL")?,
    }))
}

fn operation_counts(db: &Connection) -> Result<Value> {
    let by_state = grouped_counts(db, "SELECT state,count(*) FROM operations GROUP BY state")?;
    let unresolved = count(
        db,
        "SELECT count(*) FROM operations WHERE state IN ('queued','sending','native_accepted','outcome_unknown')",
    )?;
    let outcome_unknown = count(
        db,
        "SELECT count(*) FROM operations WHERE state='outcome_unknown'",
    )?;
    Ok(json!({"by_state":by_state,"unresolved":unresolved,"outcome_unknown":outcome_unknown}))
}

fn binding_counts(db: &Connection) -> Result<Value> {
    let by_state = grouped_counts(db, "SELECT state,count(*) FROM bindings GROUP BY state")?;
    let connected = count(
        db,
        "SELECT count(*) FROM bindings WHERE released_at_ms IS NULL AND json_extract(state_json,'$.connection')='connected'",
    )?;
    let live = count(
        db,
        "SELECT count(*) FROM bindings WHERE released_at_ms IS NULL",
    )?;
    Ok(json!({
        "recorded_total": count(db,"SELECT count(*) FROM bindings")?,
        "live_total": live,
        "by_state": by_state,
        "connected_live": connected,
    }))
}

fn new_work_state(db: &Connection) -> Result<Value> {
    let mode = meta(db, "execution_mode")?.unwrap_or(Value::Null);
    let state = mode["new_work"].as_str().unwrap_or("unknown");
    Ok(json!({"new_work":state,"source":"execution_mode"}))
}

fn page_frame(
    source_kind: &str,
    after: i64,
    next_after: i64,
    limit: i64,
    total: i64,
    limited: &projection::Limited,
) -> Result<Value> {
    projection::frame(
        source_kind,
        json!({"after":after,"next_after":next_after}),
        limited,
        limit,
        after > 0,
        next_after < total,
        limited.gap_count == 0,
        Vec::new(),
    )
}

fn page_gap_reference(item: &Value, reason: &'static str, bytes: usize) -> Result<Value> {
    let canonical = model::canonical(item)?;
    let task_id = item["task_id"].as_str();
    let attempt_id = item["attempt_id"]
        .as_str()
        .or_else(|| item["current_attempt"]["attempt_id"].as_str());
    let operation_id = item["operation_id"].as_str();
    let kind = if operation_id.is_some() {
        "operation"
    } else if attempt_id.is_some() {
        "attempt_assignment"
    } else {
        "task"
    };
    let reference = match (task_id, attempt_id, operation_id) {
        (_, _, Some(id)) => json!({"kind":kind,"operation_id":id,"attempt_id":attempt_id}),
        (Some(task), Some(attempt), _) => json!({"kind":kind,"task_id":task,"attempt_id":attempt}),
        (Some(task), _, _) => json!({"kind":kind,"task_id":task}),
        _ => json!({"kind":kind}),
    };
    Ok(json!({
        "task_id": task_id,
        "attempt_id": attempt_id,
        "operation_id": operation_id,
        "gap": {
            "reason": reason,
            "item_serialized_bytes": bytes,
            "max_single_item_bytes": projection::MAX_SINGLE_ITEM_BYTES,
            "item_digest": model::digest(canonical.as_bytes()),
            "detached_reference": reference,
        }
    }))
}

fn snapshot_field_projection(
    value: Option<&Value>,
    attempt_id: &str,
    snapshot_path: &str,
) -> Result<Value> {
    let Some(value) = value else {
        return Ok(json!({
            "status":"unavailable",
            "reason":"attempt_snapshot_field_missing",
            "reference":{"kind":"attempt_snapshot","attempt_id":attempt_id,"path":snapshot_path},
        }));
    };
    let canonical = model::canonical(value)?;
    let item_count = value
        .as_array()
        .map(|items| items.len())
        .or_else(|| value.as_object().map(|object| object.len()))
        .unwrap_or(0);
    if canonical.len() <= MAX_INSPECT_SNAPSHOT_FIELD_BYTES
        && item_count <= MAX_INSPECT_SNAPSHOT_FIELD_ITEMS
    {
        Ok(json!({
            "status":"included",
            "serialized_bytes":canonical.len(),
            "item_count":item_count,
            "value":value,
        }))
    } else {
        Ok(json!({
            "status":"detached",
            "serialized_bytes":canonical.len(),
            "item_count":item_count,
            "digest":model::digest(canonical.as_bytes()),
            "reference":{"kind":"attempt_snapshot","attempt_id":attempt_id,"path":snapshot_path},
        }))
    }
}

fn bounded_page(
    source_kind: &str,
    after: i64,
    limit: i64,
    total: i64,
    items: Vec<Value>,
) -> Result<Value> {
    let limited = projection::limit_items(items, page_gap_reference)?;
    let next_after = after + limited.consumed as i64;
    let frame = page_frame(source_kind, after, next_after, limit, total, &limited)?;
    Ok(json!({
        "items": limited.items,
        "next_after": next_after,
        "total_items": total,
        "pagination": "offset_snapshot_not_inventory_proof",
        "projection": frame,
    }))
}

fn validate_filter_text(value: Option<&str>, field: &str) -> Result<Option<String>> {
    match value {
        None => Ok(None),
        Some(text) if !text.trim().is_empty() && text.len() <= 512 && !text.contains('\0') => {
            Ok(Some(text.to_owned()))
        }
        Some(_) => Err(Error::invalid(format!("{field} must be 1..512 bytes"))),
    }
}

fn optional_i64(params_value: &Value, field: &str, default: i64) -> Result<i64> {
    match params_value.get(field) {
        None => Ok(default),
        Some(value) => value
            .as_i64()
            .ok_or_else(|| Error::invalid(format!("{field} must be an integer"))),
    }
}

fn query_tasks(
    db: &Connection,
    project_id: Option<&str>,
    task_state: Option<&str>,
    after: i64,
    limit: i64,
) -> Result<(i64, Vec<TaskRow>)> {
    let total: i64 = db.query_row(
        "SELECT count(*) FROM tasks WHERE (?1 IS NULL OR project_id=?1) AND (?2 IS NULL OR state=?2)",
        params![project_id, task_state],
        |row| row.get(0),
    )?;
    let after = after.min(total);
    let sql = "SELECT t.task_id,t.project_id,t.revision,t.state,t.accepted_attempt_id,
                      t.accepted_operation_id,t.accepted_revision,t.accepted_phase,
                      t.accepted_candidate_ref,t.spec_json,t.created_at_ms,t.updated_at_ms,
                      (SELECT a.attempt_id FROM attempts a WHERE a.task_id=t.task_id
                       AND a.released_at_ms IS NULL ORDER BY a.created_at_ms DESC,a.attempt_id DESC LIMIT 1)
               FROM tasks t WHERE (?1 IS NULL OR t.project_id=?1) AND (?2 IS NULL OR t.state=?2)
               ORDER BY t.created_at_ms,t.task_id LIMIT ?3 OFFSET ?4";
    let mut statement = db.prepare(sql)?;
    let rows = statement
        .query_map(params![project_id, task_state, limit, after], |row| {
            let spec: String = row.get(9)?;
            Ok(TaskRow {
                task_id: row.get(0)?,
                project_id: row.get(1)?,
                revision: row.get(2)?,
                state: row.get(3)?,
                accepted_attempt_id: row.get(4)?,
                accepted_operation_id: row.get(5)?,
                accepted_revision: row.get(6)?,
                accepted_phase: row.get(7)?,
                accepted_candidate_ref: row.get(8)?,
                spec: serde_json::from_str(&spec).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        9,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                created_at_ms: row.get(10)?,
                updated_at_ms: row.get(11)?,
                current_attempt_id: row.get(12)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((total, rows))
}

fn query_task(db: &Connection, task_id: &str) -> Result<TaskRow> {
    let sql = "SELECT t.task_id,t.project_id,t.revision,t.state,t.accepted_attempt_id,
                      t.accepted_operation_id,t.accepted_revision,t.accepted_phase,
                      t.accepted_candidate_ref,t.spec_json,t.created_at_ms,t.updated_at_ms,
                      (SELECT a.attempt_id FROM attempts a WHERE a.task_id=t.task_id
                       AND a.released_at_ms IS NULL ORDER BY a.created_at_ms DESC,a.attempt_id DESC LIMIT 1)
               FROM tasks t WHERE t.task_id=?1";
    db.query_row(sql, [task_id], |row| {
        let spec: String = row.get(9)?;
        Ok(TaskRow {
            task_id: row.get(0)?,
            project_id: row.get(1)?,
            revision: row.get(2)?,
            state: row.get(3)?,
            accepted_attempt_id: row.get(4)?,
            accepted_operation_id: row.get(5)?,
            accepted_revision: row.get(6)?,
            accepted_phase: row.get(7)?,
            accepted_candidate_ref: row.get(8)?,
            spec: serde_json::from_str(&spec).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            created_at_ms: row.get(10)?,
            updated_at_ms: row.get(11)?,
            current_attempt_id: row.get(12)?,
        })
    })
    .optional()?
    .ok_or_else(|| Error::new("NOT_FOUND", format!("Task {task_id}")))
}

fn get_attempt_row(db: &Connection, attempt_id: &str) -> Result<Option<AttemptRow>> {
    let raw: Option<AttemptDbRow> = db
        .query_row(
            "SELECT attempt_id,task_id,task_revision,owner_id,start_owner,start_operation_id,
                    binding_id,binding_generation,state,released_at_ms,task_snapshot_json,
                    producers_json,submission_ref,candidate_ref,created_at_ms,updated_at_ms
             FROM attempts WHERE attempt_id=?1",
            [attempt_id],
            |row| {
                Ok(AttemptDbRow {
                    attempt_id: row.get(0)?,
                    task_id: row.get(1)?,
                    task_revision: row.get(2)?,
                    owner_id: row.get(3)?,
                    start_owner: row.get(4)?,
                    start_operation_id: row.get(5)?,
                    binding_id: row.get(6)?,
                    binding_generation: row.get(7)?,
                    state: row.get(8)?,
                    released_at_ms: row.get(9)?,
                    snapshot: row.get(10)?,
                    producers: row.get(11)?,
                    submission_ref: row.get(12)?,
                    candidate_ref: row.get(13)?,
                    created_at_ms: row.get(14)?,
                    updated_at_ms: row.get(15)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| {
        Ok(AttemptRow {
            attempt_id: raw.attempt_id,
            task_id: raw.task_id,
            task_revision: raw.task_revision,
            owner_id: raw.owner_id,
            start_owner: raw.start_owner,
            start_operation_id: raw.start_operation_id,
            binding_id: raw.binding_id,
            binding_generation: raw.binding_generation,
            state: raw.state,
            released_at_ms: raw.released_at_ms,
            snapshot: serde_json::from_str(&raw.snapshot)?,
            producers: serde_json::from_str(&raw.producers)?,
            submission_ref: raw.submission_ref,
            candidate_ref: raw.candidate_ref,
            created_at_ms: raw.created_at_ms,
            updated_at_ms: raw.updated_at_ms,
        })
    })
    .transpose()
}

fn current_task(db: &Connection, task_id: &str) -> Result<Option<CurrentTaskRow>> {
    Ok(db
        .query_row(
            "SELECT revision,state,accepted_attempt_id,accepted_operation_id,
                    (SELECT a.attempt_id FROM attempts a WHERE a.task_id=tasks.task_id
                     AND a.released_at_ms IS NULL
                     ORDER BY a.created_at_ms DESC,a.attempt_id DESC LIMIT 1)
             FROM tasks WHERE task_id=?1",
            [task_id],
            |row| {
                Ok(CurrentTaskRow {
                    revision: row.get(0)?,
                    state: row.get(1)?,
                    accepted_attempt_id: row.get(2)?,
                    accepted_operation_id: row.get(3)?,
                    current_attempt_id: row.get(4)?,
                })
            },
        )
        .optional()?)
}

fn owner_profile(db: &Connection, owner_id: &str) -> Result<Value> {
    let Some(profile) = meta(db, &format!("client:{owner_id}"))? else {
        return Ok(json!({"client_id":owner_id,"status":"not_registered"}));
    };
    let role = profile["role"].as_str().unwrap_or("unknown");
    let status = if profile["disabled"] == true {
        "disabled"
    } else if matches!(role, "operator" | "manager") {
        "registered_work_owner"
    } else {
        "registered_role_not_authorized_for_work"
    };
    Ok(json!({"client_id":owner_id,"role":role,"status":status}))
}

fn path_is_absolute(path: &str) -> bool {
    let bytes = path.as_bytes();
    Path::new(path).is_absolute()
        || path.starts_with("\\\\")
        || path.starts_with("//")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/'))
}

fn safe_scope_path(path: &str) -> Value {
    if path_is_absolute(path) || path.len() > MAX_SCOPE_PATH_BYTES {
        json!({
            "status":"redacted",
            "path_digest":model::digest(path.as_bytes()),
            "serialized_bytes":path.len(),
        })
    } else {
        json!({"status":"included","path":path})
    }
}

fn scope_projection(spec: Option<&TaskSpec>) -> Value {
    let Some(scope) = spec.and_then(|spec| spec.scope.as_ref()) else {
        return json!({"status":"not_recorded","initial_paths":[],"forbidden_paths":[]});
    };
    let initial_paths: Vec<Value> = scope
        .initial_paths
        .iter()
        .take(MAX_SCOPE_PATHS)
        .map(|path| safe_scope_path(path))
        .collect();
    let forbidden_paths: Vec<Value> = scope
        .forbidden_paths
        .iter()
        .take(MAX_SCOPE_PATHS)
        .map(|path| safe_scope_path(path))
        .collect();
    let total_paths = scope.initial_paths.len() + scope.forbidden_paths.len();
    let returned_paths = initial_paths.len() + forbidden_paths.len();
    let redacted = initial_paths
        .iter()
        .chain(forbidden_paths.iter())
        .filter(|path| path["status"] == "redacted")
        .count();
    json!({
        "status": if returned_paths < total_paths || redacted > 0 {"partial"} else {"recorded"},
        "initial_paths": initial_paths,
        "initial_path_count": scope.initial_paths.len(),
        "forbidden_paths": forbidden_paths,
        "forbidden_path_count": scope.forbidden_paths.len(),
        "prerequisite_policy": &scope.prerequisite_policy,
        "redacted_path_count": redacted,
        "omitted_path_count": total_paths.saturating_sub(returned_paths),
        "coverage_complete": returned_paths == total_paths && redacted == 0,
    })
}

fn source_brief(row: &TaskRow) -> (Value, Value, Option<TaskSpec>) {
    let brief = task_sources::project_brief(&row.spec);
    let spec = serde_json::from_value::<TaskSpec>(row.spec.clone()).ok();
    (
        brief,
        json!({"kind":"task_revision","task_id":row.task_id,"task_revision":row.revision,"method":"task.get"}),
        spec,
    )
}

fn dependency_projection(db: &Connection, spec: Option<&TaskSpec>) -> Result<Value> {
    let Some(spec) = spec else {
        return Ok(json!({"status":"unknown","accepted":[],"waiting":[]}));
    };
    let mut accepted = Vec::new();
    let mut waiting = Vec::new();
    let selected: Vec<&Dependency> = spec.dependencies.iter().take(MAX_DEPENDENCIES).collect();
    let omitted_count = spec.dependencies.len().saturating_sub(selected.len());
    for dependency in selected {
        match acceptance::resolve_dependency(db, dependency) {
            Ok(acceptance_operation_id) => accepted.push(json!({
                "task_id":dependency.task_id,
                "required_revision":dependency.required_revision,
                "required_phase":dependency.required_phase,
                "acceptance_operation_id":acceptance_operation_id,
            })),
            Err(error) if error.code == "DEPENDENCY_NOT_READY" => waiting.push(json!({
                "task_id":dependency.task_id,
                "required_revision":dependency.required_revision,
                "required_phase":dependency.required_phase,
                "status":"not_accepted_or_acceptance_invalidated",
            })),
            Err(error) => return Err(error),
        }
    }
    Ok(json!({
        "status": if !waiting.is_empty() {"waiting"} else if omitted_count > 0 {"unknown"} else {"satisfied"},
        "accepted": accepted,
        "waiting": waiting,
        "omitted_count":omitted_count,
        "gaps":if omitted_count > 0 {vec!["dependency_projection_limit"]} else {Vec::<&str>::new()},
    }))
}

fn readiness(
    db: &Connection,
    task: &TaskRow,
    attempt: Option<&AttemptRow>,
    spec: Option<&TaskSpec>,
    spec_valid: bool,
    dependencies: &Value,
) -> Result<Value> {
    let new_work = meta(db, "execution_mode")?.unwrap_or(Value::Null)["new_work"]
        .as_str()
        .unwrap_or("unknown")
        .to_owned();
    let mut blockers = Vec::new();
    let mut gaps = Vec::new();
    if !spec_valid {
        gaps.push("stored_task_spec_invalid_or_unreadable");
    }
    if task.state != "open" {
        blockers.push("task_not_open");
    }
    if let Some(attempt) = attempt {
        blockers.push("current_attempt_owns_task");
        if attempt.released_at_ms.is_some() {
            gaps.push("attempt_release_state_conflicts_with_current_index");
        }
    }
    if new_work == "disabled" {
        blockers.push("new_work_disabled");
    } else if new_work != "enabled" {
        gaps.push("new_work_admission_unknown");
    }
    if dependencies["status"] == "waiting" {
        blockers.push("dependency_acceptance_missing_or_invalidated");
    } else if dependencies["status"] == "unknown" {
        gaps.push("dependency_state_unknown");
    }
    let owner_policy = spec
        .map(|spec| policy_state(spec.owner_policy_id.as_deref()))
        .transpose()?
        .unwrap_or_else(|| json!({"status":"unknown"}));
    if owner_policy["status"] == "unavailable" {
        blockers.push("owner_policy_not_accepted");
    } else if owner_policy["status"] != "accepted" {
        gaps.push("owner_policy_state_unknown");
    }
    let state = if !blockers.is_empty() {
        "blocked"
    } else {
        // No route candidate or full-scope live capacity qualification is
        // recorded for an unassigned Task, so satisfying Task claim
        // preconditions alone cannot produce a `ready` queue row.
        "unknown"
    };
    if attempt.is_none() {
        gaps.push("eligible_owner_candidates_not_recorded");
        gaps.push("route_and_full_scope_binding_capacity_not_selected");
    }
    Ok(json!({
        "state":state,
        "basis":"derived from current admission, ownership policy, dependency, and Attempt facts; no launch readiness is implied",
        "task_state":task.state,
        "current_attempt_id":task.current_attempt_id,
        "owner_policy":owner_policy,
        "claim_preconditions":if blockers.is_empty() && spec_valid && dependencies["status"] == "satisfied" && new_work == "enabled" {"satisfied"} else {"not_satisfied_or_unknown"},
        "dependencies":dependencies,
        "new_work_admission":new_work,
        "resource_capacity":"unknown_until_exact_binding_and_scope_are_selected",
        "blockers":blockers,
        "gaps":gaps,
        "route_candidates":[],
        "route_candidate_status":"not_recorded; configured routes are not qualification or runtime capacity",
    }))
}

fn policy_state(policy_id: Option<&str>) -> Result<Value> {
    match crate::policy::accepted_edition(policy_id) {
        Ok(edition) => Ok(json!({"status":"accepted","edition":edition})),
        Err(error)
            if matches!(
                error.code.as_str(),
                "OWNER_POLICY_REQUIRED" | "OWNER_POLICY_UNKNOWN"
            ) =>
        {
            Ok(json!({"status":"unavailable","reason":error.code}))
        }
        Err(error) => Err(error),
    }
}

fn task_queue_item(
    db: &Connection,
    row: &TaskRow,
    brief_limit: usize,
    config: &Config,
) -> Result<Value> {
    let attempt = row
        .current_attempt_id
        .as_deref()
        .map(|id| get_attempt_row(db, id))
        .transpose()?
        .flatten();
    // Queue readiness and the row's brief are current Task-revision facts.
    // The exact frozen Attempt brief is reserved for `agent.inspect`.
    let (brief, brief_reference, spec) = source_brief(row);
    let spec_valid = spec.as_ref().is_some_and(|spec| spec.validate().is_ok());
    let dependency_status = dependency_projection(db, spec.as_ref())?;
    let ready = readiness(
        db,
        row,
        attempt.as_ref(),
        spec.as_ref(),
        spec_valid,
        &dependency_status,
    )?;
    let brief = launcher::brief_projection(&brief, brief_reference, brief_limit)?;
    let attempt_capacity = attempt
        .as_ref()
        .map(|attempt| exact_attempt_capacity(db, attempt))
        .transpose()?
        .unwrap_or_else(|| {
            json!({
                "status":"unknown",
                "reason":"no_exact_attempt_binding_or_owner_route",
                "capacity_available":null,
            })
        });
    let task_revision_current = attempt
        .as_ref()
        .is_none_or(|attempt| attempt.task_revision == row.revision);
    let attempt_value = if let Some(attempt) = attempt.as_ref() {
        json!({
            "attempt_id":attempt.attempt_id,
            "task_revision":attempt.task_revision,
            "state":attempt.state,
            "owner":owner_profile(db,&attempt.owner_id)?,
            "created_at_ms":attempt.created_at_ms,
            "start_owner":attempt.start_owner,
            "start_operation_id":attempt.start_operation_id,
            "binding": match (&attempt.binding_id,attempt.binding_generation) {
                (Some(id),Some(generation)) => binding_summary(db,id,generation)?,
                _ => Value::Null,
            },
            "task_revision_current":attempt.task_revision == row.revision,
            "submission_ref":attempt.submission_ref,
            "candidate_ref":attempt.candidate_ref,
            "updated_at_ms":attempt.updated_at_ms,
        })
    } else {
        Value::Null
    };
    let work_scope = scope_projection(spec.as_ref());
    let queue_state = if row.state == "accepted" {
        "accepted"
    } else if row.state == "archived" {
        "archived"
    } else if let Some(attempt) = attempt.as_ref() {
        match attempt.state.as_str() {
            "submitted" => "review",
            "reserved" => "claimed",
            "running" | "needs_correction" => "running",
            "recovery_pending" => "blocked",
            _ => "blocked",
        }
    } else {
        ready["state"].as_str().unwrap_or("unknown")
    };
    let mut gaps = Vec::new();
    if !spec_valid {
        gaps.push("task_spec_unavailable_or_invalid");
    }
    if dependency_status["status"] != "satisfied" {
        gaps.push("dependency_projection_not_complete_or_not_satisfied");
    }
    if work_scope["status"] != "recorded" {
        gaps.push("work_scope_not_fully_recorded_inline");
    }
    if attempt.is_none() {
        gaps.push("route_specific_runtime_capacity_not_assessed");
    } else if !task_revision_current {
        gaps.push("current_attempt_snapshot_is_for_an_older_task_revision");
    }
    if brief["status"] != "included" {
        gaps.push("source_brief_not_inline");
    }
    Ok(json!({
        "task_id":row.task_id,
        "project_id":row.project_id,
        "task_revision":row.revision,
        "task_state":row.state,
        "state":queue_state,
        "created_at_ms":row.created_at_ms,
        "updated_at_ms":row.updated_at_ms,
        "accepted_attempt_id":row.accepted_attempt_id,
        "accepted_operation_id":row.accepted_operation_id,
        "accepted_revision":row.accepted_revision,
        "accepted_phase":row.accepted_phase,
        "accepted_candidate_ref":row.accepted_candidate_ref,
        "current_attempt":attempt_value,
        "readiness":ready,
        "capacity":attempt_capacity,
        "route_admission":exact_attempt_admission(db, attempt.as_ref(), config)?,
        "task_brief":brief,
        "work_scope":work_scope,
        "ordering":{"created_at_ms":row.created_at_ms,"tie_breaker":"task_id"},
        "priority":{"status":"not_recorded"},
        "coverage":if gaps.is_empty() {"complete"} else {"partial"},
        "gaps":gaps,
    }))
}

/// Bounded exact-pair projection of the existing Manager attention page. A
/// partial source page or detached item never becomes a claim of no attention.
fn exact_binding_manager_attention(
    db: &Connection,
    binding_id: &str,
    generation: i64,
) -> Result<Value> {
    let page = manager_exceptions_page(
        db,
        &json!({"after":0,"limit":launcher::MAX_PAGE_SIZE}),
        false,
    )?;
    let exact_items = page["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| {
            item["binding_id"].as_str() == Some(binding_id)
                && item["generation"].as_i64() == Some(generation)
        })
        .cloned()
        .collect::<Vec<_>>();
    let exact_match_count = exact_items.len();
    let exact_items_truncated = exact_match_count > MAX_ATTEMPT_BINDING_ATTENTION_ITEMS;
    let items = exact_items
        .into_iter()
        .take(MAX_ATTEMPT_BINDING_ATTENTION_ITEMS)
        .collect::<Vec<_>>();
    let source_has_more = page["source_attention"]["has_more"]
        .as_bool()
        .unwrap_or(true)
        || page["projection"]["has_newer"] == true;
    let source_gap_count = page["projection"]["gap_count"].as_i64().unwrap_or(0);
    let source_coverage_complete = page["projection"]["coverage_complete"] == true
        && source_gap_count == 0
        && !source_has_more;
    let source_page_coverage_complete = page["projection"]["coverage_complete"] == true;
    let page_has_gap = !source_page_coverage_complete || source_gap_count > 0;
    let next_after = page["next_after"].as_i64().unwrap_or(0);
    let coverage_complete = source_coverage_complete && !exact_items_truncated;
    let requery = if coverage_complete {
        Value::Null
    } else {
        json!({
            "method":"swarm.exceptions.get",
            "params":{
                "after":if exact_items_truncated || page_has_gap {0} else {next_after},
                "limit":launcher::MAX_PAGE_SIZE,
            },
        })
    };
    Ok(json!({
        "status":if coverage_complete {"complete_source_page"} else {"partial"},
        "scope":{"binding_id":binding_id,"generation":generation},
        "source":{
            "method":"swarm.exceptions.get",
            "view":"items",
            "after":0,
            "next_after":next_after,
            "total_items":page["source_attention"]["total_items"],
            "has_more":source_has_more,
            "gap_count":source_gap_count,
            "coverage_complete":source_coverage_complete,
            "page_coverage_complete":source_page_coverage_complete,
        },
        "exact_match_count_on_page":exact_match_count,
        "exact_items_truncated":exact_items_truncated,
        "coverage_complete":coverage_complete,
        "items":items,
        "requery":requery,
    }))
}

fn binding_summary(db: &Connection, binding_id: &str, generation: i64) -> Result<Value> {
    let raw: Option<BindingReadRow> = db
        .query_row(
            "SELECT state,native_scope_key,native_root_id,route_json,state_json,released_at_ms
             FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id, generation],
            |row| {
                Ok(BindingReadRow {
                    state: row.get(0)?,
                    native_scope_key: row.get(1)?,
                    native_root_id: row.get(2)?,
                    route_json: row.get(3)?,
                    state_json: row.get(4)?,
                    released_at_ms: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(json!({
            "binding_id":binding_id,
            "generation":generation,
            "status":"missing_recorded_binding",
            "manager_attention":{
                "status":"binding_record_missing",
                "scope":{"binding_id":binding_id,"generation":generation},
                "coverage_complete":false,
                "items":[],
                "requery":{"method":"swarm.exceptions.get","params":{"after":0,"limit":launcher::MAX_PAGE_SIZE}},
            },
        }));
    };
    let route: Value = serde_json::from_str(&raw.route_json)?;
    let observation: Value = serde_json::from_str(&raw.state_json)?;
    let manager_attention = exact_binding_manager_attention(db, binding_id, generation)?;
    Ok(json!({
        "binding_id":binding_id,
        "generation":generation,
        "state":raw.state,
        "released_at_ms":raw.released_at_ms,
        "native_scope_key":raw.native_scope_key,
        "native_root_id":raw.native_root_id,
        "route":{
            "alias":route["alias"],
            "runtime":route["runtime"],
            "recorded":true,
            "live_qualified":false,
        },
        "observation":{
            "connection":observation["connection"],
            "observed_at_ms":observation["observed_at_ms"],
            "native_observation_id":observation["native_observation_id"],
            "recovery_required":observation["recovery_required"],
            "waiting_for":observation["waiting_for"],
            "execution":observation["execution"].as_str(),
        },
        "manager_attention":manager_attention,
        "gaps":["route_live_qualification_not_recorded","runtime_capability_receipt_not_recorded"],
    }))
}

fn exact_attempt_admission(
    db: &Connection,
    attempt: Option<&AttemptRow>,
    config: &Config,
) -> Result<Value> {
    let Some((id, generation)) = attempt.and_then(|attempt| {
        attempt
            .binding_id
            .as_deref()
            .zip(attempt.binding_generation)
    }) else {
        return Ok(json!({"status":"route_not_selected"}));
    };
    let binding = super::operations::get_binding(db, id, generation)?;
    Ok(serde_json::to_value(
        super::runtime::binding_route_admission(db, &binding, Some(config), None)?,
    )?)
}

fn exact_attempt_capacity(db: &Connection, attempt: &AttemptRow) -> Result<Value> {
    let (Some(binding_id), Some(generation)) =
        (attempt.binding_id.as_deref(), attempt.binding_generation)
    else {
        return Ok(json!({
            "status":"unknown",
            "reason":"attempt_has_no_exact_binding",
            "capacity_available":null,
        }));
    };
    let raw_binding: Option<(String, Option<String>)> = db
        .query_row(
            "SELECT route_json,native_scope_key FROM bindings
             WHERE binding_id=?1 AND generation=?2",
            params![binding_id, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((route_json, native_scope_key)) = raw_binding else {
        return Ok(json!({
            "status":"unknown",
            "reason":"attempt_binding_record_missing",
            "capacity_available":null,
        }));
    };
    let route: Value = serde_json::from_str(&route_json)?;
    let scope = capacity::scope_facts(&route, native_scope_key.as_deref(), binding_id);
    let mut accounting = capacity::attempt_accounting(db, &scope, &attempt.attempt_id)?;
    accounting["scope"] = json!({
        "scope_key":scope["scope_key"],
        "runtime":scope["runtime"],
        "provider":scope["provider"],
        "service":scope["service"],
        "identity":scope["identity"],
    });
    accounting["scope_capacity_available"] = Value::Null;
    accounting["capacity_reason"] = if accounting["status"] == "recorded_attempt_entries" {
        json!("single-binding projection does not revalidate the full scope roster")
    } else {
        accounting["status"].clone()
    };
    Ok(accounting)
}

fn authorize_launch_project(
    db: &Connection,
    p: &Principal,
    attempt: Option<&AttemptRow>,
) -> Result<()> {
    require_manager(db, p)?;
    if p.role == Role::Manager
        && !attempt.is_some_and(|attempt| {
            attempt.owner_id == p.client_id && attempt.released_at_ms.is_none()
        })
    {
        // An unassigned Task has no project-specific manager ACL in the
        // retained schema. Only the current GM can preview that scope.
        super::gm::require_authority(db, p)?;
    }
    Ok(())
}

/// Resolve the shared manual/WorkDispatch semantic launch slot from the
/// caller's already validated actor and exact current Task assignment.
pub(crate) fn resolve_launch_slot(
    db: &Connection,
    actor: &LaunchActor,
    preview: &launcher::LaunchPreviewRequest,
) -> Result<WorkDispatchLaunchSlotOutcome> {
    let task = query_task(db, &preview.task_id)?;
    if task.revision != preview.expected_task_revision || task.state != "open" {
        // Preserve the ordinary manual launch's retained blocked receipt for
        // stale revisions. An on-behalf context must never drift from its
        // verified source assignment.
        if actor.work_dispatch_context().is_some() {
            return Err(Error::new(
                "AUTOMATION_WORK_SUBJECT_STALE",
                "WorkDispatch launch subject is no longer current",
            ));
        }
        return Ok(super::automation_work_dispatch::LaunchSlotResolution::Vacant);
    }
    let attempt = task
        .current_attempt_id
        .as_deref()
        .map(|attempt_id| get_attempt_row(db, attempt_id))
        .transpose()?
        .flatten()
        .filter(|attempt| attempt.task_id == task.task_id && attempt.released_at_ms.is_none());
    let attempt_id = attempt.as_ref().map(|attempt| attempt.attempt_id.as_str());
    actor.require_action_object(db, "swarm.launch", &task.task_id, task.revision, attempt_id)?;
    if let Some(previous) = transferred_launch_slot(db, actor, preview)? {
        return Ok(previous);
    }
    super::automation_work_dispatch::resolve_assignment_slot(
        db,
        actor.effective_manager_id(),
        &task.task_id,
        task.revision,
        attempt_id,
        preview,
    )
}

pub(crate) fn retain_launch_slot(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    preview: &launcher::LaunchPreviewRequest,
    operation_id: &str,
    now_ms: i64,
) -> Result<LaunchSlotRetentionOutcome> {
    let task = query_task(tx, &preview.task_id)?;
    let attempt = task
        .current_attempt_id
        .as_deref()
        .map(|attempt_id| get_attempt_row(tx, attempt_id))
        .transpose()?
        .flatten()
        .filter(|attempt| attempt.task_id == task.task_id && attempt.released_at_ms.is_none());
    let attempt_id = attempt.as_ref().map(|attempt| attempt.attempt_id.as_str());
    actor.require_action_object(tx, "swarm.launch", &task.task_id, task.revision, attempt_id)?;
    if let Some(previous) = transferred_launch_slot(tx, actor, preview)? {
        return Ok(match previous {
            super::automation_work_dispatch::LaunchSlotResolution::Reuse {
                operation_id,
                operation_state,
            } => super::automation_work_dispatch::LaunchSlotRetention::Reuse {
                operation_id,
                operation_state,
            },
            super::automation_work_dispatch::LaunchSlotResolution::Conflict {
                operation_id,
                operation_state,
            } => super::automation_work_dispatch::LaunchSlotRetention::Conflict {
                operation_id,
                operation_state,
            },
            super::automation_work_dispatch::LaunchSlotResolution::Vacant => {
                return Err(Error::new(
                    "AUTOMATION_TRANSFER_SLOT_CORRUPT",
                    "transfer alias lookup returned a vacant slot",
                ));
            }
        });
    }
    super::automation_work_dispatch::retain_assignment_slot(
        tx,
        actor.effective_manager_id(),
        &task.task_id,
        task.revision,
        attempt_id,
        preview,
        operation_id,
        now_ms,
    )
}

/// A transferred WorkDispatch journal keeps its old manager-keyed semantic
/// slot. Before reserving a successor-owned slot, consult every exact former
/// owner in the sealed transfer chain; an existing prior slot is reused or
/// blocks a changed request, never shadowed by a new owner-keyed effect.
fn transferred_launch_slot(
    db: &Connection,
    actor: &LaunchActor,
    preview: &launcher::LaunchPreviewRequest,
) -> Result<Option<WorkDispatchLaunchSlotOutcome>> {
    let Some(context) = actor.work_dispatch_context() else {
        return Ok(None);
    };
    let lineage = crate::automation::config::transfer_lineage(
        db,
        context.effective_manager_id(),
        context.project_id(),
        context.automation_id(),
    )?;
    let mut existing = Vec::new();
    for transfer in lineage {
        match super::automation_work_dispatch::resolve_assignment_slot(
            db,
            &transfer.former_owner_manager_id,
            &preview.task_id,
            preview.expected_task_revision,
            context.subject().attempt_id(),
            preview,
        )? {
            super::automation_work_dispatch::LaunchSlotResolution::Vacant => {}
            resolution => existing.push(resolution),
        }
    }
    match existing.len() {
        0 => Ok(None),
        1 => Ok(existing.pop()),
        _ => Err(Error::new(
            "AUTOMATION_TRANSFER_SLOT_AMBIGUOUS",
            "multiple former-owner launch slots exist for one transferred WorkDispatch subject",
        )),
    }
}

fn launch_operation_projection(
    db: &Connection,
    task_id: &str,
    exclude_operation_id: Option<&str>,
) -> Result<Value> {
    let total: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE task_id=?1 AND method='swarm.launch'
         AND (?2 IS NULL OR operation_id<>?2)",
        params![task_id, exclude_operation_id],
        |row| row.get(0),
    )?;
    let unresolved: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE task_id=?1 AND method='swarm.launch'
         AND state IN ('queued','sending','native_accepted','outcome_unknown')
         AND (?2 IS NULL OR operation_id<>?2)",
        params![task_id, exclude_operation_id],
        |row| row.get(0),
    )?;
    let unknown: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE task_id=?1 AND method='swarm.launch'
         AND state='outcome_unknown' AND (?2 IS NULL OR operation_id<>?2)",
        params![task_id, exclude_operation_id],
        |row| row.get(0),
    )?;
    let mut statement = db.prepare(
        "SELECT operation_id,state,attempt_id,created_at_ms,updated_at_ms FROM operations
         WHERE task_id=?1 AND method='swarm.launch'
         AND (?2 IS NULL OR operation_id<>?2)
         ORDER BY created_at_ms DESC,operation_id DESC LIMIT ?3",
    )?;
    let items = statement
        .query_map(
            params![task_id, exclude_operation_id, MAX_LAUNCH_OPERATION_ROWS],
            |row| {
                let operation_id: String = row.get(0)?;
                let state: String = row.get(1)?;
                let attempt_id: Option<String> = row.get(2)?;
                let created_at_ms: i64 = row.get(3)?;
                let updated_at_ms: i64 = row.get(4)?;
                Ok(json!({
                    "operation_id":operation_id,
                    "state":state,
                    "attempt_id":attempt_id,
                    "created_at_ms":created_at_ms,
                    "updated_at_ms":updated_at_ms,
                }))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(json!({
        "items":items,
        "total_items":total,
        "unresolved_count":unresolved,
        "outcome_unknown_count":unknown,
        "coverage":if total > MAX_LAUNCH_OPERATION_ROWS {"partial"} else {"complete"},
        "next_after":if total > MAX_LAUNCH_OPERATION_ROWS {
            json!(MAX_LAUNCH_OPERATION_ROWS)
        } else {
            Value::Null
        },
    }))
}

struct ConfiguredRouteModel<'a> {
    provider_id: Option<&'a str>,
    model_id: Option<&'a str>,
    variant: Option<&'a str>,
}

fn configured_route_model(route: &crate::config::Route) -> ConfiguredRouteModel<'_> {
    if let Some(owned) = &route.owned_service {
        return ConfiguredRouteModel {
            provider_id: Some(&owned.model.provider_id),
            model_id: Some(&owned.model.id),
            variant: Some(&owned.model.variant),
        };
    }
    let model = &route.native_options["model"];
    let model_id = model
        .get("id")
        .or_else(|| model.get("modelID"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 256);
    let provider_id = model
        .get("providerID")
        .or_else(|| model.get("provider_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 256);
    let variant = model
        .get("variant")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 256);
    ConfiguredRouteModel {
        provider_id,
        model_id,
        variant,
    }
}

fn safe_route_runtime(runtime: &str) -> &'static str {
    if runtime == crate::runtime::codex::RUNTIME {
        "codex"
    } else if runtime == crate::runtime::opencode_v2::RUNTIME {
        "opencode_v2"
    } else {
        "unknown"
    }
}

fn launch_route_projection(
    config: &Config,
    request: &launcher::LaunchPreviewRequest,
    hard_blocks: &mut Vec<&'static str>,
    gaps: &mut Vec<&'static str>,
) -> Value {
    let route = config
        .routes
        .iter()
        .find(|route| route.alias == request.route);
    let Some(route) = route else {
        hard_blocks.push("route_not_configured");
        gaps.push("route_live_qualification_and_provider_capacity_not_observed");
        return json!({
            "alias":request.route,
            "configured":false,
            "enabled":false,
            "live_qualified":false,
            "requested_model":request.requested_model,
            "requested_effort":request.requested_effort,
            "agent_profile":{"requested":request.agent_profile,"validation":"unknown"},
            "capability_gaps":["route_not_configured","provider_model_and_agent_catalog_not_retained"],
        });
    };
    if !route.enabled {
        hard_blocks.push("route_disabled");
    }
    let configured_model = configured_route_model(route);
    let model_status = match request.requested_model.as_deref() {
        None => "not_requested",
        Some(requested) if configured_model.model_id == Some(requested) => {
            "matches_configured_route_model_id"
        }
        Some(_) => "unknown_without_retained_provider_model_catalog",
    };
    let effort_status = match request.requested_effort.as_deref() {
        None => "not_requested",
        Some(requested) if configured_model.variant == Some(requested) => {
            "matches_configured_route_variant"
        }
        Some(_) => "unknown_without_retained_provider_model_catalog",
    };
    if model_status == "unknown_without_retained_provider_model_catalog"
        || effort_status == "unknown_without_retained_provider_model_catalog"
    {
        gaps.push("requested_provider_model_or_variant_not_validated_against_a_retained_catalog");
    }
    gaps.push("route_configuration_does_not_prove_live_runtime_qualification_or_capacity");
    gaps.push("native_agent_profile_catalog_not_retained_for_preview");
    json!({
        "alias":route.alias,
        "configured":true,
        "enabled":route.enabled,
        "runtime_kind":safe_route_runtime(&route.runtime),
        "live_qualified":false,
        "configured_model":{
            "status":if configured_model.provider_id.is_some()
                && configured_model.model_id.is_some()
                && configured_model.variant.is_some() {
                "recorded_in_route_configuration"
            } else {
                "not_recorded_in_route_configuration"
            },
            "provider_id_recorded":configured_model.provider_id.is_some(),
            "model_id_recorded":configured_model.model_id.is_some(),
            "variant_recorded":configured_model.variant.is_some(),
        },
        "requested_model":{
            "value":request.requested_model,
            "validation":model_status,
        },
        "requested_effort":{
            "value":request.requested_effort,
            "validation":effort_status,
        },
        "agent_profile":{
            "requested":request.agent_profile,
            "validation":"unknown_without_retained_native_agent_catalog",
        },
        "capability_gaps":[
            "live_route_qualification_not_observed",
            "provider_model_catalog_not_retained",
            "native_agent_profile_catalog_not_retained",
            "provider_runtime_capacity_not_selected_for_a_new_binding",
        ],
    })
}

fn launch_mcp_profile_projection(
    db: &Connection,
    config: &Config,
    actor: &LaunchActor,
    request: &launcher::LaunchPreviewRequest,
    hard_blocks: &mut Vec<&'static str>,
) -> Result<Value> {
    let Some(profile) = config.mcp.profiles.get(&request.mcp_profile) else {
        hard_blocks.push("mcp_profile_not_configured");
        return Ok(json!({
            "profile_name":request.mcp_profile,
            "status":"not_configured",
            "surface":request.mcp_surface,
        }));
    };
    let role = profile.tool_profile;
    let manager_translation = role == McpToolProfile::Manager
        && actor.role() == Role::Manager
        && profile.expected_client_id == actor.effective_manager_id();
    let launch_participant = role == McpToolProfile::Participant || manager_translation;
    let assigned_reviewer = role == McpToolProfile::AssignedReviewer;
    if !launch_participant && !assigned_reviewer {
        hard_blocks.push("mcp_profile_is_not_a_narrow_assignment_role");
    }
    if assigned_reviewer && request.purpose != "review" {
        hard_blocks.push("assigned_reviewer_profile_requires_review_purpose");
    }
    if launch_participant && request.purpose == "review" {
        hard_blocks.push("review_purpose_requires_assigned_reviewer_profile");
    }

    // An exact Manager profile can select launch work, but never supplies the
    // native role surface. The fresh Participant receives only the explicitly
    // requested Participant surface, with no inherited Manager groups or
    // manual methods.
    let identity_state = if launch_participant {
        "assignment_template"
    } else if assigned_reviewer {
        let identity = meta(db, &format!("client:{}", profile.expected_client_id))?;
        let state = match identity.as_ref() {
            None => "not_registered",
            Some(client) if client["disabled"] == true => "disabled",
            Some(client) if client["role"] != "participant" => "role_mismatch",
            Some(_) => "registered_enabled_participant",
        };
        if state != "registered_enabled_participant" {
            hard_blocks.push("configured_mcp_identity_is_not_an_enabled_participant");
        }
        state
    } else {
        "not_applicable"
    };

    if !manager_translation
        && profile
            .surface
            .as_deref()
            .is_some_and(|configured| configured != request.mcp_surface)
    {
        hard_blocks.push("requested_mcp_surface_differs_from_configured_profile_surface");
    }
    let target_role = if manager_translation {
        McpToolProfile::Participant
    } else {
        role
    };
    let target_groups = if manager_translation {
        &[][..]
    } else {
        profile.deferred_groups.as_slice()
    };
    let target_manual_tools = if manager_translation {
        &[][..]
    } else {
        profile.manual_tools.as_slice()
    };
    let surface = match crate::mcp::launch_profile_surface(
        target_role,
        &request.mcp_surface,
        target_groups,
        target_manual_tools,
    ) {
        Ok(surface) => surface,
        Err(error) if error.code == "INVALID_PARAMS" => {
            hard_blocks.push("mcp_surface_not_authorized_by_static_catalog");
            json!({
                "status":"invalid_for_profile",
                "profile_name":request.mcp_profile,
                "hard_profile":target_role,
                "surface":request.mcp_surface,
            })
        }
        Err(error) => return Err(error.into()),
    };
    Ok(json!({
        "status":if surface["surface_id"].is_string() {"validated_against_static_catalog"} else {"invalid_for_profile"},
        "profile_name":request.mcp_profile,
        "hard_profile":target_role,
        "identity":{"status":identity_state,"role":"participant"},
        "credential_issuance":if launch_participant {"required_per_launch"} else {"existing_review_assignment_required"},
        "surface":request.mcp_surface,
        "surface_facts":surface,
        "runtime_loaded":"unknown",
        "gaps":["mcp_profile_configuration_does_not_prove_native_runtime_tool_loading"],
    }))
}

fn launch_workspace_projection(request: &launcher::LaunchPreviewRequest) -> Value {
    let supported = request.workspace_policy == "manager_owned_worktree";
    json!({
        "policy":request.workspace_policy,
        "status":if supported {"not_provisioned"} else {"unsupported_policy"},
        "lease":"unknown",
        "dirty":Value::Null,
        "filesystem_inspected":false,
        "repository_handle":Value::Null,
        "worktree_handle":Value::Null,
        "branch":Value::Null,
        "baseline_commit":Value::Null,
    })
}

/// Resolve an optional Task baseline artifact to the exact Git commit it
/// records. An artifact reference alone is not a Git object ID: only a
/// complete source snapshot with verified same-project acceptance is a pin.
fn workspace_baseline_projection(
    db: &Connection,
    row: &TaskRow,
    spec: Option<&TaskSpec>,
) -> Result<Value> {
    let Some(candidate_ref) = spec.and_then(|spec| spec.baseline_candidate_ref.as_deref()) else {
        return Ok(json!({
            "status":"not_configured",
            "candidate_ref":Value::Null,
            "commit":Value::Null,
        }));
    };
    let frozen =
        super::acceptance::freeze_baseline_candidate(db, &row.project_id, Some(candidate_ref))?;
    if frozen["status"] != "verified" {
        return Ok(json!({
            "status":"unverifiable",
            "candidate_ref":candidate_ref,
            "reason":frozen["reason"],
            "commit":Value::Null,
        }));
    }
    let artifact = match super::results::get(db, candidate_ref) {
        Ok(artifact) => artifact,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(json!({
                "status":"unverifiable",
                "candidate_ref":candidate_ref,
                "reason":"baseline_source_snapshot_not_registered",
                "commit":Value::Null,
            }));
        }
        Err(error) => return Err(error),
    };
    let commit = artifact.metadata["commit"].as_str();
    if artifact.kind != "source_snapshot"
        || artifact.metadata["coverage"] != "complete"
        || artifact.metadata["task_id"] != frozen["task_id"]
        || artifact.metadata["attempt_id"] != frozen["attempt_id"]
        || artifact.metadata["task_revision"] != frozen["task_revision"]
        || commit.is_none_or(|commit| !crate::forge::valid_object_id(commit))
    {
        return Ok(json!({
            "status":"unverifiable",
            "candidate_ref":candidate_ref,
            "reason":"baseline_source_snapshot_has_no_verified_full_git_commit",
            "commit":Value::Null,
        }));
    }
    Ok(json!({
        "status":"verified_source_snapshot_commit",
        "candidate_ref":candidate_ref,
        "commit":commit,
        "artifact_digest":artifact.content_digest,
        "acceptance_task_id":frozen["task_id"],
        "acceptance_attempt_id":frozen["attempt_id"],
        "acceptance_task_revision":frozen["task_revision"],
    }))
}

fn normalized_scope_path(path: &str) -> String {
    path.replace('\\', "/").trim_end_matches('/').to_lowercase()
}

fn scope_paths_overlap(left: &str, right: &str) -> bool {
    let left = normalized_scope_path(left);
    let right = normalized_scope_path(right);
    left == right
        || left
            .strip_prefix(&right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(&left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn validate_workspace_scope_path(path: &str) -> bool {
    !path.trim().is_empty()
        && path.len() <= MAX_SCOPE_PATH_BYTES
        && !path_is_absolute(path)
        && !path.contains(['\\', ':', '\0'])
        && !path.chars().any(char::is_control)
        && !path.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part.eq_ignore_ascii_case(".git")
                || part.ends_with(['.', ' '])
        })
}

/// Admit one digest-bound launch intent. This transaction re-runs the exact
/// preview before recording a manifest. It does not claim an Attempt, open a
/// binding, dispatch work, or assert that an external effect occurred. The
/// Host prepares a verified workspace lease before advancing this intent.
fn launch_actor_manifest(actor: &LaunchActor) -> Value {
    match actor {
        LaunchActor::Direct(principal) => json!({
            "kind":"direct",
            "client_id":principal.client_id,
            "role":principal.role,
            "link_id":principal.link_id,
        }),
        LaunchActor::OnBehalf(context) => json!({
            "kind":"work_dispatch",
            "client_id":context.technical_requester_id(),
            "role":"manager",
            "link_id":null,
            "effective_manager_id":context.effective_manager_id(),
            "automation_id":context.automation_id(),
            "automation_revision":context.automation_revision(),
            "semantic_slot_id":context.semantic_slot_id(),
        }),
    }
}

fn launch_preview_value(preview: &launcher::LaunchPreviewRequest) -> Value {
    json!({
        "task_id":preview.task_id,
        "expected_task_revision":preview.expected_task_revision,
        "route":preview.route,
        "agent_profile":preview.agent_profile,
        "mcp_profile":preview.mcp_profile,
        "mcp_surface":preview.mcp_surface,
        "workspace_policy":preview.workspace_policy,
        "requested_model":preview.requested_model,
        "requested_effort":preview.requested_effort,
        "budget":preview.budget,
        "stop_conditions":preview.stop_conditions,
        "purpose":preview.purpose,
    })
}

/// Convert committed WorkDispatch facts into the same durable launch
/// Operation used by direct callers. The callback is transaction-only: it
/// reserves authority and returns a receipt, but never starts Host/native work.
pub(crate) fn admit_work_dispatch(
    tx: &Transaction<'_>,
    prepared: &super::automation_work_dispatch::PreparedWorkDispatch,
    config: &Config,
    now_ms: i64,
) -> Result<super::automation_work_dispatch::WorkDispatchOutcome> {
    use super::automation_work_dispatch::{LaunchSlotResolution, WorkDispatchOutcome};

    let context = prepared.context();
    let actor = LaunchActor::OnBehalf(Box::new(context.clone()));
    actor.require_current(tx)?;
    let preview_request = prepared.preview();
    let resolution = match resolve_launch_slot(tx, &actor, preview_request) {
        Ok(resolution) => resolution,
        Err(error) if error.code == "WORK_DISPATCH_GM_REQUIRED" => {
            return Ok(WorkDispatchOutcome::Pending {
                reason: "current_gm_required".to_owned(),
                wake_when: vec!["current_gm_changed".to_owned()],
            });
        }
        Err(error) => return Err(error),
    };
    match resolution {
        LaunchSlotResolution::Reuse { operation_id, .. } => {
            return Ok(WorkDispatchOutcome::Admitted { operation_id });
        }
        LaunchSlotResolution::Conflict { .. } => {
            return Ok(WorkDispatchOutcome::Skipped {
                reason: "semantic_launch_slot_conflict".to_owned(),
            });
        }
        LaunchSlotResolution::Vacant => {}
    }

    let preview_params = launch_preview_value(preview_request);
    let planned = match launch_preview_inner(tx, &actor, &preview_params, config, None) {
        Ok(planned) => planned,
        Err(error) if error.code == "WORK_DISPATCH_GM_REQUIRED" => {
            return Ok(WorkDispatchOutcome::Pending {
                reason: "current_gm_required".to_owned(),
                wake_when: vec!["current_gm_changed".to_owned()],
            });
        }
        Err(error) => return Err(error),
    };
    let preview = planned.effect_facts()?;
    let hard_blocks = preview["hard_blocks"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let admission: provider_conditions::RouteAdmissionProjection =
        serde_json::from_value(preview["route"]["admission"].clone())?;
    if let provider_conditions::RouteAdmissionDecision::Hold { code, .. } = admission.decision {
        return Ok(WorkDispatchOutcome::Pending {
            reason: code,
            wake_when: vec!["launch_readiness_changed".to_owned()],
        });
    }
    if let Some(reason) = hard_blocks.first().and_then(Value::as_str) {
        return Ok(WorkDispatchOutcome::Pending {
            reason: reason.chars().take(128).collect(),
            wake_when: vec![
                "launch_readiness_changed".to_owned(),
                "task_assignment_changed".to_owned(),
            ],
        });
    }
    if preview["attempt_action"] == "forbidden" {
        return Ok(WorkDispatchOutcome::Pending {
            reason: "launch_attempt_action_unavailable".to_owned(),
            wake_when: vec!["task_assignment_changed".to_owned()],
        });
    }
    let plan_digest = model::text(&preview, "plan_digest")?;
    let (request, original_request) = prepared.launch_request(plan_digest)?;
    model::validate_mutation("swarm.launch", &original_request)?;
    let canonical_request = model::canonical(&original_request)?;
    let prior: Option<(String, String)> = tx
        .query_row(
            "SELECT method,original_request_json FROM operations \
             WHERE caller_id=?1 AND client_request_id=?2",
            params![context.technical_requester_id(), request.client_request_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if prior.is_some() {
        return Err(Error::new(
            "AUTOMATION_IDEMPOTENCY_CORRUPT",
            "deterministic WorkDispatch request exists without its semantic slot",
        ));
    }

    let operation_id = model::new_id();
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json, \
         effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'swarm.launch',?4,'{}','queued',?5,?5,?5)",
        params![
            operation_id,
            context.technical_requester_id(),
            request.client_request_id,
            canonical_request,
            now_ms
        ],
    )?;
    let (result, queued) =
        launch_for_actor(tx, &actor, &original_request, config, &operation_id, now_ms)?;
    if !queued {
        return Err(Error::new(
            "AUTOMATION_LAUNCH_READINESS_CHANGED",
            "launch readiness changed before WorkDispatch admission committed",
        ));
    }
    tx.execute(
        "UPDATE operations SET state='queued',result_json=?2,settled_at_ms=NULL,updated_at_ms=?3 \
         WHERE operation_id=?1 AND caller_id=?4 AND method='swarm.launch' AND state='queued'",
        params![
            operation_id,
            model::canonical(&result)?,
            now_ms,
            context.technical_requester_id()
        ],
    )?;
    super::capacity::sync_operation(tx, &operation_id, now_ms)?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller',?1,?1,'swarm.launch',?2,?3)",
        params![operation_id, model::canonical(&result)?, now_ms],
    )?;
    Ok(WorkDispatchOutcome::Admitted { operation_id })
}

pub(super) fn launch(
    tx: &Transaction<'_>,
    p: &Principal,
    params_value: &Value,
    config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    launch_for_actor(
        tx,
        &LaunchActor::Direct(p.clone()),
        params_value,
        config,
        operation_id,
        now,
    )
}

pub(crate) fn launch_for_actor(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    params_value: &Value,
    config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let request = launcher::LaunchRequest::parse(params_value)?;
    actor.require_current(tx)?;
    let preview_params = request.preview_params();
    let planned = launch_preview_inner(tx, actor, &preview_params, config, Some(operation_id))?;
    let preview = planned.effect_facts()?;
    if preview["plan_digest"] != request.plan_digest {
        return Err(Error::new(
            "STALE_LAUNCH_PLAN",
            "launch preview changed; obtain a fresh digest-bound preview",
        ));
    }

    let blockers = preview["hard_blocks"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let blocked = !blockers.is_empty() || preview["attempt_action"] == "forbidden";
    let task_id = model::text(&preview["task"], "task_id")?;
    let task_revision = model::positive(&preview["task"], "revision")?;
    let attempt_id = preview["current_attempt"]["attempt_id"]
        .as_str()
        .map(str::to_owned);
    let launch_state = if blocked {
        "blocked"
    } else {
        "pending_workspace"
    };
    let manifest = json!({
        "manifest_version":"eliot-launch-manifest-v1",
        "state":launch_state,
        "created_at_ms":now,
        "actor":launch_actor_manifest(actor),
        "client_request_id":request.client_request_id,
        "plan_digest":request.plan_digest,
        "request":preview_params,
        "task":{
            "task_id":task_id,
            "project_id":preview["task"]["project_id"],
            "expected_revision":request.preview.expected_task_revision,
            "observed_revision":task_revision,
            "attempt_action":preview["attempt_action"],
            "attempt_id":attempt_id,
            "candidate_scope":preview["candidate_scope"],
        },
        "workspace":{
            "policy":request.preview.workspace_policy,
            "lease_state":if blocked {"not_started"} else {"pending"},
            "dirty_state":"unknown_until_verified_lease",
            "filesystem_inspected":false,
        },
        "runtime":{
            "route":preview["route"],
            "agent_profile":request.preview.agent_profile,
            "requested_model":request.preview.requested_model,
            "requested_effort":request.preview.requested_effort,
            "budget":request.preview.budget,
            "stop_conditions":request.preview.stop_conditions,
            "purpose":request.preview.purpose,
            "state":"not_started",
            "native_effect":"not_attempted",
        },
        "mcp":preview["mcp"],
        "preflight":{
            "digest_revalidated":true,
            "hard_blocks":blockers,
            "coverage":preview["coverage"],
            "gaps":preview["gaps"],
        },
        "progress":{
            "workspace_lease":if blocked {"not_started"} else {"pending"},
            "attempt_claim":"not_started",
            "binding_open":"not_started",
            "task_dispatch":"not_started",
            "capability_readback":"not_started",
        },
        "effects":"none_until_verified_workspace_lease",
    });
    validate_launch_manifest_precommit(&manifest, &planned, &request)?;
    let effective = json!({
        "operation_contract":{
            "effect_scope":"one_exact_launch_plan",
            "completion_condition":"workspace_binding_dispatch_and_capability_readback",
            "replay_policy":"same_request_id_returns_retained_launch_receipt; unknown_effects_require_readback",
            "contract_revision":"swarm-launch-v1",
        },
        "launch_manifest":manifest,
        "launch_plan_authority":planned.authority,
    });
    let effective_json = model::canonical(&effective)?;
    if effective_json.len() > MAX_LAUNCH_MANIFEST_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "complete launch manifest exceeds its retained byte boundary",
        ));
    }
    let original_request = model::canonical(params_value)?;
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4
         WHERE operation_id=?1 AND caller_id=?5 AND client_request_id=?6
           AND method='swarm.launch' AND state='queued' AND original_request_json=?7",
        params![
            operation_id,
            task_id,
            attempt_id,
            effective_json,
            actor.technical_requester_id(),
            request.client_request_id,
            original_request,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "launch Operation changed before its manifest could be committed",
        ));
    }

    if !blocked {
        // Persist the private manager attribution first. Slot verification
        // reloads an automated Operation and requires that attribution; both
        // records remain invisible until this admission transaction commits.
        if let Some(context) = actor.work_dispatch_context() {
            super::automation_work_dispatch::save_operation_link(tx, operation_id, context, now)?;
        }
        match retain_launch_slot(tx, actor, &request.preview, operation_id, now)? {
            super::automation_work_dispatch::LaunchSlotRetention::Retained => {}
            super::automation_work_dispatch::LaunchSlotRetention::Reuse { .. }
            | super::automation_work_dispatch::LaunchSlotRetention::Conflict { .. } => {
                return Err(Error::new(
                    "LAUNCH_SLOT_CONFLICT",
                    "semantic launch slot changed after admission preflight",
                ));
            }
        }
    }

    if blocked {
        return Ok((
            json!({
                "operation_id":operation_id,
                "launch_state":"blocked",
                "state":"blocked",
                "plan_digest":request.plan_digest,
                "task_id":task_id,
                "task_revision":task_revision,
                "attempt_id":attempt_id,
                "native_effect":"not_attempted",
                "hard_blocks":blockers,
                "gaps":preview["gaps"],
            }),
            false,
        ));
    }

    Ok((
        json!({
            "operation_id":operation_id,
            "launch_state":"pending_workspace",
            "state":"queued",
            "plan_digest":request.plan_digest,
            "task_id":task_id,
            "task_revision":task_revision,
            "attempt_action":preview["attempt_action"],
            "attempt_id":attempt_id,
            "workspace_lease":"pending",
            "attempt_claim":"not_started",
            "binding_open":"not_started",
            "task_dispatch":"not_started",
            "native_effect":"not_attempted",
            "capability_state":"unknown",
            "next_phase":"prepare_and_verify_registered_workspace_lease",
            "gaps":preview["gaps"],
        }),
        true,
    ))
}

fn retained_launch_manifest(db: &Connection, operation_id: &str) -> Result<Value> {
    let raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
        |row| row.get(0),
    )?;
    let effective: Value = serde_json::from_str(&raw)?;
    effective
        .get("launch_manifest")
        .cloned()
        .ok_or_else(|| Error::new("INVALID_LAUNCH_MANIFEST", "launch manifest is missing"))
}

struct LaunchBindingIdentity<'a> {
    id: &'a str,
    generation: i64,
}

struct LaunchProgress<'a> {
    manifest: Value,
    result: &'a Value,
    operation_state: &'a str,
    attempt_id: Option<&'a str>,
    binding: Option<LaunchBindingIdentity<'a>>,
    now: i64,
}

fn persist_launch_progress(
    tx: &Transaction<'_>,
    operation_id: &str,
    mut progress: LaunchProgress<'_>,
) -> Result<()> {
    let raw: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
        |row| row.get(0),
    )?;
    let mut effective: Value = serde_json::from_str(&raw)?;
    progress.manifest["state"] = json!(progress.result["launch_state"]);
    effective["launch_manifest"] = progress.manifest;
    effective["receipt"] = json!({"ok":true,"value":progress.result});
    let task_id = effective["launch_manifest"]["task"]["task_id"]
        .as_str()
        .ok_or_else(|| Error::new("INVALID_LAUNCH_MANIFEST", "launch Task identity is missing"))?;
    let settled_at = matches!(
        progress.operation_state,
        "settled" | "rejected" | "cancelled"
    )
    .then_some(progress.now);
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=COALESCE(?3,attempt_id), \
         binding_id=COALESCE(?4,binding_id),binding_generation=COALESCE(?5,binding_generation), \
         state=?6,result_json=?7,effective_request_json=?8,settled_at_ms=?9,updated_at_ms=?10 \
         WHERE operation_id=?1 AND method='swarm.launch' AND state IN ('queued','outcome_unknown')",
        params![
            operation_id,
            task_id,
            progress.attempt_id,
            progress.binding.as_ref().map(|binding| binding.id),
            progress.binding.as_ref().map(|binding| binding.generation),
            progress.operation_state,
            model::canonical(progress.result)?,
            model::canonical(&effective)?,
            settled_at,
            progress.now,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "launch Operation changed before progress could be retained",
        ));
    }
    let digest = model::digest(model::canonical(progress.result)?.as_bytes());
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller',?1,?2,'swarm.launch.progress',?3,?4)",
        params![
            format!("launch-progress:{operation_id}:{digest}"),
            operation_id,
            model::canonical(progress.result)?,
            progress.now,
        ],
    )?;
    Ok(())
}

/// Reconstruct an admitted actor from the immutable launch manifest and,
/// for WorkDispatch, its integrity-checked private Operation link.
fn retained_launch_actor(
    db: &Connection,
    operation_id: &str,
) -> Result<(Value, Value, LaunchActor)> {
    let operation = super::operations::get_operation(db, operation_id)?;
    if operation["method"] != "swarm.launch" {
        return Err(Error::new("FORBIDDEN", "Operation is not a launch"));
    }
    let manifest = retained_launch_manifest(db, operation_id)?;
    let caller_id = model::text(&operation, "caller_id")?;
    let retained_client_id = model::text(&manifest["actor"], "client_id")?;
    if caller_id == crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        if retained_client_id != caller_id || manifest["actor"]["kind"] != "work_dispatch" {
            return Err(Error::new(
                "FORBIDDEN",
                "on-behalf launch manifest does not match its technical Operation caller",
            ));
        }
        let context = super::automation_work_dispatch::context_for_operation(db, operation_id)?
            .ok_or_else(|| Error::new("FORBIDDEN", "WorkDispatch launch link is missing"))?;
        if manifest["actor"]["effective_manager_id"] != context.effective_manager_id()
            || manifest["actor"]["automation_id"] != context.automation_id()
            || manifest["actor"]["automation_revision"] != context.automation_revision()
            || manifest["actor"]["semantic_slot_id"] != context.semantic_slot_id()
        {
            return Err(Error::new(
                "FORBIDDEN",
                "on-behalf launch manifest differs from its retained Store link",
            ));
        }
        return Ok((
            operation,
            manifest,
            LaunchActor::OnBehalf(Box::new(context)),
        ));
    }

    if retained_client_id != caller_id
        || !matches!(manifest["actor"]["kind"].as_str(), None | Some("direct"))
    {
        return Err(Error::new(
            "FORBIDDEN",
            "direct launch actor does not match its admitted Operation caller",
        ));
    }
    let client_id = retained_client_id.to_owned();
    let admitted_role = model::text(&manifest["actor"], "role")?;
    let link_id = model::text(&manifest["actor"], "link_id")?.to_owned();
    let profile = meta(db, &format!("client:{client_id}"))?
        .ok_or_else(|| Error::new("FORBIDDEN", "admitted launch actor is no longer registered"))?;
    if profile["disabled"] == true || profile["role"] != admitted_role {
        return Err(Error::new(
            "FORBIDDEN",
            "admitted launch actor registration is disabled or changed role",
        ));
    }
    let role = match admitted_role {
        "operator" => Role::Operator,
        "manager" => Role::Manager,
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "launch actor role is not authorized",
            ));
        }
    };
    let principal = Principal {
        link_id,
        client_id,
        role,
    };
    let actor = LaunchActor::Direct(principal);
    actor.require_current(db)?;
    Ok((operation, manifest, actor))
}

pub(crate) fn launch_actor(db: &Connection, operation_id: &str) -> Result<LaunchActor> {
    let (_operation, manifest, actor) = retained_launch_actor(db, operation_id)?;
    let task_id = model::text(&manifest["task"], "task_id")?;
    let task_revision = model::positive(&manifest["task"], "observed_revision")?;
    let attempt_id = manifest["task"]["attempt_id"].as_str();
    actor.require_action_object(db, "swarm.launch", task_id, task_revision, attempt_id)?;
    Ok(actor)
}

/// Rehydrate an actor only for the bound, ready dispatch stage. The ordinary
/// launch actor constructor remains restricted to its original unstarted path.
pub(crate) fn dispatch_launch_actor(
    db: &Connection,
    operation_id: &str,
    dispatch_operation_id: Option<&str>,
) -> Result<LaunchActor> {
    let (_operation, manifest, actor) = retained_launch_actor(db, operation_id)?;
    let task_id = model::text(&manifest["task"], "task_id")?;
    let task_revision = model::positive(&manifest["task"], "observed_revision")?;
    let attempt_id = model::text(&manifest["task"], "attempt_id")?;
    let binding_id = model::text(&manifest["binding"], "binding_id")?;
    let binding_generation = model::positive(&manifest["binding"], "generation")?;
    actor.require_dispatch_launch_attempt(
        db,
        operation_id,
        task_id,
        task_revision,
        attempt_id,
        binding_id,
        binding_generation,
        dispatch_operation_id,
    )?;
    Ok(actor)
}

/// A launcher child's parent pointer is provenance, not an agent.configure
/// barrier. Return authority only for the exact retained opening-stage tuple.
pub(crate) fn opening_actor_for_open(
    db: &Connection,
    operation_id: &str,
    binding_id: &str,
    generation: i64,
) -> Result<Option<LaunchActor>> {
    let open = super::operations::get_operation(db, operation_id)?;
    if open["method"] != "agent.open" {
        return Ok(None);
    }
    let parent_id: Option<String> = db.query_row(
        "SELECT prerequisite_operation_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let Some(parent_id) = parent_id else {
        return Ok(None);
    };
    let parent = super::operations::get_operation(db, &parent_id)?;
    if parent["method"] != "swarm.launch" {
        return Ok(None);
    }
    let manifest = retained_launch_manifest(db, &parent_id)?;
    if open["state"] != "queued"
        || open["binding_id"] != binding_id
        || open["binding_generation"] != generation
        || open["caller_id"] != parent["caller_id"]
        || open["task_id"] != parent["task_id"]
        || open["attempt_id"] != parent["attempt_id"]
        || manifest["binding"]["operation_id"] != operation_id
    {
        return Err(Error::new(
            "FORBIDDEN",
            "launch child differs from its exact parent tuple",
        ));
    }
    let actor = launch_actor(db, &parent_id)?;
    actor.require_opening_launch_attempt(
        db,
        &parent_id,
        model::text(&parent, "task_id")?,
        model::positive(&manifest["task"], "observed_revision")?,
        model::text(&parent, "attempt_id")?,
        binding_id,
        generation,
    )?;
    Ok(Some(actor))
}

/// Bounded host selector. Unknown launches are selected for readback only;
/// callers must inspect the retained manifest phase before acting.
pub(super) fn pending_launches(db: &Connection, limit: i64) -> Result<Vec<String>> {
    if !(1..=16).contains(&limit) {
        return Err(Error::invalid(
            "pending launch limit must be from 1 through 16",
        ));
    }
    let mut statement = db.prepare(
        "SELECT operation_id FROM operations \
         WHERE method='swarm.launch' AND state IN ('queued','outcome_unknown') \
           AND json_extract(effective_request_json,'$.launch_manifest.state') \
             IN ('pending_workspace','awaiting_binding','awaiting_capability', \
                 'awaiting_participant_credential','outcome_unknown') \
         ORDER BY updated_at_ms,operation_id LIMIT ?1",
    )?;
    statement
        .query_map([limit], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

/// Turn a Host-side workspace failure into a stable launch receipt. The
/// caller supplies a small code from this allow-list, never raw process or
/// filesystem diagnostics. Unknown effects remain unknown and are not retried.
pub(super) fn fail_launch(
    tx: &Transaction<'_>,
    operation_id: &str,
    safe_code: &str,
    now: i64,
) -> Result<Value> {
    let (launch_state, operation_state, native_effect) = match safe_code {
        "workspace_stale_before_effect" => ("stale", "settled", "not_attempted"),
        "workspace_admission_rejected" | "route_admission_unavailable" => {
            ("blocked", "settled", "not_attempted")
        }
        "workspace_effect_unknown" | "binding_effect_unknown" => {
            ("outcome_unknown", "outcome_unknown", "unknown")
        }
        _ => return Err(Error::invalid("unsupported safe launch failure code")),
    };
    let operation = super::operations::get_operation(tx, operation_id)?;
    if operation["method"] != "swarm.launch" {
        return Err(Error::new("FORBIDDEN", "Operation is not a launch"));
    }
    let mut manifest = retained_launch_manifest(tx, operation_id)?;
    if operation["state"] == operation_state
        && manifest["state"] == launch_state
        && manifest["failure"]["code"] == safe_code
        && operation["result"]["failure"]["code"] == safe_code
    {
        return Ok(operation["result"].clone());
    }
    if !matches!(
        operation["state"].as_str(),
        Some("queued" | "outcome_unknown")
    ) {
        return Ok(operation["result"].clone());
    }
    manifest["failure"] = json!({"code":safe_code});
    manifest["state"] = json!(launch_state);
    manifest["runtime"]["native_effect"] = json!(native_effect);
    manifest["effects"] = json!(if native_effect == "unknown" {
        "workspace_or_binding_effect_requires_exact_readback"
    } else {
        "no_runtime_effect_was_admitted"
    });
    manifest["progress"]["workspace_lease"] = json!(if launch_state == "outcome_unknown" {
        "readback_required"
    } else {
        "stopped"
    });
    let task_id = manifest["task"]["task_id"].clone();
    let attempt_id = manifest["task"]["attempt_id"].clone();
    let result = json!({
        "operation_id":operation_id,
        "launch_state":launch_state,
        "state":operation_state,
        "plan_digest":manifest["plan_digest"],
        "task_id":task_id,
        "task_revision":manifest["task"]["observed_revision"],
        "attempt_id":attempt_id,
        "native_effect":native_effect,
        "failure":{"code":safe_code},
        "gaps":["launch_progress_requires_operator_readback_or_configuration_repair"],
    });
    persist_launch_progress(
        tx,
        operation_id,
        LaunchProgress {
            manifest,
            result: &result,
            operation_state,
            attempt_id: result["attempt_id"].as_str(),
            binding: None,
            now,
        },
    )?;
    Ok(result)
}

/// Revalidate the pending launch intent immediately before Host-side lease
/// preparation and return only the exact Task scope bound by its digest.
pub(super) fn launch_workspace_plan(
    db: &Connection,
    actor: &LaunchActor,
    operation_id: &str,
    config: &Config,
) -> Result<crate::workspace::WorkspaceLeasePlan> {
    launch_workspace_plan_inner(db, actor, operation_id, config)
}

fn launch_workspace_plan_inner(
    db: &Connection,
    actor: &LaunchActor,
    operation_id: &str,
    config: &Config,
) -> Result<crate::workspace::WorkspaceLeasePlan> {
    actor.require_current(db)?;
    require_route_admission(&current_launch_admission(db, operation_id, config)?)?;
    let operation = super::operations::get_operation(db, operation_id)?;
    let pending = operation["state"] == "queued"
        && operation["result"]["launch_state"] == "pending_workspace";
    if operation["method"] != "swarm.launch"
        || operation["caller_id"] != actor.technical_requester_id()
        || !pending
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "Operation is not a caller-owned launch awaiting workspace preparation or exact readback",
        ));
    }
    let original_request: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let original_request: Value = serde_json::from_str(&original_request)?;
    let request = launcher::LaunchRequest::parse(&original_request)?;
    let retained_request_id: String = db.query_row(
        "SELECT client_request_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    if request.client_request_id != retained_request_id {
        return Err(Error::new(
            "STALE_LAUNCH",
            "retained launch request identity changed",
        ));
    }
    let planned = launch_preview_inner(
        db,
        actor,
        &request.preview_params(),
        config,
        Some(operation_id),
    )?;
    let preview = planned.effect_facts()?;
    if preview["plan_digest"] != request.plan_digest
        || operation["task_id"] != preview["task"]["task_id"]
        || operation["attempt_id"] != preview["current_attempt"]["attempt_id"]
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "Task, Attempt, or launch plan changed before workspace preparation",
        ));
    }

    let task_id = model::text(&preview["task"], "task_id")?;
    let row = query_task(db, task_id)?;
    let spec: TaskSpec = serde_json::from_value(row.spec.clone())?;
    spec.validate()?;
    let allowed_paths = spec
        .scope
        .as_ref()
        .map(|scope| scope.initial_paths.clone())
        .unwrap_or_default();
    let forbidden_paths = spec
        .scope
        .as_ref()
        .map(|scope| scope.forbidden_paths.as_slice())
        .unwrap_or_default();
    if allowed_paths.is_empty()
        || allowed_paths.len() > MAX_SCOPE_PATHS
        || allowed_paths
            .iter()
            .any(|path| !validate_workspace_scope_path(path))
    {
        return Err(Error::new(
            "WORKSPACE_SCOPE_REQUIRED",
            "launch requires a bounded relative Task mutation scope",
        ));
    }
    if forbidden_paths
        .iter()
        .any(|path| !validate_workspace_scope_path(path))
    {
        return Err(Error::new(
            "WORKSPACE_SCOPE_INVALID",
            "Task forbidden paths are not safe repository-relative paths",
        ));
    }
    if allowed_paths.iter().any(|allowed| {
        forbidden_paths
            .iter()
            .any(|forbidden| scope_paths_overlap(allowed, forbidden))
    }) {
        return Err(Error::new(
            "WORKSPACE_SCOPE_CONFLICT",
            "Task allowed mutation scope overlaps a forbidden path",
        ));
    }

    let baseline = workspace_baseline_projection(db, &row, Some(&spec))?;
    let expected_baseline_commit = match baseline["status"].as_str() {
        Some("not_configured") => None,
        Some("verified_source_snapshot_commit") => {
            Some(model::text(&baseline, "commit")?.to_owned())
        }
        _ => {
            return Err(Error::new(
                "BASELINE_PROVENANCE_UNVERIFIED",
                "Task baseline artifact is not a verified complete source snapshot with a full Git commit",
            ));
        }
    };

    let lease_owner = if let Some(attempt_id) = row.current_attempt_id.as_deref() {
        get_attempt_row(db, attempt_id)?
            .filter(|attempt| {
                attempt.task_id == row.task_id
                    && attempt.task_revision == row.revision
                    && attempt.released_at_ms.is_none()
            })
            .map(|attempt| attempt.owner_id)
            .ok_or_else(|| Error::new("STALE_LAUNCH", "current Attempt owner is unavailable"))?
    } else {
        actor.effective_manager_id().to_owned()
    };
    let plan = crate::workspace::WorkspaceLeasePlan {
        project_id: row.project_id,
        task_id: row.task_id,
        task_revision: row.revision,
        operation_id: operation_id.to_owned(),
        plan_digest: request.plan_digest,
        owner_client_id: lease_owner,
        attempt_id: preview["current_attempt"]["attempt_id"]
            .as_str()
            .map(str::to_owned),
        allowed_paths,
        allowed_symbols: Vec::new(),
        expected_baseline_commit,
    };
    plan.validate()?;
    Ok(plan)
}

fn launch_child_request_id(operation_id: &str, phase: &str) -> String {
    format!("launch:{operation_id}:{phase}")
}

struct LaunchOpenOperationRow {
    operation_id: String,
    method: String,
    original_request_json: String,
    state: String,
    result_json: Option<String>,
    effective_request_json: String,
    prerequisite_operation_id: Option<String>,
}

fn admit_launch_open(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    parent_operation_id: &str,
    params_value: &Value,
    lease: &crate::workspace::LeaseAuthorityRef,
    now: i64,
) -> Result<(String, bool)> {
    model::validate_mutation("agent.open", params_value)?;
    let request_id = model::text(params_value, "client_request_id")?;
    let original = model::canonical(params_value)?;
    let existing: Option<LaunchOpenOperationRow> = tx
        .query_row(
            "SELECT operation_id,method,original_request_json,state,result_json,effective_request_json, \
                    prerequisite_operation_id \
             FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![actor.technical_requester_id(), request_id],
            |row| {
                Ok(LaunchOpenOperationRow {
                    operation_id: row.get(0)?,
                    method: row.get(1)?,
                    original_request_json: row.get(2)?,
                    state: row.get(3)?,
                    result_json: row.get(4)?,
                    effective_request_json: row.get(5)?,
                    prerequisite_operation_id: row.get(6)?,
                })
            },
        )
        .optional()?;
    if let Some(existing) = existing {
        if existing.method != "agent.open"
            || existing.original_request_json != original
            || existing.prerequisite_operation_id.as_deref() != Some(parent_operation_id)
        {
            return Err(Error::new(
                "REQUEST_ID_CONFLICT",
                "launch child request ID is already bound to different input or parent",
            ));
        }
        if existing.state == "outcome_unknown" {
            return Err(Error::new(
                "LAUNCH_OPEN_OUTCOME_UNKNOWN",
                "existing launch binding open requires exact readback",
            ));
        }
        let effective: Value = serde_json::from_str(&existing.effective_request_json)?;
        if effective["operation_contract"]["parent_launch_operation_id"] != parent_operation_id
            || effective["workspace_lease"]["lease_id"] != lease.lease_id
            || effective["workspace_lease"]["generation"] != lease.generation
            || effective["workspace_lease"]["binding_digest"] != lease.binding_digest
        {
            return Err(Error::new(
                "LAUNCH_OPEN_LEASE_MISMATCH",
                "existing launch child is not bound to the exact parent and held workspace lease",
            ));
        }
        let Some(result_raw) = existing.result_json else {
            return Err(Error::new(
                "LAUNCH_OPEN_READBACK_REQUIRED",
                "existing launch binding open has no retained receipt",
            ));
        };
        let result: Value = serde_json::from_str(&result_raw)?;
        model::text(&result, "binding_id")?;
        model::positive(&result, "generation")?;
        return Ok((existing.operation_id, false));
    }
    let child_id = model::new_id();
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json, \
         effective_request_json,prerequisite_operation_id,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'agent.open',?4,'{}',?5,'queued',?6,?6,?6)",
        params![
            child_id,
            actor.technical_requester_id(),
            request_id,
            original,
            parent_operation_id,
            now
        ],
    )?;
    Ok((child_id, true))
}

struct LaunchOpenReceipt<'a> {
    parent_operation_id: &'a str,
    task_id: &'a str,
    attempt_id: &'a str,
    lease: &'a crate::workspace::LeaseAuthorityRef,
    result: &'a Value,
    now: i64,
}

fn retain_launch_open(
    tx: &Transaction<'_>,
    operation_id: &str,
    receipt: LaunchOpenReceipt<'_>,
) -> Result<()> {
    let raw: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='agent.open'",
        [operation_id],
        |row| row.get(0),
    )?;
    let mut effective: Value = serde_json::from_str(&raw)?;
    effective["operation_contract"] = json!({
        "effect_scope":"one_exact_launch_binding",
        "completion_condition":"binding_ready_readback",
        "replay_policy":"exact_binding_readback_only_after_unknown",
        "parent_launch_operation_id":receipt.parent_operation_id,
    });
    effective["workspace_lease"] = json!({
        "lease_id":receipt.lease.lease_id,
        "generation":receipt.lease.generation,
        "binding_digest":receipt.lease.binding_digest,
    });
    effective["receipt"] = json!({"ok":true,"value":receipt.result});
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,result_json=?4,effective_request_json=?5, \
         updated_at_ms=?6 WHERE operation_id=?1 AND prerequisite_operation_id=?7 \
         AND method='agent.open' AND state='queued'",
        params![
            operation_id,
            receipt.task_id,
            receipt.attempt_id,
            model::canonical(receipt.result)?,
            model::canonical(&effective)?,
            receipt.now,
            receipt.parent_operation_id,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "launch agent.open child changed before receipt retention",
        ));
    }
    super::capacity::sync_operation(tx, operation_id, receipt.now)?;
    let digest = model::digest(model::canonical(receipt.result)?.as_bytes());
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller',?1,?2,'agent.open',?3,?4)",
        params![
            format!("launch-open:{operation_id}:{digest}"),
            operation_id,
            model::canonical(receipt.result)?,
            receipt.now,
        ],
    )?;
    Ok(())
}

fn final_workspace_manifest_digest(
    plan_digest: &str,
    lease: &crate::workspace::LeaseAuthorityRef,
    lease_view: &Value,
) -> Result<String> {
    let facts = json!({
        "launch_plan_digest":plan_digest,
        "lease_id":lease.lease_id,
        "registration_id":lease.registration_id,
        "registration_generation":lease.registration_generation,
        "project_id":lease.project_id,
        "task_id":lease.task_id,
        "task_revision":lease.task_revision,
        "operation_id":lease.operation_id,
        "owner_client_id":lease.owner_client_id,
        "attempt_id":lease.attempt_id,
        "generation":lease.generation,
        "baseline_commit":lease.baseline_commit,
        "branch_ref":lease.branch_ref,
        "worktree_handle":lease.worktree_handle,
        "allowed_paths":lease_view["allowed_paths"],
        "allowed_symbols":lease_view["allowed_symbols"],
        "binding_digest":lease.binding_digest,
        "clean_state":lease_view["clean_state"],
    });
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(&facts)?.as_bytes())
    ))
}

fn retain_unknown_workspace_readback(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    operation: &Value,
    mut manifest: Value,
    operation_id: &str,
    lease: &crate::workspace::LeaseAuthorityRef,
    now: i64,
) -> Result<Value> {
    actor.require_workspace_readback_authority(tx, operation_id)?;
    let result = &operation["result"];
    let task_id = model::text(&manifest["task"], "task_id")?.to_owned();
    let task_revision = model::positive(&manifest["task"], "observed_revision")?;
    let project_id = model::text(&manifest["task"], "project_id")?;
    let expected_attempt = manifest["task"]["attempt_id"].as_str().map(str::to_owned);
    if operation["method"] != "swarm.launch"
        || operation["state"] != "outcome_unknown"
        || operation["caller_id"] != actor.technical_requester_id()
        || result["launch_state"] != "outcome_unknown"
        || result["state"] != "outcome_unknown"
        || result["failure"]["code"] != "workspace_effect_unknown"
        || manifest["state"] != "outcome_unknown"
        || manifest["failure"]["code"] != "workspace_effect_unknown"
        || result["operation_id"] != operation_id
        || result["plan_digest"] != manifest["plan_digest"]
        || result["task_id"] != task_id
        || result["task_revision"] != task_revision
        || result["attempt_id"] != manifest["task"]["attempt_id"]
        || lease.state != "held"
        || lease.operation_id != operation_id
        || lease.plan_digest != manifest["plan_digest"]
        || lease.project_id != project_id
        || lease.task_id != task_id
        || lease.task_revision != task_revision
        || lease.owner_client_id != actor.effective_manager_id()
        || lease.attempt_id != expected_attempt
        || lease.generation <= 0
        || lease.registration_generation <= 0
        || lease.binding_digest.is_empty()
        || super::workspace::held_lease_for_operation(tx, operation_id)?.as_ref() != Some(lease)
    {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "unknown workspace readback does not match the exact retained launch and lease",
        ));
    }

    let original_request: String = tx.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
        |row| row.get(0),
    )?;
    let request =
        launcher::LaunchRequest::parse(&serde_json::from_str::<Value>(&original_request)?)?;
    let client_request_id: String = tx.query_row(
        "SELECT client_request_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    if request.client_request_id != client_request_id
        || request.plan_digest != manifest["plan_digest"]
        || request.preview.task_id != task_id
        || request.preview.expected_task_revision != manifest["task"]["expected_revision"]
    {
        return Err(Error::new(
            "LAUNCH_MANIFEST_CORRUPT",
            "unknown workspace readback request differs from its original plan identity",
        ));
    }

    let lease_view = super::workspace::get_lease_view(tx, lease)?;
    let digest = final_workspace_manifest_digest(&request.plan_digest, lease, &lease_view)?;
    if manifest["workspace"]["readback_status"] == "verified" {
        let saved_lease: crate::workspace::LeaseAuthorityRef = serde_json::from_value(
            manifest["workspace"]["lease_authority"].clone(),
        )
        .map_err(|_| {
            Error::new(
                "INVALID_LAUNCH_MANIFEST",
                "readback workspace authority reference is invalid",
            )
        })?;
        if saved_lease != *lease
            || result["workspace_lease"] != "held_verified_by_exact_readback"
            || model::canonical(&lease_view)? != model::canonical(&manifest["workspace"]["lease"])?
            || manifest["workspace"]["manifest_digest"] != digest
        {
            return Err(Error::new(
                "WORKSPACE_LEASE_STALE",
                "persisted readback does not match the exact held workspace lease",
            ));
        }
        return Ok(result.clone());
    }

    manifest["workspace"]["lease_state"] = json!("held");
    manifest["workspace"]["dirty_state"] = lease_view["clean_state"].clone();
    manifest["workspace"]["filesystem_inspected"] = json!(true);
    manifest["workspace"]["readback_status"] = json!("verified");
    manifest["workspace"]["lease"] = lease_view;
    manifest["workspace"]["lease_authority"] = serde_json::to_value(lease)?;
    manifest["workspace"]["manifest_digest"] = json!(digest);
    manifest["progress"]["workspace_lease"] = json!("verified_by_exact_readback");
    manifest["effects"] = json!("workspace_effect_reconciled_readonly; no_native_effect_admitted");
    let readback = json!({
        "operation_id":operation_id,
        "launch_state":"outcome_unknown",
        "state":"outcome_unknown",
        "plan_digest":request.plan_digest,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":expected_attempt,
        "workspace_lease":"held_verified_by_exact_readback",
        "native_effect":"unknown",
        "failure":{"code":"workspace_effect_unknown"},
        "gaps":["workspace_effect_reconciled_readonly","unknown_effect_remains_readback_only"],
    });
    persist_launch_progress(
        tx,
        operation_id,
        LaunchProgress {
            manifest,
            result: &readback,
            operation_state: "outcome_unknown",
            attempt_id: expected_attempt.as_deref(),
            binding: None,
            now,
        },
    )?;
    Ok(readback)
}

/// Claim/reuse the exact Attempt and reserve its first workspace-bound native
/// binding after Host evidence has moved the precise lease to held.
pub(super) fn launch_after_workspace_held(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    config: &Config,
    operation_id: &str,
    lease: &crate::workspace::LeaseAuthorityRef,
    now: i64,
) -> Result<Value> {
    let operation = super::operations::get_operation(tx, operation_id)?;
    let mut manifest = retained_launch_manifest(tx, operation_id)?;
    let recovering_unknown_workspace = operation["state"] == "outcome_unknown"
        && operation["result"]["launch_state"] == "outcome_unknown"
        && operation["result"]["failure"]["code"] == "workspace_effect_unknown"
        && manifest["state"] == "outcome_unknown"
        && manifest["failure"]["code"] == "workspace_effect_unknown";
    if recovering_unknown_workspace {
        return retain_unknown_workspace_readback(
            tx,
            actor,
            &operation,
            manifest,
            operation_id,
            lease,
            now,
        );
    }
    actor.require_current(tx)?;
    if operation["method"] != "swarm.launch"
        || operation["caller_id"] != actor.technical_requester_id()
    {
        return Err(Error::new(
            "FORBIDDEN",
            "launch is not owned by this manager",
        ));
    }
    if manifest["state"] != "pending_workspace" {
        if matches!(
            manifest["state"].as_str(),
            Some("awaiting_binding" | "awaiting_capability" | "awaiting_participant_credential")
        ) {
            return Ok(operation["result"].clone());
        }
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch is not awaiting its first verified workspace lease",
        ));
    }
    if operation["state"] != "queued" {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch Operation is no longer queued",
        ));
    }
    let admission = current_launch_admission(tx, operation_id, config)?;
    retain_launch_admission(tx, operation_id, &admission, now)?;
    match admission.decision {
        provider_conditions::RouteAdmissionDecision::Admit => {}
        provider_conditions::RouteAdmissionDecision::Hold { .. } => {
            return Ok(super::operations::get_operation(tx, operation_id)?["result"].clone());
        }
        provider_conditions::RouteAdmissionDecision::Unavailable { .. } => {
            return fail_launch(tx, operation_id, "route_admission_unavailable", now);
        }
    }
    let plan = launch_workspace_plan_inner(tx, actor, operation_id, config)?;
    if lease.state != "held"
        || lease.operation_id != operation_id
        || lease.plan_digest != plan.plan_digest
        || lease.project_id != plan.project_id
        || lease.task_id != plan.task_id
        || lease.task_revision != plan.task_revision
        || lease.owner_client_id != plan.owner_client_id
    {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "held lease does not match the exact digest-bound launch plan",
        ));
    }
    super::workspace::assert_held_for_claim(tx, lease, &plan)?;
    let verified_lease_view = super::workspace::get_lease_view(tx, lease)?;
    let request_raw: String = tx.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let launch_request =
        launcher::LaunchRequest::parse(&serde_json::from_str::<Value>(&request_raw)?)?;
    let planned = launch_preview_inner(
        tx,
        actor,
        &launch_request.preview_params(),
        config,
        Some(operation_id),
    )?;
    let preview = planned.effect_facts()?;
    if preview["plan_digest"] != launch_request.plan_digest {
        return Err(Error::new(
            "STALE_LAUNCH",
            "Task, Attempt, or launch facts changed after workspace preparation",
        ));
    }

    let task_id = model::text(&preview["task"], "task_id")?.to_owned();
    let task_revision = model::positive(&preview["task"], "revision")?;
    let attempt_action = model::text(&manifest["task"], "attempt_action")?.to_owned();
    let attempt_id = match attempt_action.as_str() {
        "claim_new" => {
            if plan.attempt_id.is_some() || lease.attempt_id.is_some() {
                return Err(Error::new(
                    "STALE_LAUNCH",
                    "new-claim launch unexpectedly has an Attempt-bound workspace lease",
                ));
            }
            let claim_request = json!({
                "client_request_id":launch_child_request_id(operation_id, "claim"),
                "task_id":task_id,
                "expected_revision":task_revision,
                "owner_id":actor.effective_manager_id(),
                "start_owner":"controller",
            });
            let claimed = super::mutate_launch_child_in_transaction(
                tx,
                actor,
                "task.claim",
                &claim_request,
                config,
                now,
                operation_id,
            )??;
            model::text(&claimed, "attempt_id")?.to_owned()
        }
        "use_existing" => {
            let existing = model::text(&manifest["task"], "attempt_id")?;
            if plan.attempt_id.as_deref() != Some(existing)
                || lease.attempt_id.as_deref() != Some(existing)
            {
                return Err(Error::new(
                    "STALE_LAUNCH",
                    "workspace lease does not bind the exact existing Attempt",
                ));
            }
            existing.to_owned()
        }
        _ => {
            return Err(Error::new(
                "STALE_LAUNCH",
                "preview no longer authorizes an exact Attempt action",
            ));
        }
    };
    let attempt_bound = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,updated_at_ms=?4 \
         WHERE operation_id=?1 AND caller_id=?5 AND method='swarm.launch' \
           AND state IN ('queued','outcome_unknown') AND task_id=?2 \
           AND (attempt_id IS NULL OR attempt_id=?3)",
        params![
            operation_id,
            task_id,
            attempt_id,
            now,
            actor.technical_requester_id()
        ],
    )?;
    if attempt_bound != 1 {
        return Err(Error::conflict(
            "launch Operation could not retain its exact current Attempt",
        ));
    }
    let task = super::tasks::get_task(tx, &task_id)?;
    let attempt = super::tasks::get_attempt(tx, &attempt_id)?;
    if task["state"] != "open"
        || task["revision"] != task_revision
        || task["current_attempt_id"].as_str() != Some(attempt_id.as_str())
        || attempt["task_id"].as_str() != Some(task_id.as_str())
        || attempt["task_revision"] != task_revision
        || attempt["owner_id"] != plan.owner_client_id
        || !attempt["released_at_ms"].is_null()
        || attempt["state"] != "reserved"
        || !attempt["binding_id"].is_null()
        || !attempt["start_operation_id"].is_null()
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "current Attempt no longer matches its unstarted launch reservation",
        ));
    }
    actor.require_claimed_launch_attempt(tx, operation_id, &task_id, task_revision, &attempt_id)?;
    let lease = if plan.attempt_id.is_some() {
        if lease.attempt_id.as_deref() != Some(attempt_id.as_str()) {
            return Err(Error::new(
                "WORKSPACE_ATTEMPT_MISMATCH",
                "lease Attempt changed",
            ));
        }
        lease.clone()
    } else {
        super::workspace::pin_lease_attempt_for_launch(tx, actor, lease, &plan, &attempt_id, now)?
    };
    super::workspace::assert_held_for_claim(tx, &lease, &plan)?;
    let lease_view = if recovering_unknown_workspace && plan.attempt_id.is_none() {
        super::workspace::get_lease_view(tx, &lease)?
    } else if recovering_unknown_workspace {
        verified_lease_view
    } else {
        super::workspace::get_lease_view(tx, &lease)?
    };
    let attempt = super::tasks::get_attempt(tx, &attempt_id)?;
    if attempt["start_owner"] != "controller" && attempt["start_owner"] != "native_manager" {
        return Err(Error::new("STALE_LAUNCH", "Attempt start owner is unknown"));
    }

    let open_request = json!({
        "client_request_id":launch_child_request_id(operation_id, "open"),
        "lane_id":format!("launch-{}", lease.lease_id),
        "route":launch_request.preview.route,
    });
    let (open_operation_id, is_new_open) =
        admit_launch_open(tx, actor, operation_id, &open_request, &lease, now)?;
    let open_result = if is_new_open {
        let result = super::operations::open_for_launch_for_actor(
            tx,
            actor,
            &open_request,
            config,
            &open_operation_id,
            now,
            &lease.lease_id,
            lease.generation,
        )?;
        retain_launch_open(
            tx,
            &open_operation_id,
            LaunchOpenReceipt {
                parent_operation_id: operation_id,
                task_id: &task_id,
                attempt_id: &attempt_id,
                lease: &lease,
                result: &result,
                now,
            },
        )?;
        result
    } else {
        super::operations::get_operation(tx, &open_operation_id)?["result"].clone()
    };
    let binding_id = model::text(&open_result, "binding_id")?.to_owned();
    let binding_generation = model::positive(&open_result, "generation")?;
    let linked = tx.execute(
        "UPDATE attempts SET binding_id=?2,binding_generation=?3,updated_at_ms=?4 \
         WHERE attempt_id=?1 AND task_id=?5 AND task_revision=?6 AND owner_id=?7 \
           AND state='reserved' AND released_at_ms IS NULL \
           AND binding_id IS NULL AND binding_generation IS NULL",
        params![
            attempt_id,
            binding_id,
            binding_generation,
            now,
            task_id,
            task_revision,
            plan.owner_client_id,
        ],
    )?;
    if linked != 1 {
        return Err(Error::conflict(
            "Attempt binding association changed before launch CAS",
        ));
    }
    let workspace_manifest_digest =
        final_workspace_manifest_digest(&launch_request.plan_digest, &lease, &lease_view)?;
    manifest["state"] = json!("awaiting_binding");
    manifest["task"]["attempt_id"] = json!(attempt_id);
    manifest["attempt"] = json!({
        "action":attempt_action,
        "start_owner":attempt["start_owner"],
        "state":"reserved",
        "claim_operation_id":if attempt_action == "claim_new" {
            Some(launch_child_request_id(operation_id, "claim"))
        } else {
            None
        },
    });
    manifest["workspace"]["lease_state"] = json!("held");
    manifest["workspace"]["dirty_state"] = json!("clean_verified");
    manifest["workspace"]["filesystem_inspected"] = json!(true);
    manifest["workspace"]["lease"] = lease_view.clone();
    manifest["workspace"]["lease_authority"] = serde_json::to_value(&lease)?;
    manifest["workspace"]["manifest_digest"] = json!(workspace_manifest_digest);
    manifest["binding"] = json!({
        "binding_id":binding_id,
        "generation":binding_generation,
        "state":open_result["state"],
        "operation_id":open_operation_id,
        "native_admission":open_result["native_admission"],
    });
    manifest["progress"]["workspace_lease"] = json!("held_verified");
    manifest["progress"]["attempt_claim"] = json!(if attempt_action == "claim_new" {
        "claimed"
    } else {
        "reused_exact_reserved_attempt"
    });
    manifest["progress"]["binding_open"] = json!("queued");
    manifest["progress"]["task_dispatch"] = json!("not_started");
    manifest["progress"]["capability_readback"] = json!("not_observed");
    manifest["runtime"]["state"] = json!("opening");
    manifest["runtime"]["native_effect"] = json!("not_observed");
    manifest["effects"] = json!("verified_workspace_lease_and_queued_binding_open");
    let result = json!({
        "operation_id":operation_id,
        "launch_state":"awaiting_binding",
        "state":"queued",
        "plan_digest":launch_request.plan_digest,
        "workspace_manifest_digest":workspace_manifest_digest,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "attempt_action":attempt_action,
        "workspace_lease":{
            "lease_id":lease.lease_id,
            "generation":lease.generation,
            "binding_digest":lease.binding_digest,
            "baseline_commit":lease.baseline_commit,
            "worktree_handle":lease.worktree_handle,
            "branch_ref":lease.branch_ref,
            "dirty":false,
        },
        "binding":{
            "operation_id":open_operation_id,
            "binding_id":binding_id,
            "generation":binding_generation,
            "state":open_result["state"],
        },
        "task_dispatch":"not_started",
        "native_effect":"not_observed",
        "capability_state":"unknown",
        "gaps":[
            "runtime_binding_readback_pending",
            "scoped_participant_credential_issuance_is_not_available",
            "native_mcp_capability_readback_is_not_recorded",
        ],
    });
    persist_launch_progress(
        tx,
        operation_id,
        LaunchProgress {
            manifest,
            result: &result,
            operation_state: "queued",
            attempt_id: Some(&attempt_id),
            binding: Some(LaunchBindingIdentity {
                id: &binding_id,
                generation: binding_generation,
            }),
            now,
        },
    )?;
    Ok(result)
}

/// Reconcile one retained launch only from exact Store readback. This path
/// never retries workspace preparation or binding admission; capability and
/// participant facts must come from their own evidence sources.
pub(super) fn reconcile_launch(
    tx: &Transaction<'_>,
    config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let operation = super::operations::get_operation(tx, operation_id)?;
    if operation["method"] != "swarm.launch" {
        return Err(Error::new("FORBIDDEN", "Operation is not a launch"));
    }
    let actor = launch_actor(tx, operation_id)?;
    let mut manifest = retained_launch_manifest(tx, operation_id)?;
    let launch_state = model::text(&manifest, "state")?;
    if launch_state == "pending_workspace" {
        return Ok(operation["result"].clone());
    }
    let recovering_unknown_binding = launch_state == "outcome_unknown"
        && manifest["failure"]["code"] == "binding_effect_unknown";
    if launch_state == "outcome_unknown" && !recovering_unknown_binding {
        // Host readback owns this case. Never ask for a second workspace or
        // binding effect from an unresolved parent Operation.
        return Ok(operation["result"].clone());
    }
    if !matches!(
        launch_state,
        "awaiting_binding" | "awaiting_participant_credential" | "awaiting_capability"
    ) && !recovering_unknown_binding
    {
        return Ok(operation["result"].clone());
    }
    let lease: crate::workspace::LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone()).map_err(|_| {
            Error::new(
                "INVALID_LAUNCH_MANIFEST",
                "workspace authority reference is invalid",
            )
        })?;
    let lease_view = match super::workspace::get_lease_view(tx, &lease) {
        Ok(view) => view,
        Err(error) if error.code == "WORKSPACE_LEASE_STALE" => {
            return fail_launch(tx, operation_id, "binding_effect_unknown", now);
        }
        Err(error) => return Err(error),
    };
    let task_id = model::text(&manifest["task"], "task_id")?.to_owned();
    let task_revision = model::positive(&manifest["task"], "observed_revision")?;
    let attempt_id = model::text(&manifest["task"], "attempt_id")?.to_owned();
    let task = super::tasks::get_task(tx, &task_id)?;
    let attempt = super::tasks::get_attempt(tx, &attempt_id)?;
    if task["state"] != "open"
        || task["revision"] != task_revision
        || task["current_attempt_id"] != attempt_id
        || attempt["task_id"] != task_id
        || attempt["task_revision"] != task_revision
        || attempt["owner_id"] != lease.owner_client_id
        || attempt["released_at_ms"].is_number()
        || attempt["binding_id"] != manifest["binding"]["binding_id"]
        || attempt["binding_generation"] != manifest["binding"]["generation"]
    {
        manifest["state"] = json!("stale");
        manifest["failure"] = json!({"code":"task_or_attempt_changed_before_binding_readback"});
        let result = json!({
            "operation_id":operation_id,
            "launch_state":"stale",
            "state":"settled",
            "plan_digest":manifest["plan_digest"],
            "task_id":task_id,
            "task_revision":task_revision,
            "attempt_id":attempt_id,
            "native_effect":"not_dispatched",
            "gaps":["task_or_attempt_changed_before_binding_readback"],
        });
        persist_launch_progress(
            tx,
            operation_id,
            LaunchProgress {
                manifest,
                result: &result,
                operation_state: "settled",
                attempt_id: Some(&attempt_id),
                binding: None,
                now,
            },
        )?;
        return Ok(result);
    }
    let open_operation_id = model::text(&manifest["binding"], "operation_id")?.to_owned();
    let open_operation = super::operations::get_operation(tx, &open_operation_id)?;
    if open_operation["method"] != "agent.open"
        || open_operation["caller_id"] != actor.technical_requester_id()
        || open_operation["prerequisite_operation_id"] != operation_id
        || open_operation["attempt_id"].as_str() != Some(attempt_id.as_str())
        || open_operation["task_id"].as_str() != Some(task_id.as_str())
    {
        return Err(Error::new(
            "LAUNCH_BINDING_READBACK_MISMATCH",
            "binding open Operation is not linked to the exact launch Attempt",
        ));
    }
    if open_operation["state"] == "outcome_unknown" {
        return fail_launch(tx, operation_id, "binding_effect_unknown", now);
    }
    if matches!(
        open_operation["state"].as_str(),
        Some("rejected" | "cancelled")
    ) {
        return fail_launch(tx, operation_id, "workspace_admission_rejected", now);
    }
    let binding_id = model::text(&manifest["binding"], "binding_id")?.to_owned();
    let binding_generation = model::positive(&manifest["binding"], "generation")?;
    let binding = super::operations::get_binding(tx, &binding_id, binding_generation)?;
    if binding["released_at_ms"].is_number() || lease_view["state"] != "held" {
        return fail_launch(tx, operation_id, "workspace_stale_before_effect", now);
    }
    if open_operation["state"] != "settled" || binding["state"] != "ready" {
        return Ok(operation["result"].clone());
    }

    if recovering_unknown_binding && let Some(object) = manifest.as_object_mut() {
        object.remove("failure");
    }

    manifest["binding"]["state"] = json!(binding["state"]);
    manifest["binding"]["native_root_id"] = binding["native_root_id"].clone();
    manifest["binding"]["native_scope_key"] = binding["native_scope_key"].clone();
    manifest["progress"]["binding_open"] = json!("ready_readback");
    manifest["runtime"]["state"] = json!("binding_ready");
    manifest["runtime"]["native_effect"] = json!("binding_ready_observed");
    manifest["progress"]["task_dispatch"] = json!("not_started");
    manifest["progress"]["capability_readback"] = json!("not_observed");
    let mcp_profile = model::text(&manifest["request"], "mcp_profile")?;
    let participant_id = manifest["participant"]["client_id"].as_str().or_else(|| {
        config
            .mcp
            .profiles
            .get(mcp_profile)
            .filter(|profile| profile.tool_profile == McpToolProfile::AssignedReviewer)
            .map(|profile| profile.expected_client_id.as_str())
    });
    let participant = participant_id
        .map(|client_id| meta(tx, &format!("client:{client_id}")))
        .transpose()?
        .flatten();
    let participant_matches = participant.as_ref().is_some_and(|registration| {
        registration["role"] == "participant"
            && registration["disabled"] != true
            && registration["task_id"].as_str() == Some(task_id.as_str())
            && registration["task_revision"] == task_revision
            && registration["attempt_id"].as_str() == Some(attempt_id.as_str())
            && registration["binding_id"].as_str() == Some(binding_id.as_str())
            && registration["binding_generation"] == binding_generation
    });
    if !participant_matches {
        manifest["state"] = json!("awaiting_participant_credential");
        let result = json!({
            "operation_id":operation_id,
            "launch_state":"awaiting_participant_credential",
            "state":"queued",
            "plan_digest":manifest["plan_digest"],
            "workspace_manifest_digest":manifest["workspace"]["manifest_digest"],
            "task_id":task_id,
            "task_revision":task_revision,
            "attempt_id":attempt_id,
            "binding":{"binding_id":binding_id,"generation":binding_generation,"state":"ready"},
            "task_dispatch":"not_started",
            "native_effect":"binding_ready_observed",
            "capability_state":"unknown",
            "gaps":["exact_scoped_participant_credential_is_not_registered"],
        });
        persist_launch_progress(
            tx,
            operation_id,
            LaunchProgress {
                manifest,
                result: &result,
                operation_state: "queued",
                attempt_id: Some(&attempt_id),
                binding: Some(LaunchBindingIdentity {
                    id: &binding_id,
                    generation: binding_generation,
                }),
                now,
            },
        )?;
        return Ok(result);
    }

    // This reconciliation branch has no capability receipt of its own. The
    // separate C8 host worker retains exact-scope native inventory,
    // session-context, and provider-request observations; dispatch admission
    // reads that current receipt without treating provider-before-transport
    // as model consumption or changing this branch's authority.
    manifest["state"] = json!("awaiting_capability");
    let result = json!({
        "operation_id":operation_id,
        "launch_state":"awaiting_capability",
        "state":"queued",
        "plan_digest":manifest["plan_digest"],
        "workspace_manifest_digest":manifest["workspace"]["manifest_digest"],
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "binding":{"binding_id":binding_id,"generation":binding_generation,"state":"ready"},
        "task_dispatch":"not_started",
        "native_effect":"binding_ready_observed",
        "capability_state":"unknown",
        "gaps":[
            "native_mcp_capability_readback_is_not_recorded",
            "task_dispatch_is_held_until_required_core_capabilities_are_verified",
        ],
    });
    persist_launch_progress(
        tx,
        operation_id,
        LaunchProgress {
            manifest,
            result: &result,
            operation_state: "queued",
            attempt_id: Some(&attempt_id),
            binding: Some(LaunchBindingIdentity {
                id: &binding_id,
                generation: binding_generation,
            }),
            now,
        },
    )?;
    Ok(result)
}

/// Read-only bounded plan over one exact Task revision and the Store's
/// current Attempt, dependency, policy, capacity, Operation, route, and MCP
/// profile facts. No filesystem or native runtime is consulted here.
pub(super) fn launch_preview(
    db: &Connection,
    p: &Principal,
    params_value: &Value,
    config: &Config,
) -> Result<Value> {
    // Keep every bounded planning read on one Store snapshot. This transaction
    // is read-only in this path; launch admission revalidates the same digest
    // in its own durable transaction before any effect.
    let tx = db.unchecked_transaction()?;
    let result =
        launch_preview_for_actor(&tx, &LaunchActor::Direct(p.clone()), params_value, config)?;
    tx.commit()?;
    Ok(result)
}

pub(crate) fn launch_preview_for_actor(
    db: &Connection,
    actor: &LaunchActor,
    params_value: &Value,
    config: &Config,
) -> Result<Value> {
    Ok(launch_preview_inner(db, actor, params_value, config, None)?.projection)
}

fn launch_preview_inner(
    db: &Connection,
    actor: &LaunchActor,
    params_value: &Value,
    config: &Config,
    exclude_operation_id: Option<&str>,
) -> Result<PlannedLaunch> {
    let request = launcher::LaunchPreviewRequest::parse(params_value)?;

    let row = query_task(db, &request.task_id)?;
    let attempt = row
        .current_attempt_id
        .as_deref()
        .map(|attempt_id| get_attempt_row(db, attempt_id))
        .transpose()?
        .flatten();
    let exact_current_attempt = attempt.as_ref().filter(|attempt| {
        attempt.task_id == row.task_id
            && attempt.released_at_ms.is_none()
            && row.current_attempt_id.as_deref() == Some(attempt.attempt_id.as_str())
    });
    actor.require_action_object(
        db,
        "swarm.launch",
        &row.task_id,
        row.revision,
        exact_current_attempt.map(|attempt| attempt.attempt_id.as_str()),
    )?;
    let task_revision_current = request.expected_task_revision == row.revision;
    let (task_brief, task_brief_reference, spec) = source_brief(&row);
    let task_brief =
        launcher::brief_projection(&task_brief, task_brief_reference, MAX_LAUNCH_BRIEF_BYTES)?;
    let spec_valid = spec.as_ref().is_some_and(|spec| spec.validate().is_ok());
    let dependencies = dependency_projection(db, spec.as_ref())?;
    let claim_readiness = readiness(
        db,
        &row,
        exact_current_attempt,
        spec.as_ref(),
        spec_valid,
        &dependencies,
    )?;
    let work_scope = scope_projection(spec.as_ref());

    let mut hard_blocks = Vec::new();
    let mut gaps = vec![
        "workspace_lease_dirty_state_and_git_baseline_not_observed",
        "execution_waits_for_a_verified_registered_workspace_lease",
        "provider_runtime_and_native_agent_capability_receipts_not_observed",
    ];
    if !task_revision_current {
        hard_blocks.push("task_revision_changed");
    }
    if row.state != "open" {
        hard_blocks.push("task_not_open");
    }
    if !spec_valid {
        hard_blocks.push("task_spec_unavailable_or_invalid");
    }
    if claim_readiness["owner_policy"]["status"] != "accepted" {
        hard_blocks.push("owner_policy_not_recognized");
    }
    if exact_current_attempt.is_none() {
        if claim_readiness["new_work_admission"] == "disabled" {
            hard_blocks.push("new_work_admission_disabled");
        } else if claim_readiness["new_work_admission"] != "enabled" {
            hard_blocks.push("new_work_admission_state_unknown");
            gaps.push("new_work_admission_state_unknown");
        }
    }
    match dependencies["status"].as_str() {
        Some("waiting") => hard_blocks.push("required_dependency_not_accepted"),
        Some("unknown") => hard_blocks.push("dependency_state_not_complete"),
        Some("satisfied") => {}
        _ => hard_blocks.push("dependency_state_unavailable"),
    }
    if task_brief["status"] != "included" {
        gaps.push("exact_current_task_brief_not_inline");
    }
    if work_scope["status"] == "not_recorded" {
        gaps.push("task_work_scope_not_recorded");
    } else if work_scope["status"] != "recorded" {
        gaps.push("some_scope_paths_are_redacted_or_omitted");
    }
    let initial_path_count = work_scope["initial_path_count"].as_u64().unwrap_or(0);
    if initial_path_count == 0 {
        hard_blocks.push("task_mutable_scope_not_recorded");
    } else if work_scope["status"] != "recorded" || work_scope["coverage_complete"] != true {
        hard_blocks.push("task_mutable_scope_not_exact");
    }
    if request.workspace_policy != "manager_owned_worktree" {
        hard_blocks.push("unsupported_workspace_policy");
    } else {
        gaps.push("manager_owned_worktree_not_provisioned_by_preview");
    }

    let mut route = launch_route_projection(config, &request, &mut hard_blocks, &mut gaps);
    if let Some(selected) = config
        .routes
        .iter()
        .find(|route| route.alias == request.route)
    {
        let selector = match super::module_handshake::selection_for_new_binding(
            db,
            actor.effective_manager_id(),
            &selected.alias,
            &selected.runtime,
            &selected.module_artifact_id,
        ) {
            Ok(selector) => selector,
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "TASK_PROMPT_CONTRACT_REQUIRED"
                        | "MODULE_ROUTE_STALE"
                        | "MODULE_DESCRIPTOR_MISSING"
                ) =>
            {
                hard_blocks.push("selected_module_contract_unavailable");
                None
            }
            Err(error) => return Err(error),
        };
        if let Err(error) = super::task_prompt::require_new_binding(selected, selector.as_ref()) {
            match error.code.as_str() {
                "ARTIFACT_RETIRED" => hard_blocks.push("route_artifact_retired"),
                "MODULE_CONTRACT_REQUIRED" => {
                    hard_blocks.push("current_module_contract_not_selected")
                }
                _ => return Err(error),
            }
        }
        route["module_contract_selector"] = json!(selector);
        let binding = exact_current_attempt
            .and_then(|attempt| {
                attempt
                    .binding_id
                    .as_deref()
                    .zip(attempt.binding_generation)
            })
            .map(|(id, generation)| super::operations::get_binding(db, id, generation))
            .transpose()?;
        let mut admission = route_admission(db, selected, binding.as_ref(), exclude_operation_id)?;
        if let Some(operation_id) = exclude_operation_id {
            let retained: Option<String> = db.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
                [operation_id],
                |row| row.get(0),
            ).optional()?;
            if let Some(raw) = retained {
                let retained: Value = serde_json::from_str(&raw)?;
                if let Some(original) = retained["launch_plan_authority"]["route"].get("admission")
                {
                    // Keep immutable preview evidence in the plan digest.
                    // Current admission is checked separately before workspace
                    // reservation and before every new native effect.
                    admission = serde_json::from_value(original.clone())?;
                }
            }
        }
        match &admission.decision {
            provider_conditions::RouteAdmissionDecision::Admit => {}
            provider_conditions::RouteAdmissionDecision::Hold { .. } => {
                hard_blocks.push("route_admission_held")
            }
            provider_conditions::RouteAdmissionDecision::Unavailable { .. } => {
                hard_blocks.push("route_admission_unavailable")
            }
        }
        route["admission"] = serde_json::to_value(admission)?;
    }
    let mcp_profile = launch_mcp_profile_projection(db, config, actor, &request, &mut hard_blocks)?;
    let baseline = workspace_baseline_projection(db, &row, spec.as_ref())?;
    if baseline["status"] == "unverifiable" {
        hard_blocks.push("pinned_baseline_commit_not_proven");
        gaps.push("task_baseline_artifact_does_not_prove_a_full_git_commit");
    }
    let launch_operations = launch_operation_projection(db, &row.task_id, exclude_operation_id)?;
    if launch_operations["unresolved_count"].as_i64().unwrap_or(0) > 0 {
        hard_blocks.push("prior_launch_operation_unresolved");
    }
    if launch_operations["coverage"] != "complete" {
        gaps.push("prior_launch_operation_page_is_partial");
    }

    let own_initial_paths = spec
        .as_ref()
        .and_then(|spec| spec.scope.as_ref())
        .map(|scope| scope.initial_paths.as_slice())
        .unwrap_or_default();
    let overlaps = current_scope_overlaps(
        db,
        exact_current_attempt.map(|attempt| attempt.attempt_id.as_str()),
        &row.project_id,
        own_initial_paths,
        PageRequest {
            after: 0,
            limit: MAX_INSPECT_OVERLAPS,
        },
    )?;
    if overlaps["coverage"] != "complete" {
        gaps.push("literal_path_overlap_coverage_is_partial_or_unknown");
    }

    let (attempt_action, attempt_block) = match exact_current_attempt {
        Some(attempt) => {
            if attempt.task_revision != row.revision {
                ("forbidden", Some("current_attempt_revision_is_stale"))
            } else if attempt.owner_id != actor.effective_manager_id() {
                (
                    "forbidden",
                    Some("current_attempt_is_owned_by_another_manager"),
                )
            } else if !matches!(
                attempt.state.as_str(),
                "reserved" | "running" | "needs_correction"
            ) {
                ("forbidden", Some("current_attempt_state_is_not_reusable"))
            } else if attempt.binding_id.is_some() {
                (
                    "forbidden",
                    Some("current_attempt_binding_is_not_linked_to_a_fresh_workspace_lease"),
                )
            } else if attempt.state != "reserved" || attempt.start_operation_id.is_some() {
                (
                    "forbidden",
                    Some("current_attempt_has_prior_start_or_nonreserved_state"),
                )
            } else {
                ("use_existing", None)
            }
        }
        None if row.current_attempt_id.is_some() => {
            ("forbidden", Some("current_attempt_record_missing"))
        }
        None if hard_blocks.is_empty() => ("claim_new", None),
        None => ("forbidden", None),
    };
    if let Some(block) = attempt_block {
        hard_blocks.push(block);
    }

    let attempt_binding = if let Some(attempt) = exact_current_attempt {
        match (&attempt.binding_id, attempt.binding_generation) {
            (Some(binding_id), Some(generation)) => {
                let detail = binding_summary(db, binding_id, generation)?;
                let route_alias = detail["route"]["alias"].as_str();
                if route_alias.is_some_and(|alias| alias != request.route) {
                    hard_blocks.push("requested_route_differs_from_current_attempt_binding");
                }
                json!({
                    "binding_id":binding_id,
                    "generation":generation,
                    "state":detail["state"],
                    "route_alias":route_alias,
                    "runtime_kind":detail["route"]["runtime"]
                        .as_str()
                        .map(safe_route_runtime)
                        .unwrap_or("unknown"),
                    "connection":detail["observation"]["connection"],
                    "live_qualified":false,
                })
            }
            _ => json!({"status":"no_exact_binding_recorded"}),
        }
    } else {
        Value::Null
    };
    let capacity = match exact_current_attempt {
        Some(attempt) => exact_attempt_capacity(db, attempt)?,
        None => json!({
            "status":"unknown",
            "capacity_available":null,
            "reason":"no_exact_attempt_binding_or_owner_route_selected",
            "full_scope_capacity_claimed":false,
        }),
    };
    if capacity["scope_capacity_available"].is_null() {
        gaps.push("full_provider_runtime_capacity_not_proven");
    }

    let attempt_projection = match exact_current_attempt {
        Some(attempt) => json!({
            "status":"current",
            "attempt_id":attempt.attempt_id,
            "task_revision":attempt.task_revision,
            "state":attempt.state,
            "owner":owner_profile(db,&attempt.owner_id)?,
            "start_owner":attempt.start_owner,
            "start_operation_id":attempt.start_operation_id,
            "created_at_ms":attempt.created_at_ms,
            "updated_at_ms":attempt.updated_at_ms,
            "binding":attempt_binding,
            "submission_ref":attempt.submission_ref,
        }),
        None => json!({
            "status":if row.current_attempt_id.is_some() {"record_missing"} else {"none"},
            "attempt_id":row.current_attempt_id,
        }),
    };
    let candidate_scope = json!({
        "current_attempt_candidate_ref":exact_current_attempt.and_then(|attempt| attempt.candidate_ref.as_deref()),
        "task_accepted_candidate_ref":row.accepted_candidate_ref,
        "candidate_artifact_bytes_read":false,
        "candidate_validation":"reference_only",
    });
    if !candidate_scope["current_attempt_candidate_ref"].is_string()
        && !candidate_scope["task_accepted_candidate_ref"].is_string()
    {
        gaps.push("no_retained_candidate_reference_for_current_scope");
    }

    let action = if hard_blocks.is_empty() {
        attempt_action
    } else {
        "forbidden"
    };
    let readiness = if !hard_blocks.is_empty() {
        "blocked"
    } else {
        // The Store has no launch manifest, route capability receipt,
        // workspace lease, or full-scope capacity proof.
        "unknown"
    };
    let workspace = launch_workspace_projection(&request);
    let detached_references = launch_detail_references(&row, &attempt_projection);
    let decision_card = launch_decision_card(LaunchDecisionCardInput {
        task_brief: &task_brief,
        task: &row,
        expected_revision: request.expected_task_revision,
        attempt: &attempt_projection,
        claim_readiness: &claim_readiness,
        dependencies: &dependencies,
        attempt_action: action,
        attempt_binding: &attempt_binding,
        capacity: &capacity,
        overlaps: &overlaps,
        route: &route,
        mcp: &mcp_profile,
        baseline: &baseline,
        workspace: &workspace,
        hard_blocks: hard_blocks.as_slice(),
        gaps: gaps.as_slice(),
        readiness,
        detail_references: &detached_references,
    });
    let mut plan = json!({
        "preview_only":true,
        "effects":"none",
        "launch_mutation":"durable_intent_pending_workspace",
        "launch_execution":"awaits_verified_workspace_lease",
        "preview_readiness":readiness,
        "task":{
            "task_id":row.task_id,
            "project_id":row.project_id,
            "revision":row.revision,
            "expected_revision":request.expected_task_revision,
            "state":row.state,
            "accepted_attempt_id":row.accepted_attempt_id,
            "accepted_operation_id":row.accepted_operation_id,
            "accepted_candidate_ref":row.accepted_candidate_ref,
            "task_brief":task_brief,
            "work_scope":work_scope,
            "updated_at_ms":row.updated_at_ms,
        },
        "attempt_action":action,
        "current_attempt":attempt_projection,
        "candidate_scope":candidate_scope,
        "queue_context":{
            "rank":{"status":"not_recorded"},
            "claim_readiness":claim_readiness,
            "dependencies":dependencies,
            "prior_launch_operations":launch_operations,
        },
        "workspace":workspace,
        "baseline":baseline,
        "route":route,
        "mcp":mcp_profile,
        "capacity":capacity,
        "overlap":overlaps,
        "requested_plan_facts":{
            "budget":request.budget,
            "stop_conditions":request.stop_conditions,
            "purpose":request.purpose,
            "enforceability":"not_recorded_until_a_durable_launch_manifest_exists",
        },
        "decision_card":decision_card.clone(),
        "hard_blocks":hard_blocks,
        "coverage":if gaps.is_empty() {"complete"} else {"partial"},
        "gaps":gaps,
    });
    let current_attempt = match exact_current_attempt {
        Some(attempt) => json!({
            "attempt_id":attempt.attempt_id,
            "task_revision":attempt.task_revision,
            "owner_id":attempt.owner_id,
            "state":attempt.state,
            "start_owner":attempt.start_owner,
            "start_operation_id":attempt.start_operation_id,
            "binding_id":attempt.binding_id,
            "binding_generation":attempt.binding_generation,
            "snapshot_sha256":model::digest(model::canonical(&attempt.snapshot)?.as_bytes()),
        }),
        None => json!({"attempt_id":row.current_attempt_id}),
    };
    let selected_route_sha256 = config
        .routes
        .iter()
        .find(|route| route.alias == request.route)
        .map(|route| {
            model::canonical(&serde_json::to_value(route)?)
                .map(|bytes| model::digest(bytes.as_bytes()))
        })
        .transpose()?;
    let selected_mcp_profile_sha256 = config
        .mcp
        .profiles
        .get(&request.mcp_profile)
        .map(|profile| {
            model::canonical(&serde_json::to_value(profile)?)
                .map(|bytes| model::digest(bytes.as_bytes()))
        })
        .transpose()?;
    let authority = LaunchPlanAuthority {
        schema_version: 1,
        task: json!({
            "task_id":row.task_id,"project_id":row.project_id,"revision":row.revision,
            "expected_revision":request.expected_task_revision,"state":row.state,
            "accepted_attempt_id":row.accepted_attempt_id,
            "accepted_operation_id":row.accepted_operation_id,
            "accepted_candidate_ref":row.accepted_candidate_ref,
        }),
        task_spec_sha256: model::digest(model::canonical(&row.spec)?.as_bytes()),
        attempt_action: action.to_owned(),
        current_attempt,
        candidate_scope,
        workspace,
        baseline,
        route,
        selected_route_sha256,
        mcp: mcp_profile,
        selected_mcp_profile_sha256,
        requested_plan_facts: launch_preview_value(&request),
        hard_blocks: hard_blocks.iter().map(|code| (*code).to_owned()).collect(),
        gaps: gaps.iter().map(|code| (*code).to_owned()).collect(),
        preview_readiness: readiness.to_owned(),
        coverage: if gaps.is_empty() {
            "complete"
        } else {
            "partial"
        }
        .to_owned(),
    };
    validate_launch_plan_authority(&authority, &request)?;
    let plan_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(&serde_json::to_value(&authority)?)?.as_bytes())
    );
    plan["plan_digest"] = json!(plan_digest);
    plan["authority_projection"] = json!("complete");
    let canonical = model::canonical(&plan)?;
    if canonical.len() <= projection::MAX_SERIALIZED_BYTES {
        return Ok(PlannedLaunch {
            authority,
            projection: plan,
            plan_digest,
        });
    }

    let fallback = json!({
        "plan_digest":plan_digest,
        "authority_projection":"detached",
        "preview_only":true,
        "effects":"none",
        "launch_mutation":"durable_intent_pending_workspace",
        "launch_execution":"awaits_verified_workspace_lease",
        "preview_readiness":readiness,
        "task":{
            "task_id":row.task_id,
            "project_id":row.project_id,
            "revision":row.revision,
            "expected_revision":request.expected_task_revision,
            "state":row.state,
        },
        "attempt_action":action,
        "current_attempt":{
            "status":attempt_projection["status"],
            "attempt_id":attempt_projection["attempt_id"],
        },
        "decision_card":decision_card.clone(),
        "context_detached":{
            "status":"detached",
            "serialized_bytes":canonical.len(),
            "digest":model::digest(canonical.as_bytes()),
            "references":detached_references.clone(),
        },
        "hard_blocks":bounded_decision_literals(&hard_blocks),
        "coverage":"partial",
        "gaps":["combined_launch_preview_exceeded_serialized_budget; use the exact linked read projections"],
    });
    let fallback_canonical = model::canonical(&fallback)?;
    if fallback_canonical.len() <= projection::MAX_SERIALIZED_BYTES {
        return Ok(PlannedLaunch {
            authority,
            projection: fallback,
            plan_digest,
        });
    }

    let terminal_fallback = json!({
        "plan_digest":plan_digest,
        "authority_projection":"detached",
        "preview_only":true,
        "effects":"none",
        "launch_mutation":"durable_intent_pending_workspace",
        "launch_execution":"awaits_verified_workspace_lease",
        "preview_readiness":readiness,
        "task":{
            "task_id":row.task_id,
            "project_id":row.project_id,
            "revision":row.revision,
            "expected_revision":request.expected_task_revision,
            "state":bounded_decision_value(&json!(row.state), MAX_DECISION_CARD_TEXT_BYTES).0,
        },
        "attempt_action":action,
        "current_attempt":{
            "status":attempt_projection["status"],
            "attempt_id":attempt_projection["attempt_id"],
        },
        "decision_card":{
            "schema_version":1,
            "status":readiness,
            "hard_blocks":bounded_decision_literals(&hard_blocks),
            "warnings":bounded_decision_literals(&gaps),
            "detail_references":detached_references.clone(),
        },
        "context_detached":{
            "status":"detached",
            "serialized_bytes":canonical.len(),
            "digest":model::digest(canonical.as_bytes()),
            "references":detached_references.clone(),
        },
        "coverage":"partial",
        "gaps":["combined_launch_preview_exceeded_serialized_budget; use the exact linked read projections"],
    });
    if model::canonical(&terminal_fallback)?.len() > projection::MAX_SERIALIZED_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "minimum exact launch projection exceeds its response byte boundary",
        ));
    }
    Ok(PlannedLaunch {
        authority,
        projection: terminal_fallback,
        plan_digest,
    })
}

fn validate_launch_plan_authority(
    authority: &LaunchPlanAuthority,
    request: &launcher::LaunchPreviewRequest,
) -> Result<()> {
    if authority.schema_version != 1
        || authority.task["task_id"] != request.task_id
        || !authority.task["project_id"].is_string()
        || authority.task["revision"]
            .as_i64()
            .is_none_or(|revision| revision <= 0)
        || !matches!(
            authority.attempt_action.as_str(),
            "claim_new" | "use_existing" | "forbidden"
        )
        || authority.route["alias"] != request.route
        || authority.mcp["profile_name"] != request.mcp_profile
        || authority.mcp["surface"] != request.mcp_surface
        || authority.workspace["policy"] != request.workspace_policy
        || !authority.candidate_scope.is_object()
        || !authority.baseline.is_object()
        || authority.requested_plan_facts != launch_preview_value(request)
        || (authority.attempt_action == "use_existing"
            && !authority.current_attempt["attempt_id"].is_string())
    {
        return Err(Error::new(
            "LAUNCH_PLAN_AUTHORITY_DAMAGED",
            "private launch authority is incomplete or inconsistent",
        ));
    }
    Ok(())
}

fn validate_launch_manifest_precommit(
    manifest: &Value,
    planned: &PlannedLaunch,
    request: &launcher::LaunchRequest,
) -> Result<()> {
    let authority = &planned.authority;
    validate_launch_plan_authority(authority, &request.preview)?;
    if manifest["manifest_version"] != "eliot-launch-manifest-v1"
        || manifest["plan_digest"] != planned.plan_digest
        || planned.plan_digest != request.plan_digest
        || manifest["request"] != authority.requested_plan_facts
        || manifest["runtime"]["route"] != authority.route
        || manifest["mcp"] != authority.mcp
        || manifest["task"]["candidate_scope"] != authority.candidate_scope
        || manifest["task"]["task_id"] != authority.task["task_id"]
        || manifest["task"]["project_id"] != authority.task["project_id"]
        || manifest["task"]["observed_revision"] != authority.task["revision"]
        || manifest["task"]["attempt_action"] != authority.attempt_action
        || manifest["task"]["attempt_id"] != authority.current_attempt["attempt_id"]
        || manifest["preflight"]["hard_blocks"] != json!(authority.hard_blocks)
    {
        return Err(Error::new(
            "LAUNCH_MANIFEST_CORRUPT",
            "manifest differs from its complete private launch authority",
        ));
    }
    Ok(())
}

struct LaunchDecisionCardInput<'a> {
    task_brief: &'a Value,
    task: &'a TaskRow,
    expected_revision: i64,
    attempt: &'a Value,
    claim_readiness: &'a Value,
    dependencies: &'a Value,
    attempt_action: &'a str,
    attempt_binding: &'a Value,
    capacity: &'a Value,
    overlaps: &'a Value,
    route: &'a Value,
    mcp: &'a Value,
    baseline: &'a Value,
    workspace: &'a Value,
    hard_blocks: &'a [&'static str],
    gaps: &'a [&'static str],
    readiness: &'a str,
    detail_references: &'a Value,
}

fn launch_decision_card(input: LaunchDecisionCardInput<'_>) -> Value {
    let LaunchDecisionCardInput {
        task_brief,
        task,
        expected_revision,
        attempt,
        claim_readiness,
        dependencies,
        attempt_action,
        attempt_binding,
        capacity,
        overlaps,
        route,
        mcp,
        baseline,
        workspace,
        hard_blocks,
        gaps,
        readiness,
        detail_references,
    } = input;
    let brief = task_brief
        .get("brief")
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or(Value::Null);
    let brief_included = task_brief["status"] == "included" && brief.is_object();
    let (objective_value, objective_truncated) = if brief_included {
        bounded_decision_value(&brief["objective"], MAX_DECISION_CARD_OBJECTIVE_BYTES)
    } else {
        (Value::Null, false)
    };
    let (phase_value, phase_truncated) = if brief_included {
        bounded_decision_value(&brief["phase"], MAX_DECISION_CARD_TEXT_BYTES)
    } else {
        (Value::Null, false)
    };
    let objective = if brief_included {
        json!({
            "status":"recorded",
            "value":objective_value,
            "phase":phase_value,
            "truncated":objective_truncated || phase_truncated,
        })
    } else {
        json!({
            "status":"unknown",
            "value":Value::Null,
            "phase":Value::Null,
            "truncated":false,
            "reason":"task_brief_is_not_inline",
        })
    };
    let (requirement_ids, requirement_count, requirement_omitted, requirement_truncated) =
        compact_requirement_ids(&brief, brief_included);
    let remaining_requirements = json!({
        "status":"unknown",
        "recorded":brief_included,
        "count":if brief_included {json!(requirement_count)} else {Value::Null},
        "ids":requirement_ids,
        "omitted":if brief_included {json!(requirement_omitted)} else {Value::Null},
        "truncated":requirement_truncated,
        "reason":"requirement_completion_is_not_projected_by_launch_preview",
    });
    let raw_owner = attempt.get("owner").cloned().unwrap_or(Value::Null);
    let owner_summary = compact_owner(&raw_owner);
    let owner_status = if attempt["status"] == "current" && raw_owner.is_object() {
        "recorded"
    } else {
        "unknown"
    };
    let binding_qualification = attempt_binding
        .get("live_qualified")
        .cloned()
        .unwrap_or(Value::Null);
    let overlap_summary = compact_overlap(overlaps);
    let claim_summary = compact_claim_readiness(claim_readiness);
    let dependency_summary = compact_dependency_projection(dependencies);
    let capacity_gap = capacity
        .get("capacity_reason")
        .or_else(|| capacity.get("reason"))
        .filter(|value| value.is_string())
        .map(|value| bounded_decision_value(value, MAX_DECISION_CARD_TEXT_BYTES).0)
        .unwrap_or(Value::Null);
    let surface_facts = mcp.get("surface_facts").cloned().unwrap_or(Value::Null);
    let evidence_gaps = json!({
        "preview":bounded_decision_literals(gaps),
        "route":bounded_decision_values(&route["capability_gaps"]),
        "mcp":bounded_decision_values(&mcp["gaps"]),
        "overlap":bounded_decision_values(&overlaps["gaps"]),
        "capacity":if capacity_gap.is_null() {Value::Array(Vec::new())} else {json!([capacity_gap])},
    });
    json!({
        "schema_version":1,
        "status":readiness,
        "planning":{
            "preview_only":true,
            "effects":"none",
            "attempt_action":attempt_action,
        },
        "task":{
            "task_id":bounded_decision_value(&json!(task.task_id), MAX_DECISION_CARD_TEXT_BYTES).0,
            "revision":task.revision,
            "expected_revision":expected_revision,
            "state":bounded_decision_value(&json!(task.state), MAX_DECISION_CARD_TEXT_BYTES).0,
        },
        "objective":objective,
        "remaining_requirements":remaining_requirements,
        "why_next":{
            "status":"unknown",
            "rank":{"status":"not_recorded"},
            "claim_readiness":claim_summary,
            "dependency_status":dependency_summary,
            "reason":"queue_priority_and_rank_are_not_recorded",
        },
        "current_owners":{
            "status":owner_status,
            "attempt_owner":owner_summary,
            "overlap_owners":{
                "coverage":overlap_summary["coverage"],
                "owner_count":overlap_summary["owner_count"],
                "owner_ids":overlap_summary["owner_ids"],
                "owner_ids_omitted":overlap_summary["owner_ids_omitted"],
                "owner_ids_truncated":overlap_summary["owner_ids_truncated"],
                "has_more":overlap_summary["has_more"],
            },
        },
        "contracts":{
            "status":"not_recorded_in_task_assignment_store",
            "requires":Value::Null,
            "provides":Value::Null,
            "items":[],
        },
        "overlap":overlap_summary,
        "workspace_lease_plan":{
            "workspace":compact_workspace_projection(workspace),
            "baseline":compact_baseline_projection(baseline),
        },
        "selection":{
            "route":bounded_decision_value(&route["alias"], MAX_DECISION_CARD_TEXT_BYTES).0,
            "route_admission":route["admission"],
            "agent_profile":compact_agent_profile(&route["agent_profile"]),
            "mcp_profile":bounded_decision_value(&mcp["profile_name"], MAX_DECISION_CARD_TEXT_BYTES).0,
            "mcp_surface":bounded_decision_value(&mcp["surface"], MAX_DECISION_CARD_TEXT_BYTES).0,
            "requested_model":compact_requested_value(&route["requested_model"]),
            "requested_effort":compact_requested_value(&route["requested_effort"]),
            "observed_qualification":{
                "route_live_qualified":route["live_qualified"],
                "mcp_runtime_loaded":mcp["runtime_loaded"],
                "binding_live_qualified":binding_qualification,
            },
        },
        "mcp_core":{
            "status":bounded_decision_value(&mcp["status"], MAX_DECISION_CARD_TEXT_BYTES).0,
            "profile":bounded_decision_value(&mcp["profile_name"], MAX_DECISION_CARD_TEXT_BYTES).0,
            "surface":bounded_decision_value(&mcp["surface"], MAX_DECISION_CARD_TEXT_BYTES).0,
            "core":bounded_decision_values(&surface_facts["core"]),
            "core_methods":bounded_decision_values(&surface_facts["core_methods"]),
            "deferred_groups":bounded_decision_values(&surface_facts["deferred_groups"]),
            "manual_tools":bounded_decision_values(&surface_facts["manual_tools"]),
            "runtime_loaded":mcp["runtime_loaded"],
        },
        "blockers":{
            "preview":bounded_decision_literals(hard_blocks),
            "claim_readiness":claim_summary["blockers"],
            "dependencies":dependency_summary,
        },
        "hard_blocks":bounded_decision_literals(hard_blocks),
        "warnings":bounded_decision_literals(gaps),
        "evidence_gaps":evidence_gaps,
        "detail_references":detail_references,
    })
}

fn launch_detail_references(task: &TaskRow, attempt: &Value) -> Value {
    let mut references = vec![json!({
        "method":"task.get",
        "params":{"task_id":task.task_id},
    })];
    if let Some(attempt_id) = attempt["attempt_id"].as_str() {
        references.push(json!({
            "method":"swarm.agent.inspect",
            "params":{
                "attempt_id":attempt_id,
                "operation_limit":1,
                "check_limit":1,
                "peer_limit":1,
                "overlap_limit":1,
            },
        }));
    }
    references.push(json!({
        "method":"swarm.queue.get",
        "params":{"project_id":task.project_id,"limit":DASHBOARD_PAGE_LIMIT},
    }));
    Value::Array(references)
}

fn bounded_decision_value(value: &Value, max_bytes: usize) -> (Value, bool) {
    let Some(text) = value.as_str() else {
        return if value.is_null() || value.is_boolean() || value.is_number() {
            (value.clone(), false)
        } else {
            (Value::Null, true)
        };
    };
    let mut end = text.len().min(max_bytes);
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    (Value::String(text[..end].to_owned()), end < text.len())
}

fn bounded_decision_values(value: &Value) -> Value {
    let Some(values) = value.as_array() else {
        return Value::Null;
    };
    Value::Array(
        values
            .iter()
            .take(MAX_DECISION_CARD_LIST_ITEMS)
            .filter(|value| value.is_string())
            .map(|value| bounded_decision_value(value, MAX_DECISION_CARD_TEXT_BYTES).0)
            .collect(),
    )
}

fn bounded_decision_literals(values: &[&str]) -> Value {
    Value::Array(
        values
            .iter()
            .take(MAX_DECISION_CARD_LIST_ITEMS)
            .map(|value| {
                bounded_decision_value(
                    &Value::String((*value).to_owned()),
                    MAX_DECISION_CARD_TEXT_BYTES,
                )
                .0
            })
            .collect(),
    )
}

fn compact_requirement_ids(brief: &Value, included: bool) -> (Value, usize, usize, bool) {
    let Some(requirements) = brief["requirements"].as_array() else {
        return (Value::Array(Vec::new()), 0, 0, false);
    };
    let mut ids = Vec::new();
    let mut truncated = false;
    for requirement in requirements.iter().take(MAX_DECISION_CARD_LIST_ITEMS) {
        if let Some(id) = requirement["id"].as_str() {
            let (value, was_truncated) =
                bounded_decision_value(&Value::String(id.to_owned()), MAX_DECISION_CARD_TEXT_BYTES);
            ids.push(value);
            truncated |= was_truncated;
        }
    }
    let count = if included { requirements.len() } else { 0 };
    let id_count = ids.len();
    (
        Value::Array(ids),
        count,
        count.saturating_sub(id_count),
        truncated,
    )
}

fn compact_owner(owner: &Value) -> Value {
    if !owner.is_object() {
        return json!({"status":"unknown","client_id":Value::Null,"role":Value::Null});
    }
    let (client_id, client_id_truncated) =
        bounded_decision_value(&owner["client_id"], MAX_DECISION_CARD_TEXT_BYTES);
    let (role, role_truncated) =
        bounded_decision_value(&owner["role"], MAX_DECISION_CARD_TEXT_BYTES);
    json!({
        "status":bounded_decision_value(&owner["status"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "client_id":client_id,
        "role":role,
        "truncated":client_id_truncated || role_truncated,
    })
}

fn compact_dependency_projection(dependencies: &Value) -> Value {
    let accepted_count = dependencies["accepted"].as_array().map_or(0, Vec::len);
    let waiting = dependencies["waiting"].as_array();
    let waiting_count = waiting.map_or(0, Vec::len);
    let mut waiting_task_ids = Vec::new();
    if let Some(waiting) = waiting {
        for item in waiting.iter().take(MAX_DECISION_CARD_LIST_ITEMS) {
            if let Some(task_id) = item["task_id"].as_str() {
                waiting_task_ids.push(
                    bounded_decision_value(
                        &Value::String(task_id.to_owned()),
                        MAX_DECISION_CARD_TEXT_BYTES,
                    )
                    .0,
                );
            }
        }
    }
    let waiting_id_count = waiting_task_ids.len();
    json!({
        "status":bounded_decision_value(&dependencies["status"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "accepted_count":accepted_count,
        "waiting_count":waiting_count,
        "waiting_task_ids":waiting_task_ids,
        "waiting_omitted":waiting_count.saturating_sub(waiting_id_count),
        "omitted_count":dependencies["omitted_count"],
        "gaps":bounded_decision_values(&dependencies["gaps"]),
    })
}

fn compact_claim_readiness(readiness: &Value) -> Value {
    json!({
        "state":bounded_decision_value(&readiness["state"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "claim_preconditions":bounded_decision_value(&readiness["claim_preconditions"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "new_work_admission":bounded_decision_value(&readiness["new_work_admission"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "resource_capacity":bounded_decision_value(&readiness["resource_capacity"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "owner_policy":{
            "status":bounded_decision_value(&readiness["owner_policy"]["status"], MAX_DECISION_CARD_TEXT_BYTES).0,
            "edition":bounded_decision_value(&readiness["owner_policy"]["edition"], MAX_DECISION_CARD_TEXT_BYTES).0,
        },
        "blockers":bounded_decision_values(&readiness["blockers"]),
        "gaps":bounded_decision_values(&readiness["gaps"]),
    })
}

fn compact_overlap(overlaps: &Value) -> Value {
    let items = overlaps["items"].as_array();
    let item_count = items.map_or(0, Vec::len);
    let mut owner_ids = Vec::new();
    let mut owner_seen = Vec::new();
    let mut owner_ids_truncated = false;
    let mut matched_path_count = 0usize;
    if let Some(items) = items {
        for item in items {
            if let Some(paths) = item["matched_paths"].as_array() {
                matched_path_count = matched_path_count.saturating_add(paths.len());
            }
            if let Some(owner_id) = item["owner_id"].as_str()
                && !owner_seen
                    .iter()
                    .any(|seen: &String| seen.as_str() == owner_id)
            {
                owner_seen.push(owner_id.to_owned());
                if owner_ids.len() < MAX_DECISION_CARD_OVERLAP_ITEMS {
                    let (owner_id, was_truncated) = bounded_decision_value(
                        &Value::String(owner_id.to_owned()),
                        MAX_DECISION_CARD_TEXT_BYTES,
                    );
                    owner_ids.push(owner_id);
                    owner_ids_truncated |= was_truncated;
                }
            }
        }
    }
    json!({
        "status":bounded_decision_value(&overlaps["status"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "coverage":bounded_decision_value(&overlaps["coverage"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "item_count":item_count,
        "owner_count":owner_seen.len(),
        "owner_ids":owner_ids,
        "owner_ids_omitted":owner_seen.len().saturating_sub(MAX_DECISION_CARD_OVERLAP_ITEMS),
        "owner_ids_truncated":owner_ids_truncated,
        "matched_path_count":matched_path_count,
        "has_more":overlaps["has_more"],
        "gaps":bounded_decision_values(&overlaps["gaps"]),
    })
}

fn compact_agent_profile(profile: &Value) -> Value {
    if !profile.is_object() {
        return bounded_decision_value(profile, MAX_DECISION_CARD_TEXT_BYTES).0;
    }
    json!({
        "requested":bounded_decision_value(&profile["requested"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "validation":bounded_decision_value(&profile["validation"], MAX_DECISION_CARD_TEXT_BYTES).0,
    })
}

fn compact_requested_value(value: &Value) -> Value {
    if value.is_object() {
        let (requested, truncated) =
            bounded_decision_value(&value["value"], MAX_DECISION_CARD_TEXT_BYTES);
        return json!({
            "value":requested,
            "validation":bounded_decision_value(&value["validation"], MAX_DECISION_CARD_TEXT_BYTES).0,
            "truncated":truncated,
        });
    }
    let (requested, truncated) = bounded_decision_value(value, MAX_DECISION_CARD_TEXT_BYTES);
    json!({
        "value":requested,
        "validation":"unknown",
        "truncated":truncated,
    })
}

fn compact_workspace_projection(workspace: &Value) -> Value {
    json!({
        "policy":bounded_decision_value(&workspace["policy"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "status":bounded_decision_value(&workspace["status"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "lease":bounded_decision_value(&workspace["lease"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "dirty":workspace["dirty"],
        "filesystem_inspected":workspace["filesystem_inspected"],
        "branch":bounded_decision_value(&workspace["branch"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "baseline_commit":bounded_decision_value(&workspace["baseline_commit"], MAX_DECISION_CARD_TEXT_BYTES).0,
    })
}

fn compact_baseline_projection(baseline: &Value) -> Value {
    json!({
        "status":bounded_decision_value(&baseline["status"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "candidate_ref":bounded_decision_value(&baseline["candidate_ref"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "commit":bounded_decision_value(&baseline["commit"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "artifact_digest":bounded_decision_value(&baseline["artifact_digest"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "acceptance_task_id":bounded_decision_value(&baseline["acceptance_task_id"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "acceptance_attempt_id":bounded_decision_value(&baseline["acceptance_attempt_id"], MAX_DECISION_CARD_TEXT_BYTES).0,
        "acceptance_task_revision":baseline["acceptance_task_revision"],
    })
}

fn queue_page(
    db: &Connection,
    params_value: &Value,
    brief_limit: usize,
    config: &Config,
) -> Result<Value> {
    model::fields(
        params_value,
        &["after", "limit", "project_id", "task_state"],
    )?;
    let page = PageRequest::parse(params_value)?;
    let project_id = validate_filter_text(
        params_value.get("project_id").and_then(Value::as_str),
        "project_id",
    )?;
    if params_value.get("project_id").is_some() && project_id.is_none() {
        return Err(Error::invalid("project_id must be a nonempty string"));
    }
    let task_state = validate_filter_text(
        params_value.get("task_state").and_then(Value::as_str),
        "task_state",
    )?;
    if params_value.get("task_state").is_some() && task_state.is_none() {
        return Err(Error::invalid("task_state must be a nonempty string"));
    }
    if task_state
        .as_deref()
        .is_some_and(|state| !matches!(state, "open" | "accepted" | "archived"))
    {
        return Err(Error::invalid(
            "task_state must be open, accepted, or archived",
        ));
    }
    let (total, rows) = query_tasks(
        db,
        project_id.as_deref(),
        task_state.as_deref(),
        page.after,
        page.limit,
    )?;
    let after = page.after.min(total);
    let items = rows
        .iter()
        .map(|row| task_queue_item(db, row, brief_limit, config))
        .collect::<Result<Vec<_>>>()?;
    bounded_page("task_queue", after, page.limit, total, items)
}

fn operation_page(db: &Connection, attempt_id: &str, page: PageRequest) -> Result<Value> {
    let total: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE attempt_id=?1",
        [attempt_id],
        |row| row.get(0),
    )?;
    let after = page.after.min(total);
    let mut statement = db.prepare(
        "SELECT operation_id,method,state,task_id,attempt_id,binding_id,binding_generation,
                prerequisite_operation_id,created_at_ms,updated_at_ms
         FROM operations WHERE attempt_id=?1
         ORDER BY created_at_ms DESC,operation_id DESC LIMIT ?2 OFFSET ?3",
    )?;
    let items = statement
        .query_map(params![attempt_id, page.limit, after], |row| {
            let state: String = row.get(2)?;
            Ok(json!({
                "operation_id":row.get::<_,String>(0)?,
                "method":row.get::<_,String>(1)?,
                "state":state,
                "task_id":row.get::<_,Option<String>>(3)?,
                "attempt_id":row.get::<_,Option<String>>(4)?,
                "binding_id":row.get::<_,Option<String>>(5)?,
                "binding_generation":row.get::<_,Option<i64>>(6)?,
                "prerequisite_operation_id":row.get::<_,Option<String>>(7)?,
                "created_at_ms":row.get::<_,i64>(8)?,
                "updated_at_ms":row.get::<_,i64>(9)?,
                "outcome_certainty":match state.as_str() {
                    "outcome_unknown" => "unknown",
                    "queued" => "not_dispatched",
                    "sending" | "native_accepted" => "external_effect_pending",
                    _ => "terminal_recorded",
                }
            }))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    bounded_page("attempt_operations", after, page.limit, total, items)
}

fn attempt_operation_state_counts(db: &Connection, attempt_id: &str) -> Result<Value> {
    let by_state = {
        let mut statement = db.prepare(
            "SELECT state,count(*) FROM operations WHERE attempt_id=?1 GROUP BY state ORDER BY state",
        )?;
        let rows = statement
            .query_map([attempt_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut by_state = Map::new();
        for (state, count) in rows {
            by_state.insert(state, json!(count));
        }
        Value::Object(by_state)
    };
    let unresolved: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE attempt_id=?1
         AND state IN ('queued','sending','native_accepted','outcome_unknown')",
        [attempt_id],
        |row| row.get(0),
    )?;
    let outcome_unknown: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE attempt_id=?1 AND state='outcome_unknown'",
        [attempt_id],
        |row| row.get(0),
    )?;
    Ok(json!({"by_state":by_state,"unresolved":unresolved,"outcome_unknown":outcome_unknown}))
}

fn check_page(db: &Connection, attempt_id: &str, page: PageRequest) -> Result<Value> {
    let total: i64 = db.query_row(
        "SELECT count(*) FROM check_runs WHERE attempt_id=?1",
        [attempt_id],
        |row| row.get(0),
    )?;
    let after = page.after.min(total);
    let mut statement = db.prepare(
        "SELECT check_id,operation_id,candidate_ref,state,exit_code,resource_claimed_at_ms,
                resource_released_at_ms,started_at_ms,finished_at_ms
         FROM check_runs WHERE attempt_id=?1 ORDER BY created_at_ms DESC,check_id DESC LIMIT ?2 OFFSET ?3",
    )?;
    let items = statement
        .query_map(params![attempt_id, page.limit, after], |row| {
            Ok(json!({
                "check_id":row.get::<_,String>(0)?,
                "operation_id":row.get::<_,String>(1)?,
                "candidate_ref":row.get::<_,String>(2)?,
                "state":row.get::<_,String>(3)?,
                "exit_code":row.get::<_,Option<i64>>(4)?,
                "resource_claimed_at_ms":row.get::<_,Option<i64>>(5)?,
                "resource_released_at_ms":row.get::<_,Option<i64>>(6)?,
                "started_at_ms":row.get::<_,Option<i64>>(7)?,
                "finished_at_ms":row.get::<_,Option<i64>>(8)?,
            }))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    bounded_page("attempt_checks", after, page.limit, total, items)
}

fn current_scope_overlaps(
    db: &Connection,
    target_attempt_id: Option<&str>,
    project_id: &str,
    own_paths: &[String],
    page: PageRequest,
) -> Result<Value> {
    if own_paths.is_empty() {
        return Ok(json!({
            "status":"unknown",
            "items":[],
            "next_after":0,
            "has_more":false,
            "coverage":"unknown",
            "gaps":["no_initial_paths_recorded_for_literal_overlap_check","workspace_git_overlap_not_observed"],
        }));
    }
    let capped_paths = own_paths
        .iter()
        .take(MAX_SCOPE_PATHS)
        .cloned()
        .collect::<Vec<_>>();
    let selected_paths: Vec<String> = capped_paths
        .into_iter()
        .filter(|path| path.len() <= MAX_SCOPE_PATH_BYTES)
        .collect();
    let skipped_path_count = own_paths.len().saturating_sub(selected_paths.len());
    if selected_paths.is_empty() {
        return Ok(json!({
            "status":"unknown",
            "items":[],
            "next_after":page.after,
            "has_more":false,
            "coverage":"partial",
            "selected_target_path_count":0,
            "skipped_target_path_count":skipped_path_count,
            "gaps":["no_bounded_literal_initial_paths_available","git_worktree_and_uncommitted_overlap_not_observed"],
        }));
    }
    let paths_json = model::canonical(&json!(selected_paths.clone()))?;
    let mut statement = db.prepare(
        "SELECT a.attempt_id,a.task_id,a.task_revision,a.owner_id,a.state,
                a.created_at_ms,
                (SELECT json_group_array(path) FROM (
                    SELECT DISTINCT own.value AS path
                    FROM json_each(?3) own
                    JOIN json_each(a.task_snapshot_json,'$.spec.scope.initial_paths') other
                      ON own.value=other.value ORDER BY own.value LIMIT ?4
                ))
         FROM attempts a JOIN tasks t ON t.task_id=a.task_id
         WHERE a.released_at_ms IS NULL AND a.attempt_id<>?1 AND t.project_id=?2
           AND EXISTS(SELECT 1 FROM json_each(a.task_snapshot_json,'$.spec.scope.initial_paths') other
                      JOIN json_each(?3) own ON own.value=other.value)
         ORDER BY a.created_at_ms,a.attempt_id LIMIT ?5 OFFSET ?6",
    )?;
    let rows = statement
        .query_map(
            params![
                target_attempt_id.unwrap_or(""),
                project_id,
                paths_json,
                i64::try_from(MAX_MATCHED_PATHS).unwrap_or(8),
                page.limit + 1,
                page.after
            ],
            |row| {
                let matched_paths: String = row.get(6)?;
                let matched_paths: Value =
                    serde_json::from_str(&matched_paths).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                Ok(json!({
                    "attempt_id":row.get::<_,String>(0)?,
                    "task_id":row.get::<_,String>(1)?,
                    "task_revision":row.get::<_,i64>(2)?,
                    "owner_id":row.get::<_,String>(3)?,
                    "attempt_state":row.get::<_,String>(4)?,
                    "created_at_ms":row.get::<_,i64>(5)?,
                    "overlap_kind":"exact_initial_path_string_match",
                    "matched_paths":matched_paths,
                }))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let truncated = rows.len() as i64 > page.limit;
    let items: Vec<Value> = rows
        .into_iter()
        .take(usize::try_from(page.limit).unwrap_or(0))
        .collect();
    let items: Vec<Value> = items
        .into_iter()
        .map(|mut item| {
            if let Some(paths) = item["matched_paths"].as_array() {
                item["matched_paths"] = json!(
                    paths
                        .iter()
                        .map(|path| {
                            path.as_str()
                                .map(safe_scope_path)
                                .unwrap_or_else(|| json!({"status":"unknown"}))
                        })
                        .collect::<Vec<_>>()
                );
            }
            item
        })
        .collect();
    let returned_match_count = items.len() as i64;
    let next_after = page.after + returned_match_count;
    let mut gaps = vec![
        "only_bounded_literal_initial_path_matches_are_compared",
        "git_worktree_and_uncommitted_overlap_not_observed",
    ];
    if skipped_path_count > 0 {
        gaps.push("some_target_paths_were_not_compared");
    }
    if truncated {
        gaps.push("overlap_result_limit_reached");
    }
    Ok(json!({
        "status":"known_exact_matches_only",
        "items":items,
        "returned_match_count":returned_match_count,
        "selected_target_path_count":selected_paths.len(),
        "skipped_target_path_count":skipped_path_count,
        "has_more":truncated,
        "next_after":next_after,
        "limit":page.limit,
        "coverage":"partial",
        "gaps":gaps,
    }))
}

fn manager_exceptions_page(
    db: &Connection,
    params_value: &Value,
    include_current_gm_actions: bool,
) -> Result<Value> {
    model::fields(params_value, &["after", "limit"])?;
    let page = PageRequest::parse(params_value)?;
    let raw = capacity::attention_report(db, ATTENTION_SCAN_LIMIT, page.after)?;
    let raw_items = raw["items"].as_array().cloned().unwrap_or_default();
    let raw_after = page.after.min(raw["total_items"].as_i64().unwrap_or(0));
    let raw_next = raw["next_after"].as_i64().unwrap_or(raw_after);
    let mut filtered = Vec::new();
    let mut raw_offsets = Vec::new();
    for (offset, item) in raw_items.iter().enumerate() {
        if item["manager_actionable"] == true {
            filtered.push(item.clone());
            raw_offsets.push(raw_after + offset as i64);
        } else if item.get("gap").is_some() {
            filtered.push(json!({
                "kind":"attention_coverage_gap",
                "gap":item["gap"],
            }));
            raw_offsets.push(raw_after + offset as i64);
        }
    }
    let filtered_total = filtered.len();
    let requested_count = usize::try_from(page.limit).unwrap_or(0).min(filtered_total);
    let selected: Vec<Value> = filtered.into_iter().take(requested_count).collect();
    let selected_offsets: Vec<i64> = raw_offsets.iter().take(requested_count).copied().collect();
    let limited = projection::limit_items(selected, page_gap_reference)?;
    let next_after = if limited.consumed == 0 {
        raw_next
    } else if limited.consumed < selected_offsets.len() {
        selected_offsets[limited.consumed - 1] + 1
    } else if filtered_total > requested_count {
        selected_offsets.last().copied().unwrap_or(raw_after) + 1
    } else {
        raw_next
    };
    let source_has_more = raw["projection"]["has_newer"] == true;
    let has_more = next_after < raw["total_items"].as_i64().unwrap_or(0) || source_has_more;
    let frame = projection::frame(
        "manager_exceptions",
        json!({"after":page.after,"next_after":next_after,"cursor_kind":"raw_attention_offset"}),
        &limited,
        ATTENTION_SCAN_LIMIT,
        page.after > 0,
        has_more,
        limited.gap_count == 0 && raw["projection"]["coverage_complete"] == true,
        Vec::new(),
    )?;
    let mut response = json!({
        "items":limited.items,
        "next_after":next_after,
        "pagination":"filtered_manager_actionable_attention; cursor is the raw attention offset",
        "source_attention":{"total_items":raw["total_items"],"next_after":raw_next,"has_more":source_has_more},
        "projection":frame,
    });
    if include_current_gm_actions {
        response["host_lifecycle"] = super::host_lifecycle::status(db)?;
        response["manager_action_required"] =
            super::operations::owned_service_start_failure_actions(db)?;
        response["runtime_dispatch_action_required"] =
            super::operations::owned_service_dispatch_failure_actions(db)?;
    }
    Ok(response)
}

fn sanitize_capacity_item(item: &Value) -> Value {
    if item.get("gap").is_some() {
        return item.clone();
    }
    let scope = &item["scope"];
    json!({
        "scope":{
            "scope_key":scope["scope_key"],
            "runtime":scope["runtime"],
            "provider":scope["provider"],
            "account":scope["account"],
            "service":scope["service"],
            "identity":scope["identity"],
            "route_alias":scope["route_alias"],
        },
        "counts":item["counts"],
        "roster":item["roster"],
        "roster_reason":item["roster_reason"],
        "historical_quota_incident_recorded":!item["historical_quota_incident"].is_null(),
        "pending_admission_recorded":item["pending_admission_recorded"],
        "new_work_enabled":item["new_work_enabled"],
        "ledger_updated_at_ms":item["ledger_updated_at_ms"],
    })
}

fn dashboard_capacity(db: &Connection) -> Result<Value> {
    let source = capacity::capacity_report(db, DASHBOARD_PAGE_LIMIT, 0)?;
    let total = source["total_items"].as_i64().unwrap_or(0);
    let items = source["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(sanitize_capacity_item)
        .collect::<Vec<_>>();
    bounded_page(
        "capacity_accounting_summary",
        0,
        DASHBOARD_PAGE_LIMIT,
        total,
        items,
    )
}

fn dashboard_summary(
    db: &Connection,
    include_manager_details: bool,
    limit: i64,
    p: &Principal,
    config: &Config,
) -> Result<Value> {
    let task_summary = task_counts(db)?;
    let attempts = attempt_counts(db)?;
    let operations = operation_counts(db)?;
    let bindings = binding_counts(db)?;
    let admission = new_work_state(db)?;
    let generated_at_ms = model::now_ms()?;
    if !include_manager_details {
        return Ok(json!({
            "status":"partial",
            "generated_at_ms":generated_at_ms,
            "host":{"admission":admission},
            "tasks":task_summary,
            "attempts":attempts,
            "bindings":bindings,
            "operations":operations,
            "coverage":"aggregate_only",
            "gaps":["observer_dashboard_omits_assignment_details_and_manager_exceptions"],
        }));
    }
    let queue_params = json!({"after":0,"limit":limit});
    let queue = queue_page(db, &queue_params, 4_096, config)?;
    let capacity = dashboard_capacity(db)?;
    let include_current_gm_actions = super::gm::require_authority(db, p).is_ok();
    let exception = manager_exceptions_page(
        db,
        &json!({"after":0,"limit":limit}),
        include_current_gm_actions,
    )?;
    let queue_partial = queue["projection"]["has_newer"] == true
        || queue["projection"]["gap_count"].as_i64().unwrap_or(0) > 0;
    let capacity_partial = capacity["projection"]["has_newer"] == true
        || capacity["projection"]["gap_count"].as_i64().unwrap_or(0) > 0;
    let exception_partial = exception["projection"]["has_newer"] == true
        || exception["projection"]["gap_count"].as_i64().unwrap_or(0) > 0;
    let response = json!({
        "status":"partial",
        "generated_at_ms":generated_at_ms,
        "host":{"admission":admission},
        "tasks":task_summary,
        "attempts":attempts,
        "bindings":bindings,
        "operations":operations,
        "queue_preview":queue,
        "capacity":capacity,
        "exceptions":exception,
        "coverage":{
            "task_attempt_binding_operation_counts":"complete_sql_aggregates",
            "queue_preview":if queue_partial {"partial"} else {"complete_page"},
            "capacity_page":if capacity_partial {"partial"} else {"complete_page"},
            "exceptions_page":if exception_partial {"partial"} else {"complete_page"},
        },
        "gaps":[
            "task_priority_not_recorded; queue order is creation time then task ID",
            "route_configuration_is_not_live_qualification_or_task_specific_capacity",
            "workspace_write_lease_and_git_overlap_are_not_recorded",
            "launch_manifest_and_runtime_capability_receipt_are_not_recorded",
            "no_global_participant_roster_or_transcript_is_projected",
        ],
        "viewer":p.client_id,
    });
    let canonical = model::canonical(&response)?;
    if canonical.len() <= projection::MAX_SERIALIZED_BYTES {
        Ok(response)
    } else {
        Ok(json!({
            "status":"partial",
            "generated_at_ms":generated_at_ms,
            "host":{"admission":admission},
            "tasks":task_summary,
            "attempts":attempts,
            "bindings":bindings,
            "operations":operations,
            "coverage":"aggregate_only_details_detached",
            "detail_projection":{
                "status":"detached",
                "serialized_bytes":canonical.len(),
                "digest":model::digest(canonical.as_bytes()),
                "requery":[
                    {"method":"swarm.queue.get","params":{"limit":DASHBOARD_PAGE_LIMIT}},
                    {"method":"report.capacity","params":{"limit":DASHBOARD_PAGE_LIMIT,"after":0}},
                    {"method":"swarm.exceptions.get","params":{"limit":limit,"after":0}},
                ],
            },
            "viewer":p.client_id,
            "gaps":["combined_dashboard_detail_pages_exceeded_serialized_budget; query each bounded projection separately"],
        }))
    }
}

/// Manager and Observer dashboard over current retained Store facts.
pub(super) fn dashboard(
    db: &Connection,
    p: &Principal,
    params_value: &Value,
    config: &Config,
) -> Result<Value> {
    require_dashboard_reader(db, p)?;
    model::fields(params_value, &["limit"])?;
    let limit = params_value
        .get("limit")
        .map(|value| {
            let limit = value
                .as_i64()
                .ok_or_else(|| Error::invalid("limit must be an integer"))?;
            if !(1..=DASHBOARD_PAGE_LIMIT).contains(&limit) {
                return Err(Error::invalid(format!(
                    "limit must be 1..={DASHBOARD_PAGE_LIMIT}"
                )));
            }
            Ok(limit)
        })
        .transpose()?
        .unwrap_or(DASHBOARD_PAGE_LIMIT);
    dashboard_summary(db, p.role != Role::Observer, limit, p, config)
}

/// Bounded manager queue read. SQL filters are applied before offset paging;
/// readiness is then derived from each returned Task/Attempt and acceptance
/// receipt without treating route configuration as runtime capacity.
pub(super) fn queue_get(
    db: &Connection,
    p: &Principal,
    params_value: &Value,
    config: &Config,
) -> Result<Value> {
    require_manager(db, p)?;
    queue_page(db, params_value, launcher::MAX_INLINE_BRIEF_BYTES, config)
}

/// Inspect one exact Attempt and its current binding/operation neighborhood.
/// A Manager sees only its own Attempt; the local Operator may inspect any.
pub(super) fn agent_inspect(
    db: &Connection,
    p: &Principal,
    params_value: &Value,
    config: &Config,
) -> Result<Value> {
    require_manager(db, p)?;
    model::fields(
        params_value,
        &[
            "attempt_id",
            "operation_after",
            "operation_limit",
            "check_after",
            "check_limit",
            "peer_after_client_id",
            "peer_limit",
            "overlap_after",
            "overlap_limit",
        ],
    )?;
    let attempt_id = model::text(params_value, "attempt_id")?.to_owned();
    let attempt = get_attempt_row(db, &attempt_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Attempt {attempt_id}")))?;
    if p.role == Role::Manager && attempt.owner_id != p.client_id {
        super::gm::require_authority(db, p)?;
    }
    let current_task = current_task(db, &attempt.task_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Task {}", attempt.task_id)))?;
    let current_revision = current_task.revision;
    let task_state = current_task.state;
    let accepted_attempt_id = current_task.accepted_attempt_id;
    let accepted_operation_id = current_task.accepted_operation_id;
    let current_attempt_id = current_task.current_attempt_id;
    let task_revision_current = current_revision == attempt.task_revision;
    let current_attempt = attempt.released_at_ms.is_none()
        && current_attempt_id.as_deref() == Some(attempt.attempt_id.as_str());
    let spec: Option<TaskSpec> = attempt
        .snapshot
        .get("spec")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok());
    let exact_brief = attempt.snapshot.get("brief").cloned().unwrap_or_else(
        || json!({"status":"unavailable","reason":"attempt_snapshot_missing_brief"}),
    );
    let brief = launcher::brief_projection(
        &exact_brief,
        json!({"kind":"attempt_snapshot","attempt_id":attempt.attempt_id,"path":"$.brief"}),
        launcher::MAX_INLINE_BRIEF_BYTES,
    )?;
    let dependencies = dependency_projection(db, spec.as_ref())?;
    let retained_dependency_acceptances = snapshot_field_projection(
        attempt.snapshot.get("dependency_acceptances"),
        &attempt.attempt_id,
        "$.dependency_acceptances",
    )?;
    let retained_owner_policy = snapshot_field_projection(
        attempt.snapshot.get("owner_policy"),
        &attempt.attempt_id,
        "$.owner_policy",
    )?;
    let producer_facts = snapshot_field_projection(
        Some(&attempt.producers),
        &attempt.attempt_id,
        "attempt.producers",
    )?;
    let binding = match (&attempt.binding_id, attempt.binding_generation) {
        (Some(id), Some(generation)) => binding_summary(db, id, generation)?,
        _ => Value::Null,
    };
    let capacity = exact_attempt_capacity(db, &attempt)?;
    let operation_counts = attempt_operation_state_counts(db, &attempt.attempt_id)?;
    let operation_cursor = PageRequest {
        after: optional_i64(params_value, "operation_after", 0)?,
        limit: optional_i64(params_value, "operation_limit", 10)?,
    };
    if operation_cursor.after < 0 || !(1..=MAX_INSPECT_OPERATIONS).contains(&operation_cursor.limit)
    {
        return Err(Error::invalid(format!(
            "operation_after must be nonnegative and operation_limit must be 1..={MAX_INSPECT_OPERATIONS}"
        )));
    }
    let operations = operation_page(db, &attempt.attempt_id, operation_cursor)?;
    let check_cursor = PageRequest {
        after: optional_i64(params_value, "check_after", 0)?,
        limit: optional_i64(params_value, "check_limit", 10)?,
    };
    if check_cursor.after < 0 || !(1..=MAX_INSPECT_CHECKS).contains(&check_cursor.limit) {
        return Err(Error::invalid(format!(
            "check_after must be nonnegative and check_limit must be 1..={MAX_INSPECT_CHECKS}"
        )));
    }
    let check_runs = check_page(db, &attempt.attempt_id, check_cursor)?;
    let scope = scope_projection(spec.as_ref());
    let own_initial_paths = spec
        .as_ref()
        .and_then(|spec| spec.scope.as_ref())
        .map(|scope| scope.initial_paths.as_slice())
        .unwrap_or_default();
    let overlap_page = PageRequest {
        after: optional_i64(params_value, "overlap_after", 0)?,
        limit: optional_i64(params_value, "overlap_limit", 10)?,
    };
    if overlap_page.after < 0 || !(1..=MAX_INSPECT_OVERLAPS).contains(&overlap_page.limit) {
        return Err(Error::invalid(format!(
            "overlap_after must be nonnegative and overlap_limit must be 1..={MAX_INSPECT_OVERLAPS}"
        )));
    }
    let overlaps = current_scope_overlaps(
        db,
        Some(&attempt.attempt_id),
        &task_project(db, &attempt.task_id)?,
        own_initial_paths,
        overlap_page,
    )?;
    let peer_page = launcher::PageRequest {
        after: 0,
        limit: optional_i64(params_value, "peer_limit", 8)?,
    };
    if !(1..=20).contains(&peer_page.limit) {
        return Err(Error::invalid("peer_limit must be 1..=20"));
    }
    let peer_after = validate_filter_text(
        params_value
            .get("peer_after_client_id")
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| Error::invalid("peer_after_client_id must be a string"))
            })
            .transpose()?,
        "peer_after_client_id",
    )?;
    let peers = if current_attempt && task_revision_current {
        super::coordination::list_scope_participants(
            db,
            p,
            &attempt.task_id,
            attempt.task_revision,
            &attempt.attempt_id,
            peer_page.limit,
            peer_after.as_deref(),
        )?
    } else {
        json!({
            "items":[],
            "task_id":attempt.task_id,
            "task_revision":attempt.task_revision,
            "attempt_id":attempt.attempt_id,
            "next_after":null,
            "coverage":"not_current",
            "gaps":["participant_index_not_projected_for_stale_attempt_or_task_revision"],
        })
    };
    let mut attention_items = Vec::new();
    if attempt.state == "submitted" {
        attention_items.push(json!({
            "kind":"manager_decision_pending",
            "source":"current_attempt_state",
            "task_id":attempt.task_id,
            "attempt_id":attempt.attempt_id,
        }));
    }
    if attempt.state == "recovery_pending" {
        attention_items.push(json!({
            "kind":"runtime_recovery_pending",
            "source":"current_attempt_state",
            "task_id":attempt.task_id,
            "attempt_id":attempt.attempt_id,
        }));
    }
    if operation_counts["outcome_unknown"].as_i64().unwrap_or(0) > 0 {
        attention_items.push(json!({
            "kind":"external_operation_outcome_unknown",
            "source":"exact_attempt_operation_state_counts",
            "count":operation_counts["outcome_unknown"],
            "task_id":attempt.task_id,
            "attempt_id":attempt.attempt_id,
        }));
    }
    let attention = json!({
        "items":attention_items,
        "attempt_state":attempt.state,
        "unresolved_operations":operation_counts["unresolved"],
        "outcome_unknown_operations":operation_counts["outcome_unknown"],
        "binding_scope_attention":"other binding attention remains available through swarm.exceptions.get",
        // Missing and explicit-null values both mean the exact attention view is unknown.
        "binding_manager_attention":binding
            .get("manager_attention")
            .cloned()
            .filter(|value| !value.is_null())
            .unwrap_or_else(|| json!({
            "status":"no_exact_binding",
            "coverage_complete":false,
            "items":[],
            "requery":{"method":"swarm.exceptions.get","params":{"after":0,"limit":launcher::MAX_PAGE_SIZE}},
        })),
    });
    let mut gaps = vec![
        "workspace_write_lease_and_current_git_state_not_recorded",
        "runtime_capability_receipt_not_recorded",
        "launch_plan_and_route_live_qualification_not_recorded",
    ];
    if !task_revision_current {
        gaps.push("attempt_snapshot_revision_differs_from_current_task_revision");
    }
    if brief["status"] != "included" {
        gaps.push("exact_retained_source_brief_not_inline; inspect attempt.get by attempt ID");
    }
    if scope["status"] == "not_recorded" {
        gaps.push("work_scope_not_recorded_on_attempt_snapshot");
    } else if scope["status"] != "recorded" {
        gaps.push("one_or_more_scope_paths_were_redacted_or_omitted");
    }
    if peers["coverage"].as_str() != Some("complete") {
        gaps.push("current_scope_participant_index_coverage_is_partial");
    }
    if overlaps["coverage"] != "complete" {
        gaps.push("git_and_nonliteral_scope_overlap_are_not_covered");
    }
    gaps.push("other_binding_scope_attention_uses_swarm.exceptions.get");
    gaps.push("relevance_ranked_peers_contracts_and_integration_cells_not_projected");
    let identity = json!({
        "task_id":attempt.task_id,
        "task_revision":attempt.task_revision,
        "current_task_revision":current_revision,
        "attempt_id":attempt.attempt_id,
        "attempt_created_at_ms":attempt.created_at_ms,
        "assignment_owner":owner_profile(db,&attempt.owner_id)?,
        "start_owner":attempt.start_owner,
        "start_operation_id":attempt.start_operation_id,
        "attempt_state":attempt.state,
        "released_at_ms":attempt.released_at_ms,
        "is_current_attempt":current_attempt,
        "task_revision_current":task_revision_current,
        "task_state":task_state,
        "accepted_attempt_id":accepted_attempt_id,
        "accepted_operation_id":accepted_operation_id,
    });
    let response = json!({
        "status":if current_attempt && task_revision_current {"current"} else {"stale"},
        "generated_at_ms":model::now_ms()?,
        "identity":identity.clone(),
        "work":{
            "task_brief":brief,
            "work_scope":scope,
            "dependency_acceptances_retained":retained_dependency_acceptances,
            "dependencies_current":dependencies,
            "owner_policy":retained_owner_policy,
            "producer_facts":producer_facts,
            "submission_ref":attempt.submission_ref,
            "candidate_ref":attempt.candidate_ref,
        },
        "runtime":{
            "binding":binding,
            "operations":operations,
            "operation_state_counts":operation_counts,
            "checks":check_runs,
            "capacity":capacity,
            "route_admission":exact_attempt_admission(db, Some(&attempt), config)?,
            "attention":attention,
        },
        "coordination":{
            "current_scope_participants":peers,
            "relevant_peers":{"status":"not_ranked_without_a_specific_relevance_selector","items":[]},
            "exact_scope_overlap":overlaps,
            "contracts":{"status":"not_recorded_in_task_assignment_store","items":[]},
            "integration_cells":{"status":"not_recorded","items":[]},
        },
        "coverage":if gaps.is_empty() {"complete"} else {"partial"},
        "gaps":gaps,
    });
    let canonical = model::canonical(&response)?;
    if canonical.len() <= projection::MAX_SERIALIZED_BYTES {
        Ok(response)
    } else {
        Ok(json!({
            "status":"partial",
            "identity":identity,
            "projection":{
                "status":"detached",
                "serialized_bytes":canonical.len(),
                "digest":model::digest(canonical.as_bytes()),
                "reference":{"kind":"exact_attempt_inspection","attempt_id":attempt.attempt_id},
            },
            "requery":{
                "method":"swarm.agent.inspect",
                "params":{
                    "attempt_id":attempt.attempt_id,
                    "operation_limit":1,
                    "check_limit":1,
                    "peer_limit":1,
                    "overlap_limit":1,
                },
            },
            "gaps":["combined_inspect_projection_exceeded_serialized_page_budget; requery with smaller detail pages"],
        }))
    }
}

fn task_project(db: &Connection, task_id: &str) -> Result<String> {
    db.query_row(
        "SELECT project_id FROM tasks WHERE task_id=?1",
        [task_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

/// Manager exceptions are the `manager_actionable` subset of the existing
/// attention projection. Filtering happens before the returned page limit;
/// the cursor remains the raw attention offset so filtered rows are not lost.
pub(super) fn exceptions_get(
    db: &Connection,
    p: &Principal,
    params_value: &Value,
) -> Result<Value> {
    require_manager(db, p)?;
    manager_exceptions_page(
        db,
        params_value,
        super::gm::require_authority(db, p).is_ok(),
    )
}
