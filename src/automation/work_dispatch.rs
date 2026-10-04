//! Typed launch settings and non-serializable manager provenance for the first
//! automated WorkDispatch consumer.
//!
//! This module prepares the exact existing launcher request. It does not
//! synthesize a Principal or authorize/start a native runtime. The Store's
//! launcher integration must consume `WorkDispatchContext` through its real
//! on-behalf actor and recheck it at effect start.

pub(crate) use super::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID;
use super::{actions::AutomationStep, authorization, config};
use crate::{
    error::{Error, Result},
    launcher::{LaunchPreviewRequest, LaunchRequest},
    model,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const MAX_TASK_ID_BYTES: usize = 512;

type CurrentAttemptRow = (
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
);
type LaunchParentRow = (
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
type LaunchOpenChildRow = (
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
);
type LaunchClaimChildRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
);
type OpeningBindingRow = (
    String,
    String,
    String,
    String,
    String,
    Option<i64>,
    Option<String>,
    Option<String>,
);
type OpeningLeaseRow = (
    String,
    i64,
    String,
    String,
    i64,
    String,
    String,
    String,
    Option<String>,
    i64,
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
);

/// Explicit per-entry values consumed by the existing digest-bound launcher
/// contract. There are deliberately no route, model, profile, budget, or stop
/// condition defaults here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkDispatchLaunchSettings {
    pub(crate) route: String,
    pub(crate) agent_profile: String,
    pub(crate) mcp_profile: String,
    pub(crate) mcp_surface: String,
    pub(crate) workspace_policy: String,
    pub(crate) requested_model: Option<String>,
    pub(crate) requested_effort: Option<String>,
    pub(crate) budget: Value,
    pub(crate) stop_conditions: Vec<String>,
    pub(crate) purpose: String,
}

impl WorkDispatchLaunchSettings {
    /// Parse the launch-settings subset by running it through the canonical
    /// LaunchPreviewRequest parser. Nullable fields remain explicit, as they
    /// are in the public launch contract.
    pub(crate) fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &[
                "route",
                "agent_profile",
                "mcp_profile",
                "mcp_surface",
                "workspace_policy",
                "requested_model",
                "requested_effort",
                "budget",
                "stop_conditions",
                "purpose",
            ],
        )?;
        let mut preview = value
            .as_object()
            .cloned()
            .ok_or_else(|| Error::invalid("work_dispatch settings must be an object"))?;
        preview.insert("task_id".to_owned(), json!("launch-settings-validation"));
        preview.insert("expected_task_revision".to_owned(), json!(1));
        let parsed = LaunchPreviewRequest::parse(&Value::Object(preview))?;
        Ok(Self {
            route: parsed.route,
            agent_profile: parsed.agent_profile,
            mcp_profile: parsed.mcp_profile,
            mcp_surface: parsed.mcp_surface,
            workspace_policy: parsed.workspace_policy,
            requested_model: parsed.requested_model,
            requested_effort: parsed.requested_effort,
            budget: parsed.budget,
            stop_conditions: parsed.stop_conditions,
            purpose: parsed.purpose,
        })
    }

    pub(crate) fn preview_params(&self, task_id: &str, task_revision: i64) -> Value {
        json!({
            "task_id":task_id,
            "expected_task_revision":task_revision,
            "route":self.route,
            "agent_profile":self.agent_profile,
            "mcp_profile":self.mcp_profile,
            "mcp_surface":self.mcp_surface,
            "workspace_policy":self.workspace_policy,
            "requested_model":self.requested_model,
            "requested_effort":self.requested_effort,
            "budget":self.budget,
            "stop_conditions":self.stop_conditions,
            "purpose":self.purpose,
        })
    }

    /// Snapshot the exact canonical launch choices retained by an admitted
    /// Operation so later checks can distinguish display-only entry revisions
    /// from changed action parameters.
    pub(crate) fn from_preview(preview: &LaunchPreviewRequest) -> Self {
        Self {
            route: preview.route.clone(),
            agent_profile: preview.agent_profile.clone(),
            mcp_profile: preview.mcp_profile.clone(),
            mcp_surface: preview.mcp_surface.clone(),
            workspace_policy: preview.workspace_policy.clone(),
            requested_model: preview.requested_model.clone(),
            requested_effort: preview.requested_effort.clone(),
            budget: preview.budget.clone(),
            stop_conditions: preview.stop_conditions.clone(),
            purpose: preview.purpose.clone(),
        }
    }
}

/// A current Task assignment that is eligible for one launch attempt. Its
/// fields have no public deserializer and are only populated from Store rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkDispatchSubject {
    task_id: String,
    project_id: String,
    task_revision: i64,
    attempt_id: Option<String>,
}

/// Verified identity of the committed Task fact that selected this launch.
/// It records provenance only; slot identity remains based on the exact Task
/// revision and its initial or existing Attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkDispatchSource {
    observation_id: i64,
    event_kind: String,
    operation_id: String,
}

impl WorkDispatchSource {
    pub(crate) fn from_committed_fact(
        observation_id: i64,
        event_kind: &str,
        operation_id: &str,
    ) -> Result<Self> {
        if observation_id <= 0
            || !matches!(event_kind, "task.create" | "task.revise" | "task.claim")
            || operation_id.is_empty()
            || operation_id.len() > 256
            || operation_id.chars().any(char::is_control)
        {
            return Err(Error::new(
                "AUTOMATION_WORK_SOURCE_GAP",
                "committed Task fact source identity is invalid",
            ));
        }
        Ok(Self {
            observation_id,
            event_kind: event_kind.to_owned(),
            operation_id: operation_id.to_owned(),
        })
    }

    pub(crate) fn event_kind(&self) -> &str {
        &self.event_kind
    }

    pub(crate) fn operation_id(&self) -> &str {
        &self.operation_id
    }
}

/// Typed evidence for the manager whose enabled automation selected this
/// exact existing assignment. This is evidence for shared authorization, not
/// a credential or Principal replacement.
#[derive(Debug, Clone)]
pub(crate) struct WorkDispatchContext {
    operation_id: Option<String>,
    technical_requester_id: String,
    effective_manager_id: String,
    automation_id: String,
    automation_revision: i64,
    project_id: String,
    launch_settings: WorkDispatchLaunchSettings,
    subject: WorkDispatchSubject,
    source: WorkDispatchSource,
    semantic_slot_id: String,
}

impl WorkDispatchContext {
    /// Reconstruct the non-serializable actor context only from a Store reader
    /// that has verified the immutable Operation link and committed source
    /// fact. Request JSON must never call this constructor.
    // The separate IDs and revisions are the exact persisted authority tuple.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_validated_operation_link(
        db: &Connection,
        operation_id: &str,
        effective_manager_id: &str,
        automation_id: &str,
        automation_revision: i64,
        project_id: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: Option<&str>,
        launch_settings: WorkDispatchLaunchSettings,
        source: WorkDispatchSource,
        expected_slot_id: &str,
    ) -> Result<Self> {
        let entry = config::load_entry(db, effective_manager_id, project_id, automation_id)?
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "linked automation entry is missing",
                )
            })?;
        config::validate_entry(&entry)?;
        if entry.owner_manager_id != effective_manager_id || entry.project_id != project_id {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "linked automation identity does not match its stored owner and project",
            ));
        }
        validate_task_id(task_id)?;
        if task_revision <= 0 || automation_revision > entry.revision || project_id.is_empty() {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "linked Task, automation, or project revision is invalid",
            ));
        }
        if let Some(attempt_id) = attempt_id {
            validate_task_id(attempt_id).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "linked Attempt identity is invalid",
                )
            })?;
        }
        let subject = WorkDispatchSubject {
            task_id: task_id.to_owned(),
            project_id: project_id.to_owned(),
            task_revision,
            attempt_id: attempt_id.map(str::to_owned),
        };
        let semantic_slot_id = subject.semantic_slot_id(effective_manager_id)?;
        if automation_revision <= 0 || semantic_slot_id != expected_slot_id {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "linked WorkDispatch slot or automation revision is invalid",
            ));
        }
        Ok(Self {
            operation_id: Some(operation_id.to_owned()),
            technical_requester_id: AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: effective_manager_id.to_owned(),
            automation_id: automation_id.to_owned(),
            automation_revision,
            project_id: project_id.to_owned(),
            launch_settings,
            subject,
            source,
            semantic_slot_id,
        })
    }

    pub(crate) fn from_current_assignment(
        db: &Connection,
        entry: &config::AutomationEntry,
        task_id: &str,
        expected_revision: i64,
        attempt_id: Option<&str>,
        source: WorkDispatchSource,
    ) -> Result<Self> {
        validate_entry_action(db, entry)?;
        validate_task_id(task_id)?;
        if let Some(attempt_id) = attempt_id {
            validate_task_id(attempt_id)?;
        }

        let task: Option<(String, i64, String)> = db
            .query_row(
                "SELECT project_id,revision,state FROM tasks WHERE task_id=?1",
                [task_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((project_id, task_revision, task_state)) = task else {
            return Err(Error::new("NOT_FOUND", "work-dispatch Task was not found"));
        };
        if project_id != entry.project_id
            || task_state != "open"
            || task_revision <= 0
            || task_revision != expected_revision
        {
            return Err(Error::new(
                "AUTOMATION_WORK_SUBJECT_STALE",
                "work-dispatch Task is outside the entry project or is not open at the exact observed revision",
            ));
        }

        let mut active = db.prepare(
            "SELECT attempt_id FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL ORDER BY attempt_id LIMIT 2",
        )?;
        let active_ids = active
            .query_map([task_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let exact_active = match attempt_id {
            Some(attempt_id) => {
                active_ids.len() == 1 && active_ids.first().map(String::as_str) == Some(attempt_id)
            }
            None => active_ids.is_empty(),
        };
        if !exact_active {
            return Err(Error::new(
                "AUTOMATION_WORK_ASSIGNMENT_STALE",
                "work dispatch requires the exact observed unassigned Task or sole unreleased Attempt",
            ));
        }
        drop(active);

        if let Some(attempt_id) = attempt_id {
            let attempt: Option<CurrentAttemptRow> = db
                .query_row(
                    "SELECT task_revision,owner_id,state,start_operation_id,binding_id,binding_generation \
                     FROM attempts WHERE attempt_id=?1 AND task_id=?2 AND released_at_ms IS NULL",
                    params![attempt_id, task_id],
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
            let Some((
                attempt_revision,
                owner,
                state,
                start_operation,
                binding_id,
                binding_generation,
            )) = attempt
            else {
                return Err(Error::new(
                    "AUTOMATION_WORK_ASSIGNMENT_STALE",
                    "exact unreleased Task assignment disappeared",
                ));
            };
            if attempt_revision != task_revision
                || owner != entry.owner_manager_id
                || state != "reserved"
                || start_operation.is_some()
                || binding_id.is_some()
                || binding_generation.is_some()
            {
                return Err(Error::new(
                    "AUTOMATION_WORK_NOT_READY",
                    "existing work dispatch currently supports only the manager's current reserved, unstarted, unbound Attempt",
                ));
            }
        }

        let subject = WorkDispatchSubject {
            task_id: task_id.to_owned(),
            project_id: project_id.clone(),
            task_revision,
            attempt_id: attempt_id.map(str::to_owned),
        };
        let semantic_slot_id = subject.semantic_slot_id(&entry.owner_manager_id)?;
        let launch_settings = entry.work_dispatch.clone().ok_or_else(|| {
            Error::new(
                "AUTOMATION_ACTION_UNAVAILABLE",
                "WorkDispatch requires explicit launch settings",
            )
        })?;
        Ok(Self {
            operation_id: None,
            technical_requester_id: AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: entry.owner_manager_id.clone(),
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            project_id,
            launch_settings,
            subject,
            source,
            semantic_slot_id,
        })
    }

    /// Recheck current enabled-entry and exact assignment evidence immediately
    /// before the shared launcher reserves or starts the effect.
    pub(crate) fn require_current(&self, db: &Connection) -> Result<()> {
        let entry = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
        if entry.revision < self.automation_revision {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current automation revision predates the retained WorkDispatch admission",
            ));
        }
        self.require_current_entry(db, self.operation_id.as_deref())?;
        let current = Self::from_current_assignment(
            db,
            &entry,
            &self.subject.task_id,
            self.subject.task_revision,
            self.subject.attempt_id.as_deref(),
            self.source.clone(),
        )?;
        if current.subject != self.subject
            || current.semantic_slot_id != self.semantic_slot_id
            || current.technical_requester_id != self.technical_requester_id
            || current.launch_settings != self.launch_settings
        {
            return Err(Error::new(
                "AUTOMATION_WORK_ASSIGNMENT_STALE",
                "Task assignment changed after work-dispatch planning",
            ));
        }
        Ok(())
    }

    /// Check the ordinary manager identity and exact Task object binding. The
    /// launch actor still performs its normal current rights/policy checks.
    pub(crate) fn require_action_object(
        &self,
        db: &Connection,
        action: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: Option<&str>,
    ) -> Result<()> {
        if action != "swarm.launch"
            || task_id != self.subject.task_id
            || task_revision != self.subject.task_revision
            || attempt_id != self.subject.attempt_id.as_deref()
        {
            return Err(Error::new(
                "FORBIDDEN",
                "on-behalf launch is outside its retained Task assignment",
            ));
        }
        if let Some(operation_id) = self.operation_id.as_deref()
            && let Some(authority) = authorization::current_transfer_workspace_readback_authority(
                db,
                operation_id,
                &self.subject.task_id,
            )?
        {
            if !authority.matches_work_dispatch(
                operation_id,
                &self.project_id,
                &self.subject.task_id,
            ) {
                return Err(Error::new(
                    "FORBIDDEN",
                    "workspace readback authority differs from the retained WorkDispatch launch",
                ));
            }
            return Ok(());
        }
        self.require_current(db)
    }

    pub(crate) fn require_workspace_readback_authority(
        &self,
        db: &Connection,
        operation_id: &str,
    ) -> Result<authorization::TransferReadbackAuthority> {
        if self.operation_id.as_deref() != Some(operation_id) {
            return Err(Error::new(
                "FORBIDDEN",
                "workspace readback is outside the retained WorkDispatch Operation",
            ));
        }
        let authority = authorization::current_transfer_workspace_readback_authority(
            db,
            operation_id,
            &self.subject.task_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "FORBIDDEN",
                "no current-GM readback authority exists for this unknown workspace effect",
            )
        })?;
        if !authority.matches_work_dispatch(operation_id, &self.project_id, &self.subject.task_id) {
            return Err(Error::new(
                "FORBIDDEN",
                "workspace readback authority differs from the retained WorkDispatch subject",
            ));
        }
        Ok(authority)
    }

    /// Distinguish the exact transferred unknown-workspace readback from a
    /// queued initial launch, which still requires the retained manager to be
    /// the designated GM.
    pub(crate) fn is_workspace_readback_authorized(&self, db: &Connection) -> Result<bool> {
        let Some(operation_id) = self.operation_id.as_deref() else {
            return Ok(false);
        };
        let Some(authority) = authorization::current_transfer_workspace_readback_authority(
            db,
            operation_id,
            &self.subject.task_id,
        )?
        else {
            return Ok(false);
        };
        if !authority.matches_work_dispatch(operation_id, &self.project_id, &self.subject.task_id) {
            return Err(Error::new(
                "FORBIDDEN",
                "workspace readback authority differs from the retained WorkDispatch subject",
            ));
        }
        Ok(true)
    }

    /// Revalidate the Task Attempt created by the existing launch flow after
    /// an initial unclaimed Task was claimed behind its held workspace lease.
    /// The operation linkage and launcher remain responsible for proving that
    /// this exact claim belongs to this launch Operation before calling here.
    pub(crate) fn require_claimed_launch_attempt(
        &self,
        db: &Connection,
        operation_id: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
    ) -> Result<()> {
        if task_id != self.subject.task_id
            || task_revision != self.subject.task_revision
            || self
                .subject
                .attempt_id
                .as_deref()
                .is_some_and(|expected| expected != attempt_id)
        {
            return Err(Error::new(
                "FORBIDDEN",
                "launch Attempt is outside the retained WorkDispatch subject",
            ));
        }
        self.require_current_entry(db, Some(operation_id))?;

        let operation: Option<(String, Option<String>, Option<String>)> = db
            .query_row(
                "SELECT method,task_id,attempt_id FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if !operation.is_some_and(|(method, operation_task, operation_attempt)| {
            method == "swarm.launch"
                && operation_task.as_deref() == Some(task_id)
                && operation_attempt.as_deref() == Some(attempt_id)
        }) {
            return Err(Error::new(
                "FORBIDDEN",
                "launch Operation is not bound to the exact post-claim Attempt",
            ));
        }
        let task: Option<(String, i64, String)> = db
            .query_row(
                "SELECT project_id,revision,state FROM tasks WHERE task_id=?1",
                [task_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if !task.is_some_and(|(project, revision, state)| {
            project == self.project_id && revision == task_revision && state == "open"
        }) {
            return Err(Error::new(
                "AUTOMATION_WORK_SUBJECT_STALE",
                "Task changed after the on-behalf launch claimed its initial Attempt",
            ));
        }
        let current_attempt: Option<String> = db
            .query_row(
                "SELECT attempt_id FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL \
                 ORDER BY created_at_ms DESC,attempt_id DESC LIMIT 1",
                [task_id],
                |row| row.get(0),
            )
            .optional()?;
        let active_count: i64 = db.query_row(
            "SELECT count(*) FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL",
            [task_id],
            |row| row.get(0),
        )?;
        let attempt_is_current = active_count == 1
            && current_attempt.as_deref() == Some(attempt_id)
            && db.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
                 AND task_revision=?3 AND owner_id=?4 AND state='reserved' \
                 AND released_at_ms IS NULL AND start_operation_id IS NULL \
                 AND binding_id IS NULL AND binding_generation IS NULL)",
                params![
                    attempt_id,
                    task_id,
                    task_revision,
                    self.effective_manager_id
                ],
                |row| row.get::<_, bool>(0),
            )?;
        if !attempt_is_current {
            return Err(Error::new(
                "AUTOMATION_WORK_ASSIGNMENT_STALE",
                "the exact initial launch Attempt is no longer current and unstarted",
            ));
        }
        Ok(())
    }

    /// Verify the exact pre-ready launch opening before an `agent.open` child
    /// is advanced. This stage accepts only the queued opening child and its
    /// unreleased `opening` binding; ready credential/MCP stages use the
    /// separate bound-launch validator below.
    // These fields are the independently checked identities of one retained launch stage.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_opening_launch_attempt(
        &self,
        db: &Connection,
        operation_id: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
        binding_id: &str,
        binding_generation: i64,
    ) -> Result<()> {
        if task_id != self.subject.task_id
            || task_revision != self.subject.task_revision
            || self
                .subject
                .attempt_id
                .as_deref()
                .is_some_and(|expected| expected != attempt_id)
            || operation_id.is_empty()
            || binding_id.is_empty()
            || binding_generation <= 0
        {
            return Err(Error::new(
                "FORBIDDEN",
                "opening launch Attempt is outside the retained WorkDispatch subject",
            ));
        }
        self.require_current_entry(db, Some(operation_id))?;

        let link = crate::store::automation_work_dispatch::operation_link(db, operation_id)?
            .ok_or_else(|| Error::new("FORBIDDEN", "launch has no retained WorkDispatch link"))?;
        let expected_source = self.linkage_value()["source"].clone();
        if link.technical_requester_id != self.technical_requester_id
            || link.effective_manager_id != self.effective_manager_id
            || link.automation_id != self.automation_id
            || link.automation_revision != self.automation_revision
            || link.project_id != self.project_id
            || link.action != "swarm.launch"
            || link.semantic_slot_id != self.semantic_slot_id
            || link.task_id != task_id
            || link.task_revision != task_revision
            || link.attempt_id != self.subject.attempt_id
            || link.source != expected_source
        {
            return Err(Error::new(
                "FORBIDDEN",
                "retained WorkDispatch link does not match the opening launch actor context",
            ));
        }

        let parent: Option<LaunchParentRow> = db
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
            original_request_json,
            effective_request_json,
        )) = parent
        else {
            return Err(Error::new("NOT_FOUND", "launch Operation was not found"));
        };
        if caller != self.technical_requester_id
            || method != "swarm.launch"
            || state != "queued"
            || operation_task.as_deref() != Some(task_id)
            || operation_attempt.as_deref() != Some(attempt_id)
            || operation_binding.as_deref() != Some(binding_id)
            || operation_generation != Some(binding_generation)
        {
            return Err(Error::new(
                "FORBIDDEN",
                "launch Operation is not bound to the exact queued opening Attempt and binding",
            ));
        }
        let original_request: Value = serde_json::from_str(&original_request_json)
            .map_err(|_| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch request is invalid"))?;
        let request = LaunchRequest::parse(&original_request)?;
        if request.client_request_id != format!("automation-work-{}", self.semantic_slot_id)
            || request.preview.task_id != task_id
            || request.preview.expected_task_revision != task_revision
            || WorkDispatchLaunchSettings::from_preview(&request.preview) != self.launch_settings
        {
            return Err(Error::new(
                "FORBIDDEN",
                "launch request differs from the retained WorkDispatch parameters",
            ));
        }

        let effective: Value = serde_json::from_str(&effective_request_json)
            .map_err(|_| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch manifest is invalid"))?;
        let manifest = effective
            .get("launch_manifest")
            .ok_or_else(|| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch manifest is missing"))?;
        let open_operation_id = manifest["binding"]["operation_id"]
            .as_str()
            .filter(|value| {
                !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
            })
            .ok_or_else(|| Error::new("LAUNCH_MANIFEST_CORRUPT", "open child is missing"))?;
        if manifest["manifest_version"] != "eliot-launch-manifest-v1"
            || manifest["state"] != "awaiting_binding"
            || manifest["plan_digest"] != request.plan_digest
            || manifest["task"]["task_id"] != task_id
            || manifest["task"]["project_id"] != self.project_id
            || manifest["task"]["expected_revision"] != task_revision
            || manifest["task"]["observed_revision"] != task_revision
            || manifest["task"]["attempt_id"] != attempt_id
            || manifest["attempt"]["state"] != "reserved"
            || manifest["attempt"]["action"] != manifest["task"]["attempt_action"]
            || manifest["workspace"]["lease_state"] != "held"
            || manifest["workspace"]["dirty_state"] != "clean_verified"
            || manifest["workspace"]["filesystem_inspected"] != true
            || manifest["progress"]["binding_open"] != "queued"
            || manifest["progress"]["task_dispatch"] != "not_started"
            || manifest["runtime"]["state"] != "opening"
            || manifest["binding"]["binding_id"] != binding_id
            || manifest["binding"]["generation"] != binding_generation
            || manifest["binding"]["state"] != "queued"
        {
            return Err(Error::new(
                "LAUNCH_MANIFEST_CORRUPT",
                "launch manifest is not in the exact queued opening phase",
            ));
        }

        let attempt_action = if self.subject.attempt_id.is_some() {
            "use_existing"
        } else {
            "claim_new"
        };
        if manifest["task"]["attempt_action"] != attempt_action
            || manifest["attempt"]["claim_operation_id"]
                != if attempt_action == "claim_new" {
                    json!(format!("launch:{operation_id}:claim"))
                } else {
                    Value::Null
                }
        {
            return Err(Error::new(
                "LAUNCH_MANIFEST_CORRUPT",
                "launch Attempt action differs from the retained WorkDispatch subject",
            ));
        }
        if self.subject.attempt_id.is_none() {
            self.require_launch_claim_child(db, operation_id, task_id, task_revision, attempt_id)?;
        }

        let lease: crate::workspace::LeaseAuthorityRef = serde_json::from_value(
            manifest["workspace"]["lease_authority"].clone(),
        )
        .map_err(|_| {
            Error::new(
                "LAUNCH_MANIFEST_CORRUPT",
                "launch held-lease authority reference is invalid",
            )
        })?;
        let lease_view = &manifest["workspace"]["lease"];
        if lease.state != "held"
            || lease.lease_id.is_empty()
            || lease.generation <= 0
            || lease.registration_generation <= 0
            || lease.operation_id != operation_id
            || lease.project_id != self.project_id
            || lease.task_id != task_id
            || lease.task_revision != task_revision
            || lease.owner_client_id != self.effective_manager_id
            || lease.attempt_id.as_deref() != Some(attempt_id)
            || lease.plan_digest != request.plan_digest
            || !crate::forge::valid_object_id(&lease.baseline_commit)
            || lease.binding_digest.is_empty()
            || lease_view["lease_id"] != lease.lease_id
            || lease_view["registration_id"] != lease.registration_id
            || lease_view["registration_generation"] != lease.registration_generation
            || lease_view["project_id"] != lease.project_id
            || lease_view["task_id"] != lease.task_id
            || lease_view["task_revision"] != lease.task_revision
            || lease_view["operation_id"] != lease.operation_id
            || lease_view["plan_digest"] != lease.plan_digest
            || lease_view["owner_client_id"] != lease.owner_client_id
            || lease_view["attempt_id"] != attempt_id
            || lease_view["generation"] != lease.generation
            || lease_view["baseline_commit"] != lease.baseline_commit
            || lease_view["branch_ref"] != lease.branch_ref
            || lease_view["worktree_handle"] != lease.worktree_handle
            || lease_view["binding_digest"] != lease.binding_digest
            || lease_view["state"] != "held"
        {
            return Err(Error::new(
                "WORKSPACE_LEASE_STALE",
                "launch manifest does not retain the exact held lease for this Attempt",
            ));
        }
        let lease_row: Option<OpeningLeaseRow> = db
            .query_row(
                "SELECT l.registration_id,l.registration_generation,l.project_id,l.task_id,\
                 l.task_revision,l.operation_id,l.plan_digest,l.owner_client_id,l.attempt_id,\
                 l.generation,l.baseline_commit,l.branch_ref,l.worktree_handle,l.binding_digest,\
                 l.state,r.state,r.generation FROM workspace_leases AS l \
                 JOIN workspace_registrations AS r ON r.registration_id=l.registration_id \
                 WHERE l.lease_id=?1",
                [&lease.lease_id],
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
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                        row.get(16)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            registration_id,
            registration_generation,
            lease_project,
            lease_task,
            lease_task_revision,
            lease_operation,
            lease_plan_digest,
            lease_owner,
            lease_attempt,
            lease_generation,
            lease_baseline,
            lease_branch,
            lease_worktree,
            lease_binding_digest,
            lease_state,
            registration_state,
            current_registration_generation,
        )) = lease_row
        else {
            return Err(Error::new(
                "WORKSPACE_LEASE_STALE",
                "launch held workspace lease is missing its current registration",
            ));
        };
        if registration_id != lease.registration_id
            || registration_generation != lease.registration_generation
            || lease_project != lease.project_id
            || lease_task != lease.task_id
            || lease_task_revision != lease.task_revision
            || lease_operation != lease.operation_id
            || lease_plan_digest != lease.plan_digest
            || lease_owner != lease.owner_client_id
            || lease_attempt != lease.attempt_id
            || lease_generation != lease.generation
            || lease_baseline != lease.baseline_commit
            || lease_branch != lease.branch_ref
            || lease_worktree != lease.worktree_handle
            || lease_binding_digest != lease.binding_digest
            || lease_state != "held"
            || registration_state != "active"
            || current_registration_generation != registration_generation
        {
            return Err(Error::new(
                "WORKSPACE_LEASE_STALE",
                "persisted held lease or active registration differs from the launch manifest",
            ));
        }
        let held_lease_count: i64 = db.query_row(
            "SELECT count(*) FROM workspace_leases WHERE operation_id=?1 AND state='held'",
            [operation_id],
            |row| row.get(0),
        )?;
        if held_lease_count != 1 {
            return Err(Error::new(
                "WORKSPACE_LEASE_AMBIGUOUS",
                "launch Operation does not have exactly one held workspace lease",
            ));
        }

        let child: Option<LaunchOpenChildRow> = db
            .query_row(
                "SELECT caller_id,method,client_request_id,prerequisite_operation_id,task_id,\
                 attempt_id,binding_id,binding_generation,state,original_request_json,effective_request_json \
                 FROM operations WHERE operation_id=?1",
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
        )) = child
        else {
            return Err(Error::new(
                "LAUNCH_OPEN_MISSING",
                "launch open child is missing",
            ));
        };
        let expected_child_request = json!({
            "client_request_id":format!("launch:{operation_id}:open"),
            "lane_id":format!("launch-{}", lease.lease_id),
            "route":request.preview.route,
        });
        let child_original: Value = serde_json::from_str(&child_original_json)
            .map_err(|_| Error::new("LAUNCH_OPEN_CORRUPT", "open child request is invalid"))?;
        if child_caller != self.technical_requester_id
            || child_method != "agent.open"
            || child_request_id != format!("launch:{operation_id}:open")
            || prerequisite.as_deref() != Some(operation_id)
            || child_task.as_deref() != Some(task_id)
            || child_attempt.as_deref() != Some(attempt_id)
            || child_binding.as_deref() != Some(binding_id)
            || child_generation != Some(binding_generation)
            || child_state != "queued"
            || model::canonical(&child_original)? != model::canonical(&expected_child_request)?
        {
            return Err(Error::new(
                "FORBIDDEN",
                "agent.open child is not the exact queued child of this launch and held lease",
            ));
        }
        let child_effective: Value = serde_json::from_str(&child_effective_json)
            .map_err(|_| Error::new("LAUNCH_OPEN_CORRUPT", "open child linkage is invalid"))?;
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
        let child_result_json: Option<String> = db
            .query_row(
                "SELECT result_json FROM operations WHERE operation_id=?1 AND method='agent.open'",
                [open_operation_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let child_result_json = child_result_json.ok_or_else(|| {
            Error::new(
                "LAUNCH_OPEN_CORRUPT",
                "queued open child has no retained receipt",
            )
        })?;
        let child_result: Value = serde_json::from_str(&child_result_json)
            .map_err(|_| Error::new("LAUNCH_OPEN_CORRUPT", "open child receipt is invalid"))?;
        if child_effective["operation_contract"]["parent_launch_operation_id"] != operation_id
            || child_effective["workspace_lease"]["lease_id"] != lease.lease_id
            || child_effective["workspace_lease"]["generation"] != lease.generation
            || child_effective["workspace_lease"]["binding_digest"] != lease.binding_digest
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
                "open child receipt does not bind the exact queued opening and held lease",
            ));
        }

        let opening_binding: Option<OpeningBindingRow> = db
            .query_row(
                "SELECT lane_id,module_instance_id,module_artifact_id,state,route_json,released_at_ms,\
                 native_root_id,native_scope_key \
                 FROM bindings WHERE binding_id=?1 AND generation=?2",
                params![binding_id, binding_generation],
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
                    ))
                },
            )
            .optional()?;
        let Some((
            lane_id,
            module_instance_id,
            module_artifact_id,
            binding_state,
            route_json,
            released,
            native_root_id,
            native_scope_key,
        )) = opening_binding
        else {
            return Err(Error::new(
                "BINDING_NOT_READY",
                "exact launch opening binding is missing",
            ));
        };
        let persisted_route: Value = serde_json::from_str(&route_json)
            .map_err(|_| Error::new("LAUNCH_OPEN_CORRUPT", "opening binding route is invalid"))?;
        if binding_state != "opening"
            || released.is_some()
            || native_root_id.is_some()
            || native_scope_key.is_some()
            || lane_id != format!("launch-{}", lease.lease_id)
            || module_instance_id != child_effective["module_instance_id"]
            || module_artifact_id != child_effective["route"]["module_artifact_id"]
            || model::canonical(&persisted_route)? != model::canonical(&child_effective["route"])?
        {
            return Err(Error::new(
                "BINDING_NOT_READY",
                "binding is not the unreleased opening bound to the exact launch lease",
            ));
        }

        let task: Option<(String, i64, String)> = db
            .query_row(
                "SELECT project_id,revision,state FROM tasks WHERE task_id=?1",
                [task_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if !task.is_some_and(|(project_id, revision, state)| {
            project_id == self.project_id && revision == task_revision && state == "open"
        }) {
            return Err(Error::new(
                "AUTOMATION_WORK_SUBJECT_STALE",
                "opening launch Task is no longer open at its retained revision",
            ));
        }
        let active_attempts: i64 = db.query_row(
            "SELECT count(*) FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL",
            [task_id],
            |row| row.get(0),
        )?;
        let exact_opening_attempt: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
             AND task_revision=?3 AND owner_id=?4 AND state='reserved' \
             AND released_at_ms IS NULL AND start_operation_id IS NULL \
             AND binding_id=?5 AND binding_generation=?6)",
            params![
                attempt_id,
                task_id,
                task_revision,
                self.effective_manager_id,
                binding_id,
                binding_generation,
            ],
            |row| row.get(0),
        )?;
        if active_attempts != 1 || !exact_opening_attempt {
            return Err(Error::new(
                "AUTOMATION_WORK_ASSIGNMENT_STALE",
                "opening Attempt is not the sole current reserved WorkDispatch assignment",
            ));
        }
        Ok(())
    }

    /// Verify the exact post-open launch chain before issuing scoped
    /// credentials or continuing the admitted launch. This accepts the
    /// operation's initial `attempt_id: None` link only when the canonical
    /// child claim and open records prove the one current bound Attempt.
    // Each argument names an independently checked identity in the admitted launch chain.
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
        self.require_bound_launch_attempt_at_dispatch_stage(
            db,
            operation_id,
            task_id,
            task_revision,
            attempt_id,
            binding_id,
            binding_generation,
            None,
        )
    }

    /// Revalidate a bound WorkDispatch launch for initial delivery. `None`
    /// requires the exact Attempt to remain unstarted; `Some` admits only the
    /// exact queued dispatch Operation already stored as its start pointer.
    // Each argument is an independent identity in the retained launch chain.
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
        require_new_work_enabled(db)?;
        let current_attempt_id: Option<String> = db
            .query_row(
                "SELECT current_attempt_id FROM tasks WHERE task_id=?1",
                [task_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        if current_attempt_id.as_deref() != Some(attempt_id) {
            return Err(Error::new(
                "AUTOMATION_WORK_ASSIGNMENT_STALE",
                "Task no longer points to this exact dispatch Attempt",
            ));
        }
        let entry = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
        if entry.revision < self.automation_revision {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current automation revision predates the retained WorkDispatch admission",
            ));
        }
        self.require_bound_launch_attempt_at_dispatch_stage(
            db,
            operation_id,
            task_id,
            task_revision,
            attempt_id,
            binding_id,
            binding_generation,
            dispatch_operation_id,
        )
    }

    // Preserve the strict bound/unstarted contract above for the older
    // credential/readiness stages; only the explicit dispatch path may pass a
    // queued task.dispatch Operation through this helper.
    #[allow(clippy::too_many_arguments)]
    fn require_bound_launch_attempt_at_dispatch_stage(
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
        if task_id != self.subject.task_id
            || task_revision != self.subject.task_revision
            || self
                .subject
                .attempt_id
                .as_deref()
                .is_some_and(|expected| expected != attempt_id)
            || operation_id.is_empty()
            || binding_id.is_empty()
            || binding_generation <= 0
            || dispatch_operation_id.is_some_and(|id| {
                id.is_empty() || id.len() > 256 || id.chars().any(char::is_control)
            })
        {
            return Err(Error::new(
                "FORBIDDEN",
                "bound launch Attempt is outside the retained WorkDispatch subject",
            ));
        }
        self.require_current_entry(db, Some(operation_id))?;

        let link = crate::store::automation_work_dispatch::operation_link(db, operation_id)?
            .ok_or_else(|| Error::new("FORBIDDEN", "launch has no retained WorkDispatch link"))?;
        let expected_source = self.linkage_value()["source"].clone();
        if link.technical_requester_id != self.technical_requester_id
            || link.effective_manager_id != self.effective_manager_id
            || link.automation_id != self.automation_id
            || link.automation_revision != self.automation_revision
            || link.project_id != self.project_id
            || link.action != "swarm.launch"
            || link.semantic_slot_id != self.semantic_slot_id
            || link.task_id != task_id
            || link.task_revision != task_revision
            || link.attempt_id != self.subject.attempt_id
            || link.source != expected_source
        {
            return Err(Error::new(
                "FORBIDDEN",
                "retained WorkDispatch link does not match the launch actor context",
            ));
        }

        let parent: Option<LaunchParentRow> = db
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
            original_request_json,
            effective_request_json,
        )) = parent
        else {
            return Err(Error::new("NOT_FOUND", "launch Operation was not found"));
        };
        if caller != self.technical_requester_id
            || method != "swarm.launch"
            || state != "queued"
            || operation_task.as_deref() != Some(task_id)
            || operation_attempt.as_deref() != Some(attempt_id)
            || operation_binding.as_deref() != Some(binding_id)
            || operation_generation != Some(binding_generation)
        {
            return Err(Error::new(
                "FORBIDDEN",
                "launch Operation is not bound to the exact queued Task, Attempt, and binding",
            ));
        }
        let original_request: Value = serde_json::from_str(&original_request_json)
            .map_err(|_| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch request is invalid"))?;
        let request = LaunchRequest::parse(&original_request)?;
        if request.client_request_id != format!("automation-work-{}", self.semantic_slot_id)
            || request.preview.task_id != task_id
            || request.preview.expected_task_revision != task_revision
            || WorkDispatchLaunchSettings::from_preview(&request.preview) != self.launch_settings
        {
            return Err(Error::new(
                "FORBIDDEN",
                "launch request differs from its retained WorkDispatch parameters",
            ));
        }
        let effective: Value = serde_json::from_str(&effective_request_json)
            .map_err(|_| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch manifest is invalid"))?;
        let manifest = effective
            .get("launch_manifest")
            .ok_or_else(|| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch manifest is missing"))?;
        let manifest_lease = &manifest["workspace"]["lease"];
        let lease_id = manifest_lease
            .get("lease_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch lease is missing"))?;
        let lease_generation = manifest_lease
            .get("generation")
            .and_then(serde_json::Value::as_i64)
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                Error::new(
                    "LAUNCH_MANIFEST_CORRUPT",
                    "launch lease generation is missing",
                )
            })?;
        let lease_digest = manifest_lease
            .get("binding_digest")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::new("LAUNCH_MANIFEST_CORRUPT", "launch lease digest is missing")
            })?;
        let open_operation_id = manifest["binding"]["operation_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new("LAUNCH_MANIFEST_CORRUPT", "open child is missing"))?;
        if manifest["manifest_version"] != "eliot-launch-manifest-v1"
            || manifest["task"]["task_id"] != task_id
            || manifest["task"]["expected_revision"] != task_revision
            || manifest["task"]["observed_revision"] != task_revision
            || manifest["task"]["attempt_id"] != attempt_id
            || manifest["binding"]["binding_id"] != binding_id
            || manifest["binding"]["generation"] != binding_generation
            || manifest["binding"]["state"] != "ready"
            || open_operation_id.len() > 256
            || open_operation_id.chars().any(char::is_control)
        {
            return Err(Error::new(
                "LAUNCH_MANIFEST_CORRUPT",
                "launch manifest does not retain the exact ready binding",
            ));
        }
        match dispatch_operation_id {
            None if manifest["progress"]["task_dispatch"] != "not_started"
                || !manifest["progress"]["task_dispatch_operation_id"].is_null()
                || !manifest["progress"]["task_dispatch_packet_digest"].is_null() =>
            {
                return Err(Error::new(
                    "AUTOMATION_WORK_ASSIGNMENT_STALE",
                    "launch is no longer at its unstarted dispatch stage",
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
                    "AUTOMATION_WORK_ASSIGNMENT_STALE",
                    "launch does not retain this exact queued dispatch stage",
                ));
            }
            _ => {}
        }

        self.require_launch_open_child(
            db,
            operation_id,
            open_operation_id,
            task_id,
            attempt_id,
            &request.preview.route,
            lease_id,
            lease_generation,
            lease_digest,
            binding_id,
            binding_generation,
        )?;
        if self.subject.attempt_id.is_none() {
            self.require_launch_claim_child(db, operation_id, task_id, task_revision, attempt_id)?;
        }

        let task: Option<(String, i64, String)> = db
            .query_row(
                "SELECT project_id,revision,state FROM tasks WHERE task_id=?1",
                [task_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if !task.is_some_and(|(project_id, revision, state)| {
            project_id == self.project_id && revision == task_revision && state == "open"
        }) {
            return Err(Error::new(
                "AUTOMATION_WORK_SUBJECT_STALE",
                "launch Task is no longer open at its retained revision",
            ));
        }
        let active_attempts: i64 = db.query_row(
            "SELECT count(*) FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL",
            [task_id],
            |row| row.get(0),
        )?;
        let bound_attempt_is_current: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
             AND task_revision=?3 AND owner_id=?4 AND state='reserved' \
             AND start_owner='controller' AND released_at_ms IS NULL \
             AND ((?7 IS NULL AND start_operation_id IS NULL) OR start_operation_id=?7) \
             AND binding_id=?5 AND binding_generation=?6)",
            params![
                attempt_id,
                task_id,
                task_revision,
                self.effective_manager_id,
                binding_id,
                binding_generation,
                dispatch_operation_id,
            ],
            |row| row.get(0),
        )?;
        if active_attempts != 1 || !bound_attempt_is_current {
            return Err(Error::new(
                "AUTOMATION_WORK_ASSIGNMENT_STALE",
                "launch Attempt is not the sole current bound assignment at this dispatch stage",
            ));
        }
        if let Some(dispatch_id) = dispatch_operation_id {
            require_work_dispatch_start_operation(
                db,
                dispatch_id,
                operation_id,
                task_id,
                task_revision,
                attempt_id,
                binding_id,
                binding_generation,
                &self.effective_manager_id,
                &effective["launch_manifest"]["plan_digest"],
                model::text(
                    &effective["launch_manifest"]["progress"],
                    "task_dispatch_packet_digest",
                )?,
            )?;
        }
        let binding: Option<(String, Option<i64>)> = db
            .query_row(
                "SELECT state,released_at_ms FROM bindings WHERE binding_id=?1 AND generation=?2",
                params![binding_id, binding_generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if !binding.is_some_and(|(state, released)| state == "ready" && released.is_none()) {
            return Err(Error::new(
                "BINDING_NOT_READY",
                "exact launch binding is not currently ready",
            ));
        }
        Ok(())
    }

    // Keep the exact parent, route, lease, and binding evidence explicit here.
    #[allow(clippy::too_many_arguments)]
    fn require_launch_open_child(
        &self,
        db: &Connection,
        parent_operation_id: &str,
        open_operation_id: &str,
        task_id: &str,
        attempt_id: &str,
        route: &str,
        lease_id: &str,
        lease_generation: i64,
        lease_digest: &str,
        binding_id: &str,
        binding_generation: i64,
    ) -> Result<()> {
        let child: Option<LaunchOpenChildRow> = db
            .query_row(
                "SELECT caller_id,method,client_request_id,prerequisite_operation_id,task_id,\
                 attempt_id,binding_id,binding_generation,state,original_request_json,effective_request_json \
                 FROM operations WHERE operation_id=?1",
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
                    ))
                },
            )
            .optional()?;
        let Some((
            caller,
            method,
            client_request_id,
            prerequisite,
            child_task,
            child_attempt,
            child_binding,
            child_generation,
            state,
            original_request_json,
            effective_request_json,
        )) = child
        else {
            return Err(Error::new(
                "LAUNCH_OPEN_MISSING",
                "launch open child was not found",
            ));
        };
        if caller != self.technical_requester_id
            || method != "agent.open"
            || client_request_id != format!("launch:{parent_operation_id}:open")
            || prerequisite.as_deref() != Some(parent_operation_id)
            || child_task.as_deref() != Some(task_id)
            || child_attempt.as_deref() != Some(attempt_id)
            || child_binding.as_deref() != Some(binding_id)
            || child_generation != Some(binding_generation)
            || state != "settled"
        {
            return Err(Error::new(
                "FORBIDDEN",
                "agent.open child is not bound to the exact parent launch and Attempt",
            ));
        }
        let original: Value = serde_json::from_str(&original_request_json).map_err(|_| {
            Error::new("LAUNCH_OPEN_CORRUPT", "agent.open child request is invalid")
        })?;
        model::fields(&original, &["client_request_id", "lane_id", "route"])?;
        if original["client_request_id"] != client_request_id
            || original["lane_id"] != format!("launch-{lease_id}")
            || original["route"] != route
        {
            return Err(Error::new(
                "FORBIDDEN",
                "agent.open child request differs from its canonical launch lease",
            ));
        }
        let effective: Value = serde_json::from_str(&effective_request_json).map_err(|_| {
            Error::new("LAUNCH_OPEN_CORRUPT", "agent.open child linkage is invalid")
        })?;
        let receipt = &effective["receipt"]["value"];
        if receipt["operation_id"] != open_operation_id
            || receipt["binding_id"] != binding_id
            || receipt["generation"] != binding_generation
            || effective["operation_contract"]["parent_launch_operation_id"] != parent_operation_id
            || effective["workspace_lease"]["lease_id"] != lease_id
            || effective["workspace_lease"]["generation"] != lease_generation
            || effective["workspace_lease"]["binding_digest"] != lease_digest
            || effective["receipt"]["ok"] != true
        {
            return Err(Error::new(
                "LAUNCH_OPEN_CORRUPT",
                "agent.open child receipt does not prove the exact parent lease and binding",
            ));
        }
        Ok(())
    }

    fn require_launch_claim_child(
        &self,
        db: &Connection,
        parent_operation_id: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
    ) -> Result<()> {
        let client_request_id = format!("launch:{parent_operation_id}:claim");
        let child: Option<LaunchClaimChildRow> = db
            .query_row(
                "SELECT operation_id,method,state,task_id,attempt_id,original_request_json \
                 FROM operations WHERE caller_id=?1 AND client_request_id=?2",
                params![self.technical_requester_id, client_request_id],
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
        let Some((child_id, method, state, child_task, child_attempt, original_json)) = child
        else {
            return Err(Error::new(
                "LAUNCH_CLAIM_MISSING",
                "initial WorkDispatch launch has no canonical Task-claim child",
            ));
        };
        let expected_request = json!({
            "client_request_id":client_request_id,
            "task_id":task_id,
            "expected_revision":task_revision,
            "owner_id":self.effective_manager_id,
            "start_owner":"controller"
        });
        let original: Value = serde_json::from_str(&original_json).map_err(|_| {
            Error::new(
                "LAUNCH_CLAIM_CORRUPT",
                "Task-claim child request is invalid",
            )
        })?;
        if method != "task.claim"
            || state != "settled"
            || child_task.as_deref() != Some(task_id)
            || child_attempt.as_deref() != Some(attempt_id)
            || model::canonical(&original)? != model::canonical(&expected_request)?
        {
            return Err(Error::new(
                "FORBIDDEN",
                "Task-claim child does not match the exact initial WorkDispatch launch",
            ));
        }
        let result_json: Option<String> = db
            .query_row(
                "SELECT result_json FROM operations WHERE operation_id=?1",
                [&child_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let result_json = result_json.ok_or_else(|| {
            Error::new(
                "LAUNCH_CLAIM_CORRUPT",
                "Task-claim child has no settled result",
            )
        })?;
        let result: Value = serde_json::from_str(&result_json).map_err(|_| {
            Error::new("LAUNCH_CLAIM_CORRUPT", "Task-claim child result is invalid")
        })?;
        if result["operation_id"] != child_id
            || result["task_id"] != task_id
            || result["attempt_id"] != attempt_id
        {
            return Err(Error::new(
                "LAUNCH_CLAIM_CORRUPT",
                "Task-claim child result does not bind the exact Attempt",
            ));
        }
        let fact_exists: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM observations WHERE source_stream_id='controller' \
             AND kind='task.claim' AND source_event_key=?1 AND operation_id=?1 AND payload_json=?2)",
            params![child_id, model::canonical(&result)?],
            |row| row.get(0),
        )?;
        if !fact_exists {
            return Err(Error::new(
                "LAUNCH_CLAIM_CORRUPT",
                "Task-claim child has no matching committed controller fact",
            ));
        }
        Ok(())
    }

    fn require_current_entry(&self, db: &Connection, operation_id: Option<&str>) -> Result<()> {
        let entry = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
        config::validate_entry(&entry)?;
        if let Some(operation_id) = operation_id
            && config::transfer_from_source(
                db,
                &self.effective_manager_id,
                &self.project_id,
                &self.automation_id,
            )?
            .is_some()
        {
            let grant = authorization::current_transfer_continuation(
                db,
                operation_id,
                "swarm.launch",
                AutomationStep::WorkDispatch,
                &self.subject.task_id,
            )?
            .ok_or_else(|| {
                Error::new(
                    "FORBIDDEN",
                    "transferred WorkDispatch Operation has no exact current-GM grant",
                )
            })?;
            if grant.historical_owner_id() != self.effective_manager_id
                || grant.historical_revision() != self.automation_revision
                || grant.current_entry().work_dispatch.as_ref() != Some(&self.launch_settings)
                || !grant.current_entry().work_dispatch_ready()
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "current GM WorkDispatch settings differ from the immutable launch request",
                ));
            }
            return Ok(());
        }
        if !entry.enabled
            || !entry.steps.contains(&AutomationStep::WorkDispatch)
            || entry.scope.work_pool_id.is_some()
            || entry.work_dispatch.as_ref() != Some(&self.launch_settings)
        {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "WorkDispatch is disabled, narrowed, or has different launch settings",
            ));
        }
        require_registered_manager(db, &self.effective_manager_id)
    }

    pub(crate) fn technical_requester_id(&self) -> &str {
        &self.technical_requester_id
    }

    pub(crate) fn effective_manager_id(&self) -> &str {
        &self.effective_manager_id
    }

    pub(crate) fn automation_id(&self) -> &str {
        &self.automation_id
    }

    pub(crate) fn automation_revision(&self) -> i64 {
        self.automation_revision
    }

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn subject(&self) -> &WorkDispatchSubject {
        &self.subject
    }

    pub(crate) fn semantic_slot_id(&self) -> &str {
        &self.semantic_slot_id
    }

    pub(crate) fn linkage_value(&self) -> Value {
        json!({
            "technical_requester_id":self.technical_requester_id,
            "effective_manager_id":self.effective_manager_id,
            "automation_id":self.automation_id,
            "automation_revision":self.automation_revision,
            "project_id":self.project_id,
            "action":"swarm.launch",
            "semantic_cause_kind":if self.subject.attempt_id.is_some() {"manager_owned_assignment"} else {"initial_task"},
            "semantic_cause_id":self.semantic_slot_id,
            "semantic_slot_id":self.semantic_slot_id,
            "task_id":self.subject.task_id,
            "task_revision":self.subject.task_revision,
            "attempt_id":self.subject.attempt_id,
            "source":{
                "observation_id":self.source.observation_id,
                "event_kind":self.source.event_kind,
                "operation_id":self.source.operation_id
            }
        })
    }
}

impl WorkDispatchSubject {
    pub(crate) fn task_id(&self) -> &str {
        &self.task_id
    }

    pub(crate) fn task_revision(&self) -> i64 {
        self.task_revision
    }

    pub(crate) fn attempt_id(&self) -> Option<&str> {
        self.attempt_id.as_deref()
    }

    pub(crate) fn semantic_slot_id(&self, manager_id: &str) -> Result<String> {
        let key = json!({
            "manager_id":manager_id,
            "task_id":self.task_id,
            "task_revision":self.task_revision,
            "attempt_id":self.attempt_id,
            "action":"swarm.launch",
            "slot_id":"primary"
        });
        Ok(model::digest(model::canonical(&key)?.as_bytes()))
    }
}

/// Build the exact launcher preview parameters from a retained Task assignment
/// and explicit entry settings. The caller must pass them to the shared
/// launcher preview using this context's on-behalf actor.
pub(crate) fn preview_request(
    context: &WorkDispatchContext,
    settings: &WorkDispatchLaunchSettings,
) -> Result<LaunchPreviewRequest> {
    let params =
        settings.preview_params(context.subject.task_id(), context.subject.task_revision());
    LaunchPreviewRequest::parse(&params)
}

/// Bind only a plan digest produced by the existing live launcher preview.
/// This constructs a typed request; it does not reserve an Operation.
pub(crate) fn launch_request(
    context: &WorkDispatchContext,
    preview: &LaunchPreviewRequest,
    plan_digest: &str,
) -> Result<(LaunchRequest, Value)> {
    if !valid_plan_digest(plan_digest) {
        return Err(Error::invalid(
            "launcher preview must return a sha256 digest before launch admission",
        ));
    }
    let client_request_id = format!("automation-work-{}", context.semantic_slot_id());
    let mut value = json!({
        "client_request_id":client_request_id,
        "plan_digest":plan_digest,
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
    });
    let request = LaunchRequest::parse(&value)?;
    if request.preview.task_id != context.subject.task_id
        || request.preview.expected_task_revision != context.subject.task_revision
    {
        return Err(Error::new(
            "AUTOMATION_WORK_PLAN_MISMATCH",
            "launch request does not match its exact retained Task assignment",
        ));
    }
    // Keep the canonical parsed projection as the persisted effective request.
    value = json!({
        "client_request_id":request.client_request_id,
        "plan_digest":request.plan_digest,
        "task_id":request.preview.task_id,
        "expected_task_revision":request.preview.expected_task_revision,
        "route":request.preview.route,
        "agent_profile":request.preview.agent_profile,
        "mcp_profile":request.preview.mcp_profile,
        "mcp_surface":request.preview.mcp_surface,
        "workspace_policy":request.preview.workspace_policy,
        "requested_model":request.preview.requested_model,
        "requested_effort":request.preview.requested_effort,
        "budget":request.preview.budget,
        "stop_conditions":request.preview.stop_conditions,
        "purpose":request.preview.purpose,
    });
    Ok((request, value))
}

fn validate_entry_action(db: &Connection, entry: &config::AutomationEntry) -> Result<()> {
    config::validate_entry(entry)?;
    if !entry.enabled || !entry.steps.contains(&AutomationStep::WorkDispatch) {
        return Err(Error::new(
            "AUTOMATION_ACTION_UNAVAILABLE",
            "entry does not currently admit work_dispatch",
        ));
    }
    if entry.scope.work_pool_id.is_some() {
        return Err(Error::new(
            "AUTOMATION_WORK_POOL_UNAVAILABLE",
            "the current Task source has no committed work-pool membership reader",
        ));
    }
    require_registered_manager(db, &entry.owner_manager_id)?;
    let current = config::load_entry(
        db,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?
    .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
    if current.revision != entry.revision
        || !current.enabled
        || !current.steps.contains(&AutomationStep::WorkDispatch)
    {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "current entry no longer admits work_dispatch",
        ));
    }
    Ok(())
}

fn require_registered_manager(db: &Connection, manager_id: &str) -> Result<()> {
    let raw: Option<String> = db
        .query_row(
            "SELECT value_json FROM meta WHERE key=?1",
            [format!("client:{manager_id}")],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Err(Error::new(
            "FORBIDDEN",
            "work-dispatch manager is not registered",
        ));
    };
    let registration: Value = serde_json::from_str(&raw)?;
    if registration["role"] != "manager" || registration["disabled"] == true {
        return Err(Error::new(
            "FORBIDDEN",
            "work dispatch requires the current enabled manager identity",
        ));
    }
    Ok(())
}

fn require_new_work_enabled(db: &Connection) -> Result<()> {
    let raw: Option<String> = db
        .query_row(
            "SELECT value_json FROM meta WHERE key='execution_mode'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let mode = raw
        .map(|raw| serde_json::from_str::<Value>(&raw))
        .transpose()?
        .unwrap_or(Value::Null);
    if mode["new_work"] != "enabled" {
        return Err(Error::new("ADMISSION_DISABLED", "new work is disabled"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn require_work_dispatch_start_operation(
    db: &Connection,
    dispatch_operation_id: &str,
    parent_operation_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    binding_id: &str,
    binding_generation: i64,
    expected_caller_id: &str,
    plan_digest: &Value,
    expected_packet_digest: &str,
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
    let dispatch: Option<DispatchRow> = db
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
    )) = dispatch
    else {
        return Err(Error::new(
            "AUTOMATION_WORK_ASSIGNMENT_STALE",
            "queued dispatch Operation was not found",
        ));
    };
    if caller != expected_caller_id
        || method != "task.dispatch"
        || state != "queued"
        || operation_task.as_deref() != Some(task_id)
        || operation_attempt.as_deref() != Some(attempt_id)
        || operation_binding.as_deref() != Some(binding_id)
        || operation_generation != Some(binding_generation)
    {
        return Err(Error::new(
            "AUTOMATION_WORK_ASSIGNMENT_STALE",
            "dispatch Operation is not the exact queued Task, Attempt, and binding child",
        ));
    }
    let original: Value = serde_json::from_str(&original_json)
        .map_err(|_| Error::new("INVALID_RECEIPT", "dispatch request is invalid"))?;
    if original["attempt_id"] != attempt_id
        || original["launch_operation_id"] != parent_operation_id
    {
        return Err(Error::new(
            "AUTOMATION_WORK_ASSIGNMENT_STALE",
            "dispatch request does not name the exact parent launch and Attempt",
        ));
    }
    let effective: Value = serde_json::from_str(&effective_json)
        .map_err(|_| Error::new("INVALID_RECEIPT", "dispatch packet is invalid"))?;
    let packet = &effective["launch_dispatch_packet"];
    let packet_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(packet)?.as_bytes())
    );
    if packet["schema_version"] != 1
        || packet["launch_operation_id"] != parent_operation_id
        || packet["plan_digest"] != *plan_digest
        || packet["task"]["task_id"] != task_id
        || packet["task"]["revision"] != task_revision
        || packet["task"]["attempt_id"] != attempt_id
        || packet_digest != expected_packet_digest
    {
        return Err(Error::new(
            "AUTOMATION_WORK_ASSIGNMENT_STALE",
            "dispatch packet does not retain the exact parent launch and Task assignment",
        ));
    }
    Ok(())
}

fn validate_task_id(task_id: &str) -> Result<()> {
    if task_id.is_empty()
        || task_id.len() > MAX_TASK_ID_BYTES
        || task_id.chars().any(char::is_control)
    {
        return Err(Error::invalid("Task or Attempt ID is invalid"));
    }
    Ok(())
}

fn valid_plan_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
