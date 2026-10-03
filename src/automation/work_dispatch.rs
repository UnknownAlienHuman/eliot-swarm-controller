//! Typed launch settings and non-serializable manager provenance for the first
//! automated WorkDispatch consumer.
//!
//! This module prepares the exact existing launcher request. It does not
//! synthesize a Principal or authorize/start a native runtime. The Store's
//! launcher integration must consume `WorkDispatchContext` through its real
//! on-behalf actor and recheck it at effect start.

pub(crate) use super::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID;
use super::{actions::AutomationStep, config};
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
        self.require_current_entry(db)?;
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
        self.require_current(db)
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
        self.require_current_entry(db)?;

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
                "bound launch Attempt is outside the retained WorkDispatch subject",
            ));
        }
        self.require_current_entry(db)?;

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
             AND released_at_ms IS NULL AND start_operation_id IS NULL \
             AND binding_id=?5 AND binding_generation=?6)",
            params![
                attempt_id,
                task_id,
                task_revision,
                self.effective_manager_id,
                binding_id,
                binding_generation
            ],
            |row| row.get(0),
        )?;
        if active_attempts != 1 || !bound_attempt_is_current {
            return Err(Error::new(
                "AUTOMATION_WORK_ASSIGNMENT_STALE",
                "launch Attempt is not the sole current bound unstarted assignment",
            ));
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

    fn require_current_entry(&self, db: &Connection) -> Result<()> {
        let entry = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
        config::validate_entry(&entry)?;
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
