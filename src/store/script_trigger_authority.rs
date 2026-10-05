//! Store-derived authority for one configured ScriptRun continuation.
//!
//! This scope is retained inside the existing on-behalf cause. It is not a
//! Principal and cannot be built from request JSON. Every admission and start
//! gate re-loads the enabled Manager, AutomationEntry, source selectors and
//! exact Task/Attempt facts from Store.

use super::{ScriptEventInvocationContext, automation_intake, bus_kernel, submissions, tasks};
use crate::{
    automation::{authorization, config, config::AutomationEntry},
    config::Config,
    error::{Error, Result},
    model,
};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const SCRIPT_RUN_SCOPE_VERSION: u32 = 1;
const SCRIPT_RUN_ACTION: &str = "script_run";

/// Persisted, sealed attribution for the automation owner and exact action
/// source/target selection. The type is Store-private; callers receive it only
/// after a Store record or current AutomationEntry has been validated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScriptRunConsumerContext {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    automation_revision: i64,
    action: String,
    script_id: String,
    source_target_digest: String,
}

impl ScriptRunConsumerContext {
    pub(crate) fn from_entry(entry: &AutomationEntry) -> Result<Self> {
        let script_id = entry
            .script_run
            .as_ref()
            .map(|settings| settings.script_id.clone())
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_ACTION_CHANGED",
                    "ScriptRun context has no selected script target",
                )
            })?;
        Ok(Self {
            schema_version: SCRIPT_RUN_SCOPE_VERSION,
            owner_manager_id: entry.owner_manager_id.clone(),
            project_id: entry.project_id.clone(),
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            action: SCRIPT_RUN_ACTION.to_owned(),
            script_id,
            source_target_digest: bus_kernel::script_run_consumer_scope_digest(entry)?,
        })
    }

    pub(crate) fn from_cause(cause: &Value) -> Result<Self> {
        let context: Self = serde_json::from_value(
            cause
                .get("automation_consumer")
                .cloned()
                .ok_or_else(|| corrupt("retained ScriptRun has no consumer context"))?,
        )
        .map_err(|_| corrupt("retained ScriptRun consumer context is invalid"))?;
        context.validate_shape()?;
        Ok(context)
    }

    pub(crate) fn with_cause(&self, cause: &Value) -> Result<Value> {
        self.validate_shape()?;
        let mut retained = cause.clone();
        if retained
            .get("automation_consumer")
            .is_some_and(|value| !value.is_null())
        {
            return Err(corrupt(
                "ScriptRun cause already contains consumer attribution",
            ));
        }
        retained["automation_consumer"] = serde_json::to_value(self)?;
        Ok(retained)
    }

    pub(crate) fn require_matches_cause(&self, cause: &Value) -> Result<()> {
        if Self::from_cause(cause)? != *self {
            return Err(corrupt(
                "ScriptRun cause differs from its retained consumer context",
            ));
        }
        Ok(())
    }

    pub(crate) fn owner_manager_id(&self) -> &str {
        &self.owner_manager_id
    }

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn automation_id(&self) -> &str {
        &self.automation_id
    }

    pub(crate) fn automation_revision(&self) -> i64 {
        self.automation_revision
    }

    pub(crate) fn matches_pending_identity(
        &self,
        owner_manager_id: &str,
        project_id: &str,
        automation_id: &str,
        automation_revision: i64,
        script_id: &str,
    ) -> bool {
        self.schema_version == SCRIPT_RUN_SCOPE_VERSION
            && self.owner_manager_id == owner_manager_id
            && self.project_id == project_id
            && self.automation_id == automation_id
            && self.automation_revision == automation_revision
            && self.action == SCRIPT_RUN_ACTION
            && self.script_id == script_id
            && self.source_target_digest.len() == 64
            && self
                .source_target_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
    }

    pub(crate) fn require_current_entry(&self, db: &Connection) -> Result<AutomationEntry> {
        self.validate_shape()?;
        authorization::require_registered_manager(db, &self.owner_manager_id)?;
        let entry = config::load_entry(
            db,
            &self.owner_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "ScriptRun automation was removed",
            )
        })?;
        config::validate_entry(&entry)?;
        self.require_entry_match(&entry)?;
        if entry.revision != self.automation_revision {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current ScriptRun entry revision differs from the retained trigger",
            ));
        }
        Ok(entry)
    }

    pub(crate) fn require_entry_match(&self, entry: &AutomationEntry) -> Result<()> {
        self.validate_shape()?;
        if entry.owner_manager_id != self.owner_manager_id
            || entry.project_id != self.project_id
            || entry.automation_id != self.automation_id
            || !entry.script_run_ready()
            || entry
                .script_run
                .as_ref()
                .is_none_or(|settings| settings.script_id != self.script_id)
            || bus_kernel::script_run_consumer_scope_digest(entry)? != self.source_target_digest
        {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current ScriptRun owner, entry, target, or event selectors changed",
            ));
        }
        Ok(())
    }

    pub(crate) fn require_current_source(
        &self,
        db: &Connection,
        app_config: &Config,
        entry: &AutomationEntry,
        cause: &Value,
    ) -> Result<ScriptEventInvocationContext> {
        self.require_entry_match(entry)?;
        authorization::require_registered_manager(db, &self.owner_manager_id)?;
        let source = match cause["kind"].as_str() {
            Some("system_event") => bus_kernel::script_event_invocation_context_for_consumer(
                db,
                app_config,
                entry,
                cause,
                &self.owner_manager_id,
            )?,
            Some("applied_submission") => self.current_submission_context(db, entry, cause)?,
            _ => {
                return Err(Error::new(
                    "AUTOMATION_ACTION_CHANGED",
                    "ScriptRun source kind is unsupported",
                ));
            }
        };
        Ok(source)
    }

    pub(crate) fn require_live_subject(
        &self,
        db: &Connection,
        source: &ScriptEventInvocationContext,
    ) -> Result<(Option<Value>, Option<Value>)> {
        match (
            source.task_id.as_deref(),
            source.task_revision,
            source.attempt_id.as_deref(),
        ) {
            (Some(task_id), Some(task_revision), Some(attempt_id)) => {
                let task = tasks::get_task(db, task_id)?;
                let attempt = tasks::get_attempt(db, attempt_id)?;
                if task["project_id"] != self.project_id
                    || task["state"] != "open"
                    || task["revision"] != task_revision
                    || task["current_attempt_id"] != attempt_id
                    || attempt["task_id"] != task_id
                    || attempt["task_revision"] != task_revision
                    || attempt["owner_id"] != self.owner_manager_id
                    || !attempt["released_at_ms"].is_null()
                {
                    return Err(Error::new(
                        "SCRIPT_SCOPE_CHANGED",
                        "ScriptRun Task/Attempt no longer belongs to the configured Manager",
                    ));
                }
                Ok((Some(task), Some(attempt)))
            }
            (None, None, None) => Ok((None, None)),
            _ => Err(Error::new(
                "SCRIPT_SCOPE_CHANGED",
                "ScriptRun source has a partial Task/Attempt identity",
            )),
        }
    }

    fn current_submission_context(
        &self,
        db: &Connection,
        entry: &AutomationEntry,
        cause: &Value,
    ) -> Result<ScriptEventInvocationContext> {
        let observation_id = model::positive(cause, "observation_id")?;
        let event =
            automation_intake::observed_event_by_id(db, observation_id)?.ok_or_else(|| {
                Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "applied-submission source observation is unavailable",
                )
            })?;
        let receipt = automation_intake::receipt_by_observation_id(
            db,
            crate::automation::intake::LocalProducer::TaskSubmission.source_id(),
            observation_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "applied-submission source receipt is unavailable",
            )
        })?;
        let expected_cause =
            super::cause_from_fact(observation_id, &receipt.payload)?.ok_or_else(|| {
                Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "source receipt is no longer an applied submission",
                )
            })?;
        if event.source_id != crate::automation::intake::LocalProducer::TaskSubmission.source_id()
            || event.event_kind
                != crate::automation::intake::LocalProducer::TaskSubmission.event_kind()
            || cause["kind"] != expected_cause.kind()
            || cause["id"] != expected_cause.id()
            || cause["observation_id"].as_i64() != Some(observation_id)
            || cause["operation_id"].as_str() != receipt.operation_id.as_deref()
        {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "retained submission cause differs from its exact source receipt",
            ));
        }
        if !bus_kernel::current_submission_scope_matches_for_owner(
            db,
            &self.owner_manager_id,
            entry,
            &event,
        )? {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "submission is not the current exact Task/Attempt owned by this Manager",
            ));
        }
        let submission_ref = model::text(cause, "id")?;
        let operation_id = model::text(cause, "operation_id")?;
        let document = submissions::document(db, submission_ref)?;
        if document["operation_id"] != operation_id
            || document["outcome"] != "applied"
            || document["task_id"].as_str().is_none_or(str::is_empty)
            || document["attempt_id"].as_str().is_none_or(str::is_empty)
            || document["candidate_ref"].as_str().is_none_or(str::is_empty)
        {
            return Err(Error::new(
                "SUBMISSION_DAMAGED",
                "applied-submission document does not retain its exact committed tuple",
            ));
        }
        let task_id = model::text(&document, "task_id")?;
        let task_revision = model::positive(&document, "task_revision")?;
        let attempt_id = model::text(&document, "attempt_id")?;
        let task = tasks::get_task(db, task_id)?;
        let attempt = tasks::get_attempt(db, attempt_id)?;
        if task["project_id"] != entry.project_id
            || task["state"] != "open"
            || task["revision"] != task_revision
            || task["current_attempt_id"] != attempt_id
            || attempt["task_id"] != task_id
            || attempt["task_revision"] != task_revision
            || attempt["owner_id"] != self.owner_manager_id
            || !attempt["released_at_ms"].is_null()
            || attempt["submission_ref"] != submission_ref
            || attempt["candidate_ref"] != document["candidate_ref"]
        {
            return Err(Error::new(
                "STALE_ATTEMPT",
                "applied-submission Task/Attempt is no longer owned and current",
            ));
        }
        if cause.get("task_id").is_some_and(|value| !value.is_null())
            && (cause["task_id"] != task["task_id"]
                || cause["task_revision"] != task["revision"]
                || cause["attempt_id"] != attempt["attempt_id"]
                || cause["candidate_ref"] != attempt["candidate_ref"])
        {
            return Err(Error::new(
                "SCRIPT_SCOPE_CHANGED",
                "retained submission context differs from its current Task/Attempt",
            ));
        }
        Ok(ScriptEventInvocationContext {
            input: json!({
                "kind":"task.submission.applied",
                "submission_ref":submission_ref,
                "operation_id":operation_id,
                "task_id":task_id,
                "task_revision":task_revision,
                "attempt_id":attempt_id,
                "candidate_ref":document["candidate_ref"]
            }),
            task_id: Some(task_id.to_owned()),
            task_revision: Some(task_revision),
            attempt_id: Some(attempt_id.to_owned()),
        })
    }

    fn validate_shape(&self) -> Result<()> {
        if self.schema_version != SCRIPT_RUN_SCOPE_VERSION
            || self.owner_manager_id.is_empty()
            || self.project_id.is_empty()
            || self.automation_id.is_empty()
            || self.automation_revision <= 0
            || self.action != SCRIPT_RUN_ACTION
            || self.script_id.is_empty()
            || self.source_target_digest.len() != 64
            || !self
                .source_target_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(corrupt("ScriptRun consumer scope has invalid fields"));
        }
        Ok(())
    }
}

fn corrupt(message: &str) -> Error {
    Error::new("AUTOMATION_LINK_CORRUPT", message)
}
