//! Pure ScriptRun action planning after the Store supplies current authorized
//! automation settings and a safe projected event.

use crate::{
    EventAction, EventMetadata, EventRule, Result, ScriptError,
    protocol::{ScriptInvocation, ScriptRunRequest, validate_input, validate_invocation_size},
    schema::{
        ScriptControllerEffect, ScriptValueSchema, validate_effect_grants, validate_script_id,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskScope {
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TriggerIdentity {
    SystemEvent {
        #[serde(default)]
        occurrence_phase: Option<String>,
        #[serde(default)]
        occurrence_id: Option<String>,
        #[serde(default)]
        task_scope: Option<TaskScope>,
    },
    AppliedSubmission {
        task_id: String,
        task_revision: i64,
        attempt_id: String,
        submission_ref: String,
    },
}

/// The Store maps its enabled `AutomationEntry` to this projection only after
/// current manager, script ownership, project, and source authority checks.
pub struct ScriptRunRoute<'a> {
    pub automation_id: &'a str,
    pub automation_revision: i64,
    pub script_id: &'a str,
    pub script_revision: i64,
    pub event_rules: &'a [EventRule],
    pub input_schema: &'a ScriptValueSchema,
    pub result_schema: &'a ScriptValueSchema,
    pub controller_effects: &'a [ScriptControllerEffect],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedScriptRun {
    pub action: EventAction,
    pub automation_id: String,
    pub automation_revision: i64,
    pub script_id: String,
    pub script_revision: i64,
    pub semantic_cause_id: String,
    pub cause: TriggerIdentity,
    pub request: ScriptRunRequest,
    pub controller_effects: Vec<ScriptControllerEffect>,
}

pub fn plan_event_script_run(
    route: &ScriptRunRoute<'_>,
    event: &EventMetadata,
    cause: TriggerIdentity,
    input: Value,
) -> Result<Option<PlannedScriptRun>> {
    crate::event::validate_event_rules(
        route.event_rules,
        &[EventAction::ReviewDispatch, EventAction::ScriptRun],
    )?;
    let mut matched = false;
    for rule in route.event_rules {
        if rule.action == EventAction::ScriptRun && rule.matches(event)? {
            matched = true;
            break;
        }
    }
    if !matched {
        return Ok(None);
    }
    validate_script_id(route.script_id)?;
    if route.automation_id.trim().is_empty()
        || route.automation_id.len() > 128
        || route.automation_revision <= 0
        || route.script_revision <= 0
    {
        return Err(ScriptError::new("INVALID_PARAMS"));
    }
    validate_effect_grants(route.controller_effects)?;
    crate::schema::validate_schema_pair(route.input_schema, route.result_schema)?;
    validate_input(route.input_schema, &input)?;
    let (semantic_cause_id, request_id, task_scope) = match &cause {
        TriggerIdentity::SystemEvent {
            occurrence_phase,
            occurrence_id,
            task_scope,
        } => {
            if let Some(scope) = task_scope {
                scope.validate()?;
            }
            let semantic_event_id = crate::event::semantic_event_id(
                event,
                occurrence_phase.as_deref(),
                occurrence_id.as_deref(),
            )?;
            (
                semantic_event_id.clone(),
                script_run_event_request_id(
                    route.automation_id,
                    &semantic_event_id,
                    route.script_id,
                )?,
                task_scope.clone(),
            )
        }
        TriggerIdentity::AppliedSubmission {
            task_id,
            task_revision,
            attempt_id,
            submission_ref,
        } => {
            if event.source_id() != "controller"
                || event.event_kind() != "task.submission"
                || event.status() != Some(crate::EventStatus::Applied)
                || task_id.trim().is_empty()
                || *task_revision <= 0
                || attempt_id.trim().is_empty()
                || submission_ref.trim().is_empty()
            {
                return Err(ScriptError::new("SCRIPT_EVENT_PROJECTION_INVALID"));
            }
            (
                submission_ref.clone(),
                script_run_submission_request_id(
                    route.automation_id,
                    task_id,
                    *task_revision,
                    attempt_id,
                    submission_ref,
                    route.script_id,
                )?,
                Some(TaskScope {
                    task_id: task_id.clone(),
                    task_revision: *task_revision,
                    attempt_id: attempt_id.clone(),
                }),
            )
        }
    };
    let request = ScriptRunRequest {
        client_request_id: request_id,
        script_id: route.script_id.to_owned(),
        expected_script_revision: route.script_revision,
        attempt_id: task_scope.as_ref().map(|scope| scope.attempt_id.clone()),
        expected_task_revision: task_scope.as_ref().map(|scope| scope.task_revision),
        input,
    };
    request.validate()?;
    Ok(Some(PlannedScriptRun {
        action: EventAction::ScriptRun,
        automation_id: route.automation_id.to_owned(),
        automation_revision: route.automation_revision,
        script_id: route.script_id.to_owned(),
        script_revision: route.script_revision,
        semantic_cause_id,
        cause,
        request,
        controller_effects: route.controller_effects.to_vec(),
    }))
}

impl PlannedScriptRun {
    /// Add the Store-assigned immutable Operation/run identities only after
    /// durable admission. This does not start a process or commit a journal.
    pub fn invocation(&self, operation_id: &str, run_id: &str) -> Result<ScriptInvocation> {
        if operation_id.trim().is_empty() || run_id.trim().is_empty() {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        let task_scope = match &self.cause {
            TriggerIdentity::SystemEvent { task_scope, .. } => task_scope.clone(),
            TriggerIdentity::AppliedSubmission {
                task_id,
                task_revision,
                attempt_id,
                ..
            } => Some(TaskScope {
                task_id: task_id.clone(),
                task_revision: *task_revision,
                attempt_id: attempt_id.clone(),
            }),
        };
        if let Some(scope) = task_scope.as_ref() {
            scope.validate()?;
        }
        let invocation = ScriptInvocation {
            protocol_version: 1,
            operation_id: operation_id.to_owned(),
            run_id: run_id.to_owned(),
            script_id: self.script_id.clone(),
            script_revision: self.script_revision,
            task_id: task_scope.as_ref().map(|scope| scope.task_id.clone()),
            task_revision: task_scope.as_ref().map(|scope| scope.task_revision),
            attempt_id: task_scope.as_ref().map(|scope| scope.attempt_id.clone()),
            input: self.request.input.clone(),
            controller_effects: self.controller_effects.clone(),
        };
        validate_invocation_size(&invocation)?;
        Ok(invocation)
    }
}

impl TaskScope {
    fn validate(&self) -> Result<()> {
        if self.task_id.trim().is_empty()
            || self.task_id.len() > 256
            || self.task_revision <= 0
            || self.attempt_id.trim().is_empty()
            || self.attempt_id.len() > 256
        {
            return Err(ScriptError::new("SCRIPT_EVENT_PROJECTION_INVALID"));
        }
        Ok(())
    }
}

pub fn script_run_event_request_id(
    automation_id: &str,
    semantic_event_id: &str,
    script_id: &str,
) -> Result<String> {
    digest_request_id(json!({
        "automation_id":automation_id,
        "semantic_event_id":semantic_event_id,
        "script_id":script_id,
        "action":"script.run"
    }))
}

pub fn script_run_submission_request_id(
    automation_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    submission_ref: &str,
    script_id: &str,
) -> Result<String> {
    digest_request_id(json!({
        "automation_id":automation_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "submission_ref":submission_ref,
        "script_id":script_id,
        "action":"script.run"
    }))
}

fn digest_request_id(identity: Value) -> Result<String> {
    Ok(crate::sha256_hex(
        crate::canonical_json(&identity)?.as_bytes(),
    ))
}
