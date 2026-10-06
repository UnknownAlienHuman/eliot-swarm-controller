//! Typed requests for durable, task-scoped Goals and one-shot reminders.
//!
//! Goals describe an existing assigned Task. They do not create a Task, start
//! a model, or infer completion from notification delivery or prose.

use crate::{
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const MAX_GOAL_ID_BYTES: usize = 128;
pub(crate) const MAX_OBJECTIVE_BYTES: usize = 32 * 1024;
pub(crate) const MAX_COOLDOWN_MS: i64 = 90 * 24 * 60 * 60 * 1000;
pub(crate) const MAX_PAGE_SIZE: i64 = 50;
pub(crate) const DEFAULT_PAGE_SIZE: i64 = 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scope {
    pub(crate) project_id: String,
    pub(crate) task_id: String,
    pub(crate) task_revision: i64,
    pub(crate) attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompletionEvidence {
    pub(crate) kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReminderConfig {
    pub(crate) due_at_ms: i64,
    pub(crate) cooldown_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) enum Mutation {
    Create {
        scope: Scope,
        goal_id: String,
        objective: String,
        completion_evidence: CompletionEvidence,
        reminder: Option<ReminderConfig>,
        enabled: bool,
    },
    Revise {
        scope: Scope,
        goal_id: String,
        expected_revision: i64,
        objective: Option<String>,
        completion_evidence: Option<CompletionEvidence>,
        reminder: Option<Option<ReminderConfig>>,
        enabled: Option<bool>,
    },
    Enable {
        scope: Scope,
        goal_id: String,
        expected_revision: i64,
    },
    Disable {
        scope: Scope,
        goal_id: String,
        expected_revision: i64,
    },
    Readback {
        scope: Scope,
        goal_id: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ReadRequest {
    Get {
        scope: Scope,
        goal_id: String,
    },
    List {
        scope: Scope,
        limit: i64,
        after_goal_id: Option<String>,
    },
}

/// Validate the closed JSON shape used by the mutation receipt path.
pub(crate) fn validate_mutation(method: &str, value: &Value) -> Result<()> {
    parse_mutation(method, value).map(|_| ())
}

pub(crate) fn parse_mutation(method: &str, value: &Value) -> Result<Mutation> {
    match method {
        "goal.create" => {
            model::fields(
                value,
                &[
                    "client_request_id",
                    "project_id",
                    "task_id",
                    "task_revision",
                    "attempt_id",
                    "goal_id",
                    "expected_revision",
                    "objective",
                    "completion_evidence",
                    "reminder",
                    "enabled",
                ],
            )?;
            require_request_id(value)?;
            if value.get("expected_revision").and_then(Value::as_i64) != Some(0) {
                return Err(Error::invalid("goal.create requires expected_revision=0"));
            }
            Ok(Mutation::Create {
                scope: parse_scope(value)?,
                goal_id: identifier(model::text(value, "goal_id")?, "goal_id")?,
                objective: objective(value)?,
                completion_evidence: completion_evidence(value.get("completion_evidence"))?,
                reminder: optional_reminder(value.get("reminder"))?,
                enabled: optional_bool(value, "enabled")?.unwrap_or(false),
            })
        }
        "goal.revise" => {
            model::fields(
                value,
                &[
                    "client_request_id",
                    "project_id",
                    "task_id",
                    "task_revision",
                    "attempt_id",
                    "goal_id",
                    "expected_revision",
                    "objective",
                    "completion_evidence",
                    "reminder",
                    "enabled",
                ],
            )?;
            require_request_id(value)?;
            let objective = value
                .get("objective")
                .map(|_| objective(value))
                .transpose()?;
            let completion_evidence = value
                .get("completion_evidence")
                .map(parse_completion_evidence)
                .transpose()?;
            let reminder = value
                .get("reminder")
                .map(parse_nullable_reminder)
                .transpose()?;
            let enabled = optional_bool(value, "enabled")?;
            Ok(Mutation::Revise {
                scope: parse_scope(value)?,
                goal_id: identifier(model::text(value, "goal_id")?, "goal_id")?,
                expected_revision: model::positive(value, "expected_revision")?,
                objective,
                completion_evidence,
                reminder,
                enabled,
            })
        }
        "goal.enable" | "goal.disable" => {
            model::fields(
                value,
                &[
                    "client_request_id",
                    "project_id",
                    "task_id",
                    "task_revision",
                    "attempt_id",
                    "goal_id",
                    "expected_revision",
                ],
            )?;
            require_request_id(value)?;
            let scope = parse_scope(value)?;
            let goal_id = identifier(model::text(value, "goal_id")?, "goal_id")?;
            let expected_revision = model::positive(value, "expected_revision")?;
            if method == "goal.enable" {
                Ok(Mutation::Enable {
                    scope,
                    goal_id,
                    expected_revision,
                })
            } else {
                Ok(Mutation::Disable {
                    scope,
                    goal_id,
                    expected_revision,
                })
            }
        }
        "goal.readback" => {
            model::fields(
                value,
                &[
                    "client_request_id",
                    "project_id",
                    "task_id",
                    "task_revision",
                    "attempt_id",
                    "goal_id",
                ],
            )?;
            require_request_id(value)?;
            Ok(Mutation::Readback {
                scope: parse_scope(value)?,
                goal_id: identifier(model::text(value, "goal_id")?, "goal_id")?,
            })
        }
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

pub(crate) fn parse_read(method: &str, value: &Value) -> Result<ReadRequest> {
    match method {
        "goal.get" => {
            model::fields(
                value,
                &[
                    "project_id",
                    "task_id",
                    "task_revision",
                    "attempt_id",
                    "goal_id",
                ],
            )?;
            Ok(ReadRequest::Get {
                scope: parse_scope(value)?,
                goal_id: identifier(model::text(value, "goal_id")?, "goal_id")?,
            })
        }
        "goal.list" => {
            model::fields(
                value,
                &[
                    "project_id",
                    "task_id",
                    "task_revision",
                    "attempt_id",
                    "limit",
                    "after_goal_id",
                ],
            )?;
            let limit = match value.get("limit") {
                None => DEFAULT_PAGE_SIZE,
                Some(_) => model::positive(value, "limit")?.min(MAX_PAGE_SIZE),
            };
            let after_goal_id = value
                .get("after_goal_id")
                .map(|value| {
                    value
                        .as_str()
                        .ok_or_else(|| Error::invalid("after_goal_id must be text"))
                        .and_then(|value| identifier(value, "after_goal_id"))
                })
                .transpose()?;
            Ok(ReadRequest::List {
                scope: parse_scope(value)?,
                limit,
                after_goal_id,
            })
        }
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

fn parse_scope(value: &Value) -> Result<Scope> {
    Ok(Scope {
        project_id: identifier(model::text(value, "project_id")?, "project_id")?,
        task_id: identifier(model::text(value, "task_id")?, "task_id")?,
        task_revision: model::positive(value, "task_revision")?,
        attempt_id: identifier(model::text(value, "attempt_id")?, "attempt_id")?,
    })
}

fn completion_evidence(value: Option<&Value>) -> Result<CompletionEvidence> {
    let value = value.ok_or_else(|| Error::invalid("completion_evidence is required"))?;
    parse_completion_evidence(value)
}

fn parse_completion_evidence(value: &Value) -> Result<CompletionEvidence> {
    model::fields(value, &["kind"])?;
    let kind = model::text(value, "kind")?;
    if kind != "task_acceptance" {
        return Err(Error::new(
            "GOAL_EVIDENCE_UNSUPPORTED",
            "completion_evidence.kind must be task_acceptance",
        ));
    }
    Ok(CompletionEvidence {
        kind: kind.to_owned(),
    })
}

fn objective(value: &Value) -> Result<String> {
    let objective = model::text(value, "objective")?;
    if objective.trim().is_empty() || objective.len() > MAX_OBJECTIVE_BYTES {
        return Err(Error::invalid(format!(
            "objective must be nonempty and at most {MAX_OBJECTIVE_BYTES} bytes"
        )));
    }
    Ok(objective.to_owned())
}

fn optional_reminder(value: Option<&Value>) -> Result<Option<ReminderConfig>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => parse_reminder(value).map(Some),
    }
}

fn parse_nullable_reminder(value: &Value) -> Result<Option<ReminderConfig>> {
    if value.is_null() {
        Ok(None)
    } else {
        parse_reminder(value).map(Some)
    }
}

fn parse_reminder(value: &Value) -> Result<ReminderConfig> {
    model::fields(value, &["due_at_ms", "cooldown_ms"])?;
    let cooldown_ms = value
        .get("cooldown_ms")
        .and_then(Value::as_i64)
        .filter(|value| (0..=MAX_COOLDOWN_MS).contains(value))
        .ok_or_else(|| {
            Error::invalid(format!(
                "cooldown_ms must be an integer from 0 through {MAX_COOLDOWN_MS}"
            ))
        })?;
    Ok(ReminderConfig {
        due_at_ms: model::positive(value, "due_at_ms")?,
        cooldown_ms,
    })
}

fn optional_bool(value: &Value, field: &str) -> Result<Option<bool>> {
    value
        .get(field)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| Error::invalid(format!("{field} must be a boolean")))
        })
        .transpose()
}

fn require_request_id(value: &Value) -> Result<()> {
    let request_id = model::text(value, "client_request_id")?;
    identifier(request_id, "client_request_id").map(|_| ())
}

fn identifier(value: &str, field: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > MAX_GOAL_ID_BYTES
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!(
            "{field} must be 1..={MAX_GOAL_ID_BYTES} bytes without whitespace"
        )));
    }
    Ok(value.to_owned())
}
