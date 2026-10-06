//! Bounded read-side contracts for the Swarm launcher surfaces.
//!
//! These types describe projections over existing Store authority. They do
//! not create Tasks, claim Attempts, inspect Git, or start native work.

use crate::{
    error::{Error, Result},
    model,
};
use serde_json::{Value, json};

/// Exact, caller-proposed launch facts. Parsing is pure: this request never
/// creates an Attempt, Operation, workspace, credential, or native process.
#[derive(Debug, Clone)]
pub(crate) struct LaunchPreviewRequest {
    pub task_id: String,
    pub expected_task_revision: i64,
    pub route: String,
    pub agent_profile: String,
    pub mcp_profile: String,
    pub mcp_surface: String,
    pub workspace_policy: String,
    pub requested_model: Option<String>,
    pub requested_effort: Option<String>,
    pub budget: Value,
    pub stop_conditions: Vec<String>,
    pub purpose: String,
}

impl LaunchPreviewRequest {
    pub(crate) fn parse(params: &Value) -> Result<Self> {
        model::fields(
            params,
            &[
                "task_id",
                "expected_task_revision",
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
        let task_id = required_launch_text(params, "task_id", 512)?;
        let expected_task_revision = params["expected_task_revision"]
            .as_i64()
            .filter(|revision| *revision > 0)
            .ok_or_else(|| Error::invalid("expected_task_revision must be a positive integer"))?;
        let route = required_launch_text(params, "route", 256)?;
        let agent_profile = required_launch_text(params, "agent_profile", 256)?;
        let mcp_profile = required_launch_text(params, "mcp_profile", 256)?;
        let mcp_surface = required_launch_text(params, "mcp_surface", 256)?;
        let workspace_policy = required_launch_text(params, "workspace_policy", 256)?;
        let requested_model = nullable_launch_text(params, "requested_model", 256)?;
        let requested_effort = nullable_launch_text(params, "requested_effort", 256)?;
        let budget = parse_launch_budget(
            params
                .get("budget")
                .ok_or_else(|| Error::invalid("budget is required"))?,
        )?;
        let stop_conditions = parse_stop_conditions(
            params
                .get("stop_conditions")
                .ok_or_else(|| Error::invalid("stop_conditions is required"))?,
        )?;
        let purpose = required_launch_text(params, "purpose", 128)?;

        Ok(Self {
            task_id,
            expected_task_revision,
            route,
            agent_profile,
            mcp_profile,
            mcp_surface,
            workspace_policy,
            requested_model,
            requested_effort,
            budget,
            stop_conditions,
            purpose,
        })
    }
}

/// Caller-owned, digest-bound request for the high-level launch mutation.
#[derive(Debug, Clone)]
pub(crate) struct LaunchRequest {
    pub client_request_id: String,
    pub plan_digest: String,
    pub preview: LaunchPreviewRequest,
}

impl LaunchRequest {
    pub(crate) fn parse(params: &Value) -> Result<Self> {
        model::fields(
            params,
            &[
                "client_request_id",
                "plan_digest",
                "task_id",
                "expected_task_revision",
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
        let client_request_id = required_launch_text(params, "client_request_id", 128)?;
        let plan_digest = required_launch_text(params, "plan_digest", 71)?;
        let digest_suffix = plan_digest.strip_prefix("sha256:").ok_or_else(|| {
            Error::invalid("plan_digest must be sha256 followed by 64 lowercase hex digits")
        })?;
        if digest_suffix.len() != 64
            || !digest_suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::invalid(
                "plan_digest must be sha256 followed by 64 lowercase hex digits",
            ));
        }

        let mut preview_params = params.clone();
        let object = preview_params
            .as_object_mut()
            .ok_or_else(|| Error::invalid("launch parameters must be an object"))?;
        object.remove("client_request_id");
        object.remove("plan_digest");
        let preview = LaunchPreviewRequest::parse(&preview_params)?;
        Ok(Self {
            client_request_id,
            plan_digest,
            preview,
        })
    }

    pub(crate) fn preview_params(&self) -> Value {
        json!({
            "task_id":self.preview.task_id,
            "expected_task_revision":self.preview.expected_task_revision,
            "route":self.preview.route,
            "agent_profile":self.preview.agent_profile,
            "mcp_profile":self.preview.mcp_profile,
            "mcp_surface":self.preview.mcp_surface,
            "workspace_policy":self.preview.workspace_policy,
            "requested_model":self.preview.requested_model,
            "requested_effort":self.preview.requested_effort,
            "budget":self.preview.budget,
            "stop_conditions":self.preview.stop_conditions,
            "purpose":self.preview.purpose,
        })
    }
}

fn required_launch_text(params: &Value, field: &str, max_bytes: usize) -> Result<String> {
    let value = params
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid(format!("{field} must be a string")))?;
    bounded_launch_text(value, field, max_bytes)
}

fn nullable_launch_text(params: &Value, field: &str, max_bytes: usize) -> Result<Option<String>> {
    let value = params
        .get(field)
        .ok_or_else(|| Error::invalid(format!("{field} is required; use null when unset")))?;
    match value {
        Value::Null => Ok(None),
        Value::String(value) => bounded_launch_text(value, field, max_bytes).map(Some),
        _ => Err(Error::invalid(format!("{field} must be a string or null"))),
    }
}

fn bounded_launch_text(value: &str, field: &str, max_bytes: usize) -> Result<String> {
    if value.trim().is_empty()
        || value.len() > max_bytes
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Error::invalid(format!(
            "{field} must be 1..={max_bytes} bytes without control characters"
        )));
    }
    Ok(value.to_owned())
}

fn parse_launch_budget(value: &Value) -> Result<Value> {
    model::fields(value, &["max_turns", "max_duration_ms", "max_cost_units"])?;
    let mut budget = serde_json::Map::new();
    for field in ["max_turns", "max_duration_ms", "max_cost_units"] {
        let amount = value.get(field).ok_or_else(|| {
            Error::invalid(format!("budget.{field} is required; use null when unset"))
        })?;
        if !amount.is_null() && amount.as_i64().is_none_or(|amount| amount < 0) {
            return Err(Error::invalid(format!(
                "budget.{field} must be a nonnegative integer or null"
            )));
        }
        budget.insert(field.to_owned(), amount.clone());
    }
    Ok(Value::Object(budget))
}

fn parse_stop_conditions(value: &Value) -> Result<Vec<String>> {
    let conditions = value
        .as_array()
        .ok_or_else(|| Error::invalid("stop_conditions must be an array"))?;
    if conditions.len() > 16 {
        return Err(Error::invalid(
            "stop_conditions must contain at most 16 items",
        ));
    }
    conditions
        .iter()
        .map(|condition| {
            let condition = condition
                .as_str()
                .ok_or_else(|| Error::invalid("each stop condition must be a string"))?;
            bounded_launch_text(condition, "stop condition", 512)
        })
        .collect()
}

/// Default rows for an interactive manager page.
pub const DEFAULT_PAGE_SIZE: i64 = 20;
/// Keep launcher pages smaller than the generic Store report ceiling.
pub const MAX_PAGE_SIZE: i64 = 50;
/// Dashboard embeds only a small preview from each underlying projection.
pub const DASHBOARD_PREVIEW_SIZE: i64 = 5;
/// Initial assignment context may include exact retained source text only
/// while the complete brief remains comfortably below a single page item.
pub const MAX_INLINE_BRIEF_BYTES: usize = 32_768;

/// Offset page contract shared by the launcher queue and exception reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRequest {
    pub after: i64,
    pub limit: i64,
}

impl PageRequest {
    pub fn parse(params: &Value) -> Result<Self> {
        let integer = |name: &str, default: i64| -> Result<i64> {
            match params.get(name) {
                None => Ok(default),
                Some(value) => value
                    .as_i64()
                    .ok_or_else(|| Error::invalid(format!("{name} must be an integer"))),
            }
        };
        let page = Self {
            after: integer("after", 0)?,
            limit: integer("limit", DEFAULT_PAGE_SIZE)?,
        };
        if !(1..=MAX_PAGE_SIZE).contains(&page.limit) || page.after < 0 {
            return Err(Error::invalid(format!(
                "limit must be 1..={MAX_PAGE_SIZE} and after nonnegative"
            )));
        }
        Ok(page)
    }
}

/// Return the exact source brief when it fits. Oversized briefs stay retained
/// in their authoritative Task/Attempt snapshot and are represented by a
/// digest-addressed reference instead of silent truncation.
pub(crate) fn brief_projection(
    brief: &Value,
    reference: Value,
    max_inline_bytes: usize,
) -> Result<Value> {
    if !brief.is_object() || brief["status"] == "unavailable" {
        return Ok(json!({
            "status": "unavailable",
            "brief": brief,
            "reference": reference,
        }));
    }
    let canonical = model::canonical(brief)?;
    if canonical.len() <= max_inline_bytes {
        Ok(json!({
            "status": "included",
            "serialized_bytes": canonical.len(),
            "brief": brief,
        }))
    } else {
        Ok(json!({
            "status": "detached",
            "serialized_bytes": canonical.len(),
            "digest": model::digest(canonical.as_bytes()),
            "reference": reference,
        }))
    }
}
