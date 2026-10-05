//! Closed public request shapes for optional GitHub source and pool methods.

use crate::{
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceInspectRequest {
    pub host: String,
    pub owner: String,
    pub repo: String,
}

impl SourceInspectRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["host", "owner", "repo"])?;
        Ok(Self {
            host: model::text(value, "host")?.to_owned(),
            owner: model::text(value, "owner")?.to_owned(),
            repo: model::text(value, "repo")?.to_owned(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSetupRequest {
    pub client_request_id: String,
    pub source_id: String,
    pub project_id: String,
    pub host: String,
    pub owner: String,
    pub repo: String,
    pub repository_id: i64,
}

impl SourceSetupRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &[
                "client_request_id",
                "source_id",
                "project_id",
                "host",
                "owner",
                "repo",
                "repository_id",
            ],
        )?;
        let request: Self = serde_json::from_value(value.clone())
            .map_err(|_| Error::invalid("GitHub source setup request is invalid"))?;
        if request.repository_id <= 0 {
            return Err(Error::invalid("repository_id must be positive"));
        }
        Ok(request)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReadRequest {
    pub source_id: String,
}

impl SourceReadRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["source_id"])?;
        Ok(Self {
            source_id: model::text(value, "source_id")?.to_owned(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePollRequest {
    pub client_request_id: String,
    pub source_id: String,
}

impl SourcePollRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["client_request_id", "source_id"])?;
        serde_json::from_value(value.clone())
            .map_err(|_| Error::invalid("GitHub source poll request is invalid"))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkPoolPreviewRequest {
    pub source_id: String,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl WorkPoolPreviewRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["source_id", "after", "limit"])?;
        let request: Self = serde_json::from_value(value.clone())
            .map_err(|_| Error::invalid("GitHub work-pool preview request is invalid"))?;
        if request
            .limit
            .is_some_and(|limit| !(1..=200).contains(&limit))
        {
            return Err(Error::invalid("limit must be between 1 and 200"));
        }
        if request.after.as_ref().is_some_and(|cursor| {
            cursor.is_empty() || cursor.len() > 512 || cursor.chars().any(char::is_control)
        }) {
            return Err(Error::invalid(
                "after cursor must be bounded printable text",
            ));
        }
        Ok(request)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkPoolApplyRequest {
    pub client_request_id: String,
    pub source_id: String,
    pub task_ids: Vec<String>,
}

/// One direct desired-state update for a source-mapped, manager-selected Issue.
/// Labels outside Eliot's reserved prefix are never accepted by this surface.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedLabelRequest {
    pub client_request_id: String,
    pub source_id: String,
    pub task_id: String,
    pub expected_task_revision: i64,
    pub label: String,
    pub present: bool,
}

impl ManagedLabelRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &[
                "client_request_id",
                "source_id",
                "task_id",
                "expected_task_revision",
                "label",
                "present",
            ],
        )?;
        let request: Self = serde_json::from_value(value.clone())
            .map_err(|_| Error::invalid("managed-label request is invalid"))?;
        if request.expected_task_revision <= 0 {
            return Err(Error::invalid("expected_task_revision must be positive"));
        }
        validate_source_id(&request.source_id)?;
        if request.task_id.trim().is_empty()
            || request.task_id.len() > 512
            || request.task_id.chars().any(char::is_control)
        {
            return Err(Error::invalid("task_id must be bounded printable text"));
        }
        // The fixed prefix gives this method a narrow ownership boundary and
        // keeps the path segment safe for GitHub's per-label endpoints.
        if !(10..=50).contains(&request.label.len())
            || !request.label.starts_with("eliot-")
            || !request
                .label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || request.label.ends_with('-')
        {
            return Err(Error::invalid(
                "label must be a 10..=50 byte lowercase eliot-* label",
            ));
        }
        Ok(request)
    }
}

/// One exact readback of a retained, unknown managed-label Operation. The
/// requested label, desired state and source are derived from that original
/// Operation; callers can identify only which unresolved receipt to inspect.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedLabelReconcileRequest {
    pub client_request_id: String,
    pub operation_id: String,
}

impl ManagedLabelReconcileRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["client_request_id", "operation_id"])?;
        let request: Self = serde_json::from_value(value.clone())
            .map_err(|_| Error::invalid("managed-label reconciliation request is invalid"))?;
        for (field, value) in [
            ("client_request_id", request.client_request_id.as_str()),
            ("operation_id", request.operation_id.as_str()),
        ] {
            if value.trim().is_empty()
                || value.len() > 128
                || value
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            {
                return Err(Error::invalid(format!(
                    "{field} must be 1..=128 bytes without whitespace"
                )));
            }
        }
        Ok(request)
    }
}

impl WorkPoolApplyRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["client_request_id", "source_id", "task_ids"])?;
        let request: Self = serde_json::from_value(value.clone())
            .map_err(|_| Error::invalid("GitHub work-pool apply request is invalid"))?;
        if request.task_ids.is_empty() || request.task_ids.len() > 200 {
            return Err(Error::invalid(
                "task_ids must contain between 1 and 200 entries",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        if request.task_ids.iter().any(|task_id| {
            task_id.trim().is_empty()
                || task_id.len() > 512
                || task_id.chars().any(char::is_control)
                || !seen.insert(task_id.as_str())
        }) {
            return Err(Error::invalid(
                "task_ids must contain unique, bounded, nonempty Task IDs",
            ));
        }
        Ok(request)
    }
}

/// Validate and return the canonical, closed parameters accepted by Store.
/// The same parser is used by the model's operation envelope so direct and
/// MCP calls cannot drift into separate GitHub request contracts.
pub fn validate_mutation(method: &str, value: &Value) -> Result<Value> {
    match method {
        "github.source.setup" => serde_json::to_value(SourceSetupRequest::parse(value)?)
            .map_err(|_| Error::invalid("GitHub source setup request is invalid")),
        "github.source.poll" => serde_json::to_value(SourcePollRequest::parse(value)?)
            .map_err(|_| Error::invalid("GitHub source poll request is invalid")),
        "github.work_pool.apply" => serde_json::to_value(WorkPoolApplyRequest::parse(value)?)
            .map_err(|_| Error::invalid("GitHub work-pool request is invalid")),
        "github.effect.managed_label" => serde_json::to_value(ManagedLabelRequest::parse(value)?)
            .map_err(|_| Error::invalid("managed-label request is invalid")),
        "github.effect.reconcile_managed_label" => {
            serde_json::to_value(ManagedLabelReconcileRequest::parse(value)?)
                .map_err(|_| Error::invalid("managed-label reconciliation request is invalid"))
        }
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

/// Validate and return the canonical, closed parameters for one read method.
pub fn validate_read(method: &str, value: &Value) -> Result<Value> {
    match method {
        "github.source.inspect" => {
            let request = SourceInspectRequest::parse(value)?;
            Ok(serde_json::json!({
                "host": request.host,
                "owner": request.owner,
                "repo": request.repo
            }))
        }
        "github.source.get" => serde_json::to_value(SourceReadRequest::parse(value)?)
            .map_err(|_| Error::invalid("GitHub source read request is invalid")),
        "github.work_pool.preview" => serde_json::to_value(WorkPoolPreviewRequest::parse(value)?)
            .map_err(|_| Error::invalid("GitHub work-pool preview request is invalid")),
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

pub(crate) fn validate_source_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(Error::invalid(
            "source_id must be 1..=128 safe ASCII characters",
        ));
    }
    Ok(())
}

pub(crate) fn validate_project_id(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(Error::invalid(
            "project_id must be 1..=128 printable characters",
        ));
    }
    Ok(())
}
